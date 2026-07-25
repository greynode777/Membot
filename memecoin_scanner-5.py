"""
Memecoin Scanner Bot v2 — многопоточный сканер токенов на Solana с системой
рейтинга (Score 0-100) вместо жёсткого pass/fail, несколькими источниками
данных, кэшированием, retry-логикой и отслеживанием динамики.

ВАЖНО:
- Это не финансовый совет и не гарантия прибыли. Большинство мемкоинов идут в ноль.
- Источники: DexScreener (boosts + profiles) без ключа; Birdeye — ОПЦИОНАЛЬНО,
  только если задан BIRDEYE_API_KEY (бесплатного тарифа может не хватать для
  постоянного опроса — проверь лимиты на birdeye.so). GMGN и pump.fun сюда
  сознательно не включены: у них жёсткая защита Cloudflare на уровне IP
  дата-центров (см. историю этого чата) — заголовками/ретраями не обходится.
- Названия полей в ответах RugCheck (holderCount, LP lock и т.д.) — предположение,
  не проверено напрямую (нет сетевого доступа при разработке). Включи
  DEBUG_MODE=1 в переменных окружения и сверь реальный JSON в scanner.log.

Все пороги и веса — в config.json (создаётся автоматически при первом запуске,
если отсутствует).

Установка: pip install -r requirements.txt
Запуск: python memecoin_scanner.py
Отладка полей API: DEBUG_MODE=1 python memecoin_scanner.py
"""

import os
import sys
import time
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============ КОНФИГ ============

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

DEFAULT_CONFIG = {
    "poll_interval_seconds": 60,
    "max_workers": 8,
    "cache_ttl_seconds": 300,
    "http_max_retries": 3,
    "http_backoff_factor": 0.5,
    "watch_window_minutes": 60,
    "min_age_minutes": 10,
    "max_age_minutes": 720,
    "min_holders": 20,
    "max_top10_holder_pct": 30.0,
    "max_insider_networks": 1,
    "min_vol_to_mcap_ratio": 0.3,
    "min_liquidity_to_mcap_ratio_full": 0.07,
    "min_liquidity_to_mcap_ratio_partial": 0.05,
    "distribution_buy_sell_ratio": 3.0,
    "distribution_price_change_max": 0.0,
    "min_buys_m5_for_zero_sells_flag": 20,
    "min_score_to_alert": 70,
    "score_weights": {
        "top10_holder_over_limit": 20,
        "holder_count_low": 10,
        "mint_not_revoked": 30,
        "freeze_not_revoked": 20,
        "insider_networks_over_limit": 15,
        "insider_networks_extra_per_network": 5,
        "vol_mcap_low": 15,
        "liquidity_mcap_low_full": 8,
        "liquidity_mcap_low_partial": 15,
        "too_young": 10,
        "too_old": 10,
        "distribution_pattern": 20,
        "lp_not_locked": 15,
        "missing_field_penalty": 4,
        "momentum_bonus_cap": 8,
        "momentum_penalty_cap": 12,
    },
    "birdeye_api_key_env": "BIRDEYE_API_KEY",
}


def load_config():
    """Загружает config.json, создаёт с дефолтами при отсутствии, докладывает
    недостающие ключи из DEFAULT_CONFIG (чтобы обновления кода не ломались
    из-за старого config.json на диске)."""
    if not os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(DEFAULT_CONFIG, f, indent=2, ensure_ascii=False)
        except Exception:
            pass
        return json.loads(json.dumps(DEFAULT_CONFIG))

    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            user_cfg = json.load(f)
    except Exception as e:
        print(f"[config] Не удалось прочитать config.json ({e}), использую дефолты")
        return json.loads(json.dumps(DEFAULT_CONFIG))

    merged = json.loads(json.dumps(DEFAULT_CONFIG))
    for key, value in user_cfg.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key].update(value)
        else:
            merged[key] = value
    return merged


CONFIG = load_config()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "ВСТАВЬ_СЮДА_ТОКЕН")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "ВСТАВЬ_СЮДА_CHAT_ID")
DEBUG_MODE = os.environ.get("DEBUG_MODE", "0").lower() in ("1", "true", "yes")

STATE_DIR = os.path.dirname(os.path.abspath(__file__))
SEEN_FILE = os.path.join(STATE_DIR, "seen_mints.json")
HISTORY_FILE = os.path.join(STATE_DIR, "token_history.json")
LOG_FILE = os.path.join(STATE_DIR, "scanner.log")


# ============ ЛОГИРОВАНИЕ ============

logger = logging.getLogger("memecoin_scanner")
logger.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)

_fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.setFormatter(_fmt)
logger.addHandler(_stream_handler)

try:
    _file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    _file_handler.setFormatter(_fmt)
    logger.addHandler(_file_handler)
except Exception as e:
    logger.warning(f"Не удалось открыть {LOG_FILE} для логирования: {e}")


# ============ HTTP-СЕССИЯ С RETRY ============

def build_session():
    session = requests.Session()
    retry = Retry(
        total=CONFIG["http_max_retries"],
        backoff_factor=CONFIG["http_backoff_factor"],
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json",
    })
    return session


SESSION = build_session()


# ============ ПРОСТОЙ THREAD-SAFE КЭШ ============

_cache_lock = threading.Lock()
_dex_cache = {}
_rug_cache = {}


def cache_get(cache, key, ttl):
    with _cache_lock:
        entry = cache.get(key)
    if entry and (time.time() - entry[0]) < ttl:
        return entry[1]
    return None


def cache_set(cache, key, value):
    with _cache_lock:
        cache[key] = (time.time(), value)


# ============ УТИЛИТЫ НОРМАЛИЗАЦИИ ДАННЫХ ============

def normalize_timestamp_ms(ts):
    """Некоторые API отдают unix-время в секундах, некоторые — в миллисекундах.
    Если число похоже на секунды (< 100 млрд), домножаем на 1000."""
    if ts is None:
        return None
    try:
        ts = float(ts)
    except (TypeError, ValueError):
        return None
    if ts < 100_000_000_000:
        return ts * 1000
    return ts


def normalize_top10_pct(value):
    """Некоторые API отдают доли (0.15), некоторые — проценты (15.0).
    Если сумма <= 1, считаем что это доля, и переводим в проценты."""
    if value is None:
        return None
    if value <= 1:
        return value * 100
    return value


def safe_div(a, b):
    if not b:
        return None
    try:
        return a / b
    except (TypeError, ZeroDivisionError):
        return None


def load_json_file(path, default):
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Не удалось прочитать {path}: {e}")
    return default


def save_json_file(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        logger.warning(f"Не удалось сохранить {path}: {e}")


# ============ ИСТОЧНИКИ ТОКЕНОВ ============

def fetch_dexscreener_boosts():
    """Продвигаемые токены DexScreener — платят за буст, поэтому это НЕ полный
    список новых токенов, только один из источников."""
    url = "https://api.dexscreener.com/token-boosts/latest/v1"
    try:
        resp = SESSION.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            return []
        return [
            {"mint": item["tokenAddress"], "source": "dexscreener_boosts"}
            for item in data
            if item.get("chainId") == "solana" and item.get("tokenAddress")
        ]
    except Exception as e:
        logger.warning(f"[fetch_dexscreener_boosts] Ошибка: {e}")
        return []


def fetch_dexscreener_profiles():
    """Последние добавленные токен-профили DexScreener — покрывает часть
    токенов, которые не покупали буст, но уже завели соцсети/описание."""
    url = "https://api.dexscreener.com/token-profiles/latest/v1"
    try:
        resp = SESSION.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            return []
        return [
            {"mint": item["tokenAddress"], "source": "dexscreener_profiles"}
            for item in data
            if item.get("chainId") == "solana" and item.get("tokenAddress")
        ]
    except Exception as e:
        logger.warning(f"[fetch_dexscreener_profiles] Ошибка: {e}")
        return []


def fetch_birdeye_trending():
    """ОПЦИОНАЛЬНЫЙ источник — требует BIRDEYE_API_KEY в переменных окружения.
    Если ключ не задан, источник тихо пропускается (не ошибка)."""
    api_key = os.environ.get(CONFIG["birdeye_api_key_env"])
    if not api_key:
        return []
    url = "https://public-api.birdeye.so/defi/token_trending"
    headers = {"X-API-KEY": api_key, "x-chain": "solana"}
    try:
        resp = SESSION.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        tokens = data.get("data", {}).get("tokens", [])
        return [
            {"mint": t["address"], "source": "birdeye"}
            for t in tokens
            if t.get("address")
        ]
    except Exception as e:
        logger.warning(f"[fetch_birdeye_trending] Ошибка: {e}")
        return []


SOURCES = [fetch_dexscreener_boosts, fetch_dexscreener_profiles, fetch_birdeye_trending]


def discover_tokens():
    """Опрашивает все источники параллельно, объединяет и убирает дубликаты
    по mint-адресу, сохраняя список источников для каждого токена."""
    found = {}
    with ThreadPoolExecutor(max_workers=len(SOURCES)) as executor:
        futures = [executor.submit(fn) for fn in SOURCES]
        for fut in as_completed(futures):
            try:
                items = fut.result()
            except Exception as e:
                logger.warning(f"[discover_tokens] Источник упал: {e}")
                continue
            for item in items:
                mint = item["mint"]
                if mint not in found:
                    found[mint] = {"mint": mint, "sources": set()}
                found[mint]["sources"].add(item["source"])

    result = []
    for entry in found.values():
        entry["sources"] = sorted(entry["sources"])
        result.append(entry)
    return result


# ============ RUGCHECK ============

def get_rugcheck_report(mint):
    cached = cache_get(_rug_cache, mint, CONFIG["cache_ttl_seconds"])
    if cached is not None:
        return cached

    url = f"https://api.rugcheck.xyz/v1/tokens/{mint}/report"
    try:
        resp = SESSION.get(url, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()

        if DEBUG_MODE:
            logger.debug(f"RugCheck raw для {mint}: {json.dumps(data, ensure_ascii=False)[:3000]}")

        top_holders = data.get("topHolders", [])
        raw_top10 = sum(h.get("pct", 0) for h in top_holders[:10]) if top_holders else None
        top10_pct = normalize_top10_pct(raw_top10)

        # НЕ ПРОВЕРЕНО НАПРЯМУЮ: ищем несколько вероятных названий поля.
        # Если явного счётчика нет — оставляем None, а не len(top_holders),
        # чтобы не занижать реальное число держателей (топ-10 списка мало
        # говорит об общем количестве). Фильтр по holder_count пропускается,
        # если значение не удалось определить, вместо автоматического отказа.
        holder_count = (
            data.get("holderCount")
            or data.get("holdersCount")
            or data.get("totalHolders")
        )
        if holder_count is None:
            holders_list = data.get("holders")
            if isinstance(holders_list, list) and len(holders_list) > len(top_holders):
                # Если список холдеров явно шире топ-10, доверяем его длине.
                holder_count = len(holders_list)

        token_info = data.get("token", {})
        mint_authority = token_info.get("mintAuthority")
        freeze_authority = token_info.get("freezeAuthority")

        insider_networks = (
            data.get("insiderNetworks")
            or data.get("graphInsiderNetworks")
            or data.get("insiderGraphs")
            or []
        )
        insider_network_count = len(insider_networks) if isinstance(insider_networks, list) else 0

        # НЕ ПРОВЕРЕНО НАПРЯМУЮ: поля LP lock/burn — предположение по структуре.
        markets = data.get("markets") or []
        lp_locked_pct = None
        if markets and isinstance(markets, list):
            lp_info = markets[0].get("lp", {}) if isinstance(markets[0], dict) else {}
            lp_locked_pct = lp_info.get("lpLockedPct") or lp_info.get("lockedPct")

        result = {
            "top10_pct": top10_pct,
            "holder_count": holder_count,
            "mint_revoked": mint_authority is None,
            "freeze_revoked": freeze_authority is None,
            "insider_network_count": insider_network_count,
            "lp_locked_pct": lp_locked_pct,
            "risks": [r.get("name") for r in data.get("risks", [])],
        }
        cache_set(_rug_cache, mint, result)
        return result
    except Exception as e:
        logger.warning(f"[get_rugcheck_report] Ошибка для {mint}: {e}")
        return None


# ============ DEXSCREENER (детали по конкретному токену) ============

def get_dexscreener_data(mint):
    cached = cache_get(_dex_cache, mint, CONFIG["cache_ttl_seconds"])
    if cached is not None:
        return cached

    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        resp = SESSION.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        pairs = data.get("pairs") or []
        if not pairs:
            return None

        pair = max(pairs, key=lambda p: p.get("liquidity", {}).get("usd", 0) or 0)
        base_token = pair.get("baseToken", {})

        market_cap = pair.get("marketCap") or pair.get("fdv")  # fallback на FDV
        pair_created_at = normalize_timestamp_ms(pair.get("pairCreatedAt"))

        txns_m5 = pair.get("txns", {}).get("m5", {}) or {}

        result = {
            "name": base_token.get("name", "?"),
            "symbol": base_token.get("symbol", "?"),
            "price_usd": float(pair.get("priceUsd", 0) or 0),
            "volume_24h": pair.get("volume", {}).get("h24", 0) or 0,
            "market_cap": market_cap,
            "liquidity_usd": pair.get("liquidity", {}).get("usd", 0) or 0,
            "pair_created_at": pair_created_at,
            "buys_m5": txns_m5.get("buys", 0) or 0,
            "sells_m5": txns_m5.get("sells", 0) or 0,
            "price_change_m5": pair.get("priceChange", {}).get("m5", 0) or 0,
            "url": pair.get("url"),
        }
        cache_set(_dex_cache, mint, result)
        return result
    except Exception as e:
        logger.warning(f"[get_dexscreener_data] Ошибка для {mint}: {e}")
        return None


# ============ СКОРИНГ ============

def stars_for_score(score):
    if score >= 95:
        return "⭐⭐⭐⭐⭐"
    if score >= 85:
        return "⭐⭐⭐⭐"
    if score >= 70:
        return "⭐⭐⭐"
    if score >= 50:
        return "⭐⭐"
    return "⭐"


def score_token(rugcheck, dexdata, prev_snapshot):
    """Возвращает (score: int 0-100, deductions: list[(points, label)],
    meta: dict с доп. информацией вроде age_minutes)."""
    w = CONFIG["score_weights"]
    score = 100
    deductions = []
    meta = {}

    def deduct(points, label):
        nonlocal score
        points = min(points, score)  # не уходим в минус
        score -= points
        deductions.append((points, label))

    if rugcheck is None or dexdata is None:
        return 0, [(100, "Недостаточно данных для оценки")], {"age_minutes": None}

    # --- Топ-10 держателей ---
    if rugcheck["top10_pct"] is not None:
        if rugcheck["top10_pct"] > CONFIG["max_top10_holder_pct"]:
            excess = rugcheck["top10_pct"] - CONFIG["max_top10_holder_pct"]
            points = w["top10_holder_over_limit"] + min(excess / 5, 20)  # чем выше excess, тем больнее
            deduct(points, f"Топ-10 держат {rugcheck['top10_pct']:.1f}% (лимит {CONFIG['max_top10_holder_pct']}%)")
    else:
        deduct(w["missing_field_penalty"], "Топ-10 холдеров: нет данных")

    # --- Количество держателей ---
    if rugcheck["holder_count"] is not None:
        if rugcheck["holder_count"] < CONFIG["min_holders"]:
            deduct(w["holder_count_low"], f"Только {rugcheck['holder_count']} держателей (минимум {CONFIG['min_holders']})")
    # если holder_count is None — фильтр пропускается вообще, без штрафа
    # (спецификация: "не выяснено -> фильтр пропускать, не отклонять")

    # --- Mint / Freeze authority ---
    if not rugcheck["mint_revoked"]:
        deduct(w["mint_not_revoked"], "Mint authority не отозван")
    if not rugcheck["freeze_revoked"]:
        deduct(w["freeze_not_revoked"], "Freeze authority не отозван")

    # --- Инсайдерские сети ---
    insider = rugcheck.get("insider_network_count", 0)
    if insider > CONFIG["max_insider_networks"]:
        extra = insider - CONFIG["max_insider_networks"]
        points = w["insider_networks_over_limit"] + extra * w["insider_networks_extra_per_network"]
        deduct(points, f"Обнаружено {insider} инсайдерских сетей (лимит {CONFIG['max_insider_networks']})")

    # --- LP locked ---
    if rugcheck.get("lp_locked_pct") is not None:
        if rugcheck["lp_locked_pct"] < 50:
            deduct(w["lp_not_locked"], f"Заблокировано только {rugcheck['lp_locked_pct']:.0f}% ликвидности")

    # --- Market Cap / Vol/MCap / Liquidity/MCap ---
    mcap = dexdata.get("market_cap")
    if mcap:
        vol_ratio = safe_div(dexdata["volume_24h"], mcap)
        if vol_ratio is not None and vol_ratio < CONFIG["min_vol_to_mcap_ratio"]:
            deduct(w["vol_mcap_low"], f"Vol/MCap = {vol_ratio:.2f} (минимум {CONFIG['min_vol_to_mcap_ratio']})")

        liq_ratio = safe_div(dexdata.get("liquidity_usd", 0), mcap)
        if liq_ratio is not None:
            if liq_ratio < CONFIG["min_liquidity_to_mcap_ratio_partial"]:
                deduct(w["liquidity_mcap_low_partial"],
                       f"Liquidity/MCap = {liq_ratio:.1%} (ниже {CONFIG['min_liquidity_to_mcap_ratio_partial']:.0%})")
            elif liq_ratio < CONFIG["min_liquidity_to_mcap_ratio_full"]:
                deduct(w["liquidity_mcap_low_full"],
                       f"Liquidity/MCap = {liq_ratio:.1%} (ниже целевых {CONFIG['min_liquidity_to_mcap_ratio_full']:.0%}, но приемлемо)")
    else:
        deduct(w["missing_field_penalty"], "Market Cap/FDV: нет данных")

    # --- Возраст токена ---
    age_minutes = None
    pair_created_at = dexdata.get("pair_created_at")
    if pair_created_at:
        age_minutes = (datetime.now().timestamp() * 1000 - pair_created_at) / 60000
        if age_minutes < CONFIG["min_age_minutes"]:
            deduct(w["too_young"], f"Токену {age_minutes:.0f} мин (мало данных, минимум {CONFIG['min_age_minutes']})")
        elif age_minutes > CONFIG["max_age_minutes"]:
            deduct(w["too_old"], f"Токену {age_minutes:.0f} мин (вероятно, хайп уже отыгран)")
    else:
        deduct(w["missing_field_penalty"], "Возраст токена: нет данных")
    meta["age_minutes"] = age_minutes

    # --- Признак распределения (киты продают в розницу) ---
    buys_m5 = dexdata.get("buys_m5", 0)
    sells_m5 = dexdata.get("sells_m5", 0)
    price_change_m5 = dexdata.get("price_change_m5", 0)
    if sells_m5 > 0 and (buys_m5 / sells_m5) >= CONFIG["distribution_buy_sell_ratio"] \
            and price_change_m5 <= CONFIG["distribution_price_change_max"]:
        ratio = buys_m5 / sells_m5
        deduct(w["distribution_pattern"],
               f"Возможное распределение: покупок в {ratio:.1f}x больше продаж, цена {price_change_m5:+.1f}%")
    elif sells_m5 == 0 and buys_m5 >= CONFIG["min_buys_m5_for_zero_sells_flag"] and price_change_m5 <= 0:
        deduct(w["distribution_pattern"],
               f"Подозрительно: {buys_m5} покупок за 5м, ни одной продажи, цена {price_change_m5:+.1f}%")

    # --- Momentum: сравнение с предыдущим снимком (динамика) ---
    if prev_snapshot:
        momentum_points = 0
        momentum_notes = []
        prev_price = prev_snapshot.get("price")
        prev_volume = prev_snapshot.get("volume_24h")
        prev_liquidity = prev_snapshot.get("liquidity")
        prev_holders = prev_snapshot.get("holder_count")

        if prev_price and dexdata["price_usd"]:
            price_delta = (dexdata["price_usd"] - prev_price) / prev_price
            momentum_points += 3 if price_delta > 0.05 else (-4 if price_delta < -0.05 else 0)
            momentum_notes.append(f"цена {price_delta:+.1%}")

        if prev_volume and dexdata["volume_24h"]:
            vol_delta = (dexdata["volume_24h"] - prev_volume) / prev_volume
            momentum_points += 2 if vol_delta > 0.1 else (-2 if vol_delta < -0.2 else 0)
            momentum_notes.append(f"объём {vol_delta:+.1%}")

        if prev_liquidity and dexdata["liquidity_usd"]:
            liq_delta = (dexdata["liquidity_usd"] - prev_liquidity) / prev_liquidity
            momentum_points += 2 if liq_delta > 0.05 else (-6 if liq_delta < -0.15 else 0)
            momentum_notes.append(f"ликвидность {liq_delta:+.1%}")
            if liq_delta < -0.15:
                momentum_notes.append("(возможен вывод ликвидности!)")

        if prev_holders is not None and rugcheck["holder_count"] is not None:
            holder_delta = rugcheck["holder_count"] - prev_holders
            momentum_points += 1 if holder_delta > 0 else 0

        cap = w["momentum_bonus_cap"]
        floor = -w["momentum_penalty_cap"]
        momentum_points = max(floor, min(cap, momentum_points))

        if momentum_points > 0:
            score = min(100, score + momentum_points)
            deductions.append((-momentum_points, f"Momentum положительный: {', '.join(momentum_notes)}"))
        elif momentum_points < 0:
            deduct(-momentum_points, f"Momentum отрицательный: {', '.join(momentum_notes)}")

    score = max(0, min(100, round(score)))
    return score, deductions, meta


# ============ TELEGRAM (MarkdownV2) ============

_MDV2_SPECIAL = set(r"_*[]()~`>#+-=|{}.!\\")


def escape_mdv2(text):
    text = str(text)
    return "".join(f"\\{ch}" if ch in _MDV2_SPECIAL else ch for ch in text)


def send_telegram_alert(mint, dexdata, rugcheck, score, deductions, sources):
    stars = stars_for_score(score)
    name = escape_mdv2(dexdata.get("name", "?"))
    symbol = escape_mdv2(dexdata.get("symbol", "?"))
    mcap = dexdata.get("market_cap") or 0
    liq = dexdata.get("liquidity_usd", 0)
    liq_mcap_pct = (liq / mcap * 100) if mcap else 0

    deduction_lines = "\n".join(
        f"{'+' if pts < 0 else '−'}{abs(pts):.0f} — {escape_mdv2(label)}"
        for pts, label in deductions
    ) or escape_mdv2("Без штрафов")

    holder_count_display = rugcheck.get("holder_count")
    holder_count_str = escape_mdv2(holder_count_display) if holder_count_display is not None else escape_mdv2("неизвестно")
    sources_str = escape_mdv2(", ".join(sources))

    mcap_str = escape_mdv2(f"${mcap:,.0f}")
    volume_str = escape_mdv2(f"${dexdata.get('volume_24h', 0):,.0f}")
    liq_str = escape_mdv2(f"${liq:,.0f}")
    liq_pct_str = escape_mdv2(f"{liq_mcap_pct:.1f}%")
    top10_str = escape_mdv2(f"{rugcheck.get('top10_pct') or 0:.1f}%")

    text = (
        f"{escape_mdv2(stars)} *Score: {score}/100*\n"
        f"*{name} \\(${symbol}\\)*\n\n"
        f"MCap: {mcap_str}\n"
        f"Объём 24ч: {volume_str}\n"
        f"Ликвидность: {liq_str} \\({liq_pct_str} от MCap\\)\n"
        f"Держатели: {holder_count_str}\n"
        f"Топ\\-10: {top10_str}\n"
        f"Источники: {sources_str}\n\n"
        f"*Детали расчёта:*\n{deduction_lines}\n\n"
        f"Mint: `{escape_mdv2(mint)}`\n"
        f"[DexScreener]({escape_mdv2(dexdata.get('url', ''))})\n\n"
        f"⚠️ NFA/DYOR"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": False,
    }
    try:
        resp = SESSION.post(url, data=payload, timeout=10)
        if resp.status_code != 200:
            logger.warning(f"[send_telegram_alert] Telegram ошибка: {resp.text}")
    except Exception as e:
        logger.warning(f"[send_telegram_alert] Ошибка: {e}")


# ============ ОБРАБОТКА ОДНОГО ТОКЕНА ============

def process_token(entry, history):
    mint = entry["mint"]
    dexdata = get_dexscreener_data(mint)
    rugcheck = get_rugcheck_report(mint)

    if dexdata is None or rugcheck is None:
        missing = []
        if dexdata is None:
            missing.append("DexScreener")
        if rugcheck is None:
            missing.append("RugCheck")
        return {"mint": mint, "status": "missing_data", "missing": missing}

    prev_snapshot = history.get(mint)
    score, deductions, meta = score_token(rugcheck, dexdata, prev_snapshot)

    history[mint] = {
        "time": datetime.now().timestamp() * 1000,
        "price": dexdata["price_usd"],
        "volume_24h": dexdata["volume_24h"],
        "liquidity": dexdata["liquidity_usd"],
        "holder_count": rugcheck["holder_count"],
    }

    age_minutes = meta.get("age_minutes")
    still_watching = age_minutes is not None and age_minutes < CONFIG["watch_window_minutes"]

    return {
        "mint": mint,
        "status": "evaluated",
        "score": score,
        "deductions": deductions,
        "still_watching": still_watching,
        "dexdata": dexdata,
        "rugcheck": rugcheck,
        "sources": entry.get("sources", []),
    }


def prune_history(history, max_age_minutes):
    """Удаляет из истории записи старше watch_window (плюс запас), чтобы файл
    не рос бесконечно."""
    cutoff = datetime.now().timestamp() * 1000 - max_age_minutes * 2 * 60000
    return {mint: snap for mint, snap in history.items() if snap.get("time", 0) > cutoff}


# ============ ОСНОВНОЙ ЦИКЛ ============

def main():
    logger.info("Запуск сканера мемкоинов v2...")
    seen = set(load_json_file(SEEN_FILE, default=[]))
    history = load_json_file(HISTORY_FILE, default={})

    while True:
        all_tokens = discover_tokens()
        candidates = [t for t in all_tokens if t["mint"] not in seen]
        logger.info(f"Источники вернули {len(all_tokens)} уникальных токенов, из них новых: {len(candidates)}")

        results = []
        if candidates:
            with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
                futures = {executor.submit(process_token, t, history): t for t in candidates}
                for fut in as_completed(futures):
                    try:
                        res = fut.result()
                    except Exception as e:
                        logger.error(f"[process_token] Необработанная ошибка: {e}")
                        continue
                    results.append(res)

        for res in results:
            mint = res["mint"]
            if res["status"] == "missing_data":
                logger.info(f"⚠️ Нет данных ({', '.join(res['missing'])}) для {mint} — попробуем позже")
                continue

            score = res["score"]
            stars = stars_for_score(score)

            if not res["still_watching"]:
                seen.add(mint)

            name = res["dexdata"].get("name", "?")
            symbol = res["dexdata"].get("symbol", "?")

            if score >= CONFIG["min_score_to_alert"]:
                logger.info(f"✅ {stars} Score {score}/100 — {name} (${symbol}) — отправляю алерт")
                send_telegram_alert(mint, res["dexdata"], res["rugcheck"], score, res["deductions"], res["sources"])
            else:
                watching_note = " [продолжаем следить]" if res["still_watching"] else ""
                logger.info(f"❌ {stars} Score {score}/100 — {name} (${symbol}){watching_note}: {res['deductions']}")

        save_json_file(SEEN_FILE, list(seen))
        save_json_file(HISTORY_FILE, prune_history(history, CONFIG["watch_window_minutes"]))

        time.sleep(CONFIG["poll_interval_seconds"])


if __name__ == "__main__":
    main()
