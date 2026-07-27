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
    "poll_interval_seconds": 600,
    "max_workers": 8,
    "dex_cache_ttl_seconds": 90,
    "rugcheck_cache_ttl_seconds": 900,
    "http_max_retries": 3,
    "http_backoff_factor": 0.5,
    "watch_window_minutes": 60,
    "heartbeat_interval_minutes": 30,
    "delisting_missing_threshold": 5,
    "min_mcap_usd": 10000,
    "min_liquidity_usd": 2000,
    "post_rec_dev_check_interval_minutes": 10,
    "post_rec_dev_alert_tiers": [0.5, 0.9, 1.0],
    "solana_rpc_url": "https://api.mainnet-beta.solana.com",
    "evaluation_checkpoints_hours": [8, 16, 36, 48, 72],
    "tracking_retention_hours": 80,
    "rug_liquidity_drop_hard_pct": 0.85,
    "rug_liquidity_drop_soft_pct": 0.6,
    "rug_confirmation_minutes": 20,
    "price_crash_warning_pct": 0.5,
    "min_age_minutes": 10,
    "max_age_minutes": 4320,
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
        "liquidity_soft_crash_penalty": 40,
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
SOLANA_RPC_URL = os.environ.get("SOLANA_RPC_URL", CONFIG["solana_rpc_url"])

STATE_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKING_FILE = os.path.join(STATE_DIR, "tracking.json")
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
    cached = cache_get(_rug_cache, mint, CONFIG["rugcheck_cache_ttl_seconds"])
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

        # НЕ ПРОВЕРЕНО НАПРЯМУЮ: адрес кошелька-создателя токена — несколько
        # вероятных названий поля. Нужен для отслеживания "продал ли дев".
        creator_address = (
            data.get("creator")
            or data.get("deployer")
            or token_info.get("creator")
            or token_info.get("deployer")
        )

        result = {
            "top10_pct": top10_pct,
            "holder_count": holder_count,
            "mint_revoked": mint_authority is None,
            "freeze_revoked": freeze_authority is None,
            "insider_network_count": insider_network_count,
            "lp_locked_pct": lp_locked_pct,
            "creator_address": creator_address,
            "risks": [r.get("name") for r in data.get("risks", [])],
        }
        cache_set(_rug_cache, mint, result)
        return result
    except Exception as e:
        logger.warning(f"[get_rugcheck_report] Ошибка для {mint}: {e}")
        return None


# ============ SOLANA RPC (баланс кошелька разработчика) ============

_dev_balance_cache = {}


def get_dev_wallet_token_balance(mint, dev_wallet_address):
    """Запрашивает у Solana RPC текущий баланс данного SPL-токена на
    кошельке разработчика. Возвращает float (в токенах, не в USD) или None
    при ошибке/отсутствии аккаунта.

    ВАЖНО: использует бесплатный публичный RPC-узел по умолчанию — он может
    рейт-лимитить или падать под нагрузкой (см. историю с pump.fun/Cloudflare
    в этом же проекте). Если увидишь частые ошибки — задай SOLANA_RPC_URL
    в переменных окружения на платный провайдер (Helius/QuickNode и т.п.,
    у обоих есть бесплatные тарифы с лимитом запросов в день)."""
    cached = cache_get(_dev_balance_cache, f"{mint}:{dev_wallet_address}", CONFIG["dex_cache_ttl_seconds"])
    if cached is not None:
        return cached

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenAccountsByOwner",
        "params": [
            dev_wallet_address,
            {"mint": mint},
            {"encoding": "jsonParsed"},
        ],
    }
    try:
        resp = SESSION.post(SOLANA_RPC_URL, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if DEBUG_MODE:
            logger.debug(f"Solana RPC raw для {mint}/{dev_wallet_address}: {json.dumps(data, ensure_ascii=False)[:2000]}")

        accounts = data.get("result", {}).get("value", [])
        total = 0.0
        for acc in accounts:
            try:
                parsed = acc["account"]["data"]["parsed"]["info"]["tokenAmount"]
                total += float(parsed.get("uiAmount") or 0)
            except (KeyError, TypeError, ValueError):
                continue

        cache_set(_dev_balance_cache, f"{mint}:{dev_wallet_address}", total)
        return total
    except Exception as e:
        logger.warning(f"[get_dev_wallet_token_balance] Ошибка для {mint}: {e}")
        return None


# ============ DEXSCREENER (детали по конкретному токену) ============

def get_dexscreener_data(mint):
    cached = cache_get(_dex_cache, mint, CONFIG["dex_cache_ttl_seconds"])
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

        # НЕ ПРОВЕРЕНО НАПРЯМУЮ: структура pair.info.socials предполагается по
        # документации DexScreener (список объектов {"type": "twitter"/"telegram", "url": ...}).
        # Свериться можно через DEBUG_MODE, если ссылки не находятся, хотя реально есть.
        info = pair.get("info", {}) or {}
        socials = info.get("socials", []) or []
        twitter_url = next((s.get("url") for s in socials if s.get("type") == "twitter"), None)
        telegram_url = next((s.get("url") for s in socials if s.get("type") == "telegram"), None)

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
            "twitter_url": twitter_url,
            "telegram_url": telegram_url,
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


def score_token(rugcheck, dexdata, tracking_entry):
    """Возвращает (score: int 0-100, deductions: list[(points, label)],
    meta: dict с доп. информацией вроде age_minutes).

    tracking_entry — накопленные данные наблюдения с момента первого обнаружения
    токена (см. update_tracking): peak_liquidity, peak_price, rugged, first_*.
    """
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

    # --- Проверка на рагпул за время наблюдения ---
    # Если в любой момент окна наблюдения ликвидность или цена рухнули
    # от своего пика — это самый сильный сигнал риска, который есть.
    # Никакие другие метрики не могут это компенсировать: score сразу 0,
    # рекомендация никогда не отправляется.
    if tracking_entry and tracking_entry.get("rugged"):
        return 0, [(100, tracking_entry.get("rugged_reason", "Обнаружен рагпул во время наблюдения"))], {"age_minutes": None}

    # --- Абсолютный минимальный размер (независимо от процентных соотношений) ---
    # Liquidity/MCap может выглядеть отлично (например 18%), но если сами числа —
    # $200 ликвидности при капе $1000, это буквально пыль: любой может двигать
    # цену на копейки. Процентные пороги такое не ловят, нужен абсолютный.
    mcap_check = dexdata.get("market_cap")
    liq_check = dexdata.get("liquidity_usd")
    if mcap_check is not None and mcap_check < CONFIG["min_mcap_usd"]:
        return 0, [(100, f"MCap ${mcap_check:,.0f} слишком мал (минимум ${CONFIG['min_mcap_usd']:,.0f}) — токен-пыль")], {"age_minutes": None}
    if liq_check is not None and liq_check < CONFIG["min_liquidity_usd"]:
        return 0, [(100, f"Ликвидность ${liq_check:,.0f} слишком мала (минимум ${CONFIG['min_liquidity_usd']:,.0f}) — токен-пыль")], {"age_minutes": None}

    # --- История исчезновения из источников данных ---
    # Не обязательно рагпул (может быть временный сбой индексации самого
    # DexScreener), но если токен пропадал из выдачи достаточно долго —
    # разумный повод быть строже, а не тихо игнорировать этот эпизод.
    if tracking_entry and tracking_entry.get("possibly_delisted"):
        deduct(30, "У токена была история исчезновения из источников данных (возможен делистинг/технические проблемы)")

    # --- Топ-10 держателей ---
    # Штраф растёт нелинейно: слегка выше лимита — терпимо, но чем ближе к
    # монопольному владению, тем быстрее набегают очки. Раньше формула была
    # слишком мягкой (даже 80%+ концентрация давала всего -30, и токен
    # проходил как "интересно понаблюдать" — это неправильно, при такой
    # концентрации пара кошельков может обрушить цену одной продажей).
    if rugcheck["top10_pct"] is not None:
        top10 = rugcheck["top10_pct"]
        limit = CONFIG["max_top10_holder_pct"]
        if top10 > limit:
            excess = top10 - limit
            # Плавная квадратичная кривая, откалиброванная так, чтобы:
            # ~35% -> deduction ~25 (score ~75, чуть выше лимита — терпимо)
            # ~50% -> deduction ~45 (score ~55)
            # ~60% -> deduction ~66 (score ~34, совпадает с потолком 40 ниже)
            # ~70% -> deduction ~91 (score ~9)
            # ~80%+ -> deduction близко к 100 (крайний риск, score близко к 0)
            points = 20 + 0.28 * (excess ** 1.5)
            deduct(points, f"Топ-10 держат {top10:.1f}% (лимит {limit}%)")
    else:
        deduct(w["missing_field_penalty"], "Топ-10 холдеров: нет данных")

    # --- Количество держателей ---
    if rugcheck["holder_count"] is not None:
        has_trading_activity = (dexdata.get("buys_m5", 0) + dexdata.get("sells_m5", 0)) > 0 or dexdata.get("volume_24h", 0) > 0
        if rugcheck["holder_count"] == 0 and has_trading_activity:
            # 0 держателей физически невозможно при реальных покупках/продажах —
            # либо баг/задержка в данных RugCheck, либо подозрение на фейковую
            # активность (самоторговля с одного кошелька для видимости объёма).
            # Штраф серьёзнее обычного "мало держателей", это отдельный красный флаг.
            deduct(60, "0 держателей при наличии торговой активности — подозрение на фейковые данные/самоторговлю")
        elif rugcheck["holder_count"] < CONFIG["min_holders"]:
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

    # --- Просадка ликвидности "мягкого" тира (60-85% от пика) ---
    # Если update_tracking уже поставил rugged=True (>85%, устойчиво), сюда мы
    # вообще не дойдём — score_token вернёт 0 раньше. А вот 60-85% — серьёзный
    # красный флаг, но не обязательно окончательный вывод денег (например,
    # мог выйти один крупный холдер) — считаем тяжёлым штрафом, а не отказом.
    if tracking_entry and tracking_entry.get("peak_liquidity"):
        peak_liq = tracking_entry["peak_liquidity"]
        current_liq = dexdata.get("liquidity_usd", 0)
        if peak_liq:
            liq_drop_now = 1 - current_liq / peak_liq
            if liq_drop_now >= CONFIG["rug_liquidity_drop_soft_pct"]:
                deduct(
                    w["liquidity_soft_crash_penalty"],
                    f"Ликвидность просела на {liq_drop_now:.0%} от пика (серьёзный риск, но не подтверждённый рагпул)"
                )

    # --- Просадка от пика цены (информационно, НЕ блокирует рекомендацию) ---
    # В отличие от падения ликвидности (жёсткий признак рагпула в update_tracking),
    # просадка цены сама по себе — обычная волатильность мемкоинов, а не
    # обязательно вывод денег. Учитываем как небольшой штраф, а не как отказ.
    if tracking_entry and tracking_entry.get("peak_price"):
        peak_price = tracking_entry["peak_price"]
        current_price = dexdata.get("price_usd", 0)
        if peak_price and current_price:
            price_drop_from_peak = 1 - current_price / peak_price
            if price_drop_from_peak >= CONFIG["price_crash_warning_pct"]:
                deduct(10, f"Цена просела на {price_drop_from_peak:.0%} от пика (волатильность, не обязательно рагпул)")

    # --- Momentum за весь период наблюдения (первое обнаружение vs сейчас) ---
    if tracking_entry:
        momentum_points = 0
        momentum_notes = []
        first_price = tracking_entry.get("first_price")
        first_liquidity = tracking_entry.get("first_liquidity")
        first_holders = tracking_entry.get("first_holder_count")

        if first_price and dexdata["price_usd"]:
            price_delta = (dexdata["price_usd"] - first_price) / first_price
            momentum_points += 3 if price_delta > 0.1 else (-4 if price_delta < -0.3 else 0)
            momentum_notes.append(f"цена с момента обнаружения {price_delta:+.1%}")

        if first_liquidity and dexdata["liquidity_usd"]:
            liq_delta = (dexdata["liquidity_usd"] - first_liquidity) / first_liquidity
            momentum_points += 2 if liq_delta > 0.05 else (-6 if liq_delta < -0.3 else 0)
            momentum_notes.append(f"ликвидность {liq_delta:+.1%}")

        if first_holders is not None and rugcheck["holder_count"] is not None:
            holder_delta = rugcheck["holder_count"] - first_holders
            momentum_points += 3 if holder_delta > 0 else (-2 if holder_delta < 0 else 0)
            momentum_notes.append(f"держатели {first_holders} → {rugcheck['holder_count']}")

        cap = w["momentum_bonus_cap"]
        floor = -w["momentum_penalty_cap"]
        momentum_points = max(floor, min(cap, momentum_points))

        if momentum_points > 0:
            score = min(100, score + momentum_points)
            deductions.append((-momentum_points, f"Рост за период наблюдения: {', '.join(momentum_notes)}"))
        elif momentum_points < 0:
            deduct(-momentum_points, f"Спад за период наблюдения: {', '.join(momentum_notes)}")

    score = max(0, min(100, round(score)))

    # --- Жёсткий потолок при критической концентрации ---
    # Даже если остальные метрики идеальны, экстремальная концентрация
    # держателей — это риск, который нельзя компенсировать хорошим объёмом
    # или ликвидностью. Пара кошельков может обрушить цену в любой момент.
    if rugcheck.get("top10_pct") is not None:
        top10 = rugcheck["top10_pct"]
        if top10 > 60:
            score = min(score, 40)
            deductions.append((0, f"Потолок score=40: топ-10 держат {top10:.1f}% (критическая концентрация)"))
        elif top10 > 45:
            score = min(score, 60)
            deductions.append((0, f"Потолок score=60: топ-10 держат {top10:.1f}% (высокая концентрация)"))

    return score, deductions, meta


# ============ TELEGRAM (MarkdownV2) ============

_MDV2_SPECIAL = set(r"_*[]()~`>#+-=|{}.!\\")


def escape_mdv2(text):
    text = str(text)
    return "".join(f"\\{ch}" if ch in _MDV2_SPECIAL else ch for ch in text)


def escape_url_mdv2(url):
    """Внутри (...) синтаксиса ссылки MarkdownV2 экранировать нужно только
    обратный слэш и закрывающую скобку — не весь набор спецсимволов,
    иначе ссылка перестанет быть валидным URL."""
    return url.replace("\\", "\\\\").replace(")", "\\)")


def format_social_line(label, url):
    """Строка для Twitter/Telegram в алерте: кликабельная ссылка, если есть,
    иначе явное 'не обнаружен' — чтобы сразу было видно отсутствие соцсетей,
    а не путать с ошибкой получения данных."""
    if url:
        return f"{label}: [Открыть]({escape_url_mdv2(url)})"
    return f"{label}: {escape_mdv2('не обнаружен')}"


def send_telegram_alert(mint, dexdata, rugcheck, score, deductions, sources, age_hours, score_history=None, dev_balance=None):
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

    twitter_line = format_social_line("Twitter", dexdata.get("twitter_url"))
    telegram_line = format_social_line("Telegram", dexdata.get("telegram_url"))

    history_line = ""
    if score_history and len(score_history) > 1:
        # Сортируем по числовому значению чекпоинта, а не по строке ("8" < "36" < "8ч" лексикографически иначе поплывёт)
        sorted_points = sorted(score_history.items(), key=lambda kv: int(kv[0]))
        history_str = " → ".join(f"{cp}ч:{sc}" for cp, sc in sorted_points)
        history_line = f"Динамика Score: {escape_mdv2(history_str)}\n"

    # Чисто информационно, НЕ влияет на score — см. обсуждение про fair-launch
    # токены, где у дева часто изначально 0 токенов, и одиночный снимок без
    # истории ненадёжен для автоматических выводов. Смотри сам(а).
    if dev_balance is not None:
        price = dexdata.get("price_usd") or 0
        mcap_for_supply = dexdata.get("market_cap") or 0
        # Оценка общего supply через MCap/price — приблизительно (для
        # большинства pump.fun fair-launch токенов MCap≈FDV, так что это
        # разумное приближение, но не гарантированно точное число).
        estimated_supply = (mcap_for_supply / price) if price > 0 else None
        pct_line = ""
        if estimated_supply:
            pct_of_supply = dev_balance / estimated_supply * 100
            pct_line = f" \\({escape_mdv2(f'{pct_of_supply:.1f}%')} от supply\\)"
        usd_value = dev_balance * price if price > 0 else 0
        dev_balance_line = (
            f"Баланс дева сейчас: {escape_mdv2(f'{dev_balance:,.0f}')} токенов{pct_line}\n"
            f"Стоимость баланса дева: {escape_mdv2(f'${usd_value:,.0f}')}\n"
        )
    else:
        dev_balance_line = f"Баланс дева: {escape_mdv2('не удалось определить')}\n"

    text = (
        f"{escape_mdv2(stars)} *Score: {score}/100*\n"
        f"*{name} \\(${symbol}\\)*\n"
        f"Токену {escape_mdv2(f'{age_hours:.1f}')} ч, признаков рагпула не обнаружено ✅\n"
        f"{history_line}\n"
        f"MCap: {mcap_str}\n"
        f"Объём 24ч: {volume_str}\n"
        f"Ликвидность: {liq_str} \\({liq_pct_str} от MCap\\)\n"
        f"Держатели: {holder_count_str}\n"
        f"Топ\\-10: {top10_str}\n"
        f"{dev_balance_line}"
        f"Источники: {sources_str}\n"
        f"{twitter_line}\n"
        f"{telegram_line}\n\n"
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
        resp = SESSION.post(url, data=payload, timeout=20)
        if resp.status_code != 200:
            logger.warning(f"[send_telegram_alert] Telegram ошибка: {resp.text}")
            return False
        return True
    except Exception as e:
        logger.warning(f"[send_telegram_alert] Ошибка: {e}")
        return False





def send_dev_dump_alert(mint, name, symbol, sold_pct, baseline, current):
    """Отправляется НЕМЕДЛЕННО (не батчится в heartbeat), потому что это
    касается токена, который бот уже порекомендовал — пользователю важно
    узнать об этом как можно быстрее, а не ждать сводку раз в 30 минут."""
    text = (
        f"🚨 *Разработчик продал токены*\n"
        f"{escape_mdv2(name)} \\(${escape_mdv2(symbol)}\\)\n"
        f"Продано {escape_mdv2(f'{sold_pct:.0%}')} от баланса на момент рекомендации\n"
        f"Было: {escape_mdv2(f'{baseline:,.0f}')} → Сейчас: {escape_mdv2(f'{current:,.0f}')}\n"
        f"Mint: `{escape_mdv2(mint)}`"
    )
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    try:
        resp = SESSION.post(url, data=payload, timeout=20)
        if resp.status_code != 200:
            logger.warning(f"[send_dev_dump_alert] Telegram ошибка: {resp.text}")
            return False
        return True
    except Exception as e:
        logger.warning(f"[send_dev_dump_alert] Ошибка: {e}")
        return False


def send_heartbeat(tracking, started_at_ms, rugs_since_last_report):
    """Отправляет короткий сигнал 'бот жив' — чтобы по молчанию дольше
    heartbeat_interval_minutes можно было понять, что сервис упал.
    Также включает список рагпулов, обнаруженных с прошлого отчёта —
    вместо отдельного сообщения на каждый (чтобы не спамить)."""
    uptime_hours = (datetime.now().timestamp() * 1000 - started_at_ms) / 3600000
    watching = sum(1 for t in tracking.values() if not t.get("recommended") and not t.get("rugged"))
    rugged_total = sum(1 for t in tracking.values() if t.get("rugged"))

    text = (
        f"🟢 *Бот работает*\n"
        f"Аптайм: {escape_mdv2(f'{uptime_hours:.1f}')} ч\n"
        f"Под наблюдением: {escape_mdv2(watching)}\n"
        f"Рагпулов поймано всего: {escape_mdv2(rugged_total)}"
    )

    if rugs_since_last_report:
        header = f"Рагпулы за последние {CONFIG['heartbeat_interval_minutes']} мин ({len(rugs_since_last_report)}):"
        text += f"\n\n🚫 *{escape_mdv2(header)}*\n"
        for name, symbol, reason in rugs_since_last_report:
            text += f"• {escape_mdv2(name)} \\(${escape_mdv2(symbol)}\\) — {escape_mdv2(reason)}\n"
    else:
        text += f"\n\nРагпулов за последние {escape_mdv2(CONFIG['heartbeat_interval_minutes'])} мин не обнаружено\\."

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    try:
        resp = SESSION.post(url, data=payload, timeout=20)
        if resp.status_code != 200:
            logger.warning(f"[send_heartbeat] Telegram ошибка: {resp.text}")
    except Exception as e:
        logger.warning(f"[send_heartbeat] Ошибка: {e}")


# ============ ОБРАБОТКА ОДНОГО ТОКЕНА (ОБНОВЛЕНИЕ ТРЕКИНГА) ============

def update_tracking(entry, tracking):
    """Обновляет запись трекинга для токена свежими данными: обновляет пики
    ликвидности/цены, проверяет условия рагпула, инициализирует запись при
    первом обнаружении. tracking — общий thread-safe словарь (доступ из
    ThreadPoolExecutor, но каждая запись правится только своим потоком по
    своему mint, гонок по разным ключам dict в CPython быть не должно)."""
    mint = entry["mint"]
    dexdata = get_dexscreener_data(mint)
    rugcheck = get_rugcheck_report(mint)
    now_ms = datetime.now().timestamp() * 1000

    if mint not in tracking:
        tracking[mint] = {
            "first_seen_ms": now_ms,
            "pair_created_at_ms": None,
            "sources": [],
            "recommended": False,
            "rugged": False,
            "rugged_reason": None,
            "rug_notified": False,
            "rug_pending_since_ms": None,
            "peak_liquidity": None,
            "peak_price": None,
            "first_liquidity": None,
            "first_price": None,
            "first_holder_count": None,
            "checkpoints_evaluated": [],
            "consecutive_missing_count": 0,
            "possibly_delisted": False,
            "score_history": {},
            "final_verdict": "watching",
            "post_rec_dev_wallet_creator": None,
            "post_rec_dev_wallet_baseline": None,
            "post_rec_dev_wallet_last_check_ms": None,
            "post_rec_dev_wallet_max_tier_alerted": 0.0,
        }

    t = tracking[mint]
    t["sources"] = sorted(set(t.get("sources", [])) | set(entry.get("sources", [])))

    if dexdata is None or rugcheck is None:
        t["consecutive_missing_count"] = t.get("consecutive_missing_count", 0) + 1
        # Только для токенов, у которых УЖЕ были нормальные данные раньше
        # (peak_liquidity известен) — иначе это просто новый токен, который
        # ещё не успел проиндексироваться, а не "исчез".
        if t["consecutive_missing_count"] >= CONFIG["delisting_missing_threshold"] and t.get("peak_liquidity") is not None:
            t["possibly_delisted"] = True
        return {"mint": mint, "status": "missing_data", "dexdata": dexdata, "rugcheck": rugcheck}

    # Данные снова получены — сбрасываем текущий счётчик подряд идущих
    # пропусков, но possibly_delisted НЕ сбрасываем: это исторический
    # сигнал "у токена уже был период исчезновения из выдачи", он должен
    # учитываться при оценке даже после того, как данные восстановились.
    t["consecutive_missing_count"] = 0

    # Реальное время создания пары в блокчейне (в отличие от first_seen_ms —
    # момента, когда ЭТОТ бот впервые заметил токен через свои источники).
    # Используем его для окна наблюдения, чтобы не ждать 8ч "по своим часам"
    # для токена, которому на самом деле уже 70 часов от роду.
    if t["pair_created_at_ms"] is None and dexdata.get("pair_created_at"):
        t["pair_created_at_ms"] = dexdata["pair_created_at"]

    liq = dexdata.get("liquidity_usd") or 0
    price = dexdata.get("price_usd") or 0
    holders = rugcheck.get("holder_count")

    if t["first_liquidity"] is None:
        t["first_liquidity"] = liq
    if t["first_price"] is None:
        t["first_price"] = price
    if t["first_holder_count"] is None:
        t["first_holder_count"] = holders

    if t["peak_liquidity"] is None or liq > t["peak_liquidity"]:
        t["peak_liquidity"] = liq
    if t["peak_price"] is None or price > t["peak_price"]:
        t["peak_price"] = price

    if not t["rugged"]:
        liq_drop_pct = (1 - liq / t["peak_liquidity"]) if t["peak_liquidity"] else 0

        if liq_drop_pct >= CONFIG["rug_liquidity_drop_hard_pct"]:
            # Просадка достаточно большая, чтобы быть похожей на вывод ликвидности.
            # Но не помечаем сразу — цена и ликвидность у мемкоинов волатильны,
            # и одиночный шумный замер (или временная просадка с восстановлением)
            # не должен НАВСЕГДА хоронить нормальный токен.
            if t["rug_pending_since_ms"] is None:
                t["rug_pending_since_ms"] = now_ms
            pending_minutes = (now_ms - t["rug_pending_since_ms"]) / 60000
            if pending_minutes >= CONFIG["rug_confirmation_minutes"]:
                t["rugged"] = True
                t["rugged_reason"] = (
                    f"Ликвидность упала с пика ${t['peak_liquidity']:,.0f} до ${liq:,.0f} "
                    f"(-{liq_drop_pct:.0%}), устойчиво {pending_minutes:.0f} мин"
                )
        else:
            # Ликвидность восстановилась выше порога — снимаем ожидание,
            # это была временная просадка, а не реальный рагпул.
            t["rug_pending_since_ms"] = None

    # Баланс дев-кошелька больше НЕ проверяется на каждом цикле — только
    # один раз, непосредственно перед отправкой рекомендации (см. main()).
    # Причины: (1) RPC-запрос на каждый отслеживаемый токен каждую минуту
    # быстро упирался в 429 Too Many Requests на бесплатном узле;
    # (2) на pump.fun токенах fair-launch у разработчика часто изначально
    # 0 токенов — сравнение с "пиком" без полноценной истории ненадёжно
    # и может давать ложные срабатывания.

    t["last_liquidity"] = liq
    t["last_price"] = price
    t["last_holder_count"] = holders
    t["last_checked_ms"] = now_ms

    return {"mint": mint, "status": "ok", "dexdata": dexdata, "rugcheck": rugcheck}


def get_current_checkpoint(age_hours, checkpoints):
    """Возвращает наибольшую контрольную точку из списка, которую токен уже
    достиг по возрасту, либо None, если он младше самой первой точки.
    Если токен обнаружен поздно и уже перескочил несколько точек разом
    (например, найден в 40ч при чекпоинтах [8,16,36,48,72]) — используется
    только ближайшая снизу (36ч), без повторов по всем пропущенным."""
    applicable = [c for c in checkpoints if age_hours >= c]
    if not applicable:
        return None
    return max(applicable)


def cleanup_tracking(tracking):
    """Убирает записи старше tracking_retention_hours, чтобы файл не рос
    бесконечно — к этому моменту токен либо уже получил вердикт, либо
    безнадёжно устарел."""
    now_ms = datetime.now().timestamp() * 1000
    retention_ms = CONFIG["tracking_retention_hours"] * 3600000
    return {
        mint: t for mint, t in tracking.items()
        if (now_ms - t.get("first_seen_ms", now_ms)) < retention_ms
    }


# ============ ОСНОВНОЙ ЦИКЛ ============

def main():
    logger.info("Запуск сканера мемкоинов v2 (с отслеживанием выживаемости 8-72ч)...")
    tracking = load_json_file(TRACKING_FILE, default={})

    started_at_ms = datetime.now().timestamp() * 1000
    last_heartbeat_ms = started_at_ms
    rugs_since_last_report = []

    # Сразу при старте (в том числе после передеплоя) — сигнал, что бот поднялся.
    send_heartbeat(tracking, started_at_ms, rugs_since_last_report)

    while True:
        all_tokens = discover_tokens()
        logger.info(f"Источники вернули {len(all_tokens)} уникальных токенов")

        # Продолжаем следить и за уже известными токенами, которым ещё не
        # вынесен вердикт (recommended=False, rugged=False), даже если их
        # больше нет в свежей выдаче источников — иначе трекинг прервётся.
        pending = [
            {"mint": m, "sources": t.get("sources", [])}
            for m, t in tracking.items()
            if not t.get("recommended") and not t.get("rugged")
        ]
        combined = {item["mint"]: item for item in pending}
        for item in all_tokens:
            combined.setdefault(item["mint"], item)
        to_process = list(combined.values())

        results = []
        if to_process:
            with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
                futures = {executor.submit(update_tracking, item, tracking): item for item in to_process}
                for fut in as_completed(futures):
                    try:
                        res = fut.result()
                    except Exception as e:
                        logger.error(f"[update_tracking] Необработанная ошибка: {e}")
                        continue
                    results.append(res)

        now_ms = datetime.now().timestamp() * 1000

        for res in results:
            mint = res["mint"]
            t = tracking.get(mint)
            if t is None:
                continue

            if res["status"] == "missing_data":
                logger.info(f"⚠️ Нет данных для {mint} — попробуем позже")
                continue

            name = res["dexdata"].get("name", "?")
            symbol = res["dexdata"].get("symbol", "?")

            # Свежеобнаруженный рагпул ИЛИ продажа разработчиком — копим для
            # сводного отчёта, не спамим отдельным сообщением на каждый случай.
            if t.get("rugged") and not t.get("rug_notified"):
                t["rug_notified"] = True
                t["recommended"] = True
                t["final_verdict"] = "rugpull"
                logger.warning(f"🚫 РАГПУЛ: {name} (${symbol}) {mint} — {t.get('rugged_reason')}")
                rugs_since_last_report.append((name, symbol, t.get("rugged_reason", "")))
                continue

            if t.get("recommended"):
                continue

            # Реальный возраст токена (от pairCreatedAt), а не от момента,
            # когда бот его заметил — иначе токен, найденный поздно, ждал бы
            # ещё 8ч "с нуля", хотя ему на самом деле уже может быть 70+ часов.
            if t.get("pair_created_at_ms"):
                age_hours = (now_ms - t["pair_created_at_ms"]) / 3600000
            else:
                # Резервный вариант, если DexScreener не отдал pairCreatedAt —
                # менее точно, но лучше, чем ничего.
                age_hours = (now_ms - t["first_seen_ms"]) / 3600000

            checkpoints = CONFIG["evaluation_checkpoints_hours"]
            checkpoint = get_current_checkpoint(age_hours, checkpoints)

            if checkpoint is None:
                # Токен младше самой первой контрольной точки — молча ждём
                continue

            if checkpoint in t.get("checkpoints_evaluated", []):
                # Эту точку уже проверяли — ждём следующую по возрасту
                continue

            # Считаем score, но НЕ фиксируем чекпоинт как "оценённый" сразу —
            # только после того, как поймём, что делать с результатом (см. ниже).
            # Раньше фиксация происходила сразу, из-за чего при сбое отправки
            # в Telegram (таймаут/сеть) чекпоинт всё равно считался пройденным,
            # и токен помечался "рекомендован", хотя сообщение НЕ дошло —
            # мониторинг дев-кошелька запускался вслепую на невидимую пользователю рекомендацию.
            score, deductions, meta = score_token(res["rugcheck"], res["dexdata"], t)
            stars = stars_for_score(score)
            is_last_checkpoint = checkpoint == max(checkpoints)

            if score >= CONFIG["min_score_to_alert"]:
                # RPC-запрос баланса дев-кошелька — ТОЛЬКО здесь, для 1-3
                # токенов за цикл, а не для каждого отслеживаемого токена
                # каждую минуту (иначе быстро упираемся в 429 у Solana RPC).
                # Информационно, НЕ влияет на score — см. обсуждение выше.
                creator_address = res["rugcheck"].get("creator_address")
                dev_balance = get_dev_wallet_token_balance(mint, creator_address) if creator_address else None

                sent_ok = send_telegram_alert(
                    mint, res["dexdata"], res["rugcheck"], score, deductions,
                    t.get("sources", []), age_hours, t.get("score_history"), dev_balance,
                )

                if sent_ok:
                    logger.info(f"✅ {stars} Score {score}/100 — {name} (${symbol}), чекпоинт {checkpoint}ч (токену {age_hours:.1f}ч) — рекомендация доставлена")
                    t.setdefault("checkpoints_evaluated", []).append(checkpoint)
                    t.setdefault("score_history", {})[str(checkpoint)] = score
                    t["recommended"] = True
                    t["final_verdict"] = "recommended"

                    # Сохраняем baseline для последующего мониторинга: теперь,
                    # когда токен уже рекомендован (маленькая выборка), можно
                    # безопасно проверять его баланс дева раз в N минут без
                    # риска забить RPC запросами — и делать это корректно,
                    # с полноценной историей, а не по одному снимку.
                    if creator_address and dev_balance is not None:
                        t["post_rec_dev_wallet_creator"] = creator_address
                        t["post_rec_dev_wallet_baseline"] = dev_balance
                        t["post_rec_dev_wallet_last_check_ms"] = now_ms
                        t["post_rec_dev_wallet_max_tier_alerted"] = 0.0
                        t["token_name"] = name
                        t["token_symbol"] = symbol
                else:
                    # Не дошло — НЕ помечаем чекпоинт, НЕ помечаем recommended.
                    # На следующем цикле опроса попробуем отправить снова
                    # (score почти наверняка останется тем же за 10 минут).
                    logger.warning(f"⚠️ Рекомендация для {name} (${symbol}) НЕ доставлена (сбой Telegram) — попробуем снова на следующем цикле")
            elif is_last_checkpoint:
                logger.info(f"❌ {stars} Score {score}/100 — {name} (${symbol}), финальный чекпоинт {checkpoint}ч — рекомендация не даётся: {deductions}")
                t.setdefault("checkpoints_evaluated", []).append(checkpoint)
                t.setdefault("score_history", {})[str(checkpoint)] = score
                t["recommended"] = True
                t["final_verdict"] = "expired"
            else:
                next_checkpoints = [c for c in checkpoints if c > checkpoint]
                next_cp = next_checkpoints[0] if next_checkpoints else None
                logger.info(f"⏳ {stars} Score {score}/100 — {name} (${symbol}), чекпоинт {checkpoint}ч не пройден, следующая попытка на {next_cp}ч: {deductions}")
                t.setdefault("checkpoints_evaluated", []).append(checkpoint)
                t.setdefault("score_history", {})[str(checkpoint)] = score
                # НЕ помечаем recommended — даём токену ещё один шанс на следующей точке

        # --- Пост-рекомендационный мониторинг дев-кошелька ---
        # Отдельный, редкий проход ТОЛЬКО по уже рекомендованным токенам
        # (маленькая выборка — единицы в день, не десятки в минуту), поэтому
        # RPC не перегружается. Здесь у нас уже есть настоящий baseline
        # (записан в момент рекомендации), так что сравнение корректно —
        # в отличие от одиночного снимка без истории на входе.
        check_interval_ms = CONFIG["post_rec_dev_check_interval_minutes"] * 60000
        for mint, t in list(tracking.items()):
            if t.get("final_verdict") != "recommended":
                continue
            if not t.get("post_rec_dev_wallet_creator") or t.get("post_rec_dev_wallet_baseline") is None:
                continue
            last_check = t.get("post_rec_dev_wallet_last_check_ms") or 0
            if (now_ms - last_check) < check_interval_ms:
                continue

            current_balance = get_dev_wallet_token_balance(mint, t["post_rec_dev_wallet_creator"])
            t["post_rec_dev_wallet_last_check_ms"] = now_ms
            if current_balance is None:
                continue  # временный сбой RPC — не делаем выводов, попробуем в следующий раз

            baseline = t["post_rec_dev_wallet_baseline"]
            if baseline and baseline > 0:
                sold_pct = 1 - current_balance / baseline
                already_alerted = t.get("post_rec_dev_wallet_max_tier_alerted", 0.0)
                # Проверяем тиры от большего к меньшему, чтобы уведомить один
                # раз именно о самом высоком новом пересечённом пороге.
                for tier in sorted(CONFIG["post_rec_dev_alert_tiers"], reverse=True):
                    if sold_pct >= tier and tier > already_alerted:
                        name_for_alert = t.get("token_name", mint)
                        symbol_for_alert = t.get("token_symbol", "?")
                        sent_ok = send_dev_dump_alert(mint, name_for_alert, symbol_for_alert, sold_pct, baseline, current_balance)
                        if sent_ok:
                            logger.warning(f"🚨 ДЕВ СЛИЛ ПОСЛЕ РЕКОМЕНДАЦИИ: {mint} — {sold_pct:.0%} от baseline {baseline:,.0f}")
                            t["post_rec_dev_wallet_max_tier_alerted"] = tier
                        else:
                            logger.warning(f"⚠️ Не удалось доставить уведомление о сливе дева для {mint} — попробуем на следующей проверке")
                        break

        tracking = cleanup_tracking(tracking)
        save_json_file(TRACKING_FILE, tracking)

        now_ms = datetime.now().timestamp() * 1000
        if (now_ms - last_heartbeat_ms) >= CONFIG["heartbeat_interval_minutes"] * 60000:
            send_heartbeat(tracking, started_at_ms, rugs_since_last_report)
            rugs_since_last_report = []
            last_heartbeat_ms = now_ms

        time.sleep(CONFIG["poll_interval_seconds"])


if __name__ == "__main__":
    main()
