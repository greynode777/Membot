"""
Обёртки для бесплатных источников данных:
- DexScreener API (без ключа, бесплатно) — цены, объёмы, ликвидность, соцсети
- Standard Solana RPC (бесплатно через Helius free tier или публичный RPC)
  — Mint/Freeze authority, крупнейшие держатели токена

Если какой-то запрос не удался (сеть, лимит, таймаут) — функции возвращают
None или пустой список вместо падения с ошибкой, чтобы один сбойный
запрос не останавливал весь цикл сканирования.
"""

import time as _time
import requests
import config


def _safe_get(url, params=None, timeout=10, retries=2, backoff_sec=3):
    """
    Обычный GET-запрос с защитой от временных сбоев.
    При ошибке 429 (превышен лимит запросов) ждёт backoff_sec секунд
    и пробует ещё раз (до retries раз), вместо того чтобы сразу
    отказаться от проверки токена.
    """
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 429 and attempt < retries:
                print(f"[data_sources] 429 (лимит запросов), жду {backoff_sec}с и повторяю: {url}")
                _time.sleep(backoff_sec)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt < retries:
                _time.sleep(backoff_sec)
                continue
            print(f"[data_sources] Ошибка запроса {url}: {e}")
            return None
    return None


# ---------------------------------------------------------------------------
# DexScreener
# ---------------------------------------------------------------------------

def get_latest_boosted_tokens():
    """Список токенов, которые сейчас продвигаются (часто новые/активные)."""
    url = f"{config.DEXSCREENER_BASE_URL}/token-boosts/latest/v1"
    data = _safe_get(url)
    return data or []


def get_latest_token_profiles():
    """Список последних добавленных токен-профилей."""
    url = f"{config.DEXSCREENER_BASE_URL}/token-profiles/latest/v1"
    data = _safe_get(url)
    return data or []


def get_pairs_for_token(chain_id, token_address):
    """
    Возвращает список торговых пар для конкретного токена
    (цена, объём, ликвидность, соцсети, возраст пары и т.д.)
    """
    url = f"{config.DEXSCREENER_BASE_URL}/token-pairs/v1/{chain_id}/{token_address}"
    data = _safe_get(url)
    return data or []


def get_best_pair_for_token(token_address, chain_id="solana"):
    """Берёт пару с наибольшей ликвидностью для токена (обычно основная)."""
    pairs = get_pairs_for_token(chain_id, token_address)
    if not pairs:
        return None
    return max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd", 0) or 0)


# ---------------------------------------------------------------------------
# Solana RPC (стандартные бесплатные методы, включая Helius free tier)
# ---------------------------------------------------------------------------

def _rpc_call(method, params):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        resp = requests.post(config.SOLANA_RPC_URL, json=payload, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        return result.get("result")
    except Exception as e:
        print(f"[data_sources] Ошибка RPC {method}: {e}")
        return None


def get_mint_and_freeze_authority(mint_address):
    """
    Проверяет, отключены ли Mint и Freeze authority у токена.
    Возвращает (mint_disabled: bool, freeze_disabled: bool) или (None, None)
    при ошибке запроса.
    """
    result = _rpc_call(
        "getAccountInfo",
        [mint_address, {"encoding": "jsonParsed"}],
    )
    if not result or not result.get("value"):
        return None, None

    try:
        parsed = result["value"]["data"]["parsed"]["info"]
        mint_disabled = parsed.get("mintAuthority") is None
        freeze_disabled = parsed.get("freezeAuthority") is None
        return mint_disabled, freeze_disabled
    except (KeyError, TypeError):
        return None, None


def get_token_supply(mint_address):
    result = _rpc_call("getTokenSupply", [mint_address])
    if not result:
        return None
    try:
        return float(result["value"]["uiAmountString"])
    except (KeyError, TypeError, ValueError):
        return None


def get_top10_concentration(mint_address):
    """
    Считает, какой % от общей эмиссии держат топ-10 кошельков.
    Использует стандартный бесплатный метод getTokenLargestAccounts.
    Возвращает процент (0-100) или None при ошибке.
    """
    largest = _rpc_call("getTokenLargestAccounts", [mint_address])
    total_supply = get_token_supply(mint_address)

    if not largest or not total_supply or total_supply == 0:
        return None

    try:
        accounts = largest["value"][:10]
        top10_sum = sum(float(a["uiAmountString"]) for a in accounts if a.get("uiAmountString"))
        return round((top10_sum / total_supply) * 100, 2)
    except (KeyError, TypeError, ValueError):
        return None


def _get_token_account_owner(token_account_address):
    """getTokenLargestAccounts возвращает адреса ТОКЕН-АККАУНТОВ, а не
    кошельков-владельцев — этот запрос достаёт реальный адрес владельца."""
    result = _rpc_call("getAccountInfo", [token_account_address, {"encoding": "jsonParsed"}])
    if not result or not result.get("value"):
        return None
    try:
        return result["value"]["data"]["parsed"]["info"]["owner"]
    except (KeyError, TypeError):
        return None


def get_top_holders(mint_address, count=10):
    """
    Возвращает список крупнейших держателей токена:
    [{"address": "кошелёк-владелец", "pct": доля от эмиссии в %}, ...]
    или None при ошибке. Делает до `count` дополнительных RPC-запросов
    (по одному на каждого держателя, чтобы найти реального владельца) —
    поэтому вызывается только по запросу пользователя (кнопка Holders),
    не в фоновых циклах.
    """
    largest = _rpc_call("getTokenLargestAccounts", [mint_address])
    total_supply = get_token_supply(mint_address)

    if not largest or not total_supply or total_supply == 0:
        return None

    try:
        accounts = largest["value"][:count]
        result = []
        for a in accounts:
            amount = float(a.get("uiAmountString") or 0)
            pct = round((amount / total_supply) * 100, 2)
            token_account_addr = a.get("address", "")
            owner = _get_token_account_owner(token_account_addr) or token_account_addr
            result.append({"address": owner, "pct": pct})
        return result
    except (KeyError, TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# RugCheck (адрес кошелька дева + статус блокировки ликвидности —
# из ОДНОГО запроса, чтобы не тратить лимит дважды)
# ---------------------------------------------------------------------------

def get_rugcheck_report(token_address):
    """Сырой отчёт RugCheck целиком, или None при ошибке/лимите."""
    url = f"https://api.rugcheck.xyz/v1/tokens/{token_address}/report"
    return _safe_get(url, retries=1, backoff_sec=2)


def extract_rugcheck_creator(report):
    """Достаёт адрес кошелька-создателя из отчёта RugCheck."""
    if not report:
        return None
    creator = report.get("creator") or report.get("creatorAddress")
    if not creator and isinstance(report.get("token"), dict):
        creator = report["token"].get("creator")
    return creator or None


def extract_rugcheck_lp_status(report):
    """
    Достаёт статус ликвидности из отчёта RugCheck.
    Возвращает True (похоже, что заблокирована/сожжена — есть локеры),
    False (явные признаки риска: уже rugged либо RugCheck сам пометил
    риск, связанный с ликвидностью), или None (не удалось определить —
    в этом случае фактор просто не участвует в скоринге, без штрафа).
    """
    if not report:
        return None

    if report.get("rugged") is True:
        return False

    risks = report.get("risks") or []
    liquidity_risk_levels = [
        r.get("level") for r in risks
        if isinstance(r, dict) and "liquidity" in str(r.get("name", "")).lower()
    ]
    if any(lvl in ("danger", "high", "warn") for lvl in liquidity_risk_levels if lvl):
        return False

    lockers = report.get("lockers")
    if lockers:
        return True

    return None  # явных сигналов не нашли — честно "не знаем"


# ---------------------------------------------------------------------------
# Баланс конкретного токена на конкретном кошельке (для отслеживания
# накапливает/распродаёт ли dev-кошелёк свою позицию)
# ---------------------------------------------------------------------------

def get_wallet_token_balance(wallet_address, mint_address):
    """
    Возвращает, сколько единиц токена mint_address лежит на кошельке
    wallet_address (суммарно по всем его token-аккаунтам для этого mint).
    None при ошибке запроса.
    """
    result = _rpc_call(
        "getTokenAccountsByOwner",
        [wallet_address, {"mint": mint_address}, {"encoding": "jsonParsed"}],
    )
    if not result:
        return None
    try:
        total = 0.0
        for account in result.get("value", []):
            info = account["account"]["data"]["parsed"]["info"]
            total += float(info["tokenAmount"]["uiAmount"] or 0)
        return total
    except (KeyError, TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Bitquery (кто покупал токен в первые секунды после создания пула —
# для эвристики детекции бандлов)
# ---------------------------------------------------------------------------

def get_early_buyers(token_mint, pool_created_ts, window_sec=30):
    """
    Возвращает словарь {"unique_buyers": int, "total_usd": float} с данными
    о покупках токена в первые window_sec секунд после создания пула,
    или None при ошибке/отсутствии токена/лимите запросов.

    ВАЖНО: формат ответа Bitquery не был протестирован вживую (нет доступа
    к сети при разработке) — если структура окажется другой, функция
    просто вернёт None вместо ошибки, безопасно для остального бота.
    """
    if not config.BITQUERY_ACCESS_TOKEN:
        return None

    since_iso = _iso_from_ts(pool_created_ts)
    till_iso = _iso_from_ts(pool_created_ts + window_sec)

    query = """
    query ($mint: String!, $since: DateTime!, $till: DateTime!) {
      Solana {
        DEXTrades(
          where: {
            Trade: { Buy: { Currency: { MintAddress: { is: $mint } } } }
            Block: { Time: { since: $since, till: $till } }
          }
          limit: { count: 200 }
        ) {
          Trade {
            Buy {
              Account { Address }
              AmountInUSD
            }
          }
        }
      }
    }
    """
    payload = {
        "query": query,
        "variables": {"mint": token_mint, "since": since_iso, "till": till_iso},
    }
    headers = {
        "Authorization": f"Bearer {config.BITQUERY_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(
            "https://streaming.bitquery.io/graphql",
            json=payload, headers=headers, timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errors"):
            print(f"[data_sources] Bitquery вернул ошибку: {data['errors']}")
            return None

        trades = (data.get("data") or {}).get("Solana", {}).get("DEXTrades") or []
        buyers = set()
        total_usd = 0.0
        for t in trades:
            buy = (t.get("Trade") or {}).get("Buy") or {}
            addr = (buy.get("Account") or {}).get("Address")
            if addr:
                buyers.add(addr)
            try:
                total_usd += float(buy.get("AmountInUSD") or 0)
            except (TypeError, ValueError):
                pass

        return {"unique_buyers": len(buyers), "total_usd": total_usd}
    except Exception as e:
        print(f"[data_sources] Ошибка запроса Bitquery: {e}")
        return None


def _iso_from_ts(unix_ts):
    import datetime
    return datetime.datetime.utcfromtimestamp(unix_ts).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# CoinGecko (опциональная проверка листинга, бесплатно, лимит по запросам)
# ---------------------------------------------------------------------------

def is_listed_on_coingecko(token_address, platform="solana"):
    """
    Возвращает True (точно листингован), False (точно НЕ листингован,
    сервер ответил 404) или None (не удалось проверить — например,
    сработал лимит запросов 429; в этом случае считаем "неизвестно",
    а не "не листингован", чтобы не наказывать токен за чужую ошибку).
    """
    url = f"https://api.coingecko.com/api/v3/coins/{platform}/contract/{token_address}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return "id" in data
        elif resp.status_code == 404:
            return False
        else:
            return None  # 429 (лимит) или другая ошибка сервера — неизвестно
    except Exception as e:
        print(f"[data_sources] Ошибка запроса CoinGecko: {e}")
        return None
