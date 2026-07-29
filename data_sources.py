"""
Обёртки для бесплатных источников данных:
- DexScreener API (без ключа, бесплатно) — цены, объёмы, ликвидность, соцсети
- Standard Solana RPC (бесплатно через Helius free tier или публичный RPC)
  — Mint/Freeze authority, крупнейшие держатели токена

Если какой-то запрос не удался (сеть, лимит, таймаут) — функции возвращают
None или пустой список вместо падения с ошибкой, чтобы один сбойный
запрос не останавливал весь цикл сканирования.
"""

import requests
import config


def _safe_get(url, params=None, timeout=10):
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[data_sources] Ошибка запроса {url}: {e}")
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


# ---------------------------------------------------------------------------
# CoinGecko (опциональная проверка листинга, бесплатно, лимит по запросам)
# ---------------------------------------------------------------------------

def is_listed_on_coingecko(token_address, platform="solana"):
    url = f"https://api.coingecko.com/api/v3/coins/{platform}/contract/{token_address}"
    data = _safe_get(url)
    return data is not None and "id" in data
