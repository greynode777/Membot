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


# ---------------------------------------------------------------------------
# RugCheck (получение адреса кошелька разработчика/создателя токена)
# ---------------------------------------------------------------------------

def get_rugcheck_creator(token_address):
    """
    Возвращает адрес кошелька-создателя (dev wallet) токена через
    бесплатный RugCheck API, или None, если не удалось определить.
    """
    url = f"https://api.rugcheck.xyz/v1/tokens/{token_address}/report"
    data = _safe_get(url, retries=1, backoff_sec=2)
    if not data:
        return None
    # Разные версии API RugCheck называют поле по-разному — проверяем варианты
    creator = data.get("creator") or data.get("creatorAddress")
    if not creator and isinstance(data.get("token"), dict):
        creator = data["token"].get("creator")
    return creator or None


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
