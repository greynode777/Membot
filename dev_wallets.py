"""
Кэш адресов кошельков разработчиков (dev wallet) по токенам.

Адрес создателя токена не меняется со временем, поэтому незачем
запрашивать RugCheck на каждую проверку — один раз узнали и запомнили
навсегда (или до тех пор, пока не понадобится обновить вручную).

Postgres (если подключена) — таблица dev_wallets. Иначе — JSON-файл.
"""

import json
import os

import config
import db
import data_sources as ds


async def get_dev_wallet(token_address):
    """
    Возвращает адрес кошелька-создателя токена. Сначала смотрит в кэш,
    если там нет — запрашивает RugCheck и сохраняет результат.
    Возвращает None, если определить не удалось (кэшируется только
    успешный результат — при неудаче пробуем снова в следующий раз).
    """
    cached = await _get_cached(token_address)
    if cached is not None:
        return cached

    creator = ds.get_rugcheck_creator(token_address)
    if creator:
        await _save_cached(token_address, creator)
    return creator


# ---------------------------------------------------------------------------
# Postgres / JSON-фолбэк
# ---------------------------------------------------------------------------

async def _get_cached(token_address):
    if db.is_configured():
        row = await db.run(
            "SELECT dev_wallet FROM dev_wallets WHERE address = %s",
            (token_address,), fetchone=True,
        )
        return row[0] if row else None
    else:
        data = _json_load()
        return data.get(token_address)


async def _save_cached(token_address, dev_wallet):
    if db.is_configured():
        await db.run(
            "INSERT INTO dev_wallets (address, dev_wallet) VALUES (%s, %s) "
            "ON CONFLICT (address) DO UPDATE SET dev_wallet = EXCLUDED.dev_wallet",
            (token_address, dev_wallet),
        )
    else:
        data = _json_load()
        data[token_address] = dev_wallet
        _json_save(data)


def _json_load():
    if not os.path.exists(config.DEV_WALLETS_FILE):
        return {}
    try:
        with open(config.DEV_WALLETS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _json_save(data):
    with open(config.DEV_WALLETS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
