"""
Кэш данных из RugCheck по токену: адрес кошелька дева + статус
блокировки ликвидности (LP).

- Адрес кошелька дева не меняется — кэшируется НАВСЕГДА.
- Статус LP теоретически может измениться (например, локер истёк) —
  кэшируется на config.RUGCHECK_LP_CACHE_TTL_SEC (по умолчанию 6 часов),
  потом обновляется.

Оба значения достаются из ОДНОГО запроса к RugCheck за один "поход",
чтобы не расходовать лимит дважды.

Postgres (если подключена) — таблица dev_wallets (name осталась
исторической, но теперь хранит оба поля). Иначе — JSON-файл.
"""

import json
import os
import time

import config
import db
import data_sources as ds


async def get_rugcheck_info(token_address):
    """
    Возвращает (dev_wallet, lp_clean):
      dev_wallet — адрес кошелька-создателя или None
      lp_clean   — True/False/None (заблокирована/риск/неизвестно)
    """
    cached = await _get_cached(token_address)
    now = time.time()

    lp_checked_ts = cached.get("lp_checked_ts") if cached else None
    lp_stale = lp_checked_ts is None or (now - lp_checked_ts) > config.RUGCHECK_LP_CACHE_TTL_SEC
    need_dev_wallet = not cached or not cached.get("dev_wallet")

    if cached and not lp_stale and not need_dev_wallet:
        return cached.get("dev_wallet"), cached.get("lp_clean")

    report = ds.get_rugcheck_report(token_address)
    if not report:
        # Не удалось обновить — отдаём то, что было в кэше (может быть частично)
        return (cached.get("dev_wallet") if cached else None), (cached.get("lp_clean") if cached else None)

    dev_wallet = ds.extract_rugcheck_creator(report) or (cached.get("dev_wallet") if cached else None)
    lp_clean = ds.extract_rugcheck_lp_status(report)

    await _save_cached(token_address, dev_wallet, lp_clean, now)
    return dev_wallet, lp_clean


# ---------------------------------------------------------------------------
# Postgres / JSON-фолбэк
# ---------------------------------------------------------------------------

async def _get_cached(token_address):
    if db.is_configured():
        row = await db.run(
            "SELECT dev_wallet, lp_clean, lp_checked_ts FROM dev_wallets WHERE address = %s",
            (token_address,), fetchone=True,
        )
        if not row:
            return None
        return {"dev_wallet": row[0], "lp_clean": row[1], "lp_checked_ts": row[2]}
    else:
        data = _json_load()
        return data.get(token_address)


async def _save_cached(token_address, dev_wallet, lp_clean, lp_checked_ts):
    if db.is_configured():
        await db.run(
            "INSERT INTO dev_wallets (address, dev_wallet, lp_clean, lp_checked_ts) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (address) DO UPDATE SET "
            "dev_wallet = EXCLUDED.dev_wallet, lp_clean = EXCLUDED.lp_clean, "
            "lp_checked_ts = EXCLUDED.lp_checked_ts",
            (token_address, dev_wallet, lp_clean, lp_checked_ts),
        )
    else:
        data = _json_load()
        data[token_address] = {
            "dev_wallet": dev_wallet,
            "lp_clean": lp_clean,
            "lp_checked_ts": lp_checked_ts,
        }
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
