"""
Портфель (/portfolio, /track, /untrack) — пользователь добавляет токен
с ценой входа, бот отслеживает текущую цену, считает PnL и присылает
алерт при значимом изменении (config.PORTFOLIO_ALERT_THRESHOLD_PCT).

Postgres (если подключена) — таблица portfolio. Иначе — JSON-файл.
"""

import json
import os
import time

import config
import db


async def add_position(address, name, entry_price, amount=None):
    """Возвращает True, если добавлено; False, если уже отслеживается."""
    if db.is_configured():
        existing = await db.run(
            "SELECT 1 FROM portfolio WHERE address = %s", (address,), fetchone=True
        )
        if existing:
            return False
        await db.run(
            "INSERT INTO portfolio (address, name, entry_price, entry_ts, amount, last_alert_pct) "
            "VALUES (%s, %s, %s, %s, %s, NULL)",
            (address, name, entry_price, time.time(), amount),
        )
        return True
    else:
        data = _json_load()
        if address in data:
            return False
        data[address] = {
            "name": name, "entry_price": entry_price, "entry_ts": time.time(),
            "amount": amount, "last_alert_pct": None,
        }
        _json_save(data)
        return True


async def remove_position(address) -> bool:
    if db.is_configured():
        existing = await db.run(
            "SELECT 1 FROM portfolio WHERE address = %s", (address,), fetchone=True
        )
        if not existing:
            return False
        await db.run("DELETE FROM portfolio WHERE address = %s", (address,))
        return True
    else:
        data = _json_load()
        if address not in data:
            return False
        del data[address]
        _json_save(data)
        return True


async def get_all_positions():
    if db.is_configured():
        rows = await db.run(
            "SELECT address, name, entry_price, entry_ts, amount, last_alert_pct "
            "FROM portfolio ORDER BY entry_ts ASC",
            fetch=True,
        )
        rows = rows or []
        return [
            {"address": r[0], "name": r[1], "entry_price": r[2], "entry_ts": r[3],
             "amount": r[4], "last_alert_pct": r[5]}
            for r in rows
        ]
    else:
        data = _json_load()
        return [{"address": addr, **entry} for addr, entry in data.items()]


async def update_last_alert(address, pct):
    if db.is_configured():
        await db.run(
            "UPDATE portfolio SET last_alert_pct = %s WHERE address = %s", (pct, address)
        )
    else:
        data = _json_load()
        if address in data:
            data[address]["last_alert_pct"] = pct
            _json_save(data)


# ---------------------------------------------------------------------------
# JSON-фолбэк
# ---------------------------------------------------------------------------

def _json_load():
    if not os.path.exists(config.PORTFOLIO_FILE):
        return {}
    try:
        with open(config.PORTFOLIO_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _json_save(data):
    with open(config.PORTFOLIO_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
