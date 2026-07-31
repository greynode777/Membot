"""
Хранит историю показателей токена во времени (для расчёта трендов —
растёт/падает ликвидность и объём).

Если Postgres подключена (db.is_configured() == True) — данные пишутся
в таблицу snapshots и переживают редеплой на Railway.
Если Postgres не подключена — используется локальный JSON-файл
(как раньше); он обнуляется при редеплое.
"""

import json
import os
import time

import config
import db


# ---------------------------------------------------------------------------
# Публичные функции (используются в main.py) — асинхронные
# ---------------------------------------------------------------------------

async def record_snapshot(token_address, liquidity_usd, volume_24h_usd, price_usd, dev_balance=None):
    if db.is_configured():
        await db.run(
            "INSERT INTO snapshots (token_address, ts, liquidity_usd, volume_24h_usd, price_usd, dev_balance) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (token_address, time.time(), liquidity_usd, volume_24h_usd, price_usd, dev_balance),
        )
    else:
        _json_record_snapshot(token_address, liquidity_usd, volume_24h_usd, price_usd, dev_balance)


async def get_trend(token_address, field, lookback_hours=24):
    """
    Сравнивает текущее значение поля с ближайшим значением
    lookback_hours назад. Возвращает 'up', 'down', 'flat' или None,
    если истории пока недостаточно.
    """
    if db.is_configured():
        return await _pg_get_trend(token_address, field, lookback_hours)
    else:
        return _json_get_trend(token_address, field, lookback_hours)


# ---------------------------------------------------------------------------
# Реализация через Postgres
# ---------------------------------------------------------------------------

_FIELD_COLUMNS = {
    "liquidity_usd": "liquidity_usd",
    "volume_24h_usd": "volume_24h_usd",
    "price_usd": "price_usd",
    "dev_balance": "dev_balance",
}


async def _pg_get_trend(token_address, field, lookback_hours):
    column = _FIELD_COLUMNS.get(field)
    if not column:
        return None

    now = time.time()
    target_ts = now - lookback_hours * 3600

    current = await db.run(
        f"SELECT {column} FROM snapshots WHERE token_address = %s ORDER BY ts DESC LIMIT 1",
        (token_address,), fetchone=True,
    )
    past = await db.run(
        f"SELECT {column} FROM snapshots WHERE token_address = %s AND ts <= %s "
        f"ORDER BY ts DESC LIMIT 1",
        (token_address, target_ts), fetchone=True,
    )

    if not current or not past:
        return None

    current_value, past_value = current[0], past[0]
    if past_value is None or current_value is None or past_value == 0:
        return None

    change_pct = (current_value - past_value) / past_value * 100
    if change_pct > 5:
        return "up"
    elif change_pct < -5:
        return "down"
    return "flat"


# ---------------------------------------------------------------------------
# Резервная реализация через JSON-файл (если Postgres не подключена)
# ---------------------------------------------------------------------------

def _json_load():
    if not os.path.exists(config.SNAPSHOTS_FILE):
        return {}
    try:
        with open(config.SNAPSHOTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _json_save(data):
    with open(config.SNAPSHOTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _json_record_snapshot(token_address, liquidity_usd, volume_24h_usd, price_usd, dev_balance=None):
    data = _json_load()
    history = data.get(token_address, [])
    history.append({
        "ts": time.time(),
        "liquidity_usd": liquidity_usd,
        "volume_24h_usd": volume_24h_usd,
        "price_usd": price_usd,
        "dev_balance": dev_balance,
    })
    data[token_address] = history[-500:]
    _json_save(data)


def _json_get_trend(token_address, field, lookback_hours=24):
    history = _json_load().get(token_address, [])
    if len(history) < 2:
        return None

    now = time.time()
    target_ts = now - lookback_hours * 3600
    past_points = [p for p in history if p["ts"] <= target_ts]
    if not past_points:
        return None

    past_value = past_points[-1].get(field)
    current_value = history[-1].get(field)
    if past_value is None or current_value is None or past_value == 0:
        return None

    change_pct = (current_value - past_value) / past_value * 100
    if change_pct > 5:
        return "up"
    elif change_pct < -5:
        return "down"
    return "flat"
