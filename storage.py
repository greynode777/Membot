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


async def record_score_history(token_address, score_pct, verdict, total_score, max_possible_score):
    """Сохраняет точку истории Score — для команды /history."""
    if db.is_configured():
        await db.run(
            "INSERT INTO score_history (token_address, ts, score_pct, verdict, "
            "total_score, max_possible_score) VALUES (%s, %s, %s, %s, %s, %s)",
            (token_address, time.time(), score_pct, verdict, total_score, max_possible_score),
        )
    else:
        _json_record_score_history(token_address, score_pct, verdict, total_score, max_possible_score)


async def get_score_history(token_address, limit=10):
    """Последние `limit` точек истории Score, от старых к новым."""
    if db.is_configured():
        rows = await db.run(
            "SELECT ts, score_pct, verdict, total_score, max_possible_score FROM score_history "
            "WHERE token_address = %s ORDER BY ts DESC LIMIT %s",
            (token_address, limit), fetch=True,
        )
        rows = rows or []
        entries = [
            {"ts": r[0], "score_pct": r[1], "verdict": r[2], "total_score": r[3], "max_possible_score": r[4]}
            for r in rows
        ]
        return list(reversed(entries))
    else:
        return _json_get_score_history(token_address, limit)


async def get_first_and_last_snapshot(token_address):
    """
    Для автообъяснения роста/падения в /history: сравнивает самый
    первый и самый свежий сохранённый снимок сырых метрик (ликвидность,
    объём, баланс дева). Возвращает (first, last) или (None, None).
    """
    if db.is_configured():
        first = await db.run(
            "SELECT liquidity_usd, volume_24h_usd, dev_balance FROM snapshots "
            "WHERE token_address = %s ORDER BY ts ASC LIMIT 1",
            (token_address,), fetchone=True,
        )
        last = await db.run(
            "SELECT liquidity_usd, volume_24h_usd, dev_balance FROM snapshots "
            "WHERE token_address = %s ORDER BY ts DESC LIMIT 1",
            (token_address,), fetchone=True,
        )
        if not first or not last:
            return None, None
        keys = ["liquidity_usd", "volume_24h_usd", "dev_balance"]
        return dict(zip(keys, first)), dict(zip(keys, last))
    else:
        history = _json_load().get(token_address, [])
        if len(history) < 2:
            return None, None
        return history[0], history[-1]


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


# ---------------------------------------------------------------------------
# JSON-фолбэк для истории Score (отдельный файл от snapshots.json)
# ---------------------------------------------------------------------------

def _score_history_load():
    if not os.path.exists(config.SCORE_HISTORY_FILE):
        return {}
    try:
        with open(config.SCORE_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _score_history_save(data):
    with open(config.SCORE_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _json_record_score_history(token_address, score_pct, verdict, total_score, max_possible_score):
    data = _score_history_load()
    history = data.get(token_address, [])
    history.append({
        "ts": time.time(),
        "score_pct": score_pct,
        "verdict": verdict,
        "total_score": total_score,
        "max_possible_score": max_possible_score,
    })
    data[token_address] = history[-200:]
    _score_history_save(data)


def _json_get_score_history(token_address, limit=10):
    history = _score_history_load().get(token_address, [])
    return history[-limit:]
