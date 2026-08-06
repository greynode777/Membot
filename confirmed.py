"""
Список "подтверждённых" токенов (получивших вердикт Strong), которые
проверяются повторно каждые 12 часов.

Автоочистка: если токен несколько проверок подряд остаётся со статусом
Skip (config.CONFIRMED_AUTO_REMOVE_AFTER_SKIPS раз) — убирается из
мониторинга автоматически. Раньше токены, попавшие сюда, оставались
навечно, даже если их баллы упали и остались плохими надолго.

Postgres (если подключена) — таблица confirmed_tokens, переживает редеплой.
Иначе — локальный JSON-файл (как раньше).
"""

import json
import os
import time

import config
import db


# ---------------------------------------------------------------------------
# Публичные функции — асинхронные
# ---------------------------------------------------------------------------

async def add_token(address, name, initial_score, initial_verdict, source="new_token_scanner"):
    if db.is_configured():
        existing = await db.run(
            "SELECT 1 FROM confirmed_tokens WHERE address = %s", (address,), fetchone=True
        )
        if existing:
            return
        await db.run(
            "INSERT INTO confirmed_tokens (address, name, source, added_ts, last_score, "
            "last_verdict, next_check_ts, skip_streak) VALUES (%s, %s, %s, %s, %s, %s, %s, 0)",
            (address, name, source, time.time(), initial_score, initial_verdict,
             time.time() + config.CONFIRMED_RECHECK_INTERVAL_SEC),
        )
    else:
        _json_add_token(address, name, initial_score, initial_verdict, source)


async def get_all():
    """Все токены под 12ч-мониторингом прямо сейчас (для команды
    /monitoring в Telegram) — без фильтра по времени следующей проверки."""
    if db.is_configured():
        rows = await db.run(
            "SELECT address, name, source, added_ts, last_score, last_verdict, "
            "next_check_ts, skip_streak FROM confirmed_tokens ORDER BY next_check_ts ASC",
            fetch=True,
        )
        rows = rows or []
        return [_row_to_dict(r) for r in rows]
    else:
        tokens = _json_load()
        return sorted(tokens, key=lambda t: t.get("next_check_ts") or 0)


async def get_due_tokens():
    if db.is_configured():
        rows = await db.run(
            "SELECT address, name, source, added_ts, last_score, last_verdict, "
            "next_check_ts, skip_streak FROM confirmed_tokens WHERE next_check_ts <= %s",
            (time.time(),), fetch=True,
        )
        rows = rows or []
        return [_row_to_dict(r) for r in rows]
    else:
        return _json_get_due_tokens()


async def update_after_check(address, new_score, new_verdict, skip_streak=0):
    """
    skip_streak — сколько проверок подряд (включая эту) вердикт остаётся
    Skip. Передаётся уже посчитанным из main.py (там же принимается
    решение — обновить или удалить, см. remove_token).
    """
    if db.is_configured():
        await db.run(
            "UPDATE confirmed_tokens SET last_score = %s, last_verdict = %s, "
            "next_check_ts = %s, skip_streak = %s WHERE address = %s",
            (new_score, new_verdict, time.time() + config.CONFIRMED_RECHECK_INTERVAL_SEC,
             skip_streak, address),
        )
    else:
        _json_update_after_check(address, new_score, new_verdict, skip_streak)


async def remove_token(address) -> bool:
    """Убирает токен из 12ч-мониторинга (автоочистка или /unmonitor)."""
    if db.is_configured():
        existing = await db.run(
            "SELECT 1 FROM confirmed_tokens WHERE address = %s", (address,), fetchone=True
        )
        if not existing:
            return False
        await db.run("DELETE FROM confirmed_tokens WHERE address = %s", (address,))
        return True
    else:
        tokens = _json_load()
        new_tokens = [t for t in tokens if t["address"] != address]
        changed = len(new_tokens) != len(tokens)
        _json_save(new_tokens)
        return changed


async def count():
    if db.is_configured():
        row = await db.run("SELECT COUNT(*) FROM confirmed_tokens", fetchone=True)
        return row[0] if row else 0
    else:
        return _json_count()


def _row_to_dict(r):
    return {
        "address": r[0], "name": r[1], "source": r[2], "added_ts": r[3],
        "last_score": r[4], "last_verdict": r[5], "next_check_ts": r[6],
        "skip_streak": r[7] if len(r) > 7 else 0,
    }


# ---------------------------------------------------------------------------
# Резервная реализация через JSON-файл
# ---------------------------------------------------------------------------

def _json_load():
    if not os.path.exists(config.CONFIRMED_TOKENS_FILE):
        return []
    try:
        with open(config.CONFIRMED_TOKENS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return []


def _json_save(data):
    with open(config.CONFIRMED_TOKENS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _json_add_token(address, name, initial_score, initial_verdict, source):
    tokens = _json_load()
    if any(t["address"] == address for t in tokens):
        return
    tokens.append({
        "address": address,
        "name": name,
        "source": source,
        "added_ts": time.time(),
        "last_score": initial_score,
        "last_verdict": initial_verdict,
        "next_check_ts": time.time() + config.CONFIRMED_RECHECK_INTERVAL_SEC,
        "skip_streak": 0,
    })
    _json_save(tokens)


def _json_get_due_tokens():
    tokens = _json_load()
    now = time.time()
    return [t for t in tokens if t["next_check_ts"] <= now]


def _json_update_after_check(address, new_score, new_verdict, skip_streak=0):
    tokens = _json_load()
    for t in tokens:
        if t["address"] == address:
            t["last_score"] = new_score
            t["last_verdict"] = new_verdict
            t["next_check_ts"] = time.time() + config.CONFIRMED_RECHECK_INTERVAL_SEC
            t["skip_streak"] = skip_streak
            break
    _json_save(tokens)


def _json_count():
    return len(_json_load())
