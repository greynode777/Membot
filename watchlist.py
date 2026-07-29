"""
Список "устоявшихся" токенов для отслеживания фундаментала.

Раньше редактировался вручную в watchlist.json. Теперь можно также
просто отправить адрес токена в Telegram-чат боту — он добавится сюда
автоматически (см. main.py -> handle_incoming_message).

Postgres (если подключена) — таблица watchlist, переживает редеплой.
Иначе — локальный JSON-файл watchlist.json (формат как раньше).
"""

import json
import os

import config
import db

_DEFAULT_MANUAL_FLAGS = {
    "audited": False,
    "team_doxxed": False,
    "active_development": False,
    "upcoming_unlock_risk": False,
}


# ---------------------------------------------------------------------------
# Публичные функции — асинхронные
# ---------------------------------------------------------------------------

async def load():
    if db.is_configured():
        rows = await db.run("SELECT address, name, manual_flags FROM watchlist", fetch=True)
        rows = rows or []
        return [{"address": r[0], "name": r[1], "manual_flags": r[2] or _DEFAULT_MANUAL_FLAGS} for r in rows]
    else:
        return _json_load_tokens()


async def exists(address) -> bool:
    if db.is_configured():
        row = await db.run("SELECT 1 FROM watchlist WHERE address = %s", (address,), fetchone=True)
        return row is not None
    else:
        return any(t.get("address") == address for t in _json_load_tokens())


async def add(address, name="", manual_flags=None) -> bool:
    """Возвращает True, если токен добавлен; False, если уже был в списке."""
    manual_flags = manual_flags or _DEFAULT_MANUAL_FLAGS
    display_name = name or address[:8]

    if db.is_configured():
        existing = await db.run("SELECT 1 FROM watchlist WHERE address = %s", (address,), fetchone=True)
        if existing:
            return False
        import json as _json
        await db.run(
            "INSERT INTO watchlist (address, name, manual_flags) VALUES (%s, %s, %s)",
            (address, display_name, _json.dumps(manual_flags)),
        )
        return True
    else:
        return _json_add_token(address, display_name, manual_flags)


async def remove(address) -> bool:
    if db.is_configured():
        existing = await db.run("SELECT 1 FROM watchlist WHERE address = %s", (address,), fetchone=True)
        if not existing:
            return False
        await db.run("DELETE FROM watchlist WHERE address = %s", (address,))
        return True
    else:
        return _json_remove_token(address)


# ---------------------------------------------------------------------------
# Резервная реализация через JSON-файл (тот же формат watchlist.json,
# что использовался в предыдущих версиях бота)
# ---------------------------------------------------------------------------

def _json_load_raw():
    if not os.path.exists(config.WATCHLIST_FILE):
        return {"tokens": []}
    try:
        with open(config.WATCHLIST_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"tokens": []}


def _json_save_raw(data):
    with open(config.WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _json_load_tokens():
    return _json_load_raw().get("tokens", [])


def _json_add_token(address, name, manual_flags):
    data = _json_load_raw()
    tokens = data.get("tokens", [])
    if any(t.get("address") == address for t in tokens):
        return False
    tokens.append({"address": address, "name": name, "manual_flags": manual_flags})
    data["tokens"] = tokens
    _json_save_raw(data)
    return True


def _json_remove_token(address):
    data = _json_load_raw()
    tokens = data.get("tokens", [])
    new_tokens = [t for t in tokens if t.get("address") != address]
    changed = len(new_tokens) != len(tokens)
    data["tokens"] = new_tokens
    _json_save_raw(data)
    return changed
