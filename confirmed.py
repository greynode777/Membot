"""
Список "подтверждённых" токенов — тех, что получили вердикт Strong
от сканера новых токенов. Они автоматически ставятся на отдельную
проверку каждые 12 часов (растёт токен фундаментально или падает).

Хранится в JSON-файле рядом с ботом (как и snapshots.json — обнуляется
при редеплое на Railway, если не подключить постоянное хранилище).
"""

import json
import os
import time
import config


def _load():
    if not os.path.exists(config.CONFIRMED_TOKENS_FILE):
        return []
    try:
        with open(config.CONFIRMED_TOKENS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return []


def _save(data):
    with open(config.CONFIRMED_TOKENS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def add_token(address, name, initial_score, initial_verdict, source="new_token_scanner"):
    """Добавляет токен в список, если его там ещё нет."""
    tokens = _load()
    if any(t["address"] == address for t in tokens):
        return  # уже отслеживается

    tokens.append({
        "address": address,
        "name": name,
        "source": source,
        "added_ts": time.time(),
        "last_score": initial_score,
        "last_verdict": initial_verdict,
        "next_check_ts": time.time() + config.CONFIRMED_RECHECK_INTERVAL_SEC,
    })
    _save(tokens)


def get_due_tokens():
    """Возвращает токены, для которых пора делать очередную 12-часовую проверку."""
    tokens = _load()
    now = time.time()
    return [t for t in tokens if t["next_check_ts"] <= now]


def update_after_check(address, new_score, new_verdict):
    tokens = _load()
    for t in tokens:
        if t["address"] == address:
            t["last_score"] = new_score
            t["last_verdict"] = new_verdict
            t["next_check_ts"] = time.time() + config.CONFIRMED_RECHECK_INTERVAL_SEC
            break
    _save(tokens)


def count():
    return len(_load())
