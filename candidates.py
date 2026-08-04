"""
Отслеживание кандидатов через жизненный цикл:

  pending (только обнаружен, младше 2ч)
     |
     v  (как только исполнилось 2ч — считаем Fundamentals Score)
  прошёл порог? --нет--> rejected (навсегда, больше не сканируется)
     |да
     v
  observed (проверяется каждые 2ч)
     |
     v  после 3-го и 6-го скана проверяем улучшение относительно
        баллов на момент попадания в observed
  улучшение есть? --да--> recommended (алерт, дальше передаётся в
                            обычный 12ч-мониторинг confirmed.py)
     |нет (и это уже 6-й скан)
     v
  rejected (навсегда)

Как и в других модулях, работает через Postgres, если она подключена,
иначе — через локальный JSON-файл. Адрес, попавший в эту таблицу в ЛЮБОМ
статусе (включая rejected), больше никогда не добавляется повторно —
это и есть "навсегда отсеян".
"""

import json
import os
import time

import config
import db


# ---------------------------------------------------------------------------
# Публичные функции — асинхронные
# ---------------------------------------------------------------------------

async def exists(address) -> bool:
    """Есть ли этот адрес уже в таблице кандидатов (в любом статусе)."""
    if db.is_configured():
        row = await db.run(
            f"SELECT 1 FROM {config.CANDIDATES_TABLE} WHERE address = %s",
            (address,), fetchone=True,
        )
        return row is not None
    else:
        return address in _json_load()


async def add_pending(address, name, discovered_ts):
    if db.is_configured():
        existing = await db.run(
            f"SELECT 1 FROM {config.CANDIDATES_TABLE} WHERE address = %s",
            (address,), fetchone=True,
        )
        if existing:
            return
        await db.run(
            f"INSERT INTO {config.CANDIDATES_TABLE} "
            "(address, name, status, discovered_ts, baseline_score_pct, "
            "last_score_pct, scan_count, next_scan_ts) "
            "VALUES (%s, %s, 'pending', %s, NULL, NULL, 0, NULL)",
            (address, name, discovered_ts),
        )
    else:
        _json_add_pending(address, name, discovered_ts)


async def get_pending_ready():
    """Возвращает pending-кандидатов, которым уже исполнилось 2ч."""
    cutoff_ts = time.time() - config.CANDIDATE_MIN_AGE_HOURS * 3600
    if db.is_configured():
        rows = await db.run(
            f"SELECT address, name, discovered_ts FROM {config.CANDIDATES_TABLE} "
            "WHERE status = 'pending' AND discovered_ts <= %s",
            (cutoff_ts,), fetch=True,
        )
        rows = rows or []
        return [{"address": r[0], "name": r[1], "discovered_ts": r[2]} for r in rows]
    else:
        data = _json_load()
        return [
            {"address": addr, **entry}
            for addr, entry in data.items()
            if entry["status"] == "pending" and entry["discovered_ts"] <= cutoff_ts
        ]


async def promote_to_observed(address, baseline_score_pct):
    next_scan_ts = time.time() + config.CANDIDATE_RECHECK_INTERVAL_SEC
    if db.is_configured():
        await db.run(
            f"UPDATE {config.CANDIDATES_TABLE} SET status = 'observed', "
            "baseline_score_pct = %s, last_score_pct = %s, scan_count = 0, "
            "next_scan_ts = %s WHERE address = %s",
            (baseline_score_pct, baseline_score_pct, next_scan_ts, address),
        )
    else:
        data = _json_load()
        if address in data:
            data[address].update({
                "status": "observed",
                "baseline_score_pct": baseline_score_pct,
                "last_score_pct": baseline_score_pct,
                "scan_count": 0,
                "next_scan_ts": next_scan_ts,
            })
            _json_save(data)


async def reject(address):
    if db.is_configured():
        await db.run(
            f"UPDATE {config.CANDIDATES_TABLE} SET status = 'rejected' WHERE address = %s",
            (address,),
        )
    else:
        data = _json_load()
        if address in data:
            data[address]["status"] = "rejected"
            _json_save(data)


async def mark_recommended(address):
    if db.is_configured():
        await db.run(
            f"UPDATE {config.CANDIDATES_TABLE} SET status = 'recommended' WHERE address = %s",
            (address,),
        )
    else:
        data = _json_load()
        if address in data:
            data[address]["status"] = "recommended"
            _json_save(data)


async def get_all_observed():
    """Все токены в статусе 'observed' прямо сейчас (для команды /observed
    в Telegram) — без фильтра по времени следующей проверки."""
    if db.is_configured():
        rows = await db.run(
            f"SELECT address, name, baseline_score_pct, last_score_pct, "
            f"scan_count, next_scan_ts FROM {config.CANDIDATES_TABLE} "
            "WHERE status = 'observed' ORDER BY next_scan_ts ASC",
            fetch=True,
        )
        rows = rows or []
        return [
            {"address": r[0], "name": r[1], "baseline_score_pct": r[2],
             "last_score_pct": r[3], "scan_count": r[4], "next_scan_ts": r[5]}
            for r in rows
        ]
    else:
        data = _json_load()
        entries = [
            {"address": addr, **entry}
            for addr, entry in data.items()
            if entry["status"] == "observed"
        ]
        return sorted(entries, key=lambda e: e.get("next_scan_ts") or 0)


async def get_due_observed():
    if db.is_configured():
        rows = await db.run(
            f"SELECT address, name, baseline_score_pct, last_score_pct, scan_count "
            f"FROM {config.CANDIDATES_TABLE} "
            "WHERE status = 'observed' AND next_scan_ts <= %s",
            (time.time(),), fetch=True,
        )
        rows = rows or []
        return [
            {"address": r[0], "name": r[1], "baseline_score_pct": r[2],
             "last_score_pct": r[3], "scan_count": r[4]}
            for r in rows
        ]
    else:
        data = _json_load()
        now = time.time()
        return [
            {"address": addr, **entry}
            for addr, entry in data.items()
            if entry["status"] == "observed" and entry.get("next_scan_ts", 0) <= now
        ]


async def update_observed(address, new_score_pct, new_scan_count):
    next_scan_ts = time.time() + config.CANDIDATE_RECHECK_INTERVAL_SEC
    if db.is_configured():
        await db.run(
            f"UPDATE {config.CANDIDATES_TABLE} SET last_score_pct = %s, "
            "scan_count = %s, next_scan_ts = %s WHERE address = %s",
            (new_score_pct, new_scan_count, next_scan_ts, address),
        )
    else:
        data = _json_load()
        if address in data:
            data[address].update({
                "last_score_pct": new_score_pct,
                "scan_count": new_scan_count,
                "next_scan_ts": next_scan_ts,
            })
            _json_save(data)


async def counts_by_status():
    """Для статистики/отладки: сколько кандидатов в каждом статусе."""
    if db.is_configured():
        rows = await db.run(
            f"SELECT status, COUNT(*) FROM {config.CANDIDATES_TABLE} GROUP BY status",
            fetch=True,
        )
        return {r[0]: r[1] for r in (rows or [])}
    else:
        data = _json_load()
        result = {}
        for entry in data.values():
            result[entry["status"]] = result.get(entry["status"], 0) + 1
        return result


# ---------------------------------------------------------------------------
# Резервная реализация через JSON-файл
# ---------------------------------------------------------------------------

def _json_load():
    if not os.path.exists(config.CANDIDATES_FILE):
        return {}
    try:
        with open(config.CANDIDATES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _json_save(data):
    with open(config.CANDIDATES_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _json_add_pending(address, name, discovered_ts):
    data = _json_load()
    if address in data:
        return
    data[address] = {
        "name": name,
        "status": "pending",
        "discovered_ts": discovered_ts,
        "baseline_score_pct": None,
        "last_score_pct": None,
        "scan_count": 0,
        "next_scan_ts": None,
    }
    _json_save(data)
