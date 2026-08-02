"""
Кэш результата проверки на бандл при запуске токена (Bitquery).

Это исторический факт о том, что происходило в первые секунды жизни
токена — он не меняется со временем, поэтому проверяется ОДИН РАЗ и
кэшируется навсегда (как и адрес кошелька дева).

Эвристика (не идеальный детектор, а разумное приближение):
  - Если уникальных кошельков-покупателей в первые
    config.BITQUERY_BUNDLE_WINDOW_SEC секунд МЕНЬШЕ
    config.BITQUERY_BUNDLE_SUSPICIOUS_MAX_BUYERS — похоже на бандл (True).
  - Если БОЛЬШЕ ИЛИ РАВНО config.BITQUERY_BUNDLE_ORGANIC_MIN_BUYERS —
    похоже на органический интерес (False).
  - Между этими порогами, или если данных нет — неизвестно (None),
    фактор просто не участвует в скоринге.

Postgres (если подключена) — таблица bundle_checks. Иначе — JSON-файл.
"""

import json
import os

import config
import data_sources as ds


async def get_bundle_status(token_address, pool_created_ts):
    """Возвращает True (похоже на бандл) / False (похоже на органику) / None."""
    cached = await _get_cached(token_address)
    if cached is not None:
        return cached  # уже проверяли — исторический факт не меняется

    if not config.BITQUERY_ACCESS_TOKEN or not pool_created_ts:
        return None

    stats = ds.get_early_buyers(
        token_address, pool_created_ts, window_sec=config.BITQUERY_BUNDLE_WINDOW_SEC
    )
    if not stats:
        return None  # не удалось получить данные — не кэшируем, попробуем в другой раз

    unique_buyers = stats["unique_buyers"]
    if unique_buyers < config.BITQUERY_BUNDLE_SUSPICIOUS_MAX_BUYERS:
        result = True
    elif unique_buyers >= config.BITQUERY_BUNDLE_ORGANIC_MIN_BUYERS:
        result = False
    else:
        result = None  # серая зона — не уверены, не кэшируем как окончательное

    if result is not None:
        await _save_cached(token_address, result)
    return result


# ---------------------------------------------------------------------------
# Postgres / JSON-фолбэк
# ---------------------------------------------------------------------------

async def _get_cached(token_address):
    import db
    if db.is_configured():
        row = await db.run(
            "SELECT bundle_detected FROM bundle_checks WHERE address = %s",
            (token_address,), fetchone=True,
        )
        return row[0] if row else None
    else:
        data = _json_load()
        return data.get(token_address)


async def _save_cached(token_address, bundle_detected):
    import db
    if db.is_configured():
        await db.run(
            "INSERT INTO bundle_checks (address, bundle_detected) VALUES (%s, %s) "
            "ON CONFLICT (address) DO UPDATE SET bundle_detected = EXCLUDED.bundle_detected",
            (token_address, bundle_detected),
        )
    else:
        data = _json_load()
        data[token_address] = bundle_detected
        _json_save(data)


def _json_load():
    if not os.path.exists(config.BUNDLE_CHECKS_FILE):
        return {}
    try:
        with open(config.BUNDLE_CHECKS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _json_save(data):
    with open(config.BUNDLE_CHECKS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
