"""
Простое хранилище снимков показателей токена во времени.

Зачем это нужно: бесплатные API не дают исторические данные напрямую
(например, "какая была ликвидность 3 дня назад"). Поэтому бот сам,
при каждом цикле опроса, сохраняет снимок (ликвидность, объём, holders)
в JSON-файл. Со временем накапливается история, и можно считать тренды
(растёт/падает).

ВАЖНО: на Railway файловая система эфемерна — при пересборке/редеплое
контейнера файл snapshots.json обнуляется. Для долгосрочного хранения
истории (недели/месяцы) в будущем стоит подключить Railway Volume или
простую базу данных (например, Postgres — Railway предлагает её в один клик).
Пока для целей трендов за несколько дней JSON-файла достаточно.
"""

import json
import os
import time
import config


def _load():
    if not os.path.exists(config.SNAPSHOTS_FILE):
        return {}
    try:
        with open(config.SNAPSHOTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save(data):
    with open(config.SNAPSHOTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def record_snapshot(token_address, liquidity_usd, volume_24h_usd, price_usd):
    """Добавляет новый снимок показателей токена с текущей меткой времени."""
    data = _load()
    history = data.get(token_address, [])
    history.append({
        "ts": time.time(),
        "liquidity_usd": liquidity_usd,
        "volume_24h_usd": volume_24h_usd,
        "price_usd": price_usd,
    })
    # Храним не более 500 последних точек на токен, чтобы файл не разрастался
    data[token_address] = history[-500:]
    _save(data)


def get_history(token_address):
    return _load().get(token_address, [])


def get_trend(token_address, field, lookback_hours=24):
    """
    Сравнивает текущее (последнее) значение поля со значением
    ближайшим к lookback_hours назад.
    Возвращает 'up', 'down', 'flat' или None (если истории пока недостаточно).
    """
    history = get_history(token_address)
    if len(history) < 2:
        return None

    now = time.time()
    target_ts = now - lookback_hours * 3600

    # Находим точку, ближайшую к целевому времени в прошлом
    past_points = [p for p in history if p["ts"] <= target_ts]
    if not past_points:
        return None  # ещё не накопили историю на нужную глубину

    past_value = past_points[-1].get(field)
    current_value = history[-1].get(field)

    if past_value is None or current_value is None or past_value == 0:
        return None

    change_pct = (current_value - past_value) / past_value * 100

    if change_pct > 5:
        return "up"
    elif change_pct < -5:
        return "down"
    else:
        return "flat"
