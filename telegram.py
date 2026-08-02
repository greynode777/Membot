"""
Отправка сообщений в Telegram с ПОДТВЕРЖДЕНИЕМ доставки.

Как это работает:
- Отправляем запрос к Telegram Bot API.
- Telegram в ответ присылает {"ok": true, ...} — это и есть подтверждение,
  что сообщение реально дошло и было принято сервером Telegram.
- Если ok=false, произошла ошибка сети, таймаут — считаем, что сообщение
  НЕ доставлено, ждём config.TELEGRAM_RETRY_INTERVAL_SEC секунд и
  пробуем снова. Повторяет бесконечно, пока не получит подтверждение.

Важно: функция асинхронная (async def) и ждёт через asyncio.sleep,
а не time.sleep — поэтому пока идут повторные попытки, остальные части
бота (сканирование новых токенов, watchlist и т.д.) продолжают
работать параллельно, ничего не блокируется.
"""

import asyncio
import time
import requests
import config


def _post_request(text: str, reply_markup=None):
    """Синхронный HTTP-запрос к Telegram. Возвращает распарсенный JSON
    или None при сетевой ошибке."""
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": config.TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        resp = requests.post(url, json=payload, timeout=10)
        return resp.json()
    except Exception as e:
        print(f"[telegram] Сетевая ошибка при отправке: {e}")
        return None


async def send_message(text: str, reply_markup=None) -> bool:
    """
    Отправляет сообщение и ждёт подтверждения доставки от Telegram API.
    Если не пришло — повторяет каждые TELEGRAM_RETRY_INTERVAL_SEC секунд,
    пока не получит успешный ответ (ok: true).

    reply_markup — опционально, словарь с inline-кнопками, например:
    {"inline_keyboard": [[{"text": "Кнопка", "callback_data": "action:data"}]]}
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("[telegram] Токен/chat_id не настроены — сообщение только в лог:")
        print(text)
        return True  # нечего повторять — Telegram просто не настроен

    attempt = 1
    while True:
        result = await asyncio.to_thread(_post_request, text, reply_markup)

        if result is not None and result.get("ok") is True:
            if attempt > 1:
                print(f"[telegram] Сообщение доставлено (попытка {attempt}).")
            return True

        error_desc = (result or {}).get("description", "нет ответа от сервера")
        print(f"[telegram] Доставка НЕ подтверждена (попытка {attempt}): {error_desc}")
        print(f"[telegram] Повтор через {config.TELEGRAM_RETRY_INTERVAL_SEC} сек...")

        attempt += 1
        await asyncio.sleep(config.TELEGRAM_RETRY_INTERVAL_SEC)


# ---------------------------------------------------------------------------
# Редактирование существующих сообщений (для интерактивных карточек —
# нажатие кнопки меняет то же самое сообщение, не создаёт новое)
# ---------------------------------------------------------------------------

def _edit_request(chat_id, message_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        resp = requests.post(url, json=payload, timeout=10)
        return resp.json()
    except Exception as e:
        print(f"[telegram] Сетевая ошибка при редактировании: {e}")
        return None


async def edit_message(chat_id, message_id, text, reply_markup=None) -> bool:
    """
    Редактирует существующее сообщение. В отличие от send_message,
    делает всего 2 попытки — если пользователь нажал кнопку, он ждёт
    ответа сразу, тут не место для минутных повторов.
    """
    for attempt in range(2):
        result = await asyncio.to_thread(_edit_request, chat_id, message_id, text, reply_markup)
        if result is not None and result.get("ok") is True:
            return True
        if attempt == 0:
            await asyncio.sleep(1)
    print(f"[telegram] Не удалось отредактировать сообщение {message_id}")
    return False


def _answer_callback_request(callback_query_id, text=None):
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    try:
        resp = requests.post(url, json=payload, timeout=10)
        return resp.json()
    except Exception as e:
        print(f"[telegram] Ошибка answerCallbackQuery: {e}")
        return None


async def answer_callback_query(callback_query_id, text=None):
    """
    Подтверждает нажатие кнопки — убирает "часики" на кнопке у
    пользователя. text (опционально) — всплывающая подсказка.
    Не критично, если не удалось — просто лог, без повторов.
    """
    await asyncio.to_thread(_answer_callback_request, callback_query_id, text)


# ---------------------------------------------------------------------------
# Получение входящих сообщений (чтобы можно было прислать боту адрес
# токена прямо в чат, а не редактировать файл вручную)
# ---------------------------------------------------------------------------

def _get_updates_request(offset):
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/getUpdates"
    # timeout=30 — это long polling: Telegram сам держит соединение открытым
    # до 30 секунд и отвечает сразу, как только придёт новое сообщение.
    # Это экономит запросы по сравнению с частым обычным опросом.
    params = {"timeout": 30}
    if offset is not None:
        params["offset"] = offset
    try:
        resp = requests.get(url, params=params, timeout=35)
        return resp.json()
    except Exception as e:
        print(f"[telegram] Ошибка получения входящих сообщений: {e}")
        return None


async def get_updates(offset=None):
    """Возвращает список новых обновлений (сообщений) от Telegram."""
    if not config.TELEGRAM_BOT_TOKEN:
        return []
    result = await asyncio.to_thread(_get_updates_request, offset)
    if not result or not result.get("ok"):
        return []
    return result.get("result", [])
