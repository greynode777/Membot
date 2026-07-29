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


def _post_request(text: str):
    """Синхронный HTTP-запрос к Telegram. Возвращает распарсенный JSON
    или None при сетевой ошибке."""
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": config.TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        return resp.json()
    except Exception as e:
        print(f"[telegram] Сетевая ошибка при отправке: {e}")
        return None


async def send_message(text: str) -> bool:
    """
    Отправляет сообщение и ждёт подтверждения доставки от Telegram API.
    Если не пришло — повторяет каждые TELEGRAM_RETRY_INTERVAL_SEC секунд,
    пока не получит успешный ответ (ok: true).
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("[telegram] Токен/chat_id не настроены — сообщение только в лог:")
        print(text)
        return True  # нечего повторять — Telegram просто не настроен

    attempt = 1
    while True:
        result = await asyncio.to_thread(_post_request, text)

        if result is not None and result.get("ok") is True:
            if attempt > 1:
                print(f"[telegram] Сообщение доставлено (попытка {attempt}).")
            return True

        error_desc = (result or {}).get("description", "нет ответа от сервера")
        print(f"[telegram] Доставка НЕ подтверждена (попытка {attempt}): {error_desc}")
        print(f"[telegram] Повтор через {config.TELEGRAM_RETRY_INTERVAL_SEC} сек...")

        attempt += 1
        await asyncio.sleep(config.TELEGRAM_RETRY_INTERVAL_SEC)
