"""
Точка входа. Запускает ПЯТЬ независимых цикла:

1. scan_new_tokens_loop()      — ищет новые токены, считает Risk Score.
                                  При вердикте Strong — сразу шлёт алерт
                                  в Telegram и добавляет токен в список
                                  "подтверждённых" для 12ч-проверок.
2. scan_established_loop()     — оценивает токены из watchlist
                                  по Fundamentals Score.
3. hourly_report_loop()        — раз в час шлёт в Telegram сводку:
                                  сколько проверено / рагпул / рекомендовано.
4. confirmed_recheck_loop()    — раз в 15 минут проверяет, не пора ли
                                  кому-то из "подтверждённых" токенов
                                  проходить очередную 12-часовую проверку;
                                  если фундаментал заметно изменился —
                                  шлёт алерт "растёт" / "падает".
5. telegram_listener_loop()    — слушает входящие сообщения в Telegram.
                                  Если прислать адрес токена (mint) —
                                  бот сам добавит его в watchlist и
                                  подтвердит добавление в ответ.

Хранение (watchlist, confirmed_tokens, snapshots) — через Postgres, если
она подключена в Railway, иначе — через локальные JSON-файлы.

При старте бот шлёт в Telegram сообщение "бот запущен".
"""

import asyncio
import re
import time

import config
import data_sources as ds
import storage
import confirmed
import watchlist
import telegram
import alerts
import db
from scoring import (
    RiskSignals, FundamentalSignals, ManualFlags,
    score_risk, score_fundamentals,
)

# Базовый формат Solana-адреса (base58, без 0/O/I/l), 32-44 символа
_SOLANA_ADDRESS_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


def is_valid_solana_address(text: str) -> bool:
    return bool(_SOLANA_ADDRESS_RE.match(text.strip()))


# Статистика за текущий час (обнуляется после каждого отчёта)
stats = {"checked": 0, "skip": 0, "watch": 0, "strong": 0}


def _reset_stats():
    stats["checked"] = 0
    stats["skip"] = 0
    stats["watch"] = 0
    stats["strong"] = 0


def _record_verdict(verdict: str):
    stats["checked"] += 1
    if verdict == "Strong":
        stats["strong"] += 1
    elif verdict == "Watch":
        stats["watch"] += 1
    else:
        stats["skip"] += 1


# ---------------------------------------------------------------------------
# Сканер НОВЫХ токенов
# ---------------------------------------------------------------------------

async def scan_new_tokens_loop():
    while True:
        try:
            await scan_new_tokens_once()
        except Exception as e:
            log(f"[new] Ошибка цикла сканирования: {e}")
        await asyncio.sleep(config.NEW_TOKEN_SCAN_INTERVAL_SEC)


async def scan_new_tokens_once():
    log("[new] Ищу новые токены (DexScreener boosted/profiles)...")
    candidates = ds.get_latest_boosted_tokens() or ds.get_latest_token_profiles()

    if not candidates:
        log("[new] Ничего не найдено в этом цикле (или DexScreener недоступен).")
        return

    for item in candidates[:10]:
        token_address = item.get("tokenAddress")
        chain_id = item.get("chainId", "solana")
        if not token_address or chain_id != "solana":
            continue

        pair = ds.get_best_pair_for_token(token_address)
        if not pair:
            continue

        pair_created_ms = pair.get("pairCreatedAt")
        age_minutes = (
            (time.time() * 1000 - pair_created_ms) / 60000 if pair_created_ms else 9999
        )
        if age_minutes > config.NEW_TOKEN_MAX_AGE_MIN:
            continue

        mint_disabled, freeze_disabled = ds.get_mint_and_freeze_authority(token_address)
        socials = (pair.get("info") or {}).get("socials", [])
        websites = (pair.get("info") or {}).get("websites", [])

        signals = RiskSignals(
            address=token_address,
            mint_disabled=bool(mint_disabled),
            freeze_disabled=bool(freeze_disabled),
            lp_clean=True,  # требует отдельной проверки LP-burn/lock — TODO
            has_twitter=any(s.get("type") == "twitter" for s in socials),
            has_telegram=any(s.get("type") == "telegram" for s in socials),
            has_website=len(websites) > 0,
            buyers=(pair.get("txns", {}).get("h24", {}) or {}).get("buys", 0),
            sellers=(pair.get("txns", {}).get("h24", {}) or {}).get("sells", 0),
        )

        result = score_risk(signals)
        _record_verdict(result["verdict"])

        name = (pair.get("baseToken") or {}).get("name", "")
        log(f"[new] {token_address[:8]}... возраст={age_minutes:.0f}мин "
            f"баллы={result['total_score']} -> {result['verdict']}")

        if result["verdict"] == "Strong":
            await telegram.send_message(alerts.new_token_recommendation(result, token_address, name))
            await confirmed.add_token(token_address, name, result["total_score"], result["verdict"])


# ---------------------------------------------------------------------------
# Сканер УСТОЯВШИХСЯ токенов (watchlist)
# ---------------------------------------------------------------------------

async def _score_fundamentals_for_address(address, manual_flags=None):
    """Общая логика: тянет данные и считает Fundamentals Score для адреса."""
    pair = ds.get_best_pair_for_token(address)
    if not pair:
        return None, None

    liquidity_usd = (pair.get("liquidity") or {}).get("usd", 0) or 0
    volume_24h = (pair.get("volume") or {}).get("h24", 0) or 0
    price_usd = float(pair.get("priceUsd", 0) or 0)
    market_cap = pair.get("marketCap", 0) or pair.get("fdv", 0) or 0
    price_change_24h = (pair.get("priceChange") or {}).get("h24", 0) or 0
    txns_24h = (pair.get("txns") or {}).get("h24", {}) or {}

    pair_created_ms = pair.get("pairCreatedAt")
    age_days = (time.time() * 1000 - pair_created_ms) / 86_400_000 if pair_created_ms else 0

    await storage.record_snapshot(address, liquidity_usd, volume_24h, price_usd)
    liquidity_trend = await storage.get_trend(address, "liquidity_usd", lookback_hours=24)
    volume_trend = await storage.get_trend(address, "volume_24h_usd", lookback_hours=24)

    top10_pct = ds.get_top10_concentration(address)
    listed_cg = ds.is_listed_on_coingecko(address)

    socials = (pair.get("info") or {}).get("socials", [])
    websites = (pair.get("info") or {}).get("websites", [])
    all_pairs = ds.get_pairs_for_token("solana", address)

    signals = FundamentalSignals(
        address=address,
        age_days=age_days,
        market_cap_usd=market_cap,
        liquidity_usd=liquidity_usd,
        volume_24h_usd=volume_24h,
        price_change_24h_pct=price_change_24h,
        buys_24h=txns_24h.get("buys", 0),
        sells_24h=txns_24h.get("sells", 0),
        top10_concentration_pct=top10_pct,
        has_twitter=any(s.get("type") == "twitter" for s in socials),
        has_telegram=any(s.get("type") == "telegram" for s in socials),
        has_website=len(websites) > 0,
        num_dex_pairs=len(all_pairs) if all_pairs else 1,
        listed_coingecko=listed_cg,
        liquidity_trend=liquidity_trend,
        volume_trend=volume_trend,
        manual=ManualFlags(**manual_flags) if manual_flags else ManualFlags(),
    )

    result = score_fundamentals(signals)
    return result, (liquidity_trend, volume_trend)


async def scan_established_loop():
    while True:
        try:
            await scan_established_once()
        except Exception as e:
            log(f"[established] Ошибка цикла сканирования: {e}")
        await asyncio.sleep(config.ESTABLISHED_SCAN_INTERVAL_SEC)


async def scan_established_once():
    tokens = await watchlist.load()
    if not tokens:
        log("[established] Список отслеживания пуст — пришлите адрес токена в Telegram, "
            "чтобы добавить, или отредактируйте watchlist.json.")
        return

    log(f"[established] Проверяю {len(tokens)} токен(ов) из списка отслеживания...")

    for entry in tokens:
        address = entry.get("address")
        if not address:
            continue

        result, trends = await _score_fundamentals_for_address(address, entry.get("manual_flags"))
        if not result:
            log(f"[established] {address[:8]}... нет данных на DexScreener, пропуск.")
            continue

        liquidity_trend, volume_trend = trends
        name = entry.get("name", address[:8])
        log(f"[established] {name}: {result['total_score']}/{result['max_possible_score']} "
            f"баллов ({result['score_pct']}%) -> {result['verdict']} "
            f"(тренд ликв.={liquidity_trend}, тренд объёма={volume_trend})")


# ---------------------------------------------------------------------------
# Почасовой отчёт в Telegram
# ---------------------------------------------------------------------------

async def hourly_report_loop():
    while True:
        await asyncio.sleep(config.HOURLY_REPORT_INTERVAL_SEC)
        try:
            report_stats = dict(stats)
            report_stats["confirmed_count"] = await confirmed.count()
            await telegram.send_message(alerts.hourly_report(report_stats))
            log(f"[report] Отправлен почасовой отчёт: {report_stats}")
        except Exception as e:
            log(f"[report] Ошибка отправки отчёта: {e}")
        finally:
            _reset_stats()


# ---------------------------------------------------------------------------
# 12-часовая перепроверка "подтверждённых" токенов
# ---------------------------------------------------------------------------

async def confirmed_recheck_loop():
    while True:
        try:
            await confirmed_recheck_once()
        except Exception as e:
            log(f"[confirmed] Ошибка цикла перепроверки: {e}")
        await asyncio.sleep(config.CONFIRMED_POLL_INTERVAL_SEC)


async def confirmed_recheck_once():
    due = await confirmed.get_due_tokens()
    if not due:
        return

    log(f"[confirmed] Пора перепроверить {len(due)} токен(ов) (12ч-цикл)...")

    for entry in due:
        address = entry["address"]
        name = entry.get("name", address[:8])

        result, trends = await _score_fundamentals_for_address(address)
        if not result:
            log(f"[confirmed] {name}: нет данных, откладываю проверку.")
            await confirmed.update_after_check(address, entry["last_score"], entry["last_verdict"])
            continue

        liquidity_trend, volume_trend = trends
        old_score = entry["last_score"]
        new_score = result["total_score"]
        delta = abs(new_score - old_score)

        log(f"[confirmed] {name}: {old_score} -> {new_score} ({result['score_pct']}%) "
            f"({result['verdict']}, тренд ликв.={liquidity_trend}, тренд объёма={volume_trend})")

        if delta >= config.CONFIRMED_SCORE_CHANGE_THRESHOLD or entry["last_verdict"] != result["verdict"]:
            await telegram.send_message(alerts.fundamentals_change_alert(
                name, address, old_score, new_score,
                entry["last_verdict"], result["verdict"],
                liquidity_trend, volume_trend, result["breakdown"],
                score_pct=result["score_pct"],
            ))

        await confirmed.update_after_check(address, new_score, result["verdict"])


# ---------------------------------------------------------------------------
# Приём сообщений в Telegram — добавление токена в watchlist по адресу
# ---------------------------------------------------------------------------

def _extract_chat_and_text(update: dict):
    msg = update.get("message") or update.get("channel_post")
    if not msg:
        return None, None
    chat_id = str(msg.get("chat", {}).get("id", ""))
    text = (msg.get("text") or "").strip()
    return chat_id, text


async def handle_incoming_message(update: dict):
    chat_id, text = _extract_chat_and_text(update)
    if not chat_id or not text:
        return

    # Отвечаем только владельцу — тому chat_id, что указан в config.py.
    # Это защита от случайных/чужих сообщений, если бот когда-то станет
    # доступен в общем чате.
    if chat_id != str(config.TELEGRAM_CHAT_ID):
        log(f"[telegram] Игнорирую сообщение от постороннего chat_id={chat_id}")
        return

    if text.startswith("/"):
        return  # игнорируем команды типа /start

    if not is_valid_solana_address(text):
        await telegram.send_message(
            f"⚠️ Не похоже на адрес токена Solana:\n<code>{text}</code>"
        )
        return

    if await watchlist.exists(text):
        await telegram.send_message(f"ℹ️ Токен <code>{text}</code> уже в списке отслеживания.")
        return

    pair = ds.get_best_pair_for_token(text)
    name = (pair.get("baseToken") or {}).get("name", "") if pair else ""

    await watchlist.add(text, name)

    if pair:
        interval_min = config.ESTABLISHED_SCAN_INTERVAL_SEC // 60
        await telegram.send_message(
            f"✅ Добавлено в отслеживание: <b>{name or text[:8]}</b>\n"
            f"<code>{text}</code>\n"
            f"Буду проверять фундаментал каждые {interval_min} мин."
        )
    else:
        await telegram.send_message(
            f"⚠️ Токен <code>{text}</code> добавлен в список, но данных на "
            f"DexScreener пока нет (возможно, ещё нет ликвидности/пары). "
            f"Проверю автоматически в следующих циклах."
        )


async def telegram_listener_loop():
    offset = None
    while True:
        try:
            updates = await telegram.get_updates(offset)
            for update in updates:
                offset = update["update_id"] + 1
                await handle_incoming_message(update)
        except Exception as e:
            log(f"[telegram-listener] Ошибка: {e}")
            await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# Запуск
# ---------------------------------------------------------------------------

async def main():
    print("=" * 60)
    print("Solana Alpha Bot v5 — new + established + Telegram (алерты и приём адресов)")
    print(f"Устоявшимся считается токен старше {config.ESTABLISHED_MIN_AGE_DAYS} дн.")
    print("Хранилище:", "Postgres" if db.is_configured() else "локальные JSON-файлы")
    print("=" * 60)

    await db.init_schema()
    await telegram.send_message(alerts.startup_alert())

    await asyncio.gather(
        scan_new_tokens_loop(),
        scan_established_loop(),
        hourly_report_loop(),
        confirmed_recheck_loop(),
        telegram_listener_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
