"""
Точка входа. Запускает СЕМЬ независимых циклов:

1. candidate_discovery_loop()  — ищет новые токены на DexScreener и
                                  регистрирует их как "ожидающих" (pending),
                                  пока им не исполнится 2 часа.
2. candidate_pending_loop()    — как только кандидату исполняется 2ч,
                                  считает Fundamentals Score. Прошёл порог —
                                  переводит в "наблюдаемые". Не прошёл —
                                  отсеивает НАВСЕГДА.
3. candidate_observed_loop()   — раз в 2ч перепроверяет "наблюдаемые".
                                  После 3-го и 6-го скана проверяет,
                                  улучшился ли фундамент относительно
                                  момента постановки на наблюдение:
                                  улучшился — алерт-рекомендация и переход
                                  в 12ч-мониторинг; нет (после 6-го) —
                                  отсеивается НАВСЕГДА.
4. scan_established_loop()     — оценивает токены из watchlist
                                  по Fundamentals Score.
5. hourly_report_loop()        — раз в час шлёт в Telegram сводку.
6. confirmed_recheck_loop()    — раз в 15 минут проверяет 12ч-мониторинг;
                                  если фундаментал заметно изменился —
                                  шлёт алерт "растёт" / "падает".
7. telegram_listener_loop()    — слушает входящие сообщения в Telegram.
                                  Если прислать адрес токена (mint) —
                                  бот сам добавит его в watchlist и
                                  подтвердит добавление в ответ.

Хранение (watchlist, candidates, confirmed_tokens, snapshots) — через
Postgres, если она подключена в Railway, иначе — через локальные
JSON-файлы.

При старте бот шлёт в Telegram сообщение "бот запущен".
"""

import asyncio
import re
import time

import config
import data_sources as ds
import storage
import confirmed
import candidates
import watchlist
import dev_wallets
import bundle_check
import telegram
import alerts
import db
from scoring import FundamentalSignals, ManualFlags, score_fundamentals

# Базовый формат Solana-адреса (base58, без 0/O/I/l), 32-44 символа
_SOLANA_ADDRESS_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


def is_valid_solana_address(text: str) -> bool:
    return bool(_SOLANA_ADDRESS_RE.match(text.strip()))


# Статистика за текущий час (обнуляется после каждого отчёта)
stats = {"promoted": 0, "rejected": 0, "recommended": 0}


def _reset_stats():
    stats["promoted"] = 0
    stats["rejected"] = 0
    stats["recommended"] = 0


# ---------------------------------------------------------------------------
# Сканер НОВЫХ токенов
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Этап 1: Обнаружение кандидатов (просто регистрируем адрес и дату
# создания, оценивать пока рано — токену ещё нет 2 часов)
# ---------------------------------------------------------------------------

async def candidate_discovery_loop():
    while True:
        try:
            await candidate_discovery_once()
        except Exception as e:
            log(f"[discovery] Ошибка цикла обнаружения: {e}")
        await asyncio.sleep(config.CANDIDATE_DISCOVERY_INTERVAL_SEC)


async def candidate_discovery_once():
    log("[discovery] Ищу новых кандидатов (DexScreener boosted/profiles)...")
    items = ds.get_latest_boosted_tokens() or ds.get_latest_token_profiles()
    if not items:
        log("[discovery] Ничего не найдено в этом цикле (или DexScreener недоступен).")
        return

    added = 0
    for item in items[:20]:
        token_address = item.get("tokenAddress")
        chain_id = item.get("chainId", "solana")
        if not token_address or chain_id != "solana":
            continue

        if await candidates.exists(token_address):
            continue  # уже видели этот адрес (в любом статусе) — не трогаем повторно

        pair = ds.get_best_pair_for_token(token_address)
        if not pair:
            continue

        pair_created_ms = pair.get("pairCreatedAt")
        discovered_ts = pair_created_ms / 1000 if pair_created_ms else time.time()
        name = (pair.get("baseToken") or {}).get("name", "")

        await candidates.add_pending(token_address, name, discovered_ts)
        added += 1
        await asyncio.sleep(1.5)  # пауза между запросами к DexScreener

    log(f"[discovery] Добавлено новых кандидатов в ожидание: {added}")


# ---------------------------------------------------------------------------
# Этап 2: Кандидаты, которым уже исполнилось 2 часа — первичный фильтр
# ---------------------------------------------------------------------------

async def candidate_pending_loop():
    while True:
        try:
            await candidate_pending_once()
        except Exception as e:
            log(f"[pending] Ошибка цикла первичного фильтра: {e}")
        await asyncio.sleep(config.CANDIDATE_PENDING_CHECK_INTERVAL_SEC)


async def candidate_pending_once():
    ready = await candidates.get_pending_ready()
    if not ready:
        return

    log(f"[pending] {len(ready)} кандидат(ов) достигли возраста "
        f"{config.CANDIDATE_MIN_AGE_HOURS}ч — считаю Fundamentals Score...")

    for entry in ready:
        address = entry["address"]
        name = entry.get("name", address[:8])

        result, _trends = await _score_fundamentals_for_address(address)
        if not result:
            log(f"[pending] {name}: нет данных на DexScreener, пропуск в этом цикле.")
            continue  # попробуем ещё раз в следующем цикле, не отсеиваем сразу

        if result["score_pct"] >= config.CANDIDATE_INITIAL_THRESHOLD_PCT:
            await candidates.promote_to_observed(address, result["score_pct"])
            stats["promoted"] += 1
            log(f"[pending] {name}: {result['score_pct']}% -> ПРОШЁЛ фильтр, "
                f"переведён в наблюдаемые.")
        else:
            await candidates.reject(address)
            stats["rejected"] += 1
            log(f"[pending] {name}: {result['score_pct']}% -> отсеян навсегда "
                f"(порог {config.CANDIDATE_INITIAL_THRESHOLD_PCT}%).")

        await asyncio.sleep(1.5)


# ---------------------------------------------------------------------------
# Этап 3: "Наблюдаемые" — проверка каждые 2ч, решение на 3-м и 6-м скане
# ---------------------------------------------------------------------------

async def candidate_observed_loop():
    while True:
        try:
            await candidate_observed_once()
        except Exception as e:
            log(f"[observed] Ошибка цикла наблюдения: {e}")
        await asyncio.sleep(config.CANDIDATE_PENDING_CHECK_INTERVAL_SEC)


async def candidate_observed_once():
    due = await candidates.get_due_observed()
    if not due:
        return

    log(f"[observed] Пора перепроверить {len(due)} наблюдаемых токен(ов)...")

    for entry in due:
        address = entry["address"]
        name = entry.get("name", address[:8])
        baseline_pct = entry["baseline_score_pct"]
        scan_count = entry["scan_count"] + 1

        result, _trends = await _score_fundamentals_for_address(address)
        if not result:
            log(f"[observed] {name}: нет данных на DexScreener, откладываю проверку.")
            await candidates.update_observed(address, entry["last_score_pct"], entry["scan_count"])
            await asyncio.sleep(1.5)
            continue

        current_pct = result["score_pct"]
        improved = (current_pct - baseline_pct) >= config.CANDIDATE_IMPROVEMENT_THRESHOLD_PCT

        log(f"[observed] {name}: скан #{scan_count}, {baseline_pct}% -> {current_pct}% "
            f"(улучшение={'да' if improved else 'нет'})")

        is_checkpoint = scan_count in (config.CANDIDATE_CHECKPOINT_SCANS, config.CANDIDATE_MAX_SCANS)

        if is_checkpoint and improved:
            await telegram.send_message(alerts.candidate_recommendation_alert(
                name, address, baseline_pct, current_pct, scan_count, result["breakdown"],
            ))
            await candidates.mark_recommended(address)
            await confirmed.add_token(address, name, result["total_score"], result["verdict"],
                                       source="candidate_pipeline")
            stats["recommended"] += 1
            log(f"[observed] {name}: рекомендован, передан в 12ч-мониторинг.")
        elif scan_count >= config.CANDIDATE_MAX_SCANS:
            await candidates.reject(address)
            stats["rejected"] += 1
            log(f"[observed] {name}: улучшений не было после {scan_count} сканов, "
                f"отсеян навсегда.")
        else:
            await candidates.update_observed(address, current_pct, scan_count)

        await asyncio.sleep(1.5)


# ---------------------------------------------------------------------------
# Сканер УСТОЯВШИХСЯ токенов (watchlist)
# ---------------------------------------------------------------------------

async def _score_fundamentals_for_address(address, manual_flags=None):
    """Общая логика: тянет данные и считает Fundamentals Score для адреса."""
    all_pairs = ds.get_pairs_for_token("solana", address)
    if not all_pairs:
        return None, None
    pair = max(all_pairs, key=lambda p: (p.get("liquidity") or {}).get("usd", 0) or 0)

    liquidity_usd = (pair.get("liquidity") or {}).get("usd", 0) or 0
    volume_24h = (pair.get("volume") or {}).get("h24", 0) or 0
    price_usd = float(pair.get("priceUsd", 0) or 0)
    market_cap = pair.get("marketCap", 0) or pair.get("fdv", 0) or 0
    price_change_24h = (pair.get("priceChange") or {}).get("h24", 0) or 0
    txns_24h = (pair.get("txns") or {}).get("h24", {}) or {}

    pair_created_ms = pair.get("pairCreatedAt")
    age_days = (time.time() * 1000 - pair_created_ms) / 86_400_000 if pair_created_ms else 0

    top10_pct = ds.get_top10_concentration(address)
    listed_cg = ds.is_listed_on_coingecko(address)

    # RugCheck: кошелёк дева (кэшируется навсегда) + статус LP (кэшируется
    # на несколько часов) — оба значения из ОДНОГО запроса к RugCheck
    dev_wallet_address, lp_clean = await dev_wallets.get_rugcheck_info(address)
    dev_balance = None
    if dev_wallet_address:
        dev_balance = ds.get_wallet_token_balance(dev_wallet_address, address)

    # Bundle detection (Bitquery) — проверяется один раз навсегда, дальше
    # результат просто читается из кэша
    pool_created_ts = pair_created_ms / 1000 if pair_created_ms else None
    bundle_detected = await bundle_check.get_bundle_status(address, pool_created_ts)

    socials = (pair.get("info") or {}).get("socials", [])
    websites = (pair.get("info") or {}).get("websites", [])
    # (all_pairs уже получены выше — не запрашиваем DexScreener повторно)

    await storage.record_snapshot(address, liquidity_usd, volume_24h, price_usd, dev_balance)
    liquidity_trend = await storage.get_trend(address, "liquidity_usd", lookback_hours=24)
    volume_trend = await storage.get_trend(address, "volume_24h_usd", lookback_hours=24)
    dev_balance_trend = await storage.get_trend(address, "dev_balance", lookback_hours=24)

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
        dev_balance_trend=dev_balance_trend,
        lp_clean=lp_clean,
        bundle_detected=bundle_detected,
        manual=ManualFlags(**manual_flags) if manual_flags else ManualFlags(),
    )

    result = score_fundamentals(signals)
    return result, (liquidity_trend, volume_trend, dev_balance_trend)


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

        liquidity_trend, volume_trend, dev_balance_trend = trends
        name = entry.get("name", address[:8])
        log(f"[established] {name}: {result['total_score']}/{result['max_possible_score']} "
            f"баллов ({result['score_pct']}%) -> {result['verdict']} "
            f"(тренд ликв.={liquidity_trend}, тренд объёма={volume_trend}, "
            f"тренд devbal={dev_balance_trend})")

        await asyncio.sleep(1.5)  # небольшая пауза, чтобы не бить по лимитам DexScreener


# ---------------------------------------------------------------------------
# Почасовой отчёт в Telegram
# ---------------------------------------------------------------------------

async def hourly_report_loop():
    while True:
        await asyncio.sleep(config.HOURLY_REPORT_INTERVAL_SEC)
        try:
            report_stats = dict(stats)
            report_stats["confirmed_count"] = await confirmed.count()
            status_counts = await candidates.counts_by_status()
            report_stats["observed_count"] = status_counts.get("observed", 0)
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

        liquidity_trend, volume_trend, dev_balance_trend = trends
        old_score = entry["last_score"]
        new_score = result["total_score"]
        delta = abs(new_score - old_score)

        log(f"[confirmed] {name}: {old_score} -> {new_score} ({result['score_pct']}%) "
            f"({result['verdict']}, тренд ликв.={liquidity_trend}, тренд объёма={volume_trend}, "
            f"тренд devbal={dev_balance_trend})")

        if delta >= config.CONFIRMED_SCORE_CHANGE_THRESHOLD or entry["last_verdict"] != result["verdict"]:
            await telegram.send_message(alerts.fundamentals_change_alert(
                name, address, old_score, new_score,
                entry["last_verdict"], result["verdict"],
                liquidity_trend, volume_trend, result["breakdown"],
                score_pct=result["score_pct"],
            ))

        await confirmed.update_after_check(address, new_score, result["verdict"])
        await asyncio.sleep(1.5)  # небольшая пауза, чтобы не бить по лимитам DexScreener


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
        command = text.strip().split()[0].lower()
        if command in ("/observed", "/наблюдение"):
            entries = await candidates.get_all_observed()
            await telegram.send_message(alerts.observed_list_message(entries))
        elif command in ("/monitoring", "/мониторинг"):
            entries = await confirmed.get_all()
            await telegram.send_message(alerts.monitoring_list_message(entries))
        elif command in ("/help", "/start"):
            await telegram.send_message(alerts.help_message())
        else:
            await telegram.send_message(
                f"Неизвестная команда: <code>{text}</code>\n\n{alerts.help_message()}"
            )
        return

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
    print("Solana Alpha Bot v11 — 2ч-фильтр + наблюдение + established + Telegram")
    print(f"Токены младше {config.CANDIDATE_MIN_AGE_HOURS}ч не оцениваются.")
    print(f"Устоявшимся (watchlist) считается токен старше {config.ESTABLISHED_MIN_AGE_DAYS} дн.")
    print("Хранилище:", "Postgres" if db.is_configured() else "локальные JSON-файлы")
    print("=" * 60)

    await db.init_schema()
    await telegram.send_message(alerts.startup_alert())

    await asyncio.gather(
        candidate_discovery_loop(),
        candidate_pending_loop(),
        candidate_observed_loop(),
        scan_established_loop(),
        hourly_report_loop(),
        confirmed_recheck_loop(),
        telegram_listener_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
