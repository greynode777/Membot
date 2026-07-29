"""
Точка входа. Запускает ЧЕТЫРЕ независимых цикла:

1. scan_new_tokens_loop()     — ищет новые токены, считает Risk Score.
                                 При вердикте Strong — сразу шлёт алерт
                                 в Telegram и добавляет токен в список
                                 "подтверждённых" для 12ч-проверок.
2. scan_established_loop()    — оценивает токены из watchlist.json
                                 по Fundamentals Score (как раньше).
3. hourly_report_loop()        — раз в час шлёт в Telegram сводку:
                                 сколько проверено / рагпул / рекомендовано.
4. confirmed_recheck_loop()    — раз в 15 минут проверяет, не пора ли
                                 кому-то из "подтверждённых" токенов
                                 проходить очередную 12-часовую проверку;
                                 если фундаментал заметно изменился —
                                 шлёт алерт "растёт" / "падает".

При старте бот сразу шлёт в Telegram сообщение "бот запущен".
"""

import asyncio
import json
import os
import time

import config
import data_sources as ds
import storage
import confirmed
import telegram
import alerts
from scoring import (
    RiskSignals, FundamentalSignals, ManualFlags,
    score_risk, score_fundamentals,
)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


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
            confirmed.add_token(token_address, name, result["total_score"], result["verdict"])


# ---------------------------------------------------------------------------
# Сканер УСТОЯВШИХСЯ токенов (watchlist.json)
# ---------------------------------------------------------------------------

def load_watchlist():
    if not os.path.exists(config.WATCHLIST_FILE):
        return []
    try:
        with open(config.WATCHLIST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("tokens", [])
    except (json.JSONDecodeError, OSError) as e:
        log(f"[established] Не удалось прочитать watchlist.json: {e}")
        return []


def _score_fundamentals_for_address(address, manual_flags=None):
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

    storage.record_snapshot(address, liquidity_usd, volume_24h, price_usd)
    liquidity_trend = storage.get_trend(address, "liquidity_usd", lookback_hours=24)
    volume_trend = storage.get_trend(address, "volume_24h_usd", lookback_hours=24)

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
    watchlist = load_watchlist()
    if not watchlist:
        log("[established] watchlist.json пуст — добавьте адреса токенов для отслеживания.")
        return

    log(f"[established] Проверяю {len(watchlist)} токен(ов) из watchlist...")

    for entry in watchlist:
        address = entry.get("address")
        if not address:
            continue

        result, trends = _score_fundamentals_for_address(address, entry.get("manual_flags"))
        if not result:
            log(f"[established] {address[:8]}... нет данных на DexScreener, пропуск.")
            continue

        liquidity_trend, volume_trend = trends
        name = entry.get("name", address[:8])
        log(f"[established] {name}: баллы={result['total_score']} -> {result['verdict']} "
            f"(тренд ликв.={liquidity_trend}, тренд объёма={volume_trend})")


# ---------------------------------------------------------------------------
# Почасовой отчёт в Telegram
# ---------------------------------------------------------------------------

async def hourly_report_loop():
    while True:
        await asyncio.sleep(config.HOURLY_REPORT_INTERVAL_SEC)
        try:
            report_stats = dict(stats)
            report_stats["confirmed_count"] = confirmed.count()
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
    due = confirmed.get_due_tokens()
    if not due:
        return

    log(f"[confirmed] Пора перепроверить {len(due)} токен(ов) (12ч-цикл)...")

    for entry in due:
        address = entry["address"]
        name = entry.get("name", address[:8])

        result, trends = _score_fundamentals_for_address(address)
        if not result:
            log(f"[confirmed] {name}: нет данных, откладываю проверку.")
            confirmed.update_after_check(address, entry["last_score"], entry["last_verdict"])
            continue

        liquidity_trend, volume_trend = trends
        old_score = entry["last_score"]
        new_score = result["total_score"]
        delta = abs(new_score - old_score)

        log(f"[confirmed] {name}: {old_score} -> {new_score} "
            f"({result['verdict']}, тренд ликв.={liquidity_trend}, тренд объёма={volume_trend})")

        if delta >= config.CONFIRMED_SCORE_CHANGE_THRESHOLD or entry["last_verdict"] != result["verdict"]:
            await telegram.send_message(alerts.fundamentals_change_alert(
                name, address, old_score, new_score,
                entry["last_verdict"], result["verdict"],
                liquidity_trend, volume_trend, result["breakdown"],
            ))

        confirmed.update_after_check(address, new_score, result["verdict"])


# ---------------------------------------------------------------------------
# Запуск
# ---------------------------------------------------------------------------

async def main():
    print("=" * 60)
    print("Solana Alpha Bot v3 — new + established + Telegram-алерты")
    print(f"Устоявшимся считается токен старше {config.ESTABLISHED_MIN_AGE_DAYS} дн.")
    print("Все источники данных — бесплатные (DexScreener + Solana RPC)")
    print("=" * 60)

    await telegram.send_message(alerts.startup_alert())

    await asyncio.gather(
        scan_new_tokens_loop(),
        scan_established_loop(),
        hourly_report_loop(),
        confirmed_recheck_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
