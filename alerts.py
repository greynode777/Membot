"""
Форматирование текстов для Telegram-алертов.
Здесь же — словарь перевода технических ключей скоринга в понятные
русские подписи, чтобы в алерте были видны реальные причины рекомендации.
"""

import config
import html


def _esc(value) -> str:
    """
    Экранирует HTML-спецсимволы (< > &) в тексте из внешних источников
    (название токена и т.п.) прямо перед вставкой в сообщение Telegram.
    Без этого токен с "<" в названии ломает парсинг HTML и вешает бота
    в бесконечном повторе отправки. Безопасно вызывать даже если значение
    уже было экранировано раньше — в худшем случае текст покажет
    "&amp;" вместо "&", что некрасиво, но не ломает бота.
    """
    if not value:
        return ""
    return html.escape(str(value))

FACTOR_LABELS = {
    # Risk (новые токены)
    "mint_disabled": "Mint отключён",
    "freeze_disabled": "Freeze отключён",
    "lp_clean": "LP чистый",
    "has_twitter": "Есть Twitter",
    "has_website": "Есть сайт",
    "has_telegram": "Есть Telegram",
    "rugcheck_low_risk": "RugCheck: низкий риск",
    "no_bundle": "Бандлы не обнаружены",
    "dev_not_sold": "Dev не продавал",
    "alpha_wallet_bought": "Купил Alpha-кошелёк",
    "buyer_growth": "Органический рост покупателей",
    "funding_wallet_clean": "Funding-кошелёк чист",
    "no_wallet_clusters": "Связанных кошельков не найдено",
    "bundle_detected": "Обнаружен бандл",
    "dev_sold": "Dev продавал",
    "funding_wallet_flagged": "Funding-кошелёк в чёрном списке",
    "wallet_cluster_detected": "Обнаружен кластер кошельков",
    "wash_trading": "Признаки Wash Trading",

    # Fundamentals (устоявшиеся токены)
    "healthy_volume_to_mcap": "Здоровое соотношение объём/капитализация",
    "liquidity_above_50k": "Ликвидность выше $50k",
    "liquidity_trend_up": "Ликвидность растёт/стабильна",
    "liquidity_trend_down": "Ликвидность падает",
    "volume_trend_up": "Объём торгов растёт",
    "volume_trend_down": "Объём торгов падает",
    "healthy_buy_sell_ratio": "Покупок заметно больше продаж",
    "unhealthy_buy_sell_ratio": "Продаж больше покупок",
    "top10_concentration_low": "Топ-10 держат < 30%",
    "top10_concentration_high": "Топ-10 держат > 60%",
    "multi_dex_presence": "Торгуется на нескольких DEX",
    "listed_coingecko": "Есть листинг на CoinGecko",
    "low_volatility": "Волатильность в норме",
    "extreme_volatility": "Аномальная волатильность",
    "age_bonus_30d": "Старше 30 дней",
    "age_bonus_90d": "Старше 90 дней",
    "audited": "Был аудит",
    "team_doxxed": "Команда публична",
    "active_development": "Активная разработка",
    "upcoming_unlock_risk": "Риск: скоро анлок токенов",
    "dev_balance_stable_or_up": "Dev не продаёт (баланс стабилен/растёт)",
    "dev_balance_down": "Dev распродаёт позицию",
    "lp_locked": "LP заблокирована/сожжена (RugCheck)",
    "lp_risk": "Риск по ликвидности (RugCheck)",
    "no_bundle_detected": "Похоже на органический интерес (Bitquery)",
    "bundle_detected_risk": "Похоже на бандл при запуске (Bitquery)",
}


def _factor_lines(breakdown: dict) -> str:
    lines = []
    for key, points in breakdown.items():
        label = FACTOR_LABELS.get(key, key)
        sign = "+" if points >= 0 else ""
        lines.append(f"  • {label}: {sign}{points}")
    return "\n".join(lines)


def factor_lines(breakdown: dict) -> str:
    """Публичная обёртка над _factor_lines — для переиспользования в
    интерактивных карточках (main.py)."""
    return _factor_lines(breakdown)


def _time_until(ts) -> str:
    if not ts:
        return "скоро"
    import time as _time
    remaining = ts - _time.time()
    if remaining <= 0:
        return "со дня на день"
    hours = int(remaining // 3600)
    minutes = int((remaining % 3600) // 60)
    if hours > 0:
        return f"через {hours}ч {minutes}м"
    return f"через {minutes}м"


def observed_list_message(entries: list) -> str:
    if not entries:
        return "📭 Сейчас никто не находится на 2-часовом наблюдении."

    lines = [f"👀 <b>На 2-часовом наблюдении: {len(entries)}</b>\n"]
    for e in entries:
        name = _esc(e.get("name")) or e["address"][:8]
        baseline = e.get("baseline_score_pct")
        last = e.get("last_score_pct")
        scan_count = e.get("scan_count", 0)
        next_check = _time_until(e.get("next_scan_ts"))
        lines.append(
            f"💎 <b>{name}</b>\n"
            f"   <code>{e['address']}</code>\n"
            f"   {baseline}% → {last}% (скан #{scan_count}/6)\n"
            f"   Следующая проверка: {next_check}\n"
        )
    return "\n".join(lines)


def monitoring_list_message(entries: list) -> str:
    if not entries:
        return "📭 Сейчас никто не находится под 12-часовым мониторингом."

    lines = [f"📡 <b>Под 12-часовым мониторингом: {len(entries)}</b>\n"]
    for e in entries:
        name = _esc(e.get("name")) or e["address"][:8]
        score = e.get("last_score")
        verdict = e.get("last_verdict")
        source = e.get("source", "")
        skip_streak = e.get("skip_streak", 0)
        next_check = _time_until(e.get("next_check_ts"))
        streak_line = (
            f"   ⚠️ Skip подряд: {skip_streak}/{config.CONFIRMED_AUTO_REMOVE_AFTER_SKIPS}\n"
            if skip_streak else ""
        )
        lines.append(
            f"💎 <b>{name}</b> ({source})\n"
            f"   <code>{e['address']}</code>\n"
            f"   Баллы: {score}, вердикт: {verdict}\n"
            f"{streak_line}"
            f"   Следующая проверка: {next_check}\n"
        )
    return "\n".join(lines)


def help_message() -> str:
    return (
        "🤖 <b>Доступные команды</b>\n\n"
        "/observed — список токенов на 2-часовом наблюдении\n"
        "/monitoring — список токенов под 12-часовым мониторингом\n"
        "/remove АДРЕС — убрать токен из watchlist\n"
        "/history АДРЕС — история изменения Score токена\n"
        "/compare АДРЕС_А АДРЕС_Б — сравнить два токена\n"
        "/portfolio — показать ваш портфель\n"
        "/track АДРЕС [цена] — добавить токен в портфель\n"
        "/untrack АДРЕС — убрать токен из портфеля\n"
        "/unmonitor АДРЕС — убрать токен из 12ч-мониторинга\n"
        "/help — это сообщение\n\n"
        "Также можно просто прислать адрес токена (mint) — он будет "
        "добавлен в список отслеживания (watchlist)."
    )


def startup_alert() -> str:
    return "🟢 <b>Solana Alpha Bot запущен</b>\nСканирую новые и устоявшиеся токены. Первый почасовой отчёт — через час."


def hourly_report(stats: dict) -> str:
    return (
        "📊 <b>Отчёт за последний час</b>\n\n"
        f"Прошли первичный фильтр (2ч, наблюдаемые): <b>{stats['promoted']}</b>\n"
        f"🔴 Отсеяно навсегда: <b>{stats['rejected']}</b>\n"
        f"🟢 Рекомендовано (улучшение фундамента): <b>{stats['recommended']}</b>\n\n"
        f"Сейчас в наблюдении (2ч-цикл): <b>{stats.get('observed_count', 0)}</b>\n"
        f"Всего под 12ч-мониторингом: <b>{stats['confirmed_count']}</b>"
    )


def new_token_recommendation(result: dict, address: str, name: str = "") -> str:
    label_name = _esc(name) or address[:8] + "..."
    return (
        "🚨 <b>НОВЫЙ ПЕРСПЕКТИВНЫЙ ТОКЕН</b> 🚨\n\n"
        f"💎 {label_name}\n"
        f"📍 <code>{address}</code>\n"
        f"📈 Балл: <b>{result['total_score']}/{result['max_possible_score']}</b> "
        f"({result['score_pct']}%) — {result['verdict']}\n\n"
        f"<b>Ключевые показатели:</b>\n{_factor_lines(result['breakdown'])}\n\n"
        f"🔗 Проверить: https://dexscreener.com/solana/{address}\n\n"
        "Токен добавлен в лист наблюдения (проверка каждые 12 часов)."
    )


def candidate_recommendation_alert(name, address, baseline_pct, current_pct, scan_count, breakdown) -> str:
    delta = round(current_pct - baseline_pct, 1)
    safe_name = _esc(name) or address[:8]
    return (
        "🚨 <b>ФУНДАМЕНТАЛ УЛУЧШИЛСЯ — РЕКОМЕНДАЦИЯ</b> 🚨\n\n"
        f"💎 {safe_name}\n"
        f"📍 <code>{address}</code>\n"
        f"Fundamentals Score: {baseline_pct}% → <b>{current_pct}%</b> (+{delta})\n"
        f"Проверок с момента наблюдения: {scan_count}\n\n"
        f"<b>Текущие показатели:</b>\n{_factor_lines(breakdown)}\n\n"
        f"🔗 https://dexscreener.com/solana/{address}\n\n"
        "Токен переведён в обычный 12-часовой мониторинг."
    )


def fundamentals_change_alert(name, address, old_score, new_score, old_verdict, new_verdict,
                               liquidity_trend, volume_trend, breakdown, score_pct=None) -> str:
    direction = "📈 РАСТЁТ" if new_score > old_score else "📉 ПАДАЕТ"
    delta = new_score - old_score
    sign = "+" if delta >= 0 else ""
    pct_line = f" ({score_pct}% от макс.)" if score_pct is not None else ""
    safe_name = _esc(name) or address[:8]
    return (
        f"{direction} <b>{safe_name}</b>\n\n"
        f"📍 <code>{address}</code>\n"
        f"Балл: {old_score} → <b>{new_score}</b> ({sign}{delta}){pct_line}\n"
        f"Вердикт: {old_verdict} → <b>{new_verdict}</b>\n"
        f"Тренд ликвидности: {liquidity_trend or 'нет данных'}\n"
        f"Тренд объёма: {volume_trend or 'нет данных'}\n\n"
        f"<b>Текущие показатели:</b>\n{_factor_lines(breakdown)}\n\n"
        f"🔗 https://dexscreener.com/solana/{address}"
    )
