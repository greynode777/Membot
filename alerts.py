"""
Форматирование текстов для Telegram-алертов.
Здесь же — словарь перевода технических ключей скоринга в понятные
русские подписи, чтобы в алерте были видны реальные причины рекомендации.
"""

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
}


def _factor_lines(breakdown: dict) -> str:
    lines = []
    for key, points in breakdown.items():
        label = FACTOR_LABELS.get(key, key)
        sign = "+" if points >= 0 else ""
        lines.append(f"  • {label}: {sign}{points}")
    return "\n".join(lines)


def startup_alert() -> str:
    return "🟢 <b>Solana Alpha Bot запущен</b>\nСканирую новые и устоявшиеся токены. Первый почасовой отчёт — через час."


def hourly_report(stats: dict) -> str:
    return (
        "📊 <b>Отчёт за последний час</b>\n\n"
        f"Проверено токенов: <b>{stats['checked']}</b>\n"
        f"🔴 Похоже на рагпул / низкий скор: <b>{stats['skip']}</b>\n"
        f"🟡 На наблюдении (Watch): <b>{stats['watch']}</b>\n"
        f"🟢 Рекомендовано (Strong): <b>{stats['strong']}</b>\n\n"
        f"Всего под наблюдением 12ч-проверки: <b>{stats['confirmed_count']}</b>"
    )


def new_token_recommendation(result: dict, address: str, name: str = "") -> str:
    label_name = name or address[:8] + "..."
    return (
        "🚨 <b>НОВЫЙ ПЕРСПЕКТИВНЫЙ ТОКЕН</b> 🚨\n\n"
        f"💎 {label_name}\n"
        f"📍 <code>{address}</code>\n"
        f"📈 Итоговый балл: <b>{result['total_score']}</b> ({result['verdict']})\n\n"
        f"<b>Ключевые показатели:</b>\n{_factor_lines(result['breakdown'])}\n\n"
        f"🔗 Проверить: https://dexscreener.com/solana/{address}\n\n"
        "Токен добавлен в лист наблюдения (проверка каждые 12 часов)."
    )


def fundamentals_change_alert(name, address, old_score, new_score, old_verdict, new_verdict,
                               liquidity_trend, volume_trend, breakdown, score_pct=None) -> str:
    direction = "📈 РАСТЁТ" if new_score > old_score else "📉 ПАДАЕТ"
    delta = new_score - old_score
    sign = "+" if delta >= 0 else ""
    pct_line = f" ({score_pct}% от макс.)" if score_pct is not None else ""
    return (
        f"{direction} <b>{name or address[:8]}</b>\n\n"
        f"📍 <code>{address}</code>\n"
        f"Балл: {old_score} → <b>{new_score}</b> ({sign}{delta}){pct_line}\n"
        f"Вердикт: {old_verdict} → <b>{new_verdict}</b>\n"
        f"Тренд ликвидности: {liquidity_trend or 'нет данных'}\n"
        f"Тренд объёма: {volume_trend or 'нет данных'}\n\n"
        f"<b>Текущие показатели:</b>\n{_factor_lines(breakdown)}\n\n"
        f"🔗 https://dexscreener.com/solana/{address}"
    )
