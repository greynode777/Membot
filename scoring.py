"""
Два скоринговых движка:

1. RiskSignals + score_risk()        — для НОВЫХ токенов (защита от скама)
2. FundamentalSignals + score_fundamentals() — для УСТОЯВШИХСЯ токенов
   (оценка перспективности на основе фундамента)

Оба возвращают итоговый балл, разбивку по факторам и вердикт.
"""

from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

import config


class Verdict(str, Enum):
    STRONG = "Strong"
    WATCH = "Watch"
    SKIP = "Skip"


# ===========================================================================
# 1. РИСК-СКОРИНГ (новые токены)
# ===========================================================================

@dataclass
class RiskSignals:
    address: str

    # --- Реально реализованные проверки (данные приходят от RPC/DexScreener) ---
    mint_disabled: Optional[bool] = None   # None = RPC-запрос не удался
    freeze_disabled: Optional[bool] = None

    has_twitter: bool = False
    has_website: bool = False
    has_telegram: bool = False

    buyers: int = 0
    sellers: int = 0

    # --- Проверки, которые ПОКА НЕ РЕАЛИЗОВАНЫ в коде (LP-lock, RugCheck,
    # анализ бандлов, funding-кошельков, кластеров, поведения дева,
    # alpha-кошельков, wash trading). Остаются None, пока реальная проверка
    # не появится — тогда просто задайте сюда True/False, и фактор
    # автоматически начнёт участвовать в расчёте. Пока что None означает
    # "не проверялось", и это НЕ даёт ни плюса, ни минуса. ---
    lp_clean: Optional[bool] = None
    rugcheck_score: Optional[int] = None
    bundle_detected: Optional[bool] = None
    funding_wallet_clean: Optional[bool] = None
    funding_wallet_flagged: Optional[bool] = None
    wallet_cluster_detected: Optional[bool] = None
    no_wallet_clusters: Optional[bool] = None
    dev_sold: Optional[bool] = None
    dev_not_sold: Optional[bool] = None
    alpha_wallet_bought: Optional[bool] = None
    wash_trading: Optional[bool] = None

    def buyer_growth_signal(self) -> bool:
        if self.buyers == 0:
            return False
        ratio = self.sellers / max(self.buyers, 1)
        return self.buyers >= 50 and ratio < 0.15


def score_risk(s: RiskSignals) -> dict:
    """
    Считает балл как ПРОЦЕНТ от максимально возможного при том, что
    реально удалось проверить — так же, как Fundamentals Score. Факторы,
    для которых проверка не реализована (LP-lock, RugCheck, бандлы,
    funding-кошельки, кластеры, поведение дева, alpha-кошельки), не
    участвуют в расчёте вообще, пока не появится реальная реализация —
    вместо того чтобы молча засчитывать их как "всё чисто".
    """
    breakdown = {}
    achieved = 0
    max_possible = 0
    pos = config.RISK_POSITIVE_WEIGHTS
    neg = config.RISK_NEGATIVE_WEIGHTS

    def add_evaluable(flag, key, evaluable=True):
        nonlocal achieved, max_possible
        if not evaluable:
            return
        max_possible += pos[key]
        if flag:
            achieved += pos[key]
            breakdown[key] = pos[key]

    def add_negative(flag, key):
        nonlocal achieved
        if flag:
            achieved += neg[key]  # значение уже отрицательное
            breakdown[key] = neg[key]

    # --- Реально реализованные проверки ---
    add_evaluable(bool(s.mint_disabled), "mint_disabled", evaluable=s.mint_disabled is not None)
    add_evaluable(bool(s.freeze_disabled), "freeze_disabled", evaluable=s.freeze_disabled is not None)
    add_evaluable(s.has_twitter, "has_twitter")
    add_evaluable(s.has_website, "has_website")
    add_evaluable(s.has_telegram, "has_telegram")
    add_evaluable(s.buyer_growth_signal(), "buyer_growth")

    # --- Пока не реализовано — участвует только если явно передано
    # значение (не None). Сейчас всегда None, значит не участвует. ---
    add_evaluable(bool(s.lp_clean), "lp_clean", evaluable=s.lp_clean is not None)
    add_evaluable(s.rugcheck_score is not None and s.rugcheck_score < 500,
                  "rugcheck_low_risk", evaluable=s.rugcheck_score is not None)
    add_evaluable(s.bundle_detected is False, "no_bundle", evaluable=s.bundle_detected is not None)
    add_evaluable(bool(s.dev_not_sold), "dev_not_sold", evaluable=s.dev_not_sold is not None)
    add_evaluable(bool(s.alpha_wallet_bought), "alpha_wallet_bought", evaluable=s.alpha_wallet_bought is not None)
    add_evaluable(bool(s.funding_wallet_clean), "funding_wallet_clean", evaluable=s.funding_wallet_clean is not None)
    add_evaluable(bool(s.no_wallet_clusters), "no_wallet_clusters", evaluable=s.no_wallet_clusters is not None)

    add_negative(s.bundle_detected is True, "bundle_detected")
    add_negative(s.dev_sold is True, "dev_sold")
    add_negative(s.funding_wallet_flagged is True, "funding_wallet_flagged")
    add_negative(s.wallet_cluster_detected is True, "wallet_cluster_detected")
    add_negative(s.wash_trading is True, "wash_trading")

    score_pct = round((achieved / max_possible) * 100, 1) if max_possible > 0 else 0

    if score_pct >= config.RISK_THRESHOLD_STRONG_PCT:
        verdict = Verdict.STRONG
    elif score_pct >= config.RISK_THRESHOLD_WATCH_PCT:
        verdict = Verdict.WATCH
    else:
        verdict = Verdict.SKIP

    return {
        "address": s.address,
        "total_score": achieved,
        "max_possible_score": max_possible,
        "score_pct": score_pct,
        "verdict": verdict.value,
        "breakdown": breakdown,
    }


# ===========================================================================
# 2. ФУНДАМЕНТАЛ-СКОРИНГ (устоявшиеся токены)
# ===========================================================================

@dataclass
class ManualFlags:
    """
    Факторы, которые бот не может проверить автоматически из бесплатных
    источников — их вы указываете сами, изучив проект (один раз, при
    добавлении в watchlist.json).
    """
    audited: bool = False
    team_doxxed: bool = False
    active_development: bool = False
    upcoming_unlock_risk: bool = False


@dataclass
class FundamentalSignals:
    address: str
    age_days: float = 0

    market_cap_usd: float = 0
    liquidity_usd: float = 0
    volume_24h_usd: float = 0
    price_change_24h_pct: float = 0

    buys_24h: int = 0
    sells_24h: int = 0

    top10_concentration_pct: Optional[float] = None

    has_twitter: bool = False
    has_website: bool = False
    has_telegram: bool = False
    num_dex_pairs: int = 1
    listed_coingecko: Optional[bool] = None  # None = не удалось проверить

    liquidity_trend: Optional[str] = None  # 'up' | 'down' | 'flat' | None
    volume_trend: Optional[str] = None
    dev_balance_trend: Optional[str] = None  # 'up' | 'down' | 'flat' | None

    manual: ManualFlags = field(default_factory=ManualFlags)

    def volume_to_mcap_ratio(self) -> Optional[float]:
        if self.market_cap_usd <= 0:
            return None
        return self.volume_24h_usd / self.market_cap_usd

    def buy_sell_ratio(self) -> Optional[float]:
        if self.sells_24h == 0:
            return None
        return self.buys_24h / self.sells_24h


def score_fundamentals(s: FundamentalSignals) -> dict:
    """
    Считает балл как ПРОЦЕНТ от максимально возможного при том объёме
    данных, который реально удалось получить — а не от фиксированной
    шкалы. Это важно: у только что добавленного токена ещё нет истории
    трендов (нужно 24ч) и может не быть ответа от CoinGecko (лимиты) —
    такой токен не должен штрафоваться за нехватку данных, которых
    физически ещё не может быть.
    """
    breakdown = {}
    achieved = 0
    max_possible = 0
    w = config.FUNDAMENTAL_WEIGHTS

    def add_evaluable(flag, key, evaluable=True):
        """flag: сработал ли позитивный фактор. evaluable: было ли вообще
        возможно его проверить (данные доступны). Если evaluable=False —
        фактор просто не участвует ни в достигнутых, ни в максимально
        возможных баллах."""
        nonlocal achieved, max_possible
        if not evaluable:
            return
        max_possible += w[key]
        if flag:
            achieved += w[key]
            breakdown[key] = w[key]

    def add_negative(flag, key):
        """Штрафные факторы не входят в max_possible (это не "баллы,
        которые можно набрать", это риск-флаги) — но если сработали,
        вычитаются из достигнутого."""
        nonlocal achieved
        if flag:
            achieved += w[key]  # значение уже отрицательное
            breakdown[key] = w[key]

    ratio = s.volume_to_mcap_ratio()
    add_evaluable(ratio is not None and 0.05 <= ratio <= 0.30,
                  "healthy_volume_to_mcap", evaluable=ratio is not None)

    add_evaluable(s.liquidity_usd >= 50_000, "liquidity_above_50k")

    add_evaluable(s.liquidity_trend in ("up", "flat"), "liquidity_trend_up",
                  evaluable=s.liquidity_trend is not None)
    add_negative(s.liquidity_trend == "down", "liquidity_trend_down")

    add_evaluable(s.volume_trend == "up", "volume_trend_up",
                  evaluable=s.volume_trend is not None)
    add_negative(s.volume_trend == "down", "volume_trend_down")

    bs_ratio = s.buy_sell_ratio()
    add_evaluable(bs_ratio is not None and bs_ratio >= 1.3, "healthy_buy_sell_ratio",
                  evaluable=bs_ratio is not None)
    add_negative(bs_ratio is not None and bs_ratio <= 0.7, "unhealthy_buy_sell_ratio")

    add_evaluable(s.top10_concentration_pct is not None and s.top10_concentration_pct < 30,
                  "top10_concentration_low", evaluable=s.top10_concentration_pct is not None)
    add_negative(s.top10_concentration_pct is not None and s.top10_concentration_pct > 60,
                 "top10_concentration_high")

    add_evaluable(s.has_twitter, "has_twitter")
    add_evaluable(s.has_website, "has_website")
    add_evaluable(s.has_telegram, "has_telegram")
    add_evaluable(s.num_dex_pairs > 1, "multi_dex_presence")
    add_evaluable(s.listed_coingecko is True, "listed_coingecko",
                  evaluable=s.listed_coingecko is not None)

    add_evaluable(abs(s.price_change_24h_pct) < 15, "low_volatility")
    add_negative(abs(s.price_change_24h_pct) > 60, "extreme_volatility")

    add_evaluable(s.age_days >= 30, "age_bonus_30d")
    add_evaluable(s.age_days >= 90, "age_bonus_90d")

    add_evaluable(s.manual.audited, "audited")
    add_evaluable(s.manual.team_doxxed, "team_doxxed")
    add_evaluable(s.manual.active_development, "active_development")
    add_negative(s.manual.upcoming_unlock_risk, "upcoming_unlock_risk")

    # Баланс кошелька дева: если он не продаёт (стабилен/растёт) — плюс;
    # если распродаёт позицию — серьёзный минус. Как и с другими трендами,
    # если данных пока недостаточно (None) — фактор просто не участвует.
    add_evaluable(s.dev_balance_trend in ("up", "flat"), "dev_balance_stable_or_up",
                  evaluable=s.dev_balance_trend is not None)
    add_negative(s.dev_balance_trend == "down", "dev_balance_down")

    score_pct = round((achieved / max_possible) * 100, 1) if max_possible > 0 else 0

    if score_pct >= config.FUNDAMENTAL_THRESHOLD_STRONG_PCT:
        verdict = Verdict.STRONG
    elif score_pct >= config.FUNDAMENTAL_THRESHOLD_WATCH_PCT:
        verdict = Verdict.WATCH
    else:
        verdict = Verdict.SKIP

    return {
        "address": s.address,
        "total_score": achieved,
        "max_possible_score": max_possible,
        "score_pct": score_pct,
        "verdict": verdict.value,
        "breakdown": breakdown,
    }
