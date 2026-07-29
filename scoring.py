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

    mint_disabled: bool = False
    freeze_disabled: bool = False
    lp_clean: bool = False

    has_twitter: bool = False
    has_website: bool = False
    has_telegram: bool = False

    rugcheck_score: Optional[int] = None

    bundle_detected: bool = False
    funding_wallet_flagged: bool = False
    funding_wallet_clean: bool = False
    wallet_cluster_detected: bool = False
    no_wallet_clusters: bool = False
    wash_trading: bool = False

    dev_sold: bool = False
    dev_not_sold: bool = False

    alpha_wallet_bought: bool = False
    buyers: int = 0
    sellers: int = 0

    def buyer_growth_signal(self) -> bool:
        if self.buyers == 0:
            return False
        ratio = self.sellers / max(self.buyers, 1)
        return self.buyers >= 50 and ratio < 0.15


def score_risk(s: RiskSignals) -> dict:
    breakdown = {}
    total = 0

    def add(flag, key, weights):
        nonlocal total
        if flag and key in weights:
            total += weights[key]
            breakdown[key] = weights[key]

    pos = config.RISK_POSITIVE_WEIGHTS
    neg = config.RISK_NEGATIVE_WEIGHTS

    add(s.mint_disabled, "mint_disabled", pos)
    add(s.freeze_disabled, "freeze_disabled", pos)
    add(s.lp_clean, "lp_clean", pos)
    add(s.has_twitter, "has_twitter", pos)
    add(s.has_website, "has_website", pos)
    add(s.has_telegram, "has_telegram", pos)
    add(s.rugcheck_score is not None and s.rugcheck_score < 500, "rugcheck_low_risk", pos)
    add(not s.bundle_detected, "no_bundle", pos)
    add(s.dev_not_sold, "dev_not_sold", pos)
    add(s.alpha_wallet_bought, "alpha_wallet_bought", pos)
    add(s.buyer_growth_signal(), "buyer_growth", pos)
    add(s.funding_wallet_clean, "funding_wallet_clean", pos)
    add(s.no_wallet_clusters, "no_wallet_clusters", pos)

    add(s.bundle_detected, "bundle_detected", neg)
    add(s.dev_sold, "dev_sold", neg)
    add(s.funding_wallet_flagged, "funding_wallet_flagged", neg)
    add(s.wallet_cluster_detected, "wallet_cluster_detected", neg)
    add(s.wash_trading, "wash_trading", neg)

    if total >= config.RISK_THRESHOLD_STRONG_BUY:
        verdict = Verdict.STRONG
    elif total >= config.RISK_THRESHOLD_WATCH:
        verdict = Verdict.WATCH
    else:
        verdict = Verdict.SKIP

    return {"address": s.address, "total_score": total, "verdict": verdict.value, "breakdown": breakdown}


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
    listed_coingecko: bool = False

    liquidity_trend: Optional[str] = None  # 'up' | 'down' | 'flat' | None
    volume_trend: Optional[str] = None

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
    breakdown = {}
    total = 0
    w = config.FUNDAMENTAL_WEIGHTS

    def add(flag, key):
        nonlocal total
        if flag:
            total += w[key]
            breakdown[key] = w[key]

    ratio = s.volume_to_mcap_ratio()
    add(ratio is not None and 0.05 <= ratio <= 0.30, "healthy_volume_to_mcap")

    add(s.liquidity_usd >= 50_000, "liquidity_above_50k")
    add(s.liquidity_trend == "up" or s.liquidity_trend == "flat", "liquidity_trend_up")
    add(s.liquidity_trend == "down", "liquidity_trend_down")

    add(s.volume_trend == "up", "volume_trend_up")
    add(s.volume_trend == "down", "volume_trend_down")

    bs_ratio = s.buy_sell_ratio()
    add(bs_ratio is not None and bs_ratio >= 1.3, "healthy_buy_sell_ratio")
    add(bs_ratio is not None and bs_ratio <= 0.7, "unhealthy_buy_sell_ratio")

    add(s.top10_concentration_pct is not None and s.top10_concentration_pct < 30,
        "top10_concentration_low")
    add(s.top10_concentration_pct is not None and s.top10_concentration_pct > 60,
        "top10_concentration_high")

    add(s.has_twitter, "has_twitter")
    add(s.has_website, "has_website")
    add(s.has_telegram, "has_telegram")
    add(s.num_dex_pairs > 1, "multi_dex_presence")
    add(s.listed_coingecko, "listed_coingecko")

    add(abs(s.price_change_24h_pct) < 15, "low_volatility")
    add(abs(s.price_change_24h_pct) > 60, "extreme_volatility")

    add(s.age_days >= 30, "age_bonus_30d")
    add(s.age_days >= 90, "age_bonus_90d")

    add(s.manual.audited, "audited")
    add(s.manual.team_doxxed, "team_doxxed")
    add(s.manual.active_development, "active_development")
    add(s.manual.upcoming_unlock_risk, "upcoming_unlock_risk")

    if total >= config.FUNDAMENTAL_THRESHOLD_STRONG:
        verdict = Verdict.STRONG
    elif total >= config.FUNDAMENTAL_THRESHOLD_WATCH:
        verdict = Verdict.WATCH
    else:
        verdict = Verdict.SKIP

    return {"address": s.address, "total_score": total, "verdict": verdict.value, "breakdown": breakdown}
