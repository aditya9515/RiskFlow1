from __future__ import annotations

from dataclasses import dataclass

from risk_service.config import Settings
from risk_service.models import Decision, FeatureSnapshot, PaymentCreatedPayload

RULE_VERSION = "rules-v1"


@dataclass(frozen=True)
class RuleResult:
    decision: Decision
    risk_score: int
    reason_codes: tuple[str, ...]


class RuleEngine:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def evaluate(self, payment: PaymentCreatedPayload, features: FeatureSnapshot) -> RuleResult:
        score = 0
        reasons: list[str] = []

        if payment.amount_minor >= self._settings.extreme_amount_minor:
            score += 70
            reasons.append("EXTREME_AMOUNT")
        elif payment.amount_minor >= self._settings.high_amount_minor:
            score += 40
            reasons.append("HIGH_AMOUNT")

        if features.velocity_5m >= 5:
            score += 40
            reasons.append("HIGH_VELOCITY_5M")
        if features.new_device:
            score += 15
            reasons.append("NEW_DEVICE")
        if features.cross_border:
            score += 25
            reasons.append("CROSS_BORDER")
        if payment.merchant_id in self._settings.risky_merchant_ids:
            score += 30
            reasons.append("RISKY_MERCHANT")
        if payment.country in self._settings.high_risk_countries:
            score += 30
            reasons.append("HIGH_RISK_COUNTRY")

        score = min(score, 100)
        if score >= self._settings.block_threshold:
            decision = Decision.BLOCK
        elif score >= self._settings.review_threshold:
            decision = Decision.REVIEW
        else:
            decision = Decision.ALLOW

        if not reasons:
            reasons.append("NO_RISK_SIGNALS")

        return RuleResult(decision=decision, risk_score=score, reason_codes=tuple(reasons))
