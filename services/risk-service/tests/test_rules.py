from dataclasses import replace
from datetime import datetime

from risk_service.config import Settings
from risk_service.models import Decision, FeatureSnapshot, PaymentCreatedEnvelope
from risk_service.rules import RuleEngine


def features(
    decision_at: datetime,
    *,
    velocity: int = 1,
    new_device: bool = False,
    cross_border: bool = False,
) -> FeatureSnapshot:
    return FeatureSnapshot(
        velocity_5m=velocity,
        new_device=new_device,
        cross_border=cross_border,
        baseline_country="IN",
        decision_at=decision_at,
    )


def test_low_risk_payment_is_allowed(
    settings: Settings, payment_event: PaymentCreatedEnvelope, decision_time: datetime
) -> None:
    result = RuleEngine(settings).evaluate(payment_event.payload, features(decision_time))

    assert result.decision is Decision.ALLOW
    assert result.risk_score == 0
    assert result.reason_codes == ("NO_RISK_SIGNALS",)


def test_high_amount_is_reviewed(
    settings: Settings, payment_event: PaymentCreatedEnvelope, decision_time: datetime
) -> None:
    payment = payment_event.payload.model_copy(update={"amount_minor": 100000})

    result = RuleEngine(settings).evaluate(payment, features(decision_time))

    assert result.decision is Decision.REVIEW
    assert result.risk_score == 40
    assert result.reason_codes == ("HIGH_AMOUNT",)


def test_extreme_amount_is_blocked(
    settings: Settings, payment_event: PaymentCreatedEnvelope, decision_time: datetime
) -> None:
    payment = payment_event.payload.model_copy(update={"amount_minor": 500000})

    result = RuleEngine(settings).evaluate(payment, features(decision_time))

    assert result.decision is Decision.BLOCK
    assert result.risk_score == 70
    assert result.reason_codes == ("EXTREME_AMOUNT",)


def test_combined_velocity_device_and_cross_border_signals_block(
    settings: Settings, payment_event: PaymentCreatedEnvelope, decision_time: datetime
) -> None:
    result = RuleEngine(settings).evaluate(
        payment_event.payload,
        features(decision_time, velocity=5, new_device=True, cross_border=True),
    )

    assert result.decision is Decision.BLOCK
    assert result.risk_score == 80
    assert result.reason_codes == ("HIGH_VELOCITY_5M", "NEW_DEVICE", "CROSS_BORDER")


def test_configured_merchant_and_country_signals_are_explainable(
    settings: Settings, payment_event: PaymentCreatedEnvelope, decision_time: datetime
) -> None:
    configured = replace(
        settings,
        risky_merchant_ids=frozenset({"merchant-1"}),
        high_risk_countries=frozenset({"IN"}),
    )

    result = RuleEngine(configured).evaluate(payment_event.payload, features(decision_time))

    assert result.decision is Decision.REVIEW
    assert result.risk_score == 60
    assert result.reason_codes == ("RISKY_MERCHANT", "HIGH_RISK_COUNTRY")
