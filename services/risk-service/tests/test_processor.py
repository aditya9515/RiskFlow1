from datetime import datetime

from risk_service.config import Settings
from risk_service.model import ModelScore, ModelScoringError
from risk_service.models import (
    Decision,
    FeatureSnapshot,
    PaymentCreatedEnvelope,
    RiskDecisionEnvelope,
)
from risk_service.processor import MODEL_FALLBACK_VERSION, DecisionProcessor
from risk_service.rules import RuleEngine


class CachedFeatureStore:
    def __init__(self, snapshot: FeatureSnapshot) -> None:
        self.snapshot = snapshot
        self.calls = 0
        self.decision: RiskDecisionEnvelope | None = None

    def load_decision(self, _event_id: object) -> RiskDecisionEnvelope | None:
        return self.decision

    def observe(self, _event: PaymentCreatedEnvelope, _decision_at: datetime) -> FeatureSnapshot:
        self.calls += 1
        return self.snapshot

    def save_decision(self, decision: RiskDecisionEnvelope) -> RiskDecisionEnvelope:
        if self.decision is None:
            self.decision = decision
        return self.decision


class FixedRiskModel:
    def __init__(self, probability: float, threshold: float = 0.1) -> None:
        self.probability = probability
        self.threshold = threshold

    def score(self, _payment: object, _features: object) -> ModelScore:
        return ModelScore(
            probability=self.probability,
            risk_score=round(self.probability * 100),
            review_threshold=self.threshold,
            model_version="xgb-test-v1",
        )


class FailingRiskModel:
    def score(self, _payment: object, _features: object) -> ModelScore:
        raise ModelScoringError("model unavailable during inference")


def test_replay_has_same_decision_id_and_timestamp(
    settings: Settings, payment_event: PaymentCreatedEnvelope, decision_time: datetime
) -> None:
    store = CachedFeatureStore(
        FeatureSnapshot(
            velocity_5m=1,
            new_device=True,
            cross_border=False,
            baseline_country="IN",
            decision_at=decision_time,
        )
    )
    processor = DecisionProcessor(
        store,
        RuleEngine(settings),
        FixedRiskModel(0.02),
        now=lambda: decision_time,
    )

    first = processor.process(payment_event)
    replay = processor.process(payment_event)

    assert first == replay
    assert first.event_id == first.payload.decision_id
    assert first.payload.source_event_id == payment_event.event_id
    assert first.payload.rule_version == "rules-v1"
    assert first.payload.model_version == "xgb-test-v1"
    assert first.schema_version == 2
    assert store.calls == 1


def test_ml_can_escalate_allow_to_review_but_not_block(
    settings: Settings, payment_event: PaymentCreatedEnvelope, decision_time: datetime
) -> None:
    store = CachedFeatureStore(
        FeatureSnapshot(
            velocity_5m=1,
            new_device=False,
            cross_border=False,
            baseline_country="IN",
            decision_at=decision_time,
        )
    )
    processor = DecisionProcessor(
        store,
        RuleEngine(settings),
        FixedRiskModel(0.8),
        now=lambda: decision_time,
    )

    result = processor.process(payment_event)

    assert result.payload.decision is Decision.REVIEW
    assert result.payload.reason_codes == ("ML_HIGH_RISK",)
    assert result.payload.rule_score == 0
    assert result.payload.model_score == 80
    assert result.payload.risk_score == 80


def test_deterministic_block_takes_precedence_over_low_model_score(
    settings: Settings, payment_event: PaymentCreatedEnvelope, decision_time: datetime
) -> None:
    store = CachedFeatureStore(
        FeatureSnapshot(
            velocity_5m=1,
            new_device=False,
            cross_border=False,
            baseline_country="IN",
            decision_at=decision_time,
        )
    )
    extreme_payment = payment_event.model_copy(
        update={"payload": payment_event.payload.model_copy(update={"amount_minor": 500000})}
    )
    processor = DecisionProcessor(
        store,
        RuleEngine(settings),
        FixedRiskModel(0.02),
        now=lambda: decision_time,
    )

    result = processor.process(extreme_payment)

    assert result.payload.decision is Decision.BLOCK
    assert result.payload.reason_codes == ("EXTREME_AMOUNT",)
    assert result.payload.risk_score == 70


def test_model_failure_sends_uncertain_payment_to_review(
    settings: Settings, payment_event: PaymentCreatedEnvelope, decision_time: datetime
) -> None:
    store = CachedFeatureStore(
        FeatureSnapshot(
            velocity_5m=1,
            new_device=False,
            cross_border=False,
            baseline_country="IN",
            decision_at=decision_time,
        )
    )
    processor = DecisionProcessor(
        store,
        RuleEngine(settings),
        FailingRiskModel(),
        fallback_review_score=settings.review_threshold,
        now=lambda: decision_time,
    )

    result = processor.process(payment_event)

    assert result.payload.decision is Decision.REVIEW
    assert result.payload.reason_codes == ("ML_UNAVAILABLE",)
    assert result.payload.risk_score == settings.review_threshold
    assert result.payload.rule_score == 0
    assert result.payload.model_score == 0
    assert result.payload.model_probability == 0
    assert result.payload.model_review_threshold == 0.5
    assert result.payload.model_version == MODEL_FALLBACK_VERSION


def test_deterministic_block_takes_precedence_when_model_is_unavailable(
    settings: Settings, payment_event: PaymentCreatedEnvelope, decision_time: datetime
) -> None:
    store = CachedFeatureStore(
        FeatureSnapshot(
            velocity_5m=1,
            new_device=False,
            cross_border=False,
            baseline_country="IN",
            decision_at=decision_time,
        )
    )
    extreme_payment = payment_event.model_copy(
        update={"payload": payment_event.payload.model_copy(update={"amount_minor": 500000})}
    )
    processor = DecisionProcessor(
        store,
        RuleEngine(settings),
        FailingRiskModel(),
        fallback_review_score=settings.review_threshold,
        now=lambda: decision_time,
    )

    result = processor.process(extreme_payment)

    assert result.payload.decision is Decision.BLOCK
    assert result.payload.reason_codes == ("EXTREME_AMOUNT", "ML_UNAVAILABLE")
    assert result.payload.risk_score == 70


def test_cached_fallback_decision_survives_model_recovery(
    settings: Settings, payment_event: PaymentCreatedEnvelope, decision_time: datetime
) -> None:
    store = CachedFeatureStore(
        FeatureSnapshot(
            velocity_5m=1,
            new_device=False,
            cross_border=False,
            baseline_country="IN",
            decision_at=decision_time,
        )
    )
    unavailable_processor = DecisionProcessor(
        store,
        RuleEngine(settings),
        FailingRiskModel(),
        fallback_review_score=settings.review_threshold,
        now=lambda: decision_time,
    )
    recovered_processor = DecisionProcessor(
        store,
        RuleEngine(settings),
        FixedRiskModel(0.02),
        fallback_review_score=settings.review_threshold,
        now=lambda: decision_time,
    )

    fallback = unavailable_processor.process(payment_event)
    replay_after_recovery = recovered_processor.process(payment_event)

    assert replay_after_recovery == fallback
    assert replay_after_recovery.payload.model_version == MODEL_FALLBACK_VERSION
    assert store.calls == 1
