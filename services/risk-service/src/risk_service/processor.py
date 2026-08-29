from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from risk_service.features import FeatureStore
from risk_service.model import RiskModel
from risk_service.models import (
    Decision,
    PaymentCreatedEnvelope,
    RiskDecisionEnvelope,
    RiskDecisionPayload,
)
from risk_service.rules import RULE_VERSION, RuleEngine


class DecisionProcessor:
    def __init__(
        self,
        feature_store: FeatureStore,
        rule_engine: RuleEngine,
        risk_model: RiskModel,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._feature_store = feature_store
        self._rule_engine = rule_engine
        self._risk_model = risk_model
        self._now = now

    def process(self, event: PaymentCreatedEnvelope) -> RiskDecisionEnvelope:
        features = self._feature_store.observe(event, self._now())
        rule_result = self._rule_engine.evaluate(event.payload, features)
        model_result = self._risk_model.score(event.payload, features)
        model_requires_review = model_result.probability >= model_result.review_threshold

        if rule_result.decision is Decision.BLOCK:
            decision = Decision.BLOCK
        elif rule_result.decision is Decision.REVIEW or model_requires_review:
            decision = Decision.REVIEW
        else:
            decision = Decision.ALLOW

        reasons = [reason for reason in rule_result.reason_codes if reason != "NO_RISK_SIGNALS"]
        if model_requires_review:
            reasons.append("ML_HIGH_RISK")
        if not reasons:
            reasons.append("NO_RISK_SIGNALS")

        decision_id = uuid5(
            NAMESPACE_URL,
            (f"riskflow:risk.decision.completed:v2:{model_result.model_version}:{event.event_id}"),
        )

        payload = RiskDecisionPayload(
            decision_id=decision_id,
            payment_id=event.payload.payment_id,
            source_event_id=event.event_id,
            decision=decision,
            risk_score=max(rule_result.risk_score, model_result.risk_score),
            rule_score=rule_result.risk_score,
            model_score=model_result.risk_score,
            model_probability=round(model_result.probability, 6),
            model_review_threshold=model_result.review_threshold,
            reason_codes=tuple(reasons),
            rule_version=RULE_VERSION,
            model_version=model_result.model_version,
            decision_at=features.decision_at,
            features=features,
        )
        return RiskDecisionEnvelope(
            event_id=decision_id,
            event_type="risk.decision.completed",
            aggregate_id=event.aggregate_id,
            schema_version=2,
            occurred_at=features.decision_at,
            trace_id=event.trace_id,
            payload=payload,
        )
