from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from risk_service.features import FeatureStore
from risk_service.models import (
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
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._feature_store = feature_store
        self._rule_engine = rule_engine
        self._now = now

    def process(self, event: PaymentCreatedEnvelope) -> RiskDecisionEnvelope:
        features = self._feature_store.observe(event, self._now())
        result = self._rule_engine.evaluate(event.payload, features)
        decision_id = uuid5(NAMESPACE_URL, f"riskflow:risk.decision.completed:{event.event_id}")

        payload = RiskDecisionPayload(
            decision_id=decision_id,
            payment_id=event.payload.payment_id,
            source_event_id=event.event_id,
            decision=result.decision,
            risk_score=result.risk_score,
            reason_codes=result.reason_codes,
            rule_version=RULE_VERSION,
            decision_at=features.decision_at,
            features=features,
        )
        return RiskDecisionEnvelope(
            event_id=decision_id,
            event_type="risk.decision.completed",
            aggregate_id=event.aggregate_id,
            schema_version=1,
            occurred_at=features.decision_at,
            trace_id=event.trace_id,
            payload=payload,
        )
