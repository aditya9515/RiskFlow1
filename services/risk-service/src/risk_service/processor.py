from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from risk_service.features import FeatureStore
from risk_service.model import ModelScore, ModelScoringError, RiskModel
from risk_service.models import (
    Decision,
    PaymentCreatedEnvelope,
    RiskDecisionEnvelope,
    RiskDecisionPayload,
)
from risk_service.rules import RULE_VERSION, RuleEngine

MODEL_FALLBACK_VERSION = "ml-unavailable-review-v1"
MODEL_FALLBACK_THRESHOLD = 0.5


class DecisionProcessor:
    def __init__(
        self,
        feature_store: FeatureStore,
        rule_engine: RuleEngine,
        risk_model: RiskModel,
        fallback_review_score: int = 40,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not 1 <= fallback_review_score <= 100:
            raise ValueError("fallback_review_score must be between 1 and 100")
        self._feature_store = feature_store
        self._rule_engine = rule_engine
        self._risk_model = risk_model
        self._fallback_review_score = fallback_review_score
        self._now = now
        self._logger = logging.getLogger("risk_service.processor")

    def process(self, event: PaymentCreatedEnvelope) -> RiskDecisionEnvelope:
        cached = self._feature_store.load_decision(event.event_id)
        if cached is not None:
            return cached

        features = self._feature_store.observe(event, self._now())
        rule_result = self._rule_engine.evaluate(event.payload, features)
        try:
            model_result = self._risk_model.score(event.payload, features)
            model_unavailable = False
        except ModelScoringError:
            self._logger.exception(
                "ML scoring unavailable; applying manual-review fallback",
                extra={"event_id": str(event.event_id), "payment_id": str(event.aggregate_id)},
            )
            model_result = ModelScore(
                probability=0.0,
                risk_score=0,
                review_threshold=MODEL_FALLBACK_THRESHOLD,
                model_version=MODEL_FALLBACK_VERSION,
            )
            model_unavailable = True
        model_requires_review = model_result.probability >= model_result.review_threshold

        if rule_result.decision is Decision.BLOCK:
            decision = Decision.BLOCK
        elif model_unavailable or rule_result.decision is Decision.REVIEW or model_requires_review:
            decision = Decision.REVIEW
        else:
            decision = Decision.ALLOW

        reasons = [reason for reason in rule_result.reason_codes if reason != "NO_RISK_SIGNALS"]
        if model_requires_review:
            reasons.append("ML_HIGH_RISK")
        if model_unavailable:
            reasons.append("ML_UNAVAILABLE")
        if not reasons:
            reasons.append("NO_RISK_SIGNALS")

        decision_id = uuid5(
            NAMESPACE_URL,
            (f"riskflow:risk.decision.completed:v2:{model_result.model_version}:{event.event_id}"),
        )

        risk_score = max(rule_result.risk_score, model_result.risk_score)
        if model_unavailable:
            risk_score = max(risk_score, self._fallback_review_score)

        payload = RiskDecisionPayload(
            decision_id=decision_id,
            payment_id=event.payload.payment_id,
            source_event_id=event.event_id,
            decision=decision,
            risk_score=risk_score,
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
        decision_envelope = RiskDecisionEnvelope(
            event_id=decision_id,
            event_type="risk.decision.completed",
            aggregate_id=event.aggregate_id,
            schema_version=2,
            occurred_at=features.decision_at,
            trace_id=event.trace_id,
            payload=payload,
        )
        return self._feature_store.save_decision(decision_envelope)
