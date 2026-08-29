from datetime import datetime

from risk_service.config import Settings
from risk_service.models import FeatureSnapshot, PaymentCreatedEnvelope
from risk_service.processor import DecisionProcessor
from risk_service.rules import RuleEngine


class CachedFeatureStore:
    def __init__(self, snapshot: FeatureSnapshot) -> None:
        self.snapshot = snapshot
        self.calls = 0

    def observe(self, _event: PaymentCreatedEnvelope, _decision_at: datetime) -> FeatureSnapshot:
        self.calls += 1
        return self.snapshot


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
    processor = DecisionProcessor(store, RuleEngine(settings), now=lambda: decision_time)

    first = processor.process(payment_event)
    replay = processor.process(payment_event)

    assert first == replay
    assert first.event_id == first.payload.decision_id
    assert first.payload.source_event_id == payment_event.event_id
    assert first.payload.rule_version == "rules-v1"
    assert store.calls == 2
