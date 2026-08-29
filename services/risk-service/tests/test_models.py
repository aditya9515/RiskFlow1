from copy import deepcopy
from typing import Any

import pytest
from pydantic import ValidationError

from risk_service.models import PaymentCreatedEnvelope


def test_valid_payment_event_is_normalized_to_utc(event_data: dict[str, Any]) -> None:
    event = PaymentCreatedEnvelope.model_validate(event_data)

    assert event.payload.currency == "USD"
    assert event.occurred_at.utcoffset().total_seconds() == 0
    assert event.aggregate_id == event.payload.payment_id


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("event_type",), "payment.created"),
        (("schema_version",), 2),
        (("payload", "amount_minor"), "1250"),
        (("payload", "currency"), "usd"),
        (("payload", "country"), "IND"),
        (("payload", "status"), "CREATED"),
        (("occurred_at",), "2026-08-30T08:00:00"),
    ],
)
def test_invalid_contract_fields_are_rejected(
    event_data: dict[str, Any], path: tuple[str, ...], value: object
) -> None:
    invalid = deepcopy(event_data)
    target = invalid
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value

    with pytest.raises(ValidationError):
        PaymentCreatedEnvelope.model_validate(invalid)


def test_aggregate_id_must_match_payment_id(event_data: dict[str, Any]) -> None:
    event_data["aggregate_id"] = "40000000-0000-4000-8000-000000000004"

    with pytest.raises(ValidationError, match="aggregate_id must match"):
        PaymentCreatedEnvelope.model_validate(event_data)


def test_unknown_fields_are_rejected(event_data: dict[str, Any]) -> None:
    event_data["unexpected"] = True

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        PaymentCreatedEnvelope.model_validate(event_data)
