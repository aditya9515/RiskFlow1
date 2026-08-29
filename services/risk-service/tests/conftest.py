from __future__ import annotations

import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from risk_service.config import Settings
from risk_service.models import PaymentCreatedEnvelope


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env({})


@pytest.fixture
def event_data() -> dict[str, Any]:
    return {
        "event_id": "10000000-0000-4000-8000-000000000001",
        "event_type": "payments.created",
        "aggregate_id": "20000000-0000-4000-8000-000000000002",
        "schema_version": 1,
        "occurred_at": "2026-08-30T08:00:00Z",
        "trace_id": "30000000-0000-4000-8000-000000000003",
        "payload": {
            "payment_id": "20000000-0000-4000-8000-000000000002",
            "customer_id": "customer-1",
            "merchant_id": "merchant-1",
            "device_id": "device-1",
            "amount_minor": 1250,
            "currency": "USD",
            "country": "IN",
            "status": "PENDING_RISK",
            "created_at": "2026-08-30T08:00:00Z",
        },
    }


@pytest.fixture
def payment_event(event_data: dict[str, Any]) -> PaymentCreatedEnvelope:
    return PaymentCreatedEnvelope.model_validate(event_data)


@pytest.fixture
def decision_time() -> datetime:
    return datetime(2026, 8, 30, 8, 0, 1, tzinfo=UTC)


@pytest.fixture
def workspace_tmp_path() -> Path:
    path = Path("build") / "pytest" / str(uuid4())
    path.mkdir(parents=True)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def with_event_id(event: PaymentCreatedEnvelope, value: int) -> PaymentCreatedEnvelope:
    event_id = UUID(int=value)
    payment_id = UUID(int=value + 1000)
    return event.model_copy(
        update={
            "event_id": event_id,
            "aggregate_id": payment_id,
            "payload": event.payload.model_copy(update={"payment_id": payment_id}),
        }
    )
