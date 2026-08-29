from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator, model_validator

NonEmptyText = Annotated[str, Field(min_length=1, max_length=255)]
Currency = Annotated[str, Field(pattern=r"^[A-Z]{3}$")]
Country = Annotated[str, Field(pattern=r"^[A-Z]{2}$")]


class EventModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class PaymentCreatedPayload(EventModel):
    payment_id: UUID
    customer_id: NonEmptyText
    merchant_id: NonEmptyText
    device_id: NonEmptyText
    amount_minor: Annotated[StrictInt, Field(gt=0)]
    currency: Currency
    country: Country
    status: Literal["PENDING_RISK"]
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def created_at_must_have_timezone(cls, value: datetime) -> datetime:
        return _as_utc(value, "created_at")


class PaymentCreatedEnvelope(EventModel):
    event_id: UUID
    event_type: Literal["payments.created"]
    aggregate_id: UUID
    schema_version: Literal[1]
    occurred_at: datetime
    trace_id: UUID
    payload: PaymentCreatedPayload

    @field_validator("occurred_at")
    @classmethod
    def occurred_at_must_have_timezone(cls, value: datetime) -> datetime:
        return _as_utc(value, "occurred_at")

    @model_validator(mode="after")
    def aggregate_must_match_payment(self) -> PaymentCreatedEnvelope:
        if self.aggregate_id != self.payload.payment_id:
            raise ValueError("aggregate_id must match payload.payment_id")
        return self


class FeatureSnapshot(EventModel):
    velocity_5m: Annotated[StrictInt, Field(ge=1)]
    new_device: bool
    cross_border: bool
    baseline_country: Country
    decision_at: datetime

    @field_validator("decision_at")
    @classmethod
    def decision_at_must_have_timezone(cls, value: datetime) -> datetime:
        return _as_utc(value, "decision_at")


class Decision(StrEnum):
    ALLOW = "ALLOW"
    REVIEW = "REVIEW"
    BLOCK = "BLOCK"


class RiskDecisionPayload(EventModel):
    decision_id: UUID
    payment_id: UUID
    source_event_id: UUID
    decision: Decision
    risk_score: Annotated[StrictInt, Field(ge=0, le=100)]
    reason_codes: Annotated[tuple[str, ...], Field(min_length=1)]
    rule_version: Literal["rules-v1"]
    decision_at: datetime
    features: FeatureSnapshot

    @field_validator("decision_at")
    @classmethod
    def decision_at_must_have_timezone(cls, value: datetime) -> datetime:
        return _as_utc(value, "decision_at")


class RiskDecisionEnvelope(EventModel):
    event_id: UUID
    event_type: Literal["risk.decision.completed"]
    aggregate_id: UUID
    schema_version: Literal[1]
    occurred_at: datetime
    trace_id: UUID
    payload: RiskDecisionPayload

    @model_validator(mode="after")
    def envelope_must_match_decision(self) -> RiskDecisionEnvelope:
        if self.event_id != self.payload.decision_id:
            raise ValueError("event_id must match payload.decision_id")
        if self.aggregate_id != self.payload.payment_id:
            raise ValueError("aggregate_id must match payload.payment_id")
        if self.occurred_at != self.payload.decision_at:
            raise ValueError("occurred_at must match payload.decision_at")
        return self


class InvalidEventPayload(EventModel):
    source_topic: NonEmptyText
    source_partition: StrictInt
    source_offset: StrictInt
    error_code: Literal["invalid_event"]
    error_message: NonEmptyText
    record_sha256: Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


class InvalidEventEnvelope(EventModel):
    event_id: UUID
    event_type: Literal["risk.input.rejected"]
    aggregate_id: UUID
    schema_version: Literal[1]
    occurred_at: datetime
    trace_id: UUID
    payload: InvalidEventPayload


def parse_payment_created(value: bytes) -> PaymentCreatedEnvelope:
    return PaymentCreatedEnvelope.model_validate_json(value)


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone")
    return value.astimezone(UTC)
