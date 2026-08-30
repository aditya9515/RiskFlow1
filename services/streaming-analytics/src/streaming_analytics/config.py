from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath

_TOPIC_PATTERN = re.compile(r"^[a-zA-Z0-9._-]{1,249}$")
_TRIGGER_PATTERN = re.compile(r"^[1-9][0-9]* (milliseconds?|seconds?|minutes?)$")


@dataclass(frozen=True)
class Settings:
    kafka_brokers: str
    payments_topic: str
    decisions_topic: str
    output_root: str
    checkpoint_root: str
    starting_offsets: str
    trigger_interval: str
    max_offsets_per_trigger: int

    @classmethod
    def from_env(cls, environment: Mapping[str, str] | None = None) -> Settings:
        values = os.environ if environment is None else environment
        kafka_brokers = _required(values, "KAFKA_BROKERS")
        payments_topic = _topic(values.get("PAYMENTS_TOPIC", "payments.created"), "PAYMENTS_TOPIC")
        decisions_topic = _topic(
            values.get("RISK_DECISIONS_TOPIC", "risk.decisions"),
            "RISK_DECISIONS_TOPIC",
        )
        if payments_topic == decisions_topic:
            raise ValueError("PAYMENTS_TOPIC and RISK_DECISIONS_TOPIC must differ")

        output_root = _absolute_path(
            values.get("SPARK_OUTPUT_ROOT", "/var/lib/riskflow/streaming/data"),
            "SPARK_OUTPUT_ROOT",
        )
        checkpoint_root = _absolute_path(
            values.get("SPARK_CHECKPOINT_ROOT", "/var/lib/riskflow/streaming/checkpoints"),
            "SPARK_CHECKPOINT_ROOT",
        )
        if output_root == checkpoint_root:
            raise ValueError("SPARK_OUTPUT_ROOT and SPARK_CHECKPOINT_ROOT must differ")

        starting_offsets = values.get("SPARK_STARTING_OFFSETS", "earliest").strip()
        if starting_offsets not in {"earliest", "latest"}:
            raise ValueError("SPARK_STARTING_OFFSETS must be earliest or latest")

        trigger_interval = values.get("SPARK_TRIGGER_INTERVAL", "5 seconds").strip()
        if not _TRIGGER_PATTERN.fullmatch(trigger_interval):
            raise ValueError(
                "SPARK_TRIGGER_INTERVAL must be a positive integer followed by "
                "milliseconds, seconds, or minutes"
            )

        raw_max_offsets = values.get("SPARK_MAX_OFFSETS_PER_TRIGGER", "10000").strip()
        try:
            max_offsets = int(raw_max_offsets)
        except ValueError as error:
            raise ValueError("SPARK_MAX_OFFSETS_PER_TRIGGER must be an integer") from error
        if max_offsets <= 0:
            raise ValueError("SPARK_MAX_OFFSETS_PER_TRIGGER must be greater than zero")

        return cls(
            kafka_brokers=kafka_brokers,
            payments_topic=payments_topic,
            decisions_topic=decisions_topic,
            output_root=output_root,
            checkpoint_root=checkpoint_root,
            starting_offsets=starting_offsets,
            trigger_interval=trigger_interval,
            max_offsets_per_trigger=max_offsets,
        )


def _required(values: Mapping[str, str], name: str) -> str:
    value = values.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    if any(character.isspace() for character in value):
        raise ValueError(f"{name} must not contain whitespace")
    return value


def _topic(raw: str, name: str) -> str:
    value = raw.strip()
    if not _TOPIC_PATTERN.fullmatch(value):
        raise ValueError(f"{name} must be a valid Kafka topic name")
    return value


def _absolute_path(raw: str, name: str) -> str:
    value = raw.strip().rstrip("/")
    path = PurePosixPath(value)
    if not value or not path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{name} must be an absolute normalized container path")
    return value
