from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Protocol

from redis import Redis

from risk_service.models import FeatureSnapshot, PaymentCreatedEnvelope


class FeatureStore(Protocol):
    def observe(self, event: PaymentCreatedEnvelope, decision_at: datetime) -> FeatureSnapshot: ...


class RedisFeatureStore:
    """Atomically deduplicates events and updates online customer features."""

    _OBSERVE_SCRIPT = """
local cached = redis.call('GET', KEYS[1])
if cached then
    return cached
end

local event_time_ms = tonumber(ARGV[1])
local window_start_ms = event_time_ms - tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', window_start_ms - 1)
redis.call('ZADD', KEYS[2], event_time_ms, ARGV[3])
local velocity = redis.call('ZCOUNT', KEYS[2], window_start_ms, event_time_ms)

local new_device = redis.call('SADD', KEYS[3], ARGV[4]) == 1
local baseline_country = redis.call('GET', KEYS[4])
if not baseline_country then
    baseline_country = ARGV[5]
    redis.call('SET', KEYS[4], baseline_country)
end
local cross_border = baseline_country ~= ARGV[5]

local snapshot = cjson.encode({
    velocity_5m = velocity,
    new_device = new_device,
    cross_border = cross_border,
    baseline_country = baseline_country,
    decision_at = ARGV[6]
})

redis.call('SETEX', KEYS[1], tonumber(ARGV[7]), snapshot)
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[8]))
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[8]))
redis.call('EXPIRE', KEYS[4], tonumber(ARGV[8]))
return snapshot
"""

    def __init__(
        self,
        client: Redis,
        key_prefix: str,
        velocity_window_seconds: int,
        event_cache_ttl_seconds: int,
        feature_state_ttl_seconds: int,
    ) -> None:
        self._client = client
        self._key_prefix = key_prefix.rstrip(":")
        self._velocity_window_ms = velocity_window_seconds * 1000
        self._event_cache_ttl_seconds = event_cache_ttl_seconds
        self._feature_state_ttl_seconds = feature_state_ttl_seconds
        self._observe_script = client.register_script(self._OBSERVE_SCRIPT)

    def ping(self) -> None:
        if not self._client.ping():
            raise ConnectionError("Redis PING did not return true")

    def observe(self, event: PaymentCreatedEnvelope, decision_at: datetime) -> FeatureSnapshot:
        payment = event.payload
        customer_prefix = f"{self._key_prefix}:customer:{payment.customer_id}"
        keys = (
            f"{self._key_prefix}:event:{event.event_id}",
            f"{customer_prefix}:velocity",
            f"{customer_prefix}:devices",
            f"{customer_prefix}:baseline_country",
        )
        occurred_at_ms = int(event.occurred_at.timestamp() * 1000)
        normalized_decision_at = decision_at.astimezone(UTC).isoformat().replace("+00:00", "Z")

        encoded = self._observe_script(
            keys=keys,
            args=(
                occurred_at_ms,
                self._velocity_window_ms,
                str(payment.payment_id),
                payment.device_id,
                payment.country,
                normalized_decision_at,
                self._event_cache_ttl_seconds,
                self._feature_state_ttl_seconds,
            ),
        )
        if isinstance(encoded, bytes):
            encoded = encoded.decode("utf-8")
        return FeatureSnapshot.model_validate(json.loads(encoded))

    def close(self) -> None:
        self._client.close()
