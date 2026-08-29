import numpy as np

from risk_service.ml_features import FEATURE_NAMES, transform_arrays, transform_payment
from risk_service.models import FeatureSnapshot, PaymentCreatedEnvelope


def test_online_and_batch_preprocessing_use_same_order(
    payment_event: PaymentCreatedEnvelope, decision_time
) -> None:
    snapshot = FeatureSnapshot(
        velocity_5m=6,
        new_device=True,
        cross_border=False,
        baseline_country="IN",
        decision_at=decision_time,
    )

    online = transform_payment(payment_event.payload, snapshot)
    batch = transform_arrays(
        np.asarray([payment_event.payload.amount_minor], dtype=np.int64),
        np.asarray([6], dtype=np.int64),
        np.asarray([True], dtype=np.bool_),
        np.asarray([False], dtype=np.bool_),
    )

    assert FEATURE_NAMES == (
        "log_amount_minor",
        "velocity_5m",
        "new_device",
        "cross_border",
    )
    np.testing.assert_array_equal(online, batch)


def test_preprocessing_rejects_different_array_lengths() -> None:
    with np.testing.assert_raises_regex(ValueError, "same length"):
        transform_arrays(
            np.asarray([100, 200], dtype=np.int64),
            np.asarray([1], dtype=np.int64),
            np.asarray([False], dtype=np.bool_),
            np.asarray([False], dtype=np.bool_),
        )
