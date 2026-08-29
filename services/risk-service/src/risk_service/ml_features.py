from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

from risk_service.models import FeatureSnapshot, PaymentCreatedPayload

PREPROCESSING_VERSION = "ml-features-v1"
FEATURE_NAMES = (
    "log_amount_minor",
    "velocity_5m",
    "new_device",
    "cross_border",
)
MAX_AMOUNT_MINOR = 2_000_000
MAX_VELOCITY_5M = 20


def transform_arrays(
    amount_minor: NDArray[np.integer],
    velocity_5m: NDArray[np.integer],
    new_device: NDArray[np.bool_],
    cross_border: NDArray[np.bool_],
) -> NDArray[np.float32]:
    lengths = {len(amount_minor), len(velocity_5m), len(new_device), len(cross_border)}
    if len(lengths) != 1:
        raise ValueError("all feature arrays must have the same length")

    clipped_amount = np.clip(amount_minor, 1, MAX_AMOUNT_MINOR)
    clipped_velocity = np.clip(velocity_5m, 1, MAX_VELOCITY_5M)
    matrix = np.column_stack(
        (
            np.log1p(clipped_amount),
            clipped_velocity,
            new_device,
            cross_border,
        )
    )
    return matrix.astype(np.float32, copy=False)


def transform_payment(
    payment: PaymentCreatedPayload, features: FeatureSnapshot
) -> NDArray[np.float32]:
    return transform_arrays(
        np.asarray([payment.amount_minor], dtype=np.int64),
        np.asarray([features.velocity_5m], dtype=np.int64),
        np.asarray([features.new_device], dtype=np.bool_),
        np.asarray([features.cross_border], dtype=np.bool_),
    )
