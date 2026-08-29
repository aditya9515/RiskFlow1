from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

SYNTHETIC_GENERATOR_VERSION = "synthetic-payments-v1"


@dataclass(frozen=True)
class SyntheticDataset:
    amount_minor: NDArray[np.int64]
    velocity_5m: NDArray[np.int64]
    new_device: NDArray[np.bool_]
    cross_border: NDArray[np.bool_]
    label: NDArray[np.int8]

    @property
    def size(self) -> int:
        return len(self.label)


def generate_synthetic_dataset(samples: int, seed: int) -> SyntheticDataset:
    """Generate reproducible, fictional payments and synthetic risk labels."""
    if samples < 1_000:
        raise ValueError("samples must be at least 1000")

    random = np.random.default_rng(seed)
    amount_minor = np.rint(random.lognormal(mean=8.9, sigma=1.15, size=samples)).astype(np.int64)
    amount_minor = np.clip(amount_minor, 100, 2_000_000)

    velocity_5m = random.poisson(lam=1.2, size=samples).astype(np.int64) + 1
    velocity_burst = random.random(samples) < 0.06
    velocity_5m[velocity_burst] += random.integers(3, 10, size=int(velocity_burst.sum()))
    velocity_5m = np.clip(velocity_5m, 1, 20)

    new_device = random.random(samples) < 0.18
    cross_border_probability = 0.06 + (0.12 * new_device.astype(np.float64))
    cross_border = random.random(samples) < cross_border_probability

    log_amount = np.log1p(amount_minor / 10_000)
    high_amount = amount_minor >= 100_000
    high_velocity = velocity_5m >= 5
    new_and_cross_border = new_device & cross_border
    log_odds = (
        -4.6
        + (0.55 * log_amount)
        + (0.85 * high_amount)
        + (1.15 * high_velocity)
        + (0.12 * np.clip(velocity_5m - 4, 0, None))
        + (0.9 * new_device)
        + (1.45 * cross_border)
        + (0.9 * new_and_cross_border)
    )
    probability = 1.0 / (1.0 + np.exp(-log_odds))
    label = random.binomial(1, probability).astype(np.int8)

    return SyntheticDataset(
        amount_minor=amount_minor,
        velocity_5m=velocity_5m,
        new_device=new_device,
        cross_border=cross_border,
        label=label,
    )
