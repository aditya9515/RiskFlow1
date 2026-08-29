import numpy as np
import pytest

from risk_service.synthetic_data import generate_synthetic_dataset


def test_same_seed_generates_identical_dataset() -> None:
    first = generate_synthetic_dataset(2_000, 1234)
    second = generate_synthetic_dataset(2_000, 1234)

    np.testing.assert_array_equal(first.amount_minor, second.amount_minor)
    np.testing.assert_array_equal(first.velocity_5m, second.velocity_5m)
    np.testing.assert_array_equal(first.new_device, second.new_device)
    np.testing.assert_array_equal(first.cross_border, second.cross_border)
    np.testing.assert_array_equal(first.label, second.label)


def test_different_seed_changes_dataset() -> None:
    first = generate_synthetic_dataset(2_000, 1234)
    second = generate_synthetic_dataset(2_000, 5678)

    assert not np.array_equal(first.amount_minor, second.amount_minor)


def test_small_dataset_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least 1000"):
        generate_synthetic_dataset(999, 1234)
