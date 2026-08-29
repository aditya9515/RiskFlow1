from pathlib import Path

import numpy as np
import pytest

from risk_service.model import XGBoostRiskModel
from risk_service.training import (
    METADATA_FILENAME,
    MODEL_FILENAME,
    TrainingConfig,
    calculate_metrics,
    select_review_threshold,
    train,
)


def test_threshold_minimizes_supplied_business_cost() -> None:
    labels = np.asarray([0, 0, 0, 1, 1], dtype=np.int8)
    probabilities = np.asarray([0.01, 0.2, 0.4, 0.3, 0.8], dtype=np.float32)

    threshold, cost = select_review_threshold(
        labels,
        probabilities,
        false_negative_cost=100,
        false_positive_cost=10,
    )

    assert threshold == pytest.approx(0.21)
    assert cost == 10


def test_metric_confusion_matrix_has_explicit_labels() -> None:
    labels = np.asarray([0, 0, 1, 1], dtype=np.int8)
    probabilities = np.asarray([0.1, 0.9, 0.2, 0.8], dtype=np.float32)

    metrics = calculate_metrics(labels, probabilities, threshold=0.5)

    assert metrics["confusion_matrix"] == {
        "true_negative": 1,
        "false_positive": 1,
        "false_negative": 1,
        "true_positive": 1,
    }


def test_small_training_run_creates_loadable_versioned_artifact(
    workspace_tmp_path: Path,
) -> None:
    metadata = train(
        workspace_tmp_path,
        TrainingConfig(
            samples=2_000,
            seed=321,
            boosting_rounds=30,
            early_stopping_rounds=5,
        ),
    )

    assert metadata["training"]["split"] == {
        "train": 1200,
        "validation": 400,
        "test": 400,
    }
    assert metadata["training"]["data_source"] == "synthetic"
    XGBoostRiskModel(
        workspace_tmp_path / MODEL_FILENAME,
        workspace_tmp_path / METADATA_FILENAME,
    )
