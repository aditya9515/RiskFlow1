from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import xgboost as xgb
from numpy.typing import NDArray
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

from risk_service.ml_features import FEATURE_NAMES, PREPROCESSING_VERSION, transform_arrays
from risk_service.synthetic_data import SYNTHETIC_GENERATOR_VERSION, generate_synthetic_dataset

MODEL_VERSION = "xgb-synthetic-v1"
MODEL_FILENAME = "risk_model_xgb_synthetic_v1.json"
METADATA_FILENAME = "risk_model_xgb_synthetic_v1.metadata.json"


@dataclass(frozen=True)
class TrainingConfig:
    samples: int = 50_000
    seed: int = 20260830
    false_negative_cost: float = 500.0
    false_positive_cost: float = 25.0
    boosting_rounds: int = 300
    early_stopping_rounds: int = 25


def train(output_directory: Path, config: TrainingConfig) -> dict[str, Any]:
    dataset = generate_synthetic_dataset(config.samples, config.seed)
    features = transform_arrays(
        dataset.amount_minor,
        dataset.velocity_5m,
        dataset.new_device,
        dataset.cross_border,
    )
    train_indices, validation_indices, test_indices = _split_indices(dataset.label, config.seed)

    train_matrix = _matrix(features[train_indices], dataset.label[train_indices])
    validation_matrix = _matrix(features[validation_indices], dataset.label[validation_indices])
    test_matrix = _matrix(features[test_indices], dataset.label[test_indices])
    parameters: dict[str, Any] = {
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "eta": 0.05,
        "max_depth": 4,
        "min_child_weight": 5,
        "subsample": 0.85,
        "colsample_bytree": 0.9,
        "lambda": 2.0,
        "alpha": 0.1,
        "tree_method": "hist",
        "seed": config.seed,
        "nthread": 1,
    }
    booster = xgb.train(
        parameters,
        train_matrix,
        num_boost_round=config.boosting_rounds,
        evals=[(validation_matrix, "validation")],
        early_stopping_rounds=config.early_stopping_rounds,
        verbose_eval=False,
    )
    best_iteration = booster.best_iteration
    prediction_range = (0, best_iteration + 1)
    validation_probabilities = booster.predict(validation_matrix, iteration_range=prediction_range)
    review_threshold, validation_cost = select_review_threshold(
        dataset.label[validation_indices],
        validation_probabilities,
        config.false_negative_cost,
        config.false_positive_cost,
    )
    test_probabilities = booster.predict(test_matrix, iteration_range=prediction_range)

    output_directory.mkdir(parents=True, exist_ok=True)
    model_path = output_directory / MODEL_FILENAME
    metadata_path = output_directory / METADATA_FILENAME
    booster.save_model(model_path)
    artifact_sha256 = hashlib.sha256(model_path.read_bytes()).hexdigest()

    validation_metrics = calculate_metrics(
        dataset.label[validation_indices], validation_probabilities, review_threshold
    )
    test_metrics = calculate_metrics(
        dataset.label[test_indices], test_probabilities, review_threshold
    )
    test_cost = calculate_business_cost(
        test_metrics["confusion_matrix"],
        config.false_negative_cost,
        config.false_positive_cost,
    )
    metadata: dict[str, Any] = {
        "schema_version": 1,
        "model_version": MODEL_VERSION,
        "model_type": "XGBoost binary classifier",
        "artifact_sha256": artifact_sha256,
        "preprocessing_version": PREPROCESSING_VERSION,
        "feature_names": list(FEATURE_NAMES),
        "best_iteration": best_iteration,
        "review_threshold": round(review_threshold, 6),
        "training": {
            "data_source": "synthetic",
            "synthetic_data_notice": (
                "Metrics describe reproducible fictional data and are not banking performance."
            ),
            "generator_version": SYNTHETIC_GENERATOR_VERSION,
            "seed": config.seed,
            "samples": dataset.size,
            "split": {
                "train": len(train_indices),
                "validation": len(validation_indices),
                "test": len(test_indices),
            },
            "positive_rate": round(float(dataset.label.mean()), 6),
        },
        "model_parameters": parameters,
        "threshold_selection": {
            "selected_on": "validation",
            "false_negative_cost": config.false_negative_cost,
            "false_positive_cost": config.false_positive_cost,
            "validation_cost": round(validation_cost, 6),
        },
        "validation_metrics": validation_metrics,
        "test_metrics": {
            **test_metrics,
            "business_cost": round(test_cost, 6),
            "business_cost_per_1000": round(test_cost / len(test_indices) * 1000, 6),
        },
        "training_config": asdict(config),
    }
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return metadata


def select_review_threshold(
    labels: NDArray[np.int8],
    probabilities: NDArray[np.floating],
    false_negative_cost: float,
    false_positive_cost: float,
) -> tuple[float, float]:
    if false_negative_cost <= 0 or false_positive_cost <= 0:
        raise ValueError("business costs must be positive")

    candidates = np.linspace(0.01, 0.99, 99)
    best_threshold = 0.5
    best_cost = float("inf")
    for threshold in candidates:
        predicted = probabilities >= threshold
        false_negative = int(((labels == 1) & ~predicted).sum())
        false_positive = int(((labels == 0) & predicted).sum())
        cost = (false_negative * false_negative_cost) + (false_positive * false_positive_cost)
        if cost < best_cost:
            best_threshold = float(threshold)
            best_cost = float(cost)
    return best_threshold, best_cost


def calculate_metrics(
    labels: NDArray[np.int8],
    probabilities: NDArray[np.floating],
    threshold: float,
) -> dict[str, Any]:
    predicted = probabilities >= threshold
    matrix = confusion_matrix(labels, predicted, labels=[0, 1])
    true_negative, false_positive, false_negative, true_positive = (
        int(value) for value in matrix.ravel()
    )
    return {
        "precision": round(float(precision_score(labels, predicted, zero_division=0)), 6),
        "recall": round(float(recall_score(labels, predicted, zero_division=0)), 6),
        "f1": round(float(f1_score(labels, predicted, zero_division=0)), 6),
        "pr_auc": round(float(average_precision_score(labels, probabilities)), 6),
        "roc_auc": round(float(roc_auc_score(labels, probabilities)), 6),
        "confusion_matrix": {
            "true_negative": true_negative,
            "false_positive": false_positive,
            "false_negative": false_negative,
            "true_positive": true_positive,
        },
    }


def calculate_business_cost(
    matrix: dict[str, int], false_negative_cost: float, false_positive_cost: float
) -> float:
    return (matrix["false_negative"] * false_negative_cost) + (
        matrix["false_positive"] * false_positive_cost
    )


def _split_indices(
    labels: NDArray[np.int8], seed: int
) -> tuple[NDArray[np.int64], NDArray[np.int64], NDArray[np.int64]]:
    indices = np.arange(len(labels), dtype=np.int64)
    train_validation, test = train_test_split(
        indices,
        test_size=0.2,
        random_state=seed,
        stratify=labels,
    )
    train, validation = train_test_split(
        train_validation,
        test_size=0.25,
        random_state=seed,
        stratify=labels[train_validation],
    )
    return train, validation, test


def _matrix(features: NDArray[np.float32], labels: NDArray[np.int8]) -> xgb.DMatrix:
    return xgb.DMatrix(features, label=labels, feature_names=list(FEATURE_NAMES))


def main() -> int:
    parser = argparse.ArgumentParser(description="Train RiskFlow's synthetic XGBoost model")
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts"))
    parser.add_argument("--samples", type=int, default=50_000)
    parser.add_argument("--seed", type=int, default=20260830)
    arguments = parser.parse_args()

    metadata = train(
        arguments.output_dir,
        TrainingConfig(samples=arguments.samples, seed=arguments.seed),
    )
    summary = {
        "model_version": metadata["model_version"],
        "review_threshold": metadata["review_threshold"],
        "test_metrics": metadata["test_metrics"],
        "synthetic_data_notice": metadata["training"]["synthetic_data_notice"],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
