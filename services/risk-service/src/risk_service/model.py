from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import xgboost as xgb

from risk_service.ml_features import FEATURE_NAMES, PREPROCESSING_VERSION, transform_payment
from risk_service.models import FeatureSnapshot, PaymentCreatedPayload


class ModelArtifactError(ValueError):
    """Raised when a model and its metadata do not form a valid artifact."""


class ModelScoringError(RuntimeError):
    """Raised when a loaded model cannot score a payment safely."""


@dataclass(frozen=True)
class ModelScore:
    probability: float
    risk_score: int
    review_threshold: float
    model_version: str


class RiskModel(Protocol):
    def score(self, payment: PaymentCreatedPayload, features: FeatureSnapshot) -> ModelScore: ...


class UnavailableRiskModel:
    """Signals that model inference is unavailable so policy can fail safely."""

    def score(self, _payment: PaymentCreatedPayload, _features: FeatureSnapshot) -> ModelScore:
        raise ModelScoringError("risk model is unavailable")


class XGBoostRiskModel:
    def __init__(self, model_path: Path, metadata_path: Path) -> None:
        metadata = _load_metadata(metadata_path)
        _verify_artifact(model_path, metadata)

        if metadata.get("schema_version") != 1:
            raise ModelArtifactError("model metadata schema version is unsupported")
        self._model_version = _required_text(metadata, "model_version")
        self._review_threshold = _required_probability(metadata, "review_threshold")
        self._best_iteration = _required_non_negative_int(metadata, "best_iteration")
        if metadata.get("preprocessing_version") != PREPROCESSING_VERSION:
            raise ModelArtifactError("model preprocessing version is unsupported")
        if tuple(metadata.get("feature_names", ())) != FEATURE_NAMES:
            raise ModelArtifactError("model feature ordering is unsupported")

        self._booster = xgb.Booster()
        try:
            self._booster.load_model(model_path)
        except xgb.XGBoostError as error:
            raise ModelArtifactError(f"load XGBoost artifact: {error}") from error

    @property
    def model_version(self) -> str:
        return self._model_version

    @property
    def review_threshold(self) -> float:
        return self._review_threshold

    def score(self, payment: PaymentCreatedPayload, features: FeatureSnapshot) -> ModelScore:
        try:
            matrix = xgb.DMatrix(
                transform_payment(payment, features), feature_names=list(FEATURE_NAMES)
            )
            prediction = self._booster.predict(
                matrix,
                iteration_range=(0, self._best_iteration + 1),
            )
        except Exception as error:
            raise ModelScoringError("XGBoost scoring failed") from error
        probability = float(prediction[0])
        return ModelScore(
            probability=probability,
            risk_score=min(100, max(0, round(probability * 100))),
            review_threshold=self._review_threshold,
            model_version=self._model_version,
        )


def _load_metadata(path: Path) -> dict[str, object]:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ModelArtifactError(f"load model metadata: {error}") from error
    if not isinstance(decoded, dict):
        raise ModelArtifactError("model metadata must be a JSON object")
    return decoded


def _verify_artifact(path: Path, metadata: dict[str, object]) -> None:
    try:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as error:
        raise ModelArtifactError(f"read model artifact: {error}") from error
    if metadata.get("artifact_sha256") != digest:
        raise ModelArtifactError("model artifact SHA-256 does not match metadata")


def _required_text(metadata: dict[str, object], key: str) -> str:
    value = metadata.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ModelArtifactError(f"model metadata {key} must be non-empty text")
    return value


def _required_probability(metadata: dict[str, object], key: str) -> float:
    value = metadata.get(key)
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise ModelArtifactError(f"model metadata {key} must be numeric")
    probability = float(value)
    if not 0 < probability < 1:
        raise ModelArtifactError(f"model metadata {key} must be between 0 and 1")
    return probability


def _required_non_negative_int(metadata: dict[str, object], key: str) -> int:
    value = metadata.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ModelArtifactError(f"model metadata {key} must be a non-negative integer")
    return value
