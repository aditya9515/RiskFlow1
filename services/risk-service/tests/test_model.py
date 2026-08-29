from __future__ import annotations

import json
from pathlib import Path

import pytest

from risk_service.model import ModelArtifactError, XGBoostRiskModel
from risk_service.models import FeatureSnapshot, PaymentCreatedEnvelope

ARTIFACTS = Path(__file__).parents[1] / "artifacts"
MODEL_PATH = ARTIFACTS / "risk_model_xgb_synthetic_v1.json"
METADATA_PATH = ARTIFACTS / "risk_model_xgb_synthetic_v1.metadata.json"


def test_versioned_model_artifact_loads_and_scores_deterministically(
    payment_event: PaymentCreatedEnvelope, decision_time
) -> None:
    model = XGBoostRiskModel(MODEL_PATH, METADATA_PATH)
    features = FeatureSnapshot(
        velocity_5m=1,
        new_device=True,
        cross_border=False,
        baseline_country="IN",
        decision_at=decision_time,
    )

    first = model.score(payment_event.payload, features)
    second = model.score(payment_event.payload, features)

    assert first == second
    assert 0 <= first.probability <= 1
    assert first.model_version == "xgb-synthetic-v1"
    assert first.review_threshold == 0.05


def test_artifact_checksum_mismatch_is_rejected(workspace_tmp_path: Path) -> None:
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    metadata["artifact_sha256"] = "0" * 64
    invalid_metadata = workspace_tmp_path / "metadata.json"
    invalid_metadata.write_text(json.dumps(metadata), encoding="utf-8")

    with pytest.raises(ModelArtifactError, match="SHA-256"):
        XGBoostRiskModel(MODEL_PATH, invalid_metadata)
