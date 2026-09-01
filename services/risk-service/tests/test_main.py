from __future__ import annotations

import logging
from dataclasses import replace

from risk_service.config import Settings
from risk_service.main import load_risk_model
from risk_service.model import UnavailableRiskModel, XGBoostRiskModel


def test_missing_model_artifact_enables_review_fallback(
    settings: Settings, workspace_tmp_path
) -> None:
    missing = workspace_tmp_path / "missing-model.json"
    fallback = load_risk_model(
        replace(settings, model_path=missing, model_metadata_path=missing),
        logging.getLogger("test.risk-service"),
    )

    assert isinstance(fallback, UnavailableRiskModel)


def test_valid_model_artifact_still_loads(settings: Settings) -> None:
    model = load_risk_model(settings, logging.getLogger("test.risk-service"))

    assert isinstance(model, XGBoostRiskModel)
