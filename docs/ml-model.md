# RiskFlow synthetic XGBoost model

## Scope and warning

`xgb-synthetic-v1` is an engineering demonstration trained entirely on reproducible fictional payments. Its metrics do not measure banking, card-network, merchant, or production fraud performance. It must not be described as a production fraud model.

The model is used as a second decision layer. It may escalate an otherwise allowed payment to `REVIEW`; it cannot directly produce `BLOCK`. Deterministic rules retain authority to block clear policy violations.

## Reproducible data

`synthetic-payments-v1` uses NumPy's seeded random generator with seed `20260830`. It generates 50,000 fictional payments with a deliberately imbalanced synthetic risk label. The generator models correlations between amount, short-term velocity, new devices, and cross-border activity, followed by seeded Bernoulli sampling.

No real customer, merchant, device, transaction, or banking data is present. Generated rows are not committed because the source code and seed recreate them exactly.

The stratified split is performed before model training:

| Split | Rows | Purpose |
| --- | ---: | --- |
| Train | 30,000 | Fit XGBoost trees |
| Validation | 10,000 | Early stopping and threshold selection |
| Test | 10,000 | Final untouched evaluation |

The synthetic positive-label rate is `3.628%`.

## Preprocessing and feature contract

Training and online scoring both call `ml-features-v1`, preventing training-serving feature drift. Feature ordering is fixed:

1. `log_amount_minor` — `log1p` of integer minor units, clipped at 2,000,000.
2. `velocity_5m` — customer payments observed in five minutes, clipped to 20.
3. `new_device` — whether Redis has not previously seen the device for the customer.
4. `cross_border` — whether the country differs from the customer's first observed country.

The first observed country is an online heuristic, not verified residency. Redis feature state expires after 30 days by default.

## Threshold and business cost

The review threshold is selected only on validation predictions. The fictional cost assumptions are:

- false negative: `500` cost units;
- false positive/manual review: `25` cost units.

Searching thresholds from `0.01` through `0.99` selected `0.05` as the lowest validation cost. These costs are project assumptions, not values supplied by a bank.

## Measured synthetic test results

These results come from the untouched 10,000-row synthetic test split:

| Metric | Measured value |
| --- | ---: |
| Precision | 0.133333 |
| Recall | 0.506887 |
| F1 | 0.211130 |
| PR-AUC | 0.198821 |
| ROC-AUC | 0.764561 |

Confusion matrix at the `0.05` review threshold:

| | Predicted low risk | Predicted review |
| --- | ---: | ---: |
| Synthetic negative | 8,441 | 1,196 |
| Synthetic positive | 179 | 184 |

Measured test cost is `119,400` cost units, or `11,940` per 1,000 synthetic payments under the stated assumptions.

## Versioned artifacts

- Model: `services/risk-service/artifacts/risk_model_xgb_synthetic_v1.json`
- Metadata: `services/risk-service/artifacts/risk_model_xgb_synthetic_v1.metadata.json`
- Model SHA-256: `3f30aaf2a4a7d720c183bab36439a7d40e92b18f82df9f8134da6fe8be32a111`
- Metadata SHA-256: `e8a0197adc0e8d0ff64e50dbe8b6daba383eb248bcbbda577691943228309602`

The service verifies the model hash, metadata schema, preprocessing version, and feature ordering before consuming Kafka records. Startup fails rather than silently using an unknown artifact.

## Reproduce training

From `services/risk-service`:

```powershell
& .\.venv\Scripts\python.exe -m risk_service.training `
    --output-dir artifacts `
    --samples 50000 `
    --seed 20260830
```

With the pinned environment and CPU training configuration, the checkpoint rerun produced byte-identical model and metadata files.

## Limitations and next improvements

- Labels are synthetic and inherit the generator's assumptions.
- The model uses only four online features; it has no merchant history, customer profile, failure history, or graph signals.
- Probability calibration has not been evaluated on real data.
- Drift detection, model registry promotion, shadow evaluation, and champion/challenger controls remain future work.
- A real deployment would require representative governed data, privacy review, bias analysis, monitoring, validation, and formal model-risk approval.
