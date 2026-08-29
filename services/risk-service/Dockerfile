FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update \
    && apt-get install --no-install-recommends --yes libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY services/risk-service/pyproject.toml ./
COPY services/risk-service/src ./src
COPY services/risk-service/artifacts ./artifacts

RUN pip install --no-cache-dir . \
    && addgroup --system riskflow \
    && adduser --system --ingroup riskflow riskflow

USER riskflow

CMD ["python", "-m", "risk_service.main"]
