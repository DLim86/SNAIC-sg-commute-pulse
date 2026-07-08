FROM python:3.12-slim

WORKDIR /app

# libgomp1 is required by scikit-learn for parallel tree fitting
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project

COPY . .

ENV PATH="/app/.venv/bin:$PATH"

RUN mkdir -p db data/raw models

CMD ["python", "scripts/ingest.py"]
