FROM python:3.12-slim

WORKDIR /app

# libgomp1 is required by scikit-learn for parallel tree fitting
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p db data/raw models

CMD ["python", "scripts/ingest.py"]
