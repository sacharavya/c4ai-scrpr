FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY app ./app
COPY config ./config
COPY scripts ./scripts
COPY source_registry ./source_registry
COPY tests ./tests
COPY data ./data

RUN pip install --upgrade pip \
    && pip install .[dev]

ENV DATA_ROOT=/app/data

CMD ["python", "-m", "app.main", "crawl", "--type", "events", "--limit", "50"]
