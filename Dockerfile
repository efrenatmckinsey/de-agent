# hydra-ingest worker image — used by docker-compose (infra/) and ECS/Kubernetes in cloud paths.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt pyproject.toml ./
COPY src ./src
COPY config ./config

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Default: idle container for local stacks; override in orchestration with your worker command.
CMD ["sleep", "infinity"]
