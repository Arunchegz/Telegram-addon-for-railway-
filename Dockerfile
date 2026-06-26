FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

RUN useradd -m -u 1001 appuser && \
    mkdir -p /tmp/tgstream && \
    chown -R appuser /app /tmp/tgstream

USER appuser
EXPOSE 8000
