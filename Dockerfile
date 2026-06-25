FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Non-root for security
RUN useradd -m -u 1001 appuser && chown -R appuser /app
USER appuser

EXPOSE 8000

CMD ["gunicorn", "main:app", "--worker-class", "uvicorn.workers.UvicornWorker", "--workers", "2", "--bind", "0.0.0.0:8000"]