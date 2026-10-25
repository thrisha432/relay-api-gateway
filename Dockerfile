# Relay gateway and mock backend image (same image, different command).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Pinned runtime dependencies first, so this layer caches across code changes.
COPY requirements.lock ./
RUN pip install -r requirements.lock

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-deps .

COPY config ./config

# Never run as root inside the container.
RUN useradd --system --uid 10001 relay
USER relay

ENV RELAY_CONFIG=/app/config/routes.yaml
EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=2s --start-period=5s --retries=5 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health/live', timeout=1).status == 200 else 1)"

CMD ["python", "-m", "relay"]
