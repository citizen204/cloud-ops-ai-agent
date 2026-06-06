# ── Stage 1: dependency builder ──────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# Install only build-time tools; no dev dependencies leak into the final image.
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: runtime image ────────────────────────────────────────────────────
FROM python:3.12-slim

# Non-root user for principle-of-least-privilege.
RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# Copy pre-built packages from builder.
COPY --from=builder /install /usr/local

# Copy application source.
COPY cloud_ops_ai_agent/ ./cloud_ops_ai_agent/
COPY config.json .

# Switch to non-root user.
USER appuser

# Uvicorn listens on all interfaces; Railway / Render map $PORT automatically.
ENV PORT=8000 \
    PYTHONUNBUFFERED=1 \
    CONFIG_PATH=/app/config.json

EXPOSE $PORT

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:${PORT}/health')"

CMD ["sh", "-c", "uvicorn cloud_ops_ai_agent.api.main:app --host 0.0.0.0 --port ${PORT}"]
