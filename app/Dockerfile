# =============================================================================
# Stage 1: Build React/Vite frontend
# =============================================================================
FROM node:20-alpine AS frontend-builder

WORKDIR /workspace/frontend

# Install dependencies first (better layer caching)
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci --prefer-offline

# Copy source and build
COPY frontend/ ./
RUN npm run build

# =============================================================================
# Stage 2: Python runtime
# =============================================================================
FROM python:3.11-slim AS runtime

# ── OS-level hardening & utilities ───────────────────────────────────────────
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── Non-root user (required for Azure Government App Service) ─────────────────
RUN groupadd --gid 1001 appgroup \
    && useradd --uid 1001 --gid appgroup --shell /bin/bash --create-home appuser

# ── Application directory ─────────────────────────────────────────────────────
WORKDIR /app

# Copy Python requirements and install before copying source (cache layer)
COPY backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy backend source
COPY backend/ ./backend/

# Copy built frontend into the location the FastAPI app expects
COPY --from=frontend-builder /workspace/frontend/dist ./frontend/dist

# ── Routes package init (ensure importable) ───────────────────────────────────
RUN test -f ./backend/routes/__init__.py || touch ./backend/routes/__init__.py

# ── Ownership ─────────────────────────────────────────────────────────────────
RUN chown -R appuser:appgroup /app

USER appuser

# ── Environment defaults (override via App Service / docker run -e) ───────────
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8000 \
    DEMO_MODE=false \
    AUTH_ENABLED=false

EXPOSE 8000

# ── Health check ──────────────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

# ── Entrypoint ────────────────────────────────────────────────────────────────
CMD ["python", "-m", "uvicorn", "backend.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "2", \
     "--log-level", "info"]
