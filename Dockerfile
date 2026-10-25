# Stage 1: build the React dashboard
FROM node:26-alpine AS dashboard
WORKDIR /ui
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY dashboard/ ./
RUN npm run build

# Stage 2: Python runtime with the jobq package, the demo app and the built UI
FROM python:3.11-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app
RUN addgroup --system jobq && adduser --system --ingroup jobq jobq
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir .
COPY examples ./examples
COPY --from=dashboard /ui/dist ./dashboard/dist
USER jobq
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=2).status == 200 else 1)" || exit 1
CMD ["sh", "-c", "exec jobq dashboard --app examples.demo_app:app --port ${PORT:-8000}"]
