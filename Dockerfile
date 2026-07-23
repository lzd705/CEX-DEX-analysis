FROM node:22-slim AS frontend-dependencies

WORKDIR /app/dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --omit=dev

FROM python:3.13-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MARKET_DATA_DIR=/app/data/local \
    PORT=8765

RUN groupadd --system dashboard \
    && useradd --system --gid dashboard --home /app dashboard \
    && mkdir -p /app/data/local \
    && chown -R dashboard:dashboard /app/data

COPY --chown=dashboard:dashboard dashboard ./dashboard
COPY --from=frontend-dependencies /app/dashboard/node_modules /app/dashboard/node_modules

USER dashboard
EXPOSE 8765
VOLUME ["/app/data/local"]

CMD ["sh", "-c", "python3 dashboard/server.py --host 0.0.0.0 --port ${PORT:-8765}"]
