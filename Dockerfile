FROM node:22-slim AS frontend-dependencies

WORKDIR /app/dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --omit=dev

FROM python:3.13-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8765

RUN groupadd --system dashboard \
    && useradd --system --gid dashboard --home /app dashboard

COPY --chown=dashboard:dashboard dashboard ./dashboard
COPY --chown=dashboard:dashboard data/public ./data/public
COPY --from=frontend-dependencies /app/dashboard/node_modules /app/dashboard/node_modules

USER dashboard
EXPOSE 8765

CMD ["sh", "-c", "python3 dashboard/server.py --public --host 0.0.0.0 --port ${PORT:-8765}"]
