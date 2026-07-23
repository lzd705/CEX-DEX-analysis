# Public deployment boundary

Market Monitor is read-only. It has no browser endpoint for changing data, no
administrator form, and no persistent user state.

Production should run the application container separately from its data:

```bash
docker build -t cex-dex-market-monitor .
docker run --rm \
  -p 8765:8765 \
  --mount type=bind,src=/srv/cex-dex/current,dst=/app/data/local,readonly \
  cex-dex-market-monitor
```

The mounted directory must contain:

- `cex_exchange_volume_daily.csv`
- `dex_pool_volume_daily.csv`

Deploy a new application commit without changing the data directory. Publish a
new reviewed data snapshot by atomically switching `/srv/cex-dex/current` to a
new version and restarting the container. Keep prior snapshot directories for
rollback.

The server exposes only static files, `GET /api/market`, and `GET /health`.
Security headers block framing, external scripts, device permissions, and
cross-origin content. A reverse proxy should provide HTTPS, access logs, rate
limits, and the public domain.
