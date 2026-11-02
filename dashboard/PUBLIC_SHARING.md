# Public deployment boundary

Market Monitor is read-only for ordinary visitors. Administrator APIs remain
absent unless `ADMIN_ENABLED=true`; a username or password hash alone does not
expose them. Keep `ADMIN_ENABLED=false` in the public process.

For a local container smoke test, bind the published port to host loopback and
keep the application separate from its data:

```bash
docker build -t cex-dex-market-monitor .
docker run --rm \
  -p 127.0.0.1:8765:8765 \
  -e ADMIN_ENABLED=false \
  --mount type=bind,src=/srv/cex-dex/current,dst=/app/data/local,readonly \
  cex-dex-market-monitor
```

Do not expose this container port directly to the Internet. The production
path is the loopback-only systemd service in
`deploy/systemd/cex-dex-dashboard.service.in` behind the HTTPS and rate-limited
Nginx configuration in `deploy/nginx/cex-dex-dashboard.conf.in`. Keep port 8765
closed externally.

The mounted directory must contain:

- `cex_exchange_volume_daily.csv`
- `dex_pool_volume_daily.csv`
- `cex_depth_latest.csv`
- `dex_depth_latest.csv`
- `cex_execution_cost_latest.csv`
- `dex_execution_cost_latest.csv`
- `dex_pool_tvl_latest.csv`

Deploy a new application commit without changing the data directory. In the
production systemd layout, publish a reviewed data snapshot by atomically
switching `/srv/cex-dex/current`, restarting
`cex-dex-dashboard.service`, and running
`scripts/check_dashboard_health.py`. Keep prior application releases and data
snapshots for rollback.

That read-only mount is appropriate when the administrator surface is disabled.
If administrator operations are later required, use a separate reviewed
loopback service and restricted access path. Set `ADMIN_ENABLED=true`,
`ADMIN_LOGIN_REQUIRED=true`, `ADMIN_ALLOW_OPEN_LOCAL=false`, a generated
password verifier, and `ADMIN_COOKIE_SECURE=true`. The public Nginx hostname
continues to return 404 for administrator routes.

Security headers block framing, external scripts, device permissions, and
cross-origin content. A reverse proxy must provide HTTPS, access logs, rate
limits, and the public domain.

Production collection timers and the writable-data boundary are documented in
`docs/collection-operations.md`. Do not install the timers against a read-only
data mount. Process supervision, HTTPS, health checks, rollback, and the
dry-run-first CEX-depth raw retention script/timer are documented in
`docs/production-hardening.md`. The retention files are
`scripts/retain_cex_depth_raw.py` and
`deploy/systemd/cex-dex-cex-depth-retention.service.in` plus
`deploy/systemd/cex-dex-cex-depth-retention.timer`.
