# Production hardening and rollback

This runbook keeps the Python process private on loopback and exposes only the
read-only dashboard through an HTTPS reverse proxy. The administrator surface
remains disabled on the public hostname.

## Required operator inputs

Before deployment, choose:

- `@DOMAIN@`: the DNS name whose A/AAAA records point to the server;
- `@SERVICE_USER@` and `@SERVICE_GROUP@`: an unprivileged account that owns only
  the runtime data directories;
- `@PROJECT_ROOT@`: preferably a stable symlink such as
  `/srv/cex-dex/app/current`;
- TLS certificates for `@DOMAIN@`, normally issued and renewed by Certbot or the
  host's certificate manager.

Do not use the raw IP address as a substitute for the missing HTTPS domain.

## Install the process supervisor

Render `deploy/systemd/cex-dex-dashboard.service.in` by replacing the three
placeholders, install it as
`/etc/systemd/system/cex-dex-dashboard.service`, and install a reviewed copy of
`deploy/dashboard.env.example` as `/etc/cex-dex/dashboard.env`:

```bash
sudo install -d -m 0750 /etc/cex-dex
sudo install -m 0600 deploy/dashboard.env.example /etc/cex-dex/dashboard.env
sudo systemctl daemon-reload
sudo systemctl enable --now cex-dex-dashboard.service
```

The service binds only to `127.0.0.1:8765`, restarts after failures, runs
without Linux capabilities, and writes logs to journald. Verify:

```bash
python3 scripts/check_dashboard_health.py
systemctl is-active cex-dex-dashboard.service
journalctl -u cex-dex-dashboard.service --since today
```

Use `journalctl --vacuum-time=30d` for an immediate cleanup. The optional
`deploy/systemd/journald-cex-dex.conf.example` documents a compressed 30-day,
1 GiB host-wide policy; it affects every journald service and therefore needs a
host-level review.

### Unprivileged supervisor fallback

If the operator cannot install a system service yet, render
`deploy/systemd/cex-dex-dashboard-user.service.in` with the absolute project
root and reviewed bind address. Install it as
`~/.config/systemd/user/cex-dex-dashboard.service`, then run:

```bash
loginctl enable-linger "$USER"
systemctl --user daemon-reload
systemctl --user enable --now cex-dex-dashboard.service
systemctl --user is-active cex-dex-dashboard.service
```

This fallback explicitly keeps `ADMIN_ENABLED=false`, restarts the process
after failure, and survives logout only when linger is enabled. Prefer
`@BIND_HOST@=127.0.0.1` with the HTTPS proxy below. A non-loopback bind is a
temporary compatibility choice, not a substitute for TLS, rate limiting, or
closing port 8765.

## Configure HTTPS

Render `deploy/nginx/cex-dex-dashboard.conf.in` with the real domain, install it
under the Nginx `http` configuration, validate with `nginx -t`, and reload
Nginx. The example:

- redirects HTTP to HTTPS;
- terminates TLS 1.2/1.3;
- adds HSTS;
- rate-limits public requests;
- records access and error logs;
- proxies to loopback only;
- returns 404 for the administrator page and APIs.

Keep port 8765 closed in the cloud firewall and host firewall. Only ports 80
and 443 should be Internet-facing.

## Administrator policy

Leave `ADMIN_ENABLED=false` for the public process. If an administrator process
is later required, create a separate reviewed service and access path:

1. keep its application bind on loopback;
2. set `ADMIN_ENABLED=true`, `ADMIN_LOGIN_REQUIRED=true`, and
   `ADMIN_ALLOW_OPEN_LOCAL=false`;
3. generate a real verifier with `scripts/admin_password.py`;
4. set `ADMIN_COOKIE_SECURE=true`;
5. restrict the route through an SSH tunnel, VPN, or IP allowlist;
6. verify CSRF, login rate limiting, and access logs before use.

`ADMIN_LOGIN_REQUIRED=false` does not enable open mode by itself. Local open
mode additionally requires `ADMIN_ALLOW_OPEN_LOCAL=true`, and the server will
refuse a non-loopback bind.

## Application and data rollback

Deploy immutable application directories by commit and point
`/srv/cex-dex/app/current` at the selected release. Keep runtime data snapshots
under a separate versioned directory and point `/srv/cex-dex/current` at the
reviewed snapshot.

Before restarting the service, run a compatibility preflight from the
prospective release with the exact Python interpreter used by `ExecStart`:

```bash
python3 --version
python3 -m py_compile dashboard/server.py dashboard/market_facts.py \
  scripts/check_dashboard_release.py
python3 -c "import dashboard.server; import dashboard.market_facts"
```

The supported production baseline is Python 3.8.10. The import check is
required in addition to compilation because some newer typing expressions are
syntactically valid on Python 3.8 but still fail while a module is imported.
Keep the old process running during this preflight. If any command fails,
restore the previous application target without restarting, so the failed
release never becomes the serving process.

After switching either symlink:

```bash
sudo systemctl restart cex-dex-dashboard.service
python3 scripts/check_dashboard_health.py
python3 scripts/check_dashboard_release.py --base-url http://127.0.0.1:8765
```

For an unprivileged user service, use the same checks but restart with:

```bash
systemctl --user restart cex-dex-dashboard.service
systemctl --user is-active cex-dex-dashboard.service
python3 scripts/check_dashboard_health.py
python3 scripts/check_dashboard_release.py --base-url http://127.0.0.1:8765
```

If `/health`, the executable release smoke, or browser smoke fails, atomically
switch the relevant symlink back to the previous known-good release or data
snapshot. Restart with either `sudo systemctl restart
cex-dex-dashboard.service` or `systemctl --user restart
cex-dex-dashboard.service`, matching the active supervisor, then rerun the full
check set. Do not delete the previous release or data snapshot until the new
version has passed `/health`, the Screener summary, one single-Token catalog,
the full audit catalog, compare, quality, execution-cost, and browser smoke
tests. The summary and Token catalog checks must also confirm a matching
non-empty `data_generation`.

## CEX depth raw-response retention

`scripts/retain_cex_depth_raw.py` is non-destructive unless `--apply` is
present. The default policy keeps seven days as individual JSON files,
compresses older snapshot directories into verified `.tar.gz` archives, and
expires archives after 30 days.

Review the exact plan first:

```bash
python3 scripts/retain_cex_depth_raw.py
```

Then apply the same policy explicitly:

```bash
python3 scripts/retain_cex_depth_raw.py --apply
```

The script refuses `/`, the home directory, the repository root, and generic
`data` or `data/raw` directories. Its target must be a dedicated directory
named `cex-depth`; symlinks and special files are not archived.

For automation, render and install
`cex-dex-cex-depth-retention.service.in` and its timer only after the dry-run
output has been reviewed. Enabling the timer is the explicit authorization for
daily `--apply`. Adjust the 7/30-day values in the rendered unit if regulatory
or research reproducibility requirements demand longer retention.

## Cache generation behavior

The process retains only the active published source generation. When SQLite,
TVL, CEX depth, DEX depth, CEX execution cost, or DEX execution cost signatures
change, all assembled payload, catalog, and encoded-response caches from the
previous generation are cleared. The public generation identifier also
includes the summary/catalog contract versions, so a schema deployment cannot
reuse a browser catalog from an older contract. Source signatures include
modification/change time, size, inode, and a path identity that is exposed only
as a hash. If a publication crosses a cold response build, that response is
discarded and retried instead of labeling old facts with a new generation. The
encoded response cache is reset when its one-minute freshness bucket changes.
Within one generation,
large assembled payload caches retain at most eight date-window variants and
the serialized-response cache retains at most 64 variants. This prevents
repeated publications or abnormal custom-window traffic from retaining
hundreds of complete payloads in process memory.
