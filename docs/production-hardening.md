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
- `MARKET_DATA_DIR`: an absolute directory containing the published SQLite,
  CSV, quality, registry, collection-lock, and raw-response files;
- `MARKET_CEX_INSTRUMENT_LIFECYCLE`: exactly
  `MARKET_DATA_DIR/cex_instrument_lifecycle.json`, so the dashboard reads the
  atomically refreshed daily manifest rather than the tracked seed file;
- `ADMIN_JOB_DIR`: an absolute directory for administrator job records. It may
  live under `MARKET_DATA_DIR`, but it does not have to;
- TLS certificates for `@DOMAIN@`, normally issued and renewed by Certbot or the
  host's certificate manager.

Do not use the raw IP address as a substitute for the missing HTTPS domain.
The runtime paths are not restricted to `/srv`; `/var/lib`, a mounted data
volume, or another reviewed absolute path is supported. Do not use `/` itself,
relative paths, or paths containing whitespace.

## Install the process supervisor

Render the environment file and systemd services together so the values loaded
as `MARKET_DATA_DIR` and `ADMIN_JOB_DIR` exactly match the paths granted by
`ReadWritePaths`. systemd does not expand environment-file variables inside
filesystem-hardening directives, so manually copying the example without
rendering is invalid.

The collector uses a staging directory beside `MARKET_DATA_DIR`. For example,
`/data/market/published` uses `/data/market/.published-processed`. Create all
three writable directories before service startup, render the templates, and
install the generated files:

```bash
sudo install -d -o market-monitor -g market-monitor -m 0750 \
  /data/market/published \
  /data/market/.published-processed \
  /data/market/admin/jobs
python3 deploy/render_runtime_templates.py \
  --output-dir /tmp/cex-dex-rendered \
  --project-root /opt/cex-dex/app/current \
  --service-user market-monitor \
  --service-group market-monitor \
  --market-data-dir /data/market/published \
  --admin-job-dir /data/market/admin/jobs
sudo install -d -m 0750 /etc/cex-dex
sudo install -m 0600 /tmp/cex-dex-rendered/dashboard.env \
  /etc/cex-dex/dashboard.env
sudo install -m 0644 /tmp/cex-dex-rendered/cex-dex-dashboard.service \
  /etc/systemd/system/cex-dex-dashboard.service
sudo install -m 0644 /tmp/cex-dex-rendered/cex-dex-daily.service \
  /etc/systemd/system/cex-dex-daily.service
sudo install -m 0644 /tmp/cex-dex-rendered/cex-dex-depth.service \
  /etc/systemd/system/cex-dex-depth.service
sudo install -m 0644 deploy/systemd/cex-dex-daily.timer \
  /etc/systemd/system/cex-dex-daily.timer
sudo install -m 0644 deploy/systemd/cex-dex-depth.timer \
  /etc/systemd/system/cex-dex-depth.timer
sudo systemctl daemon-reload
sudo systemctl enable --now \
  cex-dex-dashboard.service \
  cex-dex-daily.timer \
  cex-dex-depth.timer
```

The dashboard service binds only to `127.0.0.1:8765` and restarts after
failures. The dashboard and both collectors run as the same explicitly rendered
unprivileged account, without Linux capabilities, and write logs to journald.
`ProtectSystem=strict` keeps the rest of the filesystem read-only. Their
explicit write allowlists cover:

- `MARKET_DATA_DIR`, including publication files, `admin/token_registry.json`,
  `collection/collection.lock`, quality evidence, and raw snapshots;
- the derived collector staging directory beside `MARKET_DATA_DIR`;
- `ADMIN_JOB_DIR`, even when operator jobs are stored outside the market-data
  tree.

Verify the rendered contract before starting the service:

```bash
grep -F "MARKET_DATA_DIR=/data/market/published" \
  /etc/cex-dex/dashboard.env
grep -F \
  "MARKET_CEX_INSTRUMENT_LIFECYCLE=/data/market/published/cex_instrument_lifecycle.json" \
  /etc/cex-dex/dashboard.env
grep -F "ReadWritePaths=/data/market/published" \
  /etc/systemd/system/cex-dex-dashboard.service
systemd-analyze verify /etc/systemd/system/cex-dex-dashboard.service
systemd-analyze verify /etc/systemd/system/cex-dex-daily.service
systemd-analyze verify /etc/systemd/system/cex-dex-depth.service
python3 scripts/check_dashboard_health.py
systemctl is-active cex-dex-dashboard.service
systemctl list-timers cex-dex-daily.timer cex-dex-depth.timer
journalctl -u cex-dex-dashboard.service --since today
```

Before calling a release current, confirm that the daily lifecycle step wrote
fresh root evidence at the exact path read by the dashboard:

```bash
python3 scripts/run_collection_cycle.py --profile daily --publish-local \
  --data-dir /data/market/published
jq '{checked_at_utc,response_sha256,inventory_count,configured_market_count,review_count}' \
  /data/market/published/cex_instrument_lifecycle.json
```

Use `journalctl --vacuum-time=30d` for an immediate cleanup. The optional
`deploy/systemd/journald-cex-dex.conf.example` documents a compressed 30-day,
1 GiB host-wide policy; it affects every journald service and therefore needs a
host-level review.

### Unprivileged supervisor fallback

If the operator cannot install a system service yet, use
`deploy/render_runtime_templates.py` with the same absolute project root and
`MARKET_DATA_DIR` used by the user collection timers. The renderer pins the
dashboard to loopback and embeds both the runtime data directory and exact CEX
lifecycle manifest path. Install the rendered
`cex-dex-dashboard-user.service` as
`~/.config/systemd/user/cex-dex-dashboard.service`, then run:

```bash
loginctl enable-linger "$USER"
systemctl --user daemon-reload
systemctl --user enable --now cex-dex-dashboard.service
systemctl --user is-active cex-dex-dashboard.service
```

This fallback explicitly keeps `ADMIN_ENABLED=false`, restarts the process
after failure, and survives logout only when linger is enabled. Use the HTTPS
or connection-capped loopback proxy below for public access rather than
changing the rendered bind address.

### Connection-capped demo proxy

When the demo host has no Nginx or TLS endpoint yet, the bounded public
Add Token and quality-retry actions must not be enabled by weakening the
application's loopback guard. Instead:

1. run `cex-dex-dashboard.service` on `127.0.0.1:8766`;
2. render `deploy/systemd/cex-dex-dashboard-proxy.socket.in` with the reviewed
   private/NAT bind address and install both proxy units under the same user
   supervisor;
3. keep `ADMIN_ENABLED=false` and
   `TRUST_LOOPBACK_PROXY_CLIENT_IP=false`;
4. enable the app service, then the proxy socket, and verify both the loopback
   health endpoint and the public endpoint.

The proxy uses `/lib/systemd/systemd-socket-proxyd --connections-max=64`, so
slow public connections cannot create an unbounded number of application
threads. It is a raw TCP proxy: it provides no TLS and does not overwrite
`X-Real-IP`. With proxy-header trust disabled, the application deliberately
treats all demo users as one conservative global rate-limit bucket. Never set
`TRUST_LOOPBACK_PROXY_CLIENT_IP=true` for this topology because a client could
supply that header itself.

For user-level collection timers, use `scripts/install_collection_timers.sh`
with an absolute `MARKET_DATA_DIR`. It renders the dedicated
`cex-dex-daily-user.service.in` and `cex-dex-depth-user.service.in` templates,
embedding that path rather than attempting to read the system-only
`/etc/cex-dex/dashboard.env`.

## Exact Uniswap V3 staged launch and rollback

The exact V3 launch tool is an operator ledger, not a deployment manager. It
requires Python 3.8 or newer on Linux, `fcntl` file locking, a user systemd
manager, and the existing collection/dashboard/release dependencies. It never
uses `sudo`, SSH, changes a Git or application pointer, edits environment or
unit files, or starts/stops the production dashboard. Server-specific unit
state, journal/cgroup/OOM evidence, the external application switch, and the
browser smoke check remain operator evidence.

Choose a fresh launch directory and a fresh data-directory sibling for the
candidate. The collection runner also creates a processed sibling named
`.v3-stage-YYYYMMDD-processed` when the stage basename is
`v3-stage-YYYYMMDD`; both roots must be new and are bound into the stage
receipt. Do not point either root at the live data directory, one of its
descendants, a symlink, or an existing rehearsal.

Every invocation takes the same immutable parameters:

```bash
TARGET_SHA="$(git rev-parse HEAD)"
PREVIOUS_APP_SHA="<the 40-64 lowercase hex SHA reported by the live old app>"
DATA_DIR="/absolute/live/market-data"
LAUNCH_DIR="/absolute/private/v3-launch-YYYYMMDD"
STAGE_DIR="/absolute/live/v3-stage-YYYYMMDD"

python3 scripts/uniswap_v3_launch.py preflight \
  --data-dir "$DATA_DIR" --launch-dir "$LAUNCH_DIR" \
  --stage-dir "$STAGE_DIR" --target-sha "$TARGET_SHA" \
  --previous-app-sha "$PREVIOUS_APP_SHA"
```

Without `--execute`, every phase prints only a redacted plan. It does not make
a directory, write a receipt, query systemd, acquire/create the collection
lock, start a process, run collection, publish, or make a network request.
The launcher disables bytecode writes before importing project modules, so
direct `--help` and plan execution do not create project-local `__pycache__`
artifacts. Python interpreter or site initialization outside the project is an
operating-system/runtime concern and is not controlled by this script.
Review that plan, then add `--execute` and run exactly one phase at a time in
this order:

```text
preflight -> pause -> backup -> stage -> verify-stage
```

`pause` manages only these fixed user units:
`cex-dex-daily.timer`, `cex-dex-depth.timer`, and their matching `.service`
units. It captures the exact timer enabled/active states, disables and stops
the timers, stops both oneshot services, verifies the services inactive, and
proves the live collection lock can be acquired. Every later phase that reads,
copies, promotes, restores, or validates live state rechecks the paused state
and holds that same live lock for the complete phase. The staged collection
uses its separate lock below `STAGE_DIR`.

`backup` stores the five fixed logical public files and their SHA-256, byte
count, presence, and original mode. The launch/backup directories are `0700`;
backup bytes and canonical receipts are `0600`, created with exclusive writes
and fsynced. On a first launch,
`uniswap_v3_exact_latest.json` may be represented only as `{"exists":false}`;
the tool never fabricates an empty sidecar.

`stage` copies only the live database, DEX daily input, and DEX depth history
needed by a fresh full candidate. Its raw and processed observations are new.
It runs the complete unfiltered `dex_depth` profile with local publication and
the exact V3 requirement directed at the stage. `verify-stage` starts a
loopback target dashboard with live unchanged facts plus staged DEX depth,
execution, and sidecar overrides. It additionally sets
`MARKET_UNISWAP_V3_EXACT_RAW_ROOT=STAGE_DIR/raw/dex-depth`, then invokes the
normal release checker with the unchanged target-SHA requirement.

After `verify-stage` succeeds, preserve this cutover order:

1. Keep the daily/depth timers paused.
2. Stop the production dashboard outside the launch tool.
3. Run the `promote --execute` phase. It revalidates the Task 4 raw candidate,
   public receipt, retained private receipt, staged root bindings, and live
   baseline CAS. It first installs only the identical canonical private receipt
   at `DATA_DIR/raw/dex-depth/<snapshot_id>/uniswap_v3_exact_validation.json`,
   then replaces the fixed five-file public bundle. It does not copy the whole
   staged raw snapshot.
4. Switch the reviewed application pointer to `TARGET_SHA` outside the tool.
5. Start the target dashboard and perform the normal target-SHA health,
   release, and browser checks.
6. Run `resume --execute`. It independently reruns the normal target-SHA
   release checker after proving the promoted public sidecar still equals the
   live trusted private receipt, then restores exactly the timer
   enabled/active states recorded by `pause`.

Do not promote while the old production dashboard is still serving, and do
not resume timers merely because the external application switch succeeded.
The launcher deliberately cannot make or infer either event. Keep both
managed timers disabled and both services inactive throughout the hold point,
and prevent all manual or non-cooperating writers from touching the live data
root. The path-based precommit checks do not pin root names or protect the
final replacement boundary from a late external writer or root replacement.

If forward validation fails, keep the timers paused and use this rollback
order:

1. Stop the production dashboard.
2. Restore the previous application pointer outside the tool.
3. Run `restore --execute`. Restore uses CAS against the exact promoted
   generation and restores every original byte and mode. An initially absent
   sidecar returns to absence within the ordinary-I/O transaction. A trusted
   private receipt created by this launch is removed in that transaction; a
   byte-identical receipt that predated the launch is validated and preserved.
4. Start the old dashboard and validate its previous application SHA and
   current data health.
5. Run `resume --execute`; rollback resume repeats that previous-SHA health
   validation before restoring the recorded timer states.

Each state-changing phase consumes the canonical predecessor receipt, verifies
its SHA binding, and creates exactly one next receipt with `O_EXCL`. Missing,
reordered, replayed, tampered, or drifted phases fail closed. Portable receipts
contain no absolute production path, RPC URL, environment content, or secret.

The forward publisher reuses the existing bounded atomic five-file helper.
Restore uses a launch-local replace-or-remove transaction so first-sidecar
absence is supported. Both restore pre-call bytes after ordinary I/O errors;
neither is a claim of multi-file crash atomicity across power loss or kernel
failure, and neither protects against a non-cooperating late writer after the
last path-based state check. If five-file promotion fails, the tool removes
only a trusted receipt it created; a different or drifted live receipt fails
closed.

Phase receipt creation follows promotion or restore. If that receipt write
fails after live bytes changed, keep the dashboard stopped and every managed
timer/service paused. Do not retry blindly: compare the fixed five live files
and trusted receipt against the retained backup, staged candidate, and their
checksums, perform the appropriate manual checksummed forward or recovery
action, and create no replacement ledger receipt until the live generation is
unambiguously established. A failed `resume` leaves no resume completion
receipt and attempts to return both timers and both services to
disabled/inactive. Retry only after that compensation is verified. If
compensation itself fails, reconcile every managed unit state manually and
confirm the safe paused state before revalidating the predecessor.

The normal forward dashboard uses the default live raw root, not a permanent
stage-root override. Retain the private launch backup, receipts, complete
staged raw/TVL evidence, validation receipt, and staged processed root through
the complete observation and rollback window.

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
export CEX_DEX_EXPECTED_APPLICATION_SHA="$(git rev-parse HEAD)"
export CEX_DEX_EXPECTED_ASSET_SHA="$(python3 -c \
  'from dashboard.server import static_asset_sha; print(static_asset_sha())')"
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
python3 scripts/check_dashboard_release.py \
  --base-url http://127.0.0.1:8765 \
  --expected-application-sha "$CEX_DEX_EXPECTED_APPLICATION_SHA" \
  --expected-asset-sha "$CEX_DEX_EXPECTED_ASSET_SHA"
```

For an unprivileged user service, use the same checks but restart with:

```bash
systemctl --user restart cex-dex-dashboard.service
systemctl --user is-active cex-dex-dashboard.service
python3 scripts/check_dashboard_health.py
python3 scripts/check_dashboard_release.py \
  --base-url http://127.0.0.1:8765 \
  --expected-application-sha "$CEX_DEX_EXPECTED_APPLICATION_SHA" \
  --expected-asset-sha "$CEX_DEX_EXPECTED_ASSET_SHA"
```

Both expected values must be computed from the immutable prospective release
before the restart. Do not copy either value from the post-restart `/health`
response: that would only compare the process with its own claim. The release
checker independently hashes the versioned CSS/JavaScript responses and rejects
an application SHA, asset SHA, or served-byte mismatch.

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
The rendered retention unit passes
`--root MARKET_DATA_DIR/raw/cex-depth` explicitly and grants write access only
to that external directory; it no longer assumes raw snapshots live under the
application checkout.

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
