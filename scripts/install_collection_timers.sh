#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
market_data_dir="${MARKET_DATA_DIR:-$project_root/data/local}"
admin_job_dir="${ADMIN_JOB_DIR:-$market_data_dir/admin/jobs}"
rendered_dir="$(mktemp -d)"

cleanup() {
  rm -rf -- "$rendered_dir"
}
trap cleanup EXIT

python3 "$project_root/deploy/render_runtime_templates.py" \
  --output-dir "$rendered_dir" \
  --project-root "$project_root" \
  --service-user "$(id -un)" \
  --service-group "$(id -gn)" \
  --market-data-dir "$market_data_dir" \
  --admin-job-dir "$admin_job_dir"

mkdir -p "$unit_dir"
for service in cex-dex-daily cex-dex-depth; do
  install -m 0644 \
    "$rendered_dir/$service-user.service" \
    "$unit_dir/$service.service"
  install -m 0644 \
    "$project_root/deploy/systemd/$service.timer" \
    "$unit_dir/$service.timer"
done

systemctl --user daemon-reload
systemctl --user enable --now cex-dex-daily.timer cex-dex-depth.timer
systemctl --user list-timers cex-dex-daily.timer cex-dex-depth.timer
