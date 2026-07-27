#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
template_dir="$project_root/deploy/systemd"

mkdir -p "$unit_dir"
for service in cex-dex-daily cex-dex-depth; do
  sed "s|@PROJECT_ROOT@|$project_root|g" \
    "$template_dir/$service.service.in" > "$unit_dir/$service.service"
  install -m 0644 "$template_dir/$service.timer" "$unit_dir/$service.timer"
done

systemctl --user daemon-reload
systemctl --user enable --now cex-dex-daily.timer cex-dex-depth.timer
systemctl --user list-timers cex-dex-daily.timer cex-dex-depth.timer
