"""Shared contract for the dashboard assets reachable without admin access."""

from __future__ import annotations


# Each tuple is (public URL name, source path relative to dashboard/static).
# Protected admin assets are deliberately absent: the public release checker
# must be able to reproduce the health endpoint's asset fingerprint without
# weakening the admin surface's fail-closed routing.
PUBLIC_STATIC_ASSET_SOURCES = (
    ("actions.css", "actions.css"),
    ("actions.js", "actions.js"),
    ("app.js", "app.js"),
    ("navigation.js", "navigation.js"),
    ("styles.css", "styles.css"),
    ("vendor/lucide.js", "vendor/lucide.min.js"),
)

PUBLIC_STATIC_ASSET_FILENAMES = tuple(
    served_name for served_name, _source_path in PUBLIC_STATIC_ASSET_SOURCES
)
