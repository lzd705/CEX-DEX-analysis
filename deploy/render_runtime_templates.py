#!/usr/bin/env python3
"""Render production runtime templates with explicit writable paths.

systemd deliberately does not expand variables loaded from EnvironmentFile in
ReadWritePaths. Rendering the same validated absolute paths into both the
environment file and service hardening rules prevents a configuration that
looks correct but fails only when an administrator job attempts to publish.
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Dict


DEPLOY_ROOT = Path(__file__).resolve().parent
SYSTEMD_ROOT = DEPLOY_ROOT / "systemd"
PLACEHOLDER = re.compile(r"@[A-Z][A-Z0-9_]*@")
ACCOUNT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]*")
OUTPUTS = {
    DEPLOY_ROOT / "dashboard.env.example": ("dashboard.env", 0o600),
    SYSTEMD_ROOT / "cex-dex-dashboard.service.in": (
        "cex-dex-dashboard.service",
        0o644,
    ),
    SYSTEMD_ROOT / "cex-dex-cex-depth-retention.service.in": (
        "cex-dex-cex-depth-retention.service",
        0o644,
    ),
    SYSTEMD_ROOT / "cex-dex-daily.service.in": (
        "cex-dex-daily.service",
        0o644,
    ),
    SYSTEMD_ROOT / "cex-dex-depth.service.in": (
        "cex-dex-depth.service",
        0o644,
    ),
    SYSTEMD_ROOT / "cex-dex-daily-user.service.in": (
        "cex-dex-daily-user.service",
        0o644,
    ),
    SYSTEMD_ROOT / "cex-dex-depth-user.service.in": (
        "cex-dex-depth-user.service",
        0o644,
    ),
}


def validated_absolute_path(value: str, name: str, *, allow_root: bool = False) -> Path:
    if any(character.isspace() for character in value) or any(
        character in value for character in ("#", "@", "%")
    ):
        raise ValueError(f"{name} must not contain whitespace, '#', '@', or '%'")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path")
    normalized = Path(os.path.normpath(str(path)))
    if normalized == Path("/") and not allow_root:
        raise ValueError(f"{name} must not be the filesystem root")
    return normalized


def render_text(template: str, replacements: Dict[str, str], source: Path) -> str:
    rendered = template
    for placeholder, value in replacements.items():
        rendered = rendered.replace(f"@{placeholder}@", value)
    unresolved = sorted(set(PLACEHOLDER.findall(rendered)))
    if unresolved:
        raise ValueError(
            f"{source.name} has unresolved placeholders: {', '.join(unresolved)}"
        )
    return rendered


def render_templates(
    *,
    output_dir: Path,
    project_root: Path,
    service_user: str,
    service_group: str,
    market_data_dir: Path,
    admin_job_dir: Path,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    if market_data_dir == project_root / "data/local":
        market_work_dir = project_root / "data/processed"
    else:
        market_work_dir = market_data_dir.parent / f".{market_data_dir.name}-processed"
    replacements = {
        "PROJECT_ROOT": str(project_root),
        "SERVICE_USER": service_user,
        "SERVICE_GROUP": service_group,
        "MARKET_DATA_DIR": str(market_data_dir),
        "MARKET_WORK_DIR": str(market_work_dir),
        "ADMIN_JOB_DIR": str(admin_job_dir),
    }
    written = []
    for source, (filename, mode) in OUTPUTS.items():
        rendered = render_text(
            source.read_text(encoding="utf-8"),
            replacements,
            source,
        )
        destination = output_dir / filename
        destination.write_text(rendered, encoding="utf-8")
        destination.chmod(mode)
        written.append(destination)
    return written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render systemd and environment templates for one runtime."
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--project-root", required=True)
    parser.add_argument("--service-user", required=True)
    parser.add_argument("--service-group", required=True)
    parser.add_argument("--market-data-dir", required=True)
    parser.add_argument("--admin-job-dir", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if ACCOUNT_NAME.fullmatch(args.service_user) is None:
        raise ValueError("service-user must be a valid account name")
    if ACCOUNT_NAME.fullmatch(args.service_group) is None:
        raise ValueError("service-group must be a valid group name")
    output_dir = validated_absolute_path(args.output_dir, "output-dir")
    project_root = validated_absolute_path(args.project_root, "project-root")
    market_data_dir = validated_absolute_path(
        args.market_data_dir,
        "market-data-dir",
    )
    admin_job_dir = validated_absolute_path(args.admin_job_dir, "admin-job-dir")
    written = render_templates(
        output_dir=output_dir,
        project_root=project_root,
        service_user=args.service_user,
        service_group=args.service_group,
        market_data_dir=market_data_dir,
        admin_job_dir=admin_job_dir,
    )
    for path in written:
        print(path)


if __name__ == "__main__":
    main()
