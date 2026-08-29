#!/usr/bin/env python3
"""Build one deterministic SHIB V2/V2 historical-replay snapshot offline."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.shib_v2_research import (
    ResearchContractError,
    build_research_snapshot,
    load_research_registry,
    validate_research_evidence,
    validate_research_snapshot,
)
from scripts.shib_v2_research_io import (
    atomic_write_canonical_json,
    load_bounded_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a deterministic offline SHIB V2/V2 research snapshot"
    )
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--application-sha", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        registry_payload = load_bounded_json(args.registry, "registry")
        registry = load_research_registry(registry_payload)
    except (OSError, ResearchContractError):
        print("registry_invalid", file=sys.stderr)
        return 1

    try:
        os.stat(args.evidence, follow_symlinks=False)
    except FileNotFoundError:
        print("evidence_not_evaluated", file=sys.stderr)
        return 2
    except OSError:
        print("evidence_failed", file=sys.stderr)
        return 1

    try:
        evidence_payload = load_bounded_json(args.evidence, "evidence")
        evidence = validate_research_evidence(evidence_payload, registry)
        snapshot = build_research_snapshot(
            evidence, registry, args.application_sha
        )
        snapshot = validate_research_snapshot(snapshot, evidence, registry)
        atomic_write_canonical_json(args.output, snapshot)
    except (OSError, ResearchContractError):
        print("evidence_failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
