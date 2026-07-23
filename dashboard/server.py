"""Serve the local market-structure dashboard and persist workspace state."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import threading
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_ROOT = Path(__file__).resolve().parent
STATIC_ROOT = DASHBOARD_ROOT / "static"
STATE_DIR = DASHBOARD_ROOT / "state"
STATE_PATH = STATE_DIR / "work_status.local.json"
HISTORY_PATH = STATE_DIR / "work_history.local.jsonl"
STATE_TEMPLATE_PATH = STATE_DIR / "work_status.example.json"
PUBLIC_SNAPSHOT_PATH = PROJECT_ROOT / "data/public/research/factor_panel.csv"
PUBLIC_SCOPE_SENSITIVITY_PATH = PROJECT_ROOT / "data/public/research/cex_scope_sensitivity.csv"
RUNTIME = {"public": False}
STATE_LOCK = threading.Lock()

DATA_CANDIDATES = [
    PROJECT_ROOT / "data/a_review/research/factor_panel.csv",
    PROJECT_ROOT / "data/research/factor_panel.csv",
    PROJECT_ROOT / "data/processed/research_panel.csv",
    PROJECT_ROOT / "data/processed/merged_volume_panel.csv",
]

FACTOR_RESULT_CANDIDATES = [
    PROJECT_ROOT / "data/a_review/research/candidate_factor_forward_returns.csv",
    PROJECT_ROOT / "data/research/candidate_factor_forward_returns.csv",
]

COVERAGE_CANDIDATES = [
    PROJECT_ROOT / "data/a_review/research/coverage_summary.csv",
    PROJECT_ROOT / "data/research/coverage_summary.csv",
]

SCOPE_SENSITIVITY_PATH = DASHBOARD_ROOT / "data/cex_scope_sensitivity.csv"
VENDOR_FILES = {
    "/vendor/chart.umd.js": DASHBOARD_ROOT / "node_modules/chart.js/dist/chart.umd.min.js",
    "/vendor/chartjs-adapter-date-fns.js": DASHBOARD_ROOT
    / "node_modules/chartjs-adapter-date-fns/dist/chartjs-adapter-date-fns.bundle.min.js",
    "/vendor/lucide.js": DASHBOARD_ROOT / "node_modules/lucide/dist/umd/lucide.min.js",
}


def metric(label: str, unit: str, value_format: str, category: str) -> dict[str, str]:
    return {"label": label, "unit": unit, "format": value_format, "category": category}


METRICS = {
    "price_usd": metric("价格", "USD", "currency", "市场基础"),
    "cex_volume_usd": metric("CEX 成交额", "USD / day", "currency", "市场基础"),
    "dex_volume_usd": metric("DEX 成交额", "USD / day", "currency", "市场基础"),
    "dex_share": metric("Observed DEX Share", "%", "percent", "市场基础"),
    "cex_to_dex_ratio": metric("CEX / DEX", "x", "ratio", "市场基础"),
    "return_1d": metric("1 日收益", "%", "percent", "收益与风险"),
    "future_return_7d": metric("未来 7 日收益", "%", "percent", "收益与风险"),
    "mom_7d": metric("7 日动量", "%", "percent", "收益与风险"),
    "mom_14d": metric("14 日动量", "%", "percent", "收益与风险"),
    "mom_30d": metric("30 日动量", "%", "percent", "收益与风险"),
    "mom_7d_skip_1d": metric("跳过当日的 7 日动量", "%", "percent", "收益与风险"),
    "reversal_1d": metric("1 日反转", "%", "percent", "收益与风险"),
    "realized_vol_14d": metric("14 日实现波动率", "%", "percent", "收益与风险"),
    "momentum_vol_adj_7d": metric("波动调整 7 日动量", "score", "number", "收益与风险"),
    "cex_vol_z": metric("CEX Volume Z", "z-score", "number", "成交量"),
    "dex_vol_z": metric("DEX Volume Z", "z-score", "number", "成交量"),
    "total_vol_z": metric("Total Volume Z", "z-score", "number", "成交量"),
    "cex_vol_growth_7d": metric("CEX 7 日量增速", "log ratio", "number", "成交量"),
    "dex_vol_growth_7d": metric("DEX 7 日量增速", "log ratio", "number", "成交量"),
    "total_vol_growth_7d": metric("总成交量 7 日增速", "log ratio", "number", "成交量"),
    "joint_vol_z_mean": metric("CEX+DEX 联合放量", "score", "number", "成交量"),
    "cex_dex_z_product": metric("CEX×DEX 放量确认", "score", "number", "成交量"),
    "cex_dex_ratio_z": metric("CEX/DEX 比率 Z", "z-score", "number", "CEX/DEX 结构"),
    "dex_share_z": metric("DEX Share Z", "z-score", "number", "CEX/DEX 结构"),
    "dex_share_change_7d": metric("DEX Share 7 日变化", "change", "number", "CEX/DEX 结构"),
    "volume_divergence": metric("Volume Divergence", "z-score", "number", "CEX/DEX 结构"),
    "volume_growth_divergence_7d": metric("7 日量增速背离", "log ratio", "number", "CEX/DEX 结构"),
    "dex_share_x_dex_vol_z": metric("DEX Share×DEX Volume Z", "score", "number", "CEX/DEX 结构"),
    "dex_volume_to_tvl": metric("DEX Volume / TVL", "x", "ratio", "DEX 流动性"),
    "dex_volume_to_tvl_z": metric("DEX Volume / TVL Z", "z-score", "number", "DEX 流动性"),
    "dex_pool_tvl_growth_7d": metric("DEX Pool TVL 7 日增速", "log ratio", "number", "DEX 流动性"),
    "top_pool_volume_share": metric("Top Pool Share", "%", "percent", "DEX 流动性"),
    "dex_pool_herfindahl": metric("DEX Pool HHI", "index", "number", "DEX 流动性"),
    "dex_pool_diversification": metric("DEX Pool 分散度", "index", "number", "DEX 流动性"),
    "dex_pool_concentration_change_7d": metric("Pool 集中度 7 日变化", "change", "number", "DEX 流动性"),
    "active_pool_count": metric("活跃池数量", "pools", "integer", "DEX 流动性"),
    "cex_volume_confirmed_mom_7d": metric("CEX 放量确认动量", "score", "number", "复合因子"),
    "dex_volume_confirmed_mom_7d": metric("DEX 放量确认动量", "score", "number", "复合因子"),
    "joint_volume_confirmed_mom_7d": metric("CEX+DEX 确认动量", "score", "number", "复合因子"),
    "dex_share_confirmed_mom_7d": metric("DEX Share 确认动量", "score", "number", "复合因子"),
    "pool_diversified_dex_mom_7d": metric("池分散度确认动量", "score", "number", "复合因子"),
}

BASE_COLUMNS = [
    "date",
    "token_symbol",
    "price_usd",
    "cex_volume_usd",
    "dex_volume_usd",
    "dex_share",
    "cex_to_dex_ratio",
    "exchange_count",
    "pool_count",
    "selected_chains",
    "included_exchanges",
    "included_dexes",
]


def find_first_existing(paths: list[Path]) -> Path | None:
    """Return the first existing file from a priority-ordered list."""
    return next((path for path in paths if path.exists()), None)


def resolve_panel_path() -> Path:
    """Resolve the richest available research panel, with an env override."""
    override = os.environ.get("DASHBOARD_DATA")
    if override:
        path = Path(override).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"DASHBOARD_DATA does not exist: {path}")
        if RUNTIME["public"]:
            safe_roots = [
                (PROJECT_ROOT / "data/public").resolve(),
                (PROJECT_ROOT / "data/mock").resolve(),
                (DASHBOARD_ROOT / "sample").resolve(),
            ]
            if not any(path.is_relative_to(root) for root in safe_roots):
                raise PermissionError("Public mode only accepts curated or synthetic public data")
        return path

    if RUNTIME["public"]:
        if not PUBLIC_SNAPSHOT_PATH.exists():
            raise FileNotFoundError(f"Public research snapshot does not exist: {PUBLIC_SNAPSHOT_PATH}")
        return PUBLIC_SNAPSHOT_PATH

    path = find_first_existing(DATA_CANDIDATES)
    if path is None:
        candidates = ", ".join(str(item.relative_to(PROJECT_ROOT)) for item in DATA_CANDIDATES)
        raise FileNotFoundError(f"No dashboard panel found. Checked: {candidates}")
    return path


def parse_value(value: str | None) -> Any:
    """Convert CSV scalars to JSON-safe values without inventing missing data."""
    if value is None or value.strip() == "":
        return None
    try:
        number = float(value)
    except ValueError:
        return value
    if not math.isfinite(number):
        return None
    if number.is_integer() and abs(number) < 9_007_199_254_740_991:
        return int(number)
    return number


def read_csv(path: Path, columns: list[str] | None = None) -> tuple[list[dict[str, Any]], list[str]]:
    """Read a CSV into JSON-safe dictionaries, optionally selecting columns."""
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        source_columns = reader.fieldnames or []
        selected = source_columns if columns is None else [column for column in columns if column in source_columns]
        rows = [{column: parse_value(row.get(column)) for column in selected} for row in reader]
    return rows, source_columns


def relative_path(path: Path) -> str:
    """Render project paths without exposing unrelated local directories."""
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def file_version(path: Path) -> str:
    """Return a short content fingerprint for data lineage and history."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()[:16]


def is_synthetic_path(path: Path) -> bool:
    resolved = path.resolve()
    roots = [(PROJECT_ROOT / "data/mock").resolve(), (DASHBOARD_ROOT / "sample").resolve()]
    return any(resolved.is_relative_to(root) for root in roots)


def build_dashboard_payload() -> dict[str, Any]:
    """Build the compact source-backed payload consumed by the browser app."""
    panel_path = resolve_panel_path()
    _, source_columns = read_csv(panel_path, [])
    available_metrics = [name for name in METRICS if name in source_columns]
    selected_columns = list(dict.fromkeys(BASE_COLUMNS + available_metrics))
    rows, _ = read_csv(panel_path, selected_columns)

    tokens = sorted({str(row["token_symbol"]) for row in rows if row.get("token_symbol")})
    dates = sorted(str(row["date"]) for row in rows if row.get("date"))
    non_null_cex = sum(row.get("cex_volume_usd") is not None for row in rows)
    non_null_dex = sum(row.get("dex_volume_usd") is not None for row in rows)

    panel_factor_path = panel_path.parent / "candidate_factor_forward_returns.csv"
    panel_coverage_path = panel_path.parent / "coverage_summary.csv"
    factor_path = panel_factor_path if panel_factor_path.exists() else find_first_existing(FACTOR_RESULT_CANDIDATES)
    factor_rows = read_csv(factor_path)[0] if factor_path else []
    coverage_path = panel_coverage_path if panel_coverage_path.exists() else find_first_existing(COVERAGE_CANDIDATES)
    coverage_rows = read_csv(coverage_path)[0] if coverage_path else []
    scope_path = PUBLIC_SCOPE_SENSITIVITY_PATH if RUNTIME["public"] else SCOPE_SENSITIVITY_PATH
    scope_rows = read_csv(scope_path)[0] if scope_path.exists() else []

    stat = panel_path.stat()
    return {
        "metadata": {
            "source_path": relative_path(panel_path),
            "source_modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
            "data_version": file_version(panel_path),
            "row_count": len(rows),
            "token_count": len(tokens),
            "start_date": dates[0] if dates else None,
            "end_date": dates[-1] if dates else None,
            "cex_completeness": non_null_cex / len(rows) if rows else 0,
            "dex_completeness": non_null_dex / len(rows) if rows else 0,
            "grain": "token-day",
            "scope_note": "Observed DEX Share compares captured sources; it is not a global market-share estimate.",
            "access_mode": "public_read_only" if RUNTIME["public"] else "private_workspace",
            "synthetic": is_synthetic_path(panel_path),
        },
        "tokens": tokens,
        "metrics": {name: METRICS[name] for name in available_metrics},
        "rows": rows,
        "coverage": coverage_rows,
        "factor_results": factor_rows,
        "scope_sensitivity": scope_rows,
    }


def default_state() -> dict[str, Any]:
    """Load the committed initial workspace state."""
    if STATE_TEMPLATE_PATH.exists():
        with STATE_TEMPLATE_PATH.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    return {"selected_tokens": [], "metric": "dex_share", "notes": "", "checklist": []}


def load_state() -> dict[str, Any]:
    """Load local state, creating it from the template when needed."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    if not STATE_PATH.exists():
        save_state(default_state(), record_history=False)
    with STATE_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def sanitize_state(payload: dict[str, Any]) -> dict[str, Any]:
    """Accept only bounded dashboard workspace fields."""
    selected_tokens = payload.get("selected_tokens", [])
    if not isinstance(selected_tokens, list):
        selected_tokens = []
    selected_tokens = [str(value)[:20] for value in selected_tokens[:20]]

    checklist = payload.get("checklist", [])
    if not isinstance(checklist, list):
        checklist = []
    clean_checklist = []
    for item in checklist[:30]:
        if not isinstance(item, dict):
            continue
        clean_checklist.append(
            {
                "id": str(item.get("id", ""))[:50],
                "label": str(item.get("label", ""))[:160],
                "done": bool(item.get("done", False)),
            }
        )

    metric = str(payload.get("metric", "dex_share"))
    if metric not in METRICS:
        metric = "dex_share"

    return {
        "selected_tokens": selected_tokens,
        "metric": metric,
        "date_start": str(payload.get("date_start", ""))[:10],
        "date_end": str(payload.get("date_end", ""))[:10],
        "normalize": bool(payload.get("normalize", False)),
        "notes": str(payload.get("notes", ""))[:10_000],
        "checklist": clean_checklist,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }


def build_history_entry(state: dict[str, Any]) -> dict[str, Any]:
    """Build one append-only workspace snapshot with its data version."""
    metadata = build_dashboard_payload()["metadata"]
    done_count = sum(item["done"] for item in state["checklist"])
    return {
        "event_id": str(uuid.uuid4()),
        "saved_at": state["saved_at"],
        "data": {
            "source_path": metadata["source_path"],
            "data_version": metadata["data_version"],
            "row_count": metadata["row_count"],
            "start_date": metadata["start_date"],
            "end_date": metadata["end_date"],
        },
        "workspace": {
            "selected_tokens": state["selected_tokens"],
            "metric": state["metric"],
            "date_start": state["date_start"],
            "date_end": state["date_end"],
            "normalize": state["normalize"],
            "notes": state["notes"],
            "checklist": state["checklist"],
            "completed_tasks": done_count,
            "total_tasks": len(state["checklist"]),
        },
    }


def append_history(entry: dict[str, Any]) -> None:
    """Append one durable JSON line without rewriting prior history."""
    with HISTORY_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False, separators=(",", ":")))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def load_history(limit: int = 50) -> list[dict[str, Any]]:
    """Return the newest valid workspace snapshots first."""
    if not HISTORY_PATH.exists():
        return []
    entries = []
    with HISTORY_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(entry, dict):
                entries.append(entry)
    return list(reversed(entries[-max(1, min(limit, 200)) :]))


def public_state() -> dict[str, Any]:
    """Return a non-sensitive, non-persistent state for public visitors."""
    return {
        "selected_tokens": [],
        "metric": "dex_share",
        "date_start": "",
        "date_end": "",
        "normalize": False,
        "notes": "",
        "checklist": [],
        "saved_at": None,
    }


def save_state(payload: dict[str, Any], record_history: bool = True) -> dict[str, Any]:
    """Persist state atomically so an interrupted save cannot corrupt it."""
    if RUNTIME["public"]:
        raise PermissionError("Public dashboard mode is read-only")
    clean = sanitize_state(payload)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    temporary_path = STATE_PATH.with_suffix(".tmp")
    with STATE_LOCK:
        with temporary_path.open("w", encoding="utf-8") as handle:
            json.dump(clean, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary_path.replace(STATE_PATH)
        if record_history:
            append_history(build_history_entry(clean))
    return clean


class DashboardHandler(SimpleHTTPRequestHandler):
    """Static file and JSON API handler."""

    server_version = "MarketStructureDashboard"
    sys_version = ""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(STATIC_ROOT), **kwargs)

    def send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def end_headers(self) -> None:
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data: blob:; connect-src 'self'; object-src 'none'; "
            "base-uri 'none'; frame-ancestors 'none'",
        )
        super().end_headers()

    def list_directory(self, path: str) -> None:
        self.send_error(HTTPStatus.NOT_FOUND)
        return None

    def translate_path(self, path: str) -> str:
        """Expose only the pinned browser dependencies needed by the app."""
        request_path = urlparse(path).path
        if request_path in VENDOR_FILES:
            return str(VENDOR_FILES[request_path])
        return super().translate_path(path)

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/api/dashboard":
            try:
                self.send_json(build_dashboard_payload())
            except Exception as error:
                self.send_json({"error": str(error)}, HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        if path == "/api/state":
            self.send_json(public_state() if RUNTIME["public"] else load_state())
            return
        if path == "/api/history":
            self.send_json([] if RUNTIME["public"] else load_history())
            return
        if path == "/health":
            self.send_json({"status": "ok"})
            return
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        if urlparse(self.path).path != "/api/state":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if RUNTIME["public"]:
            self.send_json({"error": "Public dashboard mode is read-only"}, HTTPStatus.FORBIDDEN)
            return
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length > 128_000:
                raise ValueError("State payload is too large")
            payload = json.loads(self.rfile.read(content_length) or b"{}")
            if not isinstance(payload, dict):
                raise ValueError("State payload must be an object")
            self.send_json(save_state(payload))
        except (ValueError, json.JSONDecodeError) as error:
            self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)

    def log_message(self, format: str, *args: Any) -> None:
        if self.path != "/health":
            super().log_message(format, *args)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local market-structure dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--data", help="Research panel path; overrides automatic discovery")
    parser.add_argument("--public", action="store_true", help="Use curated public data and disable private workspace writes")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    RUNTIME["public"] = args.public
    if args.data:
        data_path = Path(args.data).expanduser()
        if not data_path.is_absolute() and not data_path.exists():
            data_path = PROJECT_ROOT / data_path
        os.environ["DASHBOARD_DATA"] = str(data_path.resolve())
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"Dashboard running at http://{args.host}:{args.port}")
    print(f"Data source: {relative_path(resolve_panel_path())}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
