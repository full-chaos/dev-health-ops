from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.storage import detect_db_type

AUDIT_PROVIDERS = ("jira", "github", "gitlab", "synthetic")

REQUIRED_CONFIG_KEYS: Dict[str, Sequence[str]] = {
    "jira": ("JIRA_BASE_URL", "JIRA_EMAIL", "JIRA_API_TOKEN"),
    "github": ("GITHUB_TOKEN",),
    "gitlab": ("GITLAB_TOKEN",),
    "synthetic": (),
}

COLLECTOR_SPECS: Dict[str, Dict[str, Sequence[str]]] = {
    "jira": {
        "modules": ("providers/jira/client.py",),
        "entrypoints": ("fetch_jira_work_items_with_extras", "fetch_jira_work_items"),
    },
    "github": {
        "modules": ("providers/github/client.py",),
        "entrypoints": ("fetch_github_work_items", "fetch_github_project_v2_items"),
    },
    "gitlab": {
        "modules": ("providers/gitlab/client.py",),
        "entrypoints": ("fetch_gitlab_work_items",),
    },
    "synthetic": {
        "modules": ("metrics/work_items.py",),
        "entrypoints": ("fetch_synthetic_work_items",),
    },
}

REQUIRED_TABLES = ("work_items", "work_item_transitions")
REQUIRED_COLUMNS = {
    "work_items": {
        "repo_id",
        "work_item_id",
        "provider",
        "title",
        "type",
        "status",
        "status_raw",
        "project_key",
        "project_id",
        "assignees",
        "reporter",
        "created_at",
        "updated_at",
        "started_at",
        "completed_at",
        "closed_at",
        "labels",
        "story_points",
        "sprint_id",
        "sprint_name",
        "parent_id",
        "epic_id",
        "url",
        "last_synced",
    },
    "work_item_transitions": {
        "repo_id",
        "work_item_id",
        "occurred_at",
        "provider",
        "from_status",
        "to_status",
        "from_status_raw",
        "to_status_raw",
        "actor",
        "last_synced",
    },
}


def parse_provider_list(raw: Optional[str]) -> List[str]:
    if raw is None or not str(raw).strip():
        return list(AUDIT_PROVIDERS)
    normalized = str(raw).strip().lower()
    if normalized in {"all", "*"}:
        return list(AUDIT_PROVIDERS)
    parts = [p.strip().lower() for p in normalized.split(",") if p.strip()]
    unknown = [p for p in parts if p not in AUDIT_PROVIDERS]
    if unknown:
        raise ValueError(f"Unknown provider(s): {', '.join(sorted(set(unknown)))}")
    return parts


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except FileNotFoundError:
        return ""


def _has_def(text: str, name: str) -> bool:
    return re.search(rf"\bdef\s+{re.escape(name)}\b", text) is not None


def _find_in_files(token: str, files: Iterable[Path]) -> bool:
    for path in files:
        if token in _read_text(path):
            return True
    return False


def _list_files(root: Path, suffix: str) -> List[Path]:
    if not root.exists():
        return []
    return [p for p in root.rglob(f"*{suffix}") if p.is_file()]


def _collector_check(repo_root: Path) -> Dict[str, Dict[str, Any]]:
    work_items_text = _read_text(repo_root / "metrics/work_items.py")
    results: Dict[str, Dict[str, Any]] = {}
    for provider, spec in COLLECTOR_SPECS.items():
        modules = [repo_root / path for path in spec.get("modules", [])]
        missing_modules = [str(path) for path in modules if not path.exists()]
        entrypoints = list(spec.get("entrypoints", []))
        entrypoint_ok = any(_has_def(work_items_text, name) for name in entrypoints)
        missing_entrypoints = [] if entrypoint_ok else entrypoints
        ok = not missing_modules and entrypoint_ok
        results[provider] = {
            "status": "ok" if ok else "missing",
            "missing_modules": missing_modules,
            "missing_entrypoints": missing_entrypoints,
        }
    return results


def _config_check(repo_root: Path) -> Dict[str, Dict[str, Any]]:
    code_files = (
        _list_files(repo_root / "providers", ".py")
        + _list_files(repo_root / "metrics", ".py")
        + [repo_root / "cli.py"]
    )
    doc_files = [repo_root / "README.md"] + _list_files(repo_root / "docs", ".md")
    results: Dict[str, Dict[str, Any]] = {}
    for provider, keys in REQUIRED_CONFIG_KEYS.items():
        missing_code = [k for k in keys if not _find_in_files(k, code_files)]
        missing_docs = [k for k in keys if not _find_in_files(k, doc_files)]
        ok = not missing_code and not missing_docs
        results[provider] = {
            "status": "ok" if ok else "missing",
            "missing_in_code": missing_code,
            "missing_in_docs": missing_docs,
        }
    return results


def _sink_check(repo_root: Path) -> Dict[str, Dict[str, Any]]:
    sink_text = _read_text(repo_root / "metrics/sinks/clickhouse.py")
    writers_ok = all(
        _has_def(sink_text, name)
        for name in ("write_work_items", "write_work_item_transitions")
    )
    missing_writers = []
    if not writers_ok:
        missing_writers = ["write_work_items", "write_work_item_transitions"]

    job_text = _read_text(repo_root / "metrics/job_work_items.py")
    results: Dict[str, Dict[str, Any]] = {}
    for provider in AUDIT_PROVIDERS:
        mapping_ok = provider in job_text
        missing = []
        if missing_writers:
            missing.extend(missing_writers)
        if not mapping_ok:
            missing.append("provider_mapping")
        ok = writers_ok and mapping_ok
        results[provider] = {
            "status": "ok" if ok else "missing",
            "missing": missing,
        }
    return results


def _extract_work_item_provider_choices(cli_text: str) -> List[str]:
    match = re.search(
        r"wi\.add_argument\([\s\S]*?--provider[\s\S]*?choices=\[(.*?)\]",
        cli_text,
    )
    if not match:
        return []
    choices = match.group(1)
    return re.findall(r"[\"']([a-zA-Z0-9_]+)[\"']", choices)


def _commands_check(repo_root: Path) -> Dict[str, Dict[str, Any]]:
    cli_text = _read_text(repo_root / "cli.py")
    provider_choices = set(_extract_work_item_provider_choices(cli_text))
    job_text = _read_text(repo_root / "metrics/job_work_items.py")
    job_entrypoint_ok = _has_def(job_text, "run_work_items_sync_job")

    results: Dict[str, Dict[str, Any]] = {}
    for provider in AUDIT_PROVIDERS:
        cli_ok = provider in provider_choices
        job_ok = job_entrypoint_ok and provider in job_text
        ok = cli_ok or job_ok
        missing = []
        if not ok:
            missing.append("sync_work_items_command")
        results[provider] = {
            "status": "ok" if ok else "missing",
            "missing": missing,
        }
    return results


def _migration_tables(repo_root: Path, tables: Sequence[str]) -> List[str]:
    migrations_dir = repo_root / "migrations/clickhouse"
    if not migrations_dir.exists():
        return list(tables)
    files = _list_files(migrations_dir, ".sql")
    found: Dict[str, bool] = {table: False for table in tables}
    patterns = {
        table: re.compile(
            rf"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?{re.escape(table)}\b",
            re.IGNORECASE,
        )
        for table in tables
    }
    for path in files:
        text = _read_text(path)
        for table, pattern in patterns.items():
            if found[table]:
                continue
            if pattern.search(text):
                found[table] = True
    return [table for table, present in found.items() if not present]


def _query_dicts(
    client: Any, query: str, parameters: Dict[str, Any]
) -> List[Dict[str, Any]]:
    result = client.query(query, parameters=parameters)
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


def _schema_check(db_url: str, repo_root: Path) -> Dict[str, Any]:
    missing_migrations = _migration_tables(repo_root, REQUIRED_TABLES)
    info: Dict[str, Any] = {
        "status": "unchecked",
        "checked": False,
        "missing_tables": [],
        "missing_columns": {},
        "missing_migrations": missing_migrations,
        "db_backend": None,
        "db_error": None,
    }

    try:
        backend = detect_db_type(db_url)
    except Exception as exc:
        info["db_error"] = f"detect_db_type_failed: {exc}"
        backend = None

    info["db_backend"] = backend
    if backend != "clickhouse":
        if missing_migrations:
            info["status"] = "missing"
        return info

    try:
        sink = ClickHouseMetricsSink(db_url)
    except Exception as exc:
        info["db_error"] = str(exc)
        if missing_migrations:
            info["status"] = "missing"
        return info

    try:
        client = sink.client
        info["checked"] = True
        table_rows = _query_dicts(
            client,
            """
            SELECT name
            FROM system.tables
            WHERE database = currentDatabase()
              AND name IN %(tables)s
            """,
            {"tables": list(REQUIRED_TABLES)},
        )
        present_tables = {row.get("name") for row in table_rows}
        missing_tables = [t for t in REQUIRED_TABLES if t not in present_tables]
        info["missing_tables"] = missing_tables

        missing_columns: Dict[str, List[str]] = {}
        for table in REQUIRED_TABLES:
            if table in missing_tables:
                continue
            col_rows = _query_dicts(
                client,
                """
                SELECT name
                FROM system.columns
                WHERE database = currentDatabase()
                  AND table = %(table)s
                """,
                {"table": table},
            )
            present_columns = {row.get("name") for row in col_rows}
            required_cols = REQUIRED_COLUMNS.get(table, set())
            missing = sorted(required_cols - present_columns)
            if missing:
                missing_columns[table] = missing
        info["missing_columns"] = missing_columns

        if missing_tables or missing_columns:
            info["status"] = "missing"
        else:
            info["status"] = "ok"
    except Exception as exc:
        info["db_error"] = str(exc)
        if missing_migrations:
            info["status"] = "missing"
    finally:
        sink.close()

    return info


def compile_coverage_report(
    *,
    providers: Sequence[str],
    collector: Dict[str, Dict[str, Any]],
    config: Dict[str, Dict[str, Any]],
    schema: Dict[str, Any],
    sink: Dict[str, Dict[str, Any]],
    commands: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "providers": {},
        "schema": schema,
        "overall_ok": True,
    }

    schema_status = schema.get("status")
    schema_ok = schema_status != "missing"

    overall_ok = True
    for provider in providers:
        collector_entry = collector.get(provider, {"status": "missing"})
        config_entry = config.get(provider, {"status": "missing"})
        sink_entry = sink.get(provider, {"status": "missing"})
        commands_entry = commands.get(provider, {"status": "missing"})

        provider_ok = (
            collector_entry.get("status") == "ok"
            and config_entry.get("status") == "ok"
            and sink_entry.get("status") == "ok"
            and commands_entry.get("status") == "ok"
            and schema_ok
        )
        overall_ok = overall_ok and provider_ok

        report["providers"][provider] = {
            "collector": collector_entry,
            "config": config_entry,
            "schema": schema,
            "sink": sink_entry,
            "commands": commands_entry,
            "overall": "ok" if provider_ok else "missing",
        }

    report["overall_ok"] = overall_ok
    return report


def run_coverage_audit(*, db_url: str, providers: Sequence[str]) -> Dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[3]
    collector = _collector_check(repo_root)
    config = _config_check(repo_root)
    sink = _sink_check(repo_root)
    commands = _commands_check(repo_root)
    schema = _schema_check(db_url, repo_root)
    return compile_coverage_report(
        providers=providers,
        collector=collector,
        config=config,
        schema=schema,
        sink=sink,
        commands=commands,
    )


def _render_table(headers: List[str], rows: List[List[str]]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def _row(cells: List[str]) -> str:
        padded = [cell.ljust(widths[idx]) for idx, cell in enumerate(cells)]
        return f"| {' | '.join(padded)} |"

    lines = [_row(headers)]
    lines.append(_row(["-" * w for w in widths]))
    for row in rows:
        lines.append(_row(row))
    return "\n".join(lines)


def format_coverage_table(report: Dict[str, Any]) -> str:
    providers = report.get("providers", {})
    rows: List[List[str]] = []
    for provider in providers:
        entry = providers.get(provider, {})
        rows.append(
            [
                provider,
                entry.get("collector", {}).get("status", "missing"),
                entry.get("config", {}).get("status", "missing"),
                entry.get("schema", {}).get("status", "missing"),
                entry.get("sink", {}).get("status", "missing"),
                entry.get("commands", {}).get("status", "missing"),
                entry.get("overall", "missing"),
            ]
        )

    return _render_table(
        [
            "provider",
            "collector",
            "config",
            "schema",
            "sink",
            "commands",
            "overall",
        ],
        rows,
    )


def _serialize_report(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _serialize_report(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_serialize_report(val) for val in value]
    return value


def format_coverage_json(report: Dict[str, Any]) -> str:
    return json.dumps(_serialize_report(report), indent=2, sort_keys=True)


def coverage_failed(report: Dict[str, Any]) -> bool:
    return not bool(report.get("overall_ok"))


def register_commands(audit_subparsers: argparse._SubParsersAction) -> None:
    audit_coverage = audit_subparsers.add_parser(
        "coverage", help="Audit provider implementation coverage."
    )
    audit_coverage.add_argument(
        "--db", required=True, help="Database connection string."
    )
    audit_coverage.add_argument(
        "--provider",
        help="Comma-separated list of providers to audit (default: all).",
    )
    audit_coverage.add_argument(
        "--format", choices=["table", "json"], default="table", help="Output format."
    )
    audit_coverage.set_defaults(func=_cmd_audit_coverage)


def _cmd_audit_coverage(ns: argparse.Namespace) -> int:
    import logging

    logger = logging.getLogger(__name__)
    try:
        providers = parse_provider_list(ns.provider)
        report = run_coverage_audit(db_url=ns.db, providers=providers)
        if ns.format == "json":
            print(format_coverage_json(report))
        else:
            print(format_coverage_table(report))
        return 0 if report.get("overall_ok") else 1
    except Exception as e:
        logger.error(f"Coverage audit failed: {e}")
        return 1
