from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import logging

from dev_health_ops.storage import detect_db_type


_STOP_KEYWORDS = {
    "DEFAULT",
    "CODEC",
    "MATERIALIZED",
    "ALIAS",
    "COMMENT",
    "TTL",
}


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except FileNotFoundError:
        return ""


def _strip_db_prefix(table: str) -> str:
    if "." in table:
        return table.split(".", 1)[-1]
    return table


def _extract_parenthesized(text: str, start: int) -> Optional[Tuple[str, int]]:
    depth = 0
    for idx in range(start, len(text)):
        ch = text[idx]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return text[start + 1 : idx], idx
    return None


def _extract_type_segment(value: str) -> str:
    depth = 0
    for idx, ch in enumerate(value):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        elif depth == 0 and ch.isspace():
            remainder = value[idx:].lstrip()
            token = remainder.split(None, 1)[0].upper() if remainder else ""
            if token in _STOP_KEYWORDS:
                return value[:idx].strip()
    return value.strip()


def _split_column_definition(definition: str) -> Optional[Tuple[str, str]]:
    stripped = definition.strip().rstrip(",")
    if not stripped or stripped.startswith("--"):
        return None
    if stripped.upper().startswith(("PRIMARY KEY", "CONSTRAINT", "INDEX")):
        return None
    parts = stripped.split(None, 1)
    if len(parts) < 2:
        return None
    name = parts[0].strip('`"')
    type_segment = _extract_type_segment(parts[1].strip())
    if not name or not type_segment:
        return None
    return name, type_segment


def _iter_create_table_blocks(text: str) -> Iterable[Tuple[str, str]]:
    pattern = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([`\"\w\.]+)",
        re.IGNORECASE,
    )
    for match in pattern.finditer(text):
        table = match.group(1).strip('`"')
        start = text.find("(", match.end())
        if start == -1:
            continue
        block = _extract_parenthesized(text, start)
        if not block:
            continue
        columns_block, _end = block
        yield table, columns_block


def _iter_clickhouse_add_columns(text: str) -> Iterable[Tuple[str, str, str]]:
    current_table: Optional[str] = None
    for raw_line in text.splitlines():
        line = raw_line.split("--", 1)[0].strip()
        if not line:
            continue
        table_match = re.search(r"ALTER\s+TABLE\s+([`\"\w\.]+)", line, re.IGNORECASE)
        if table_match:
            current_table = table_match.group(1).strip('`"')
        if "ADD COLUMN" not in line.upper():
            continue
        table = current_table
        if not table:
            continue
        fragment = line[line.upper().find("ADD COLUMN") + len("ADD COLUMN") :].strip()
        fragment = re.sub(
            r"(?i)\bIF\s+NOT\s+EXISTS\b",
            "",
            fragment,
        ).strip()
        fragment = fragment.rstrip(",;")
        parsed = _split_column_definition(fragment)
        if not parsed:
            continue
        column, col_type = parsed
        yield table, column, col_type


def _normalize_clickhouse_type(type_name: str) -> str:
    return re.sub(r"\s+", "", type_name or "").lower()


def _build_clickhouse_expected_schema(
    migrations_dir: Path,
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[Any, List[str]]]]:
    expected: Dict[str, Dict[str, str]] = {}
    table_migrations: Dict[str, set[str]] = {}
    column_migrations: Dict[Tuple[str, str], set[str]] = {}

    for path in sorted(migrations_dir.glob("*.sql")):
        text = _read_text(path)
        for table, columns_block in _iter_create_table_blocks(text):
            table = _strip_db_prefix(table)
            table_migrations.setdefault(table, set()).add(path.name)
            for raw_line in columns_block.splitlines():
                line = raw_line.split("--", 1)[0].strip()
                if not line:
                    continue
                parsed = _split_column_definition(line)
                if not parsed:
                    continue
                column, col_type = parsed
                expected.setdefault(table, {})[column] = col_type
                column_migrations.setdefault((table, column), set()).add(path.name)

        for table, column, col_type in _iter_clickhouse_add_columns(text):
            table = _strip_db_prefix(table)
            expected.setdefault(table, {})[column] = col_type
            table_migrations.setdefault(table, set()).add(path.name)
            column_migrations.setdefault((table, column), set()).add(path.name)

    return expected, {
        "tables": {k: sorted(v) for k, v in table_migrations.items()},
        "columns": {k: sorted(v) for k, v in column_migrations.items()},
    }


def _normalize_sqlalchemy_type(type_obj: Any, dialect_name: str) -> str:
    from sqlalchemy import Boolean, Date, DateTime, Float, Integer, JSON, Numeric
    from sqlalchemy import String, Text, CHAR
    from sqlalchemy.dialects.postgresql import UUID as PGUUID
    from dev_health_ops.models.git import GUID

    if isinstance(type_obj, (Integer,)):
        return "int"
    if isinstance(type_obj, (Float, Numeric)):
        return "float"
    if isinstance(type_obj, (DateTime,)):
        return "datetime"
    if isinstance(type_obj, (Date,)):
        return "date"
    if isinstance(type_obj, (Boolean,)):
        return "bool"
    if isinstance(type_obj, (JSON,)):
        return "json" if dialect_name == "postgres" else "text"
    if isinstance(type_obj, (GUID, PGUUID)):
        return "uuid" if dialect_name == "postgres" else "text"
    if isinstance(type_obj, (Text, String, CHAR)):
        return "text"
    return "text"


def _normalize_inspected_type(type_obj: Any, dialect_name: str) -> str:
    name = (type_obj.__class__.__name__ if type_obj is not None else "").lower()
    if "uuid" in name:
        return "uuid" if dialect_name == "postgres" else "text"
    if "int" in name:
        return "int"
    if "bool" in name:
        return "bool"
    if "float" in name or "real" in name or "numeric" in name or "decimal" in name:
        return "float"
    if "date" in name or "time" in name:
        return "datetime"
    if "json" in name:
        return "json" if dialect_name == "postgres" else "text"
    if "char" in name or "text" in name or "string" in name:
        return "text"
    return "text"


def _build_sqlalchemy_expected_schema(
    dialect_name: str,
) -> Dict[str, Dict[str, str]]:
    from dev_health_ops.models.git import Base
    import dev_health_ops.models.teams  # noqa: F401

    expected: Dict[str, Dict[str, str]] = {}
    for table in Base.metadata.sorted_tables:
        columns = {}
        for col in table.columns:
            columns[col.name] = _normalize_sqlalchemy_type(col.type, dialect_name)
        expected[table.name] = columns
    return expected


def _query_dicts(
    client: Any, query: str, parameters: Dict[str, Any]
) -> List[Dict[str, Any]]:
    result = client.query(query, parameters=parameters)
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


def _fetch_clickhouse_schema(
    client: Any, tables: Iterable[str]
) -> Tuple[set[str], Dict[str, Dict[str, str]]]:
    table_list = list(tables)
    if not table_list:
        return set(), {}
    rows = _query_dicts(
        client,
        """
        SELECT name
        FROM system.tables
        WHERE database = currentDatabase()
          AND name IN %(tables)s
        """,
        {"tables": table_list},
    )
    present_tables = {row.get("name") for row in rows}
    schema: Dict[str, Dict[str, str]] = {}
    for table in present_tables:
        col_rows = _query_dicts(
            client,
            """
            SELECT name, type
            FROM system.columns
            WHERE database = currentDatabase()
              AND table = %(table)s
            """,
            {"table": table},
        )
        schema[table] = {
            row.get("name"): str(row.get("type") or "") for row in col_rows
        }
    return present_tables, schema


def _find_migration_matches(files: Iterable[Path], tokens: Iterable[str]) -> List[str]:
    token_list = [token for token in tokens if token]
    matches: List[str] = []
    for path in files:
        text = _read_text(path)
        if all(token in text for token in token_list):
            matches.append(path.name)
    return matches


def _compare_schema(
    expected: Dict[str, Dict[str, str]],
    actual: Dict[str, Dict[str, Any]],
    normalize_expected,
    normalize_actual,
) -> Tuple[List[str], Dict[str, List[str]], Dict[str, Dict[str, Dict[str, str]]]]:
    missing_tables: List[str] = []
    missing_columns: Dict[str, List[str]] = {}
    type_mismatches: Dict[str, Dict[str, Dict[str, str]]] = {}

    for table, expected_cols in expected.items():
        actual_cols = actual.get(table)
        if actual_cols is None:
            missing_tables.append(table)
            continue
        for column, expected_type in expected_cols.items():
            if column not in actual_cols:
                missing_columns.setdefault(table, []).append(column)
                continue
            actual_type = actual_cols.get(column)
            if normalize_expected(expected_type) != normalize_actual(actual_type):
                type_mismatches.setdefault(table, {})[column] = {
                    "expected": str(expected_type),
                    "actual": str(actual_type),
                }

    missing_tables.sort()
    for table in list(missing_columns.keys()):
        missing_columns[table] = sorted(missing_columns[table])
    return missing_tables, missing_columns, type_mismatches


def _build_clickhouse_migration_hints(
    migrations: Dict[str, Dict[Any, List[str]]],
    missing_tables: List[str],
    missing_columns: Dict[str, List[str]],
    type_mismatches: Dict[str, Dict[str, Dict[str, str]]],
) -> Dict[str, Dict[str, List[str]]]:
    table_hints: Dict[str, List[str]] = {}
    column_hints: Dict[str, List[str]] = {}
    for table in missing_tables:
        table_hints[table] = migrations.get("tables", {}).get(table, [])
    for table, columns in missing_columns.items():
        for column in columns:
            key = (table, column)
            column_hints[f"{table}.{column}"] = migrations.get("columns", {}).get(
                key, []
            )
    for table, columns in type_mismatches.items():
        for column in columns.keys():
            key = (table, column)
            column_hints.setdefault(
                f"{table}.{column}",
                migrations.get("columns", {}).get(key, []),
            )
    return {"tables": table_hints, "columns": column_hints}


def _build_sql_migration_hints(
    repo_root: Path,
    missing_tables: List[str],
    missing_columns: Dict[str, List[str]],
    type_mismatches: Dict[str, Dict[str, Dict[str, str]]],
) -> Dict[str, Dict[str, List[str]]]:
    migrations_dir = repo_root / "alembic" / "versions"
    files = list(migrations_dir.glob("*.py")) if migrations_dir.exists() else []
    table_hints: Dict[str, List[str]] = {}
    column_hints: Dict[str, List[str]] = {}
    for table in missing_tables:
        table_hints[table] = _find_migration_matches(files, [table])
    for table, columns in missing_columns.items():
        for column in columns:
            column_hints[f"{table}.{column}"] = _find_migration_matches(
                files, [table, column]
            )
    for table, columns in type_mismatches.items():
        for column in columns.keys():
            column_hints.setdefault(
                f"{table}.{column}",
                _find_migration_matches(files, [table, column]),
            )
    return {"tables": table_hints, "columns": column_hints}


def run_schema_audit(*, db_url: str) -> Dict[str, Any]:
    base_report = {
        "status": "unchecked",
        "checked": False,
        "db_backend": None,
        "db_error": None,
        "missing_tables": [],
        "missing_columns": {},
        "type_mismatches": {},
        "migration_hints": {"tables": {}, "columns": {}},
    }

    try:
        backend = detect_db_type(db_url)
    except Exception as exc:
        base_report["db_error"] = str(exc)
        return base_report

    base_report["db_backend"] = backend
    repo_root = Path(__file__).resolve().parents[3]

    if backend == "clickhouse":
        migrations_dir = repo_root / "migrations" / "clickhouse"
        expected, migrations = _build_clickhouse_expected_schema(migrations_dir)
        try:
            from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

            sink = ClickHouseMetricsSink(db_url)
        except Exception as exc:
            base_report["db_error"] = str(exc)
            return base_report

        try:
            sink.client.command("SELECT 1")
            _present, actual = _fetch_clickhouse_schema(sink.client, expected.keys())
            missing_tables, missing_columns, type_mismatches = _compare_schema(
                expected,
                actual,
                _normalize_clickhouse_type,
                _normalize_clickhouse_type,
            )
            base_report.update(
                {
                    "checked": True,
                    "missing_tables": missing_tables,
                    "missing_columns": missing_columns,
                    "type_mismatches": type_mismatches,
                    "migration_hints": _build_clickhouse_migration_hints(
                        migrations,
                        missing_tables,
                        missing_columns,
                        type_mismatches,
                    ),
                }
            )
            base_report["status"] = (
                "missing"
                if missing_tables or missing_columns or type_mismatches
                else "ok"
            )
        except Exception as exc:
            base_report["db_error"] = str(exc)
        finally:
            sink.close()
        return base_report

    if backend in {"postgres", "sqlite"}:
        from sqlalchemy import create_engine, inspect

        db_url_normalized = db_url
        if backend == "postgres":
            db_url_normalized = db_url.replace(
                "postgresql+asyncpg://", "postgresql://", 1
            )
        if backend == "sqlite":
            db_url_normalized = db_url.replace("sqlite+aiosqlite://", "sqlite://", 1)

        expected = _build_sqlalchemy_expected_schema(backend)
        engine = None
        try:
            engine = create_engine(db_url_normalized, echo=False)
            with engine.connect() as conn:
                inspector = inspect(conn)
                table_names = set(inspector.get_table_names())
                actual: Dict[str, Dict[str, Any]] = {}
                for table in expected.keys():
                    if table not in table_names:
                        continue
                    cols = inspector.get_columns(table)
                    actual[table] = {col.get("name"): col.get("type") for col in cols}
        except Exception as exc:
            base_report["db_error"] = str(exc)
            return base_report
        finally:
            if engine is not None:
                try:
                    engine.dispose()
                except Exception as exc:
                    logging.warning("Failed to dispose SQLAlchemy engine: %s", exc)

        missing_tables, missing_columns, type_mismatches = _compare_schema(
            expected,
            actual,
            lambda t: t,
            lambda t: _normalize_inspected_type(t, backend),
        )
        base_report.update(
            {
                "checked": True,
                "missing_tables": missing_tables,
                "missing_columns": missing_columns,
                "type_mismatches": type_mismatches,
                "migration_hints": _build_sql_migration_hints(
                    repo_root,
                    missing_tables,
                    missing_columns,
                    type_mismatches,
                ),
            }
        )
        base_report["status"] = (
            "missing" if missing_tables or missing_columns or type_mismatches else "ok"
        )
        return base_report

    base_report["db_error"] = f"unsupported backend: {backend}"
    return base_report


def format_schema_report(report: Dict[str, Any]) -> str:
    status = report.get("status", "unchecked")
    backend = report.get("db_backend") or "unknown"
    lines = [f"status: {status}", f"backend: {backend}"]

    missing_tables = report.get("missing_tables") or []
    if missing_tables:
        lines.append("missing_tables:")
        for table in missing_tables:
            migrations = (
                report.get("migration_hints", {}).get("tables", {}).get(table, [])
            )
            hint = f" (migrations: {', '.join(migrations)})" if migrations else ""
            lines.append(f"- {table}{hint}")

    missing_columns = report.get("missing_columns") or {}
    if missing_columns:
        lines.append("missing_columns:")
        for table, columns in missing_columns.items():
            for column in columns:
                key = f"{table}.{column}"
                migrations = (
                    report.get("migration_hints", {}).get("columns", {}).get(key, [])
                )
                hint = f" (migrations: {', '.join(migrations)})" if migrations else ""
                lines.append(f"- {key}{hint}")

    type_mismatches = report.get("type_mismatches") or {}
    if type_mismatches:
        lines.append("type_mismatches:")
        for table, columns in type_mismatches.items():
            for column, detail in columns.items():
                key = f"{table}.{column}"
                expected = detail.get("expected")
                actual = detail.get("actual")
                migrations = (
                    report.get("migration_hints", {}).get("columns", {}).get(key, [])
                )
                hint = f" (migrations: {', '.join(migrations)})" if migrations else ""
                lines.append(f"- {key} expected {expected} got {actual}{hint}")

    return "\n".join(lines)


def register_commands(audit_subparsers: argparse._SubParsersAction) -> None:
    audit_schema = audit_subparsers.add_parser(
        "schema", help="Verify DB schema is current."
    )
    audit_schema.add_argument("--db", required=True, help="Database connection string.")
    audit_schema.set_defaults(func=_cmd_audit_schema)


def _cmd_audit_schema(ns: argparse.Namespace) -> int:
    logger = logging.getLogger(__name__)
    try:
        report = run_schema_audit(db_url=ns.db)
        print(format_schema_report(report))
        return 0 if report.get("status") == "ok" else 1
    except Exception as e:
        logger.error(f"Schema audit failed: {e}")
        return 1
