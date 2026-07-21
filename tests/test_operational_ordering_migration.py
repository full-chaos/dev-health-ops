from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType

from dev_health_ops.migrations.clickhouse import split_sql_statements
from dev_health_ops.models.operational import OPERATIONAL_ENTITY_TABLES

_ROOT = Path(__file__).parents[1]
_MIGRATIONS = _ROOT / "src/dev_health_ops/migrations/clickhouse"
_MIGRATION_066 = _MIGRATIONS / "066_operational_canonical.sql"
_MIGRATION_067 = _MIGRATIONS / "067_operational_ordering_contract.py"
_CURRENT_READ_MODULES = (
    _ROOT / "src/dev_health_ops/storage/clickhouse.py",
    _ROOT / "src/dev_health_ops/backfill/operational_clickhouse.py",
    _ROOT / "src/dev_health_ops/metrics/active_incidents.py",
    _ROOT / "src/dev_health_ops/work_graph/operational_edges.py",
    _ROOT / "src/dev_health_ops/api/graphql/resolvers/work_graph.py",
)
_CANONICAL_FINAL = re.compile(
    r"\b(?:FROM|JOIN)\s+operational_[a-z_]+(?:\s+AS\s+\w+)?\s+FINAL\b",
    re.IGNORECASE,
)


def _load_migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "operational_ordering_migration", _MIGRATION_067
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _legacy_statements() -> dict[str, str]:
    statements = split_sql_statements(_MIGRATION_066.read_text(encoding="utf-8"))
    return {
        match.group(1): statement
        for statement in statements
        if (match := re.search(r"CREATE TABLE IF NOT EXISTS (\w+)", statement))
    }


def test_ordering_migration_rewrites_every_actual_canonical_table() -> None:
    # Given: migration 066's exact twelve-table legacy schema.
    migration = _load_migration()
    legacy = _legacy_statements()

    # When: each table is rewritten for its candidate-preserving shadow.
    rewritten = {
        table: migration._rewrite_ddl(statement, table, f"{table}__ordering_v2")
        for table, statement in legacy.items()
    }

    # Then: every actual family has the four fields, constraint, engine, and total key.
    assert set(migration.TABLES) == set(OPERATIONAL_ENTITY_TABLES.values())
    assert len(rewritten) == 12
    for statement in rewritten.values():
        assert "source_revision UInt128" in statement
        assert "source_conflict_key String" in statement
        assert "ingest_revision UInt128" in statement
        assert "ordering_contract UInt8" in statement
        assert (
            "CONSTRAINT ordering_contract_v2 CHECK ordering_contract = 2" in statement
        )
        assert "ENGINE = ReplacingMergeTree(ingest_revision)" in statement
        assert "PRIMARY KEY (org_id, id)" in statement
        assert (
            "ORDER BY (org_id, id, source_revision, source_conflict_key)" in statement
        )


def test_ordering_migration_source_is_raw_batched_retry_safe_and_atomic() -> None:
    # Given: the implementation-time Python shadow migration.
    source = _MIGRATION_067.read_text(encoding="utf-8")

    # When: its data-copy and swap protocol is inspected.
    protocol_markers = {
        "raw_stream": "query_row_block_stream" in source,
        "atomic_swap": "EXCHANGE TABLES" in source,
        "resume_shadow": "_resume_exchanged_shadow" in source,
        "candidate_count": "_candidate_count" in source,
        "logical_coverage": "_logical_count" in source,
        "maximum_tuple": "_maximum_tuple" in source,
    }

    # Then: it never reads with FINAL and carries every required convergence proof.
    assert " FINAL" not in source.upper()
    assert all(protocol_markers.values())


def test_canonical_current_reads_have_no_direct_final_or_bare_argmax() -> None:
    # Given: every Task 1 owned shared canonical-reader module.
    sources = {path: path.read_text(encoding="utf-8") for path in _CURRENT_READ_MODULES}

    # When: direct FINAL and legacy bare-argMax current selection are scanned.
    final_offenders = {
        path.relative_to(_ROOT): _CANONICAL_FINAL.findall(source)
        for path, source in sources.items()
        if _CANONICAL_FINAL.search(source)
    }
    argmax_offenders = {
        path.relative_to(_ROOT)
        for path, source in sources.items()
        if path.name == "active_incidents.py" and "argMax(" in source
    }

    # Then: all canonical winners flow through the centralized total-tuple helper.
    assert final_offenders == {}
    assert argmax_offenders == set()
