"""Guard the metrics.daily families manifest against table-name drift (CHAOS-4246).

``internal/jobs/metrics/daily/families.json`` is a declared ledger of which
ClickHouse table(s) each sub-family of the daily metrics job writes. It is
inventory metadata only -- no Go runtime code reads it as a dispatch switch
(confirmed by grep, CHAOS-4246 job-inventory audit) -- so nothing catches it
drifting from the actual sink code. Two families had already drifted: the
``benchmarking`` family declared ``benchmarking_rollups`` (a table that does
not exist; the real table is ``testops_benchmark_insights``, among others),
and the ``testops_risk`` family declared ``release_confidence_daily`` /
``quality_drag_daily`` / ``pipeline_stability_daily`` (none of which exist;
the real tables are ``testops_release_confidence`` / ``testops_quality_drag``
/ ``testops_pipeline_stability``). A wrong table name in this ledger is
silent -- the compute/write code doesn't consult it -- so the only way to
catch it is to check every declared table actually exists in the ClickHouse
migration chain.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from dev_health_ops.migrations.clickhouse import strip_line_comments

_REPO_ROOT = Path(__file__).resolve().parents[1]
_FAMILIES_JSON = (
    _REPO_ROOT / "internal" / "jobs" / "metrics" / "daily" / "families.json"
)
_MIGRATIONS_DIR = _REPO_ROOT / "src" / "dev_health_ops" / "migrations" / "clickhouse"

_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:default\.)?`?([a-zA-Z0-9_]+)`?",
    re.IGNORECASE,
)


def _tables_created_in_migrations() -> set[str]:
    """Every table name any committed .sql migration CREATEs.

    Only .sql migrations are scanned, matching the convention in
    test_clickhouse_migration_splitter.py. Python migrations (*.py in this
    directory) exist but none currently CREATE a metrics.daily output table
    (verified by hand for this ticket); a family that only became reachable
    through one would need this helper extended.
    """
    tables: set[str] = set()
    for path in sorted(_MIGRATIONS_DIR.glob("*.sql")):
        cleaned = strip_line_comments(path.read_text())
        for match in _CREATE_TABLE_RE.finditer(cleaned):
            tables.add(match.group(1))
    return tables


def _families_manifest() -> list[dict]:
    return json.loads(_FAMILIES_JSON.read_text())["families"]


def test_every_family_has_a_non_empty_writes_list():
    families = _families_manifest()
    assert families, "families.json declared no families"
    for family in families:
        assert family.get("writes"), (
            f"family {family.get('name')!r} declares no output tables"
        )


def test_every_declared_output_table_exists_in_clickhouse_migrations():
    """Every table families.json says a family writes must actually exist.

    A name here that isn't backed by a real CREATE TABLE is either a typo
    (caught immediately) or a table that was renamed/dropped without the
    ledger being updated (the exact drift this ticket found for
    `benchmarking` and `testops_risk`).
    """
    real_tables = _tables_created_in_migrations()
    offenders: list[str] = []
    for family in _families_manifest():
        name = family["name"]
        for table in family.get("writes", []):
            if table not in real_tables:
                offenders.append(f"{name}: writes {table!r} (no such table)")
    assert not offenders, (
        "families.json declares output tables that don't exist in "
        "src/dev_health_ops/migrations/clickhouse/*.sql:\n" + "\n".join(offenders)
    )
