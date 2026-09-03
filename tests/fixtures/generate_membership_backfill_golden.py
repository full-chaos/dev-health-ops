"""Golden vector for the Go membership-backfill port (CHAOS-4282).

Captured from the REAL producer -- dev_health_ops.work_graph.investment.
backfill.backfill_memberships -- so the Go port is measured against the
function that actually runs, not against a reading of it. Same shape as
generate_capacity_forecast_golden.py: no container, no live database. A FAKE
sink (FakeMembershipSink below) intercepts every query_dicts/write_*/close
call and dispatches on the QUERY TEXT, matching this repo's own precedent for
the recommendations loader corpus (an injected fake client dispatching on
table name) -- backfill_memberships calls create_sink(dsn) internally, so the
fake is wired in by patching that one name in the backfill module, not by
touching backfill.py itself.

The fixture is deliberately the SAME shape used on the Go side (see
membership_native_test.go's twoDisjointComponentEdges/matchedAndSkippedUnitIDs):
two disjoint (pull_request, issue) edge pairs, so components() produces
exactly two work units. Only one of the two has a persisted
work_unit_investments row -- the other exercises the skip branch. This lets
one fixture pin BOTH the matched-projection shape (multi-membership rows,
theme before subcategory, node order) and the skipped-unit count in the same
run, matching backfill_memberships' own return-stats contract.

computed_at and run_id are EXCLUDED from the captured rows: both are stamped
fresh every real run (uuid4 for run_id, datetime.now(timezone.utc) for
computed_at) and are therefore never a parity target -- the Go port stamps its
own equally-fresh values from the SAME call shape, not from this fixture.
Portability checklist applied (this lane's own brief): no sys.version,
sys.executable, platform.platform()/machine(), __file__ paths, timestamps, or
tempfile paths appear in the payload -- platform.python_version() would be
fine but is not needed here since nothing in the captured shape is
interpreter-version-sensitive.
"""

from __future__ import annotations

import json
import sys
from typing import Any
from unittest.mock import patch

from dev_health_ops.metrics.schemas import (
    WorkUnitMembershipRecord,
    WorkUnitMembershipRunRecord,
    WorkUnitScopedMembershipRunRecord,
)
from dev_health_ops.work_graph.investment.backfill import (
    MembershipBackfillConfig,
    backfill_memberships,
)

ORG_ID = "org-membership-golden"

# Two disjoint edges -> two components -> two work_unit_ids. Matches the Go
# fixture's twoDisjointComponentEdges() exactly: (pull_request:1, issue:1) and
# (pull_request:2, issue:2), so the SAME units.WorkUnitID computation on the Go
# side must reproduce the ids this fixture keys its investment row under.
EDGE_ROWS: list[dict[str, Any]] = [
    {
        "edge_id": "e1",
        "source_type": "pull_request",
        "source_id": "1",
        "target_type": "issue",
        "target_id": "1",
        "edge_type": "relates_to",
        "repo_id": "11111111-1111-1111-1111-111111111111",
        "provider": "github",
        "provenance": "native",
        "confidence": 0.9,
        "evidence": "",
    },
    {
        "edge_id": "e2",
        "source_type": "pull_request",
        "source_id": "2",
        "target_type": "issue",
        "target_id": "2",
        "edge_type": "relates_to",
        "repo_id": "11111111-1111-1111-1111-111111111111",
        "provider": "github",
        "provenance": "native",
        "confidence": 0.9,
        "evidence": "",
    },
]

# Multi-membership theme distribution: both categories clear the 0.2
# threshold, so BOTH rows are emitted (not just the argmax), plus one
# subcategory below-threshold-but-argmax row -- the same shape
# units.BuildMembershipRecords' own golden corpus exercises, exercised here
# through the FULL backfill_memberships orchestration instead of the bare
# projection helper.
THEME_DISTRIBUTION = {"feature_delivery": 0.65, "maintenance": 0.35}
SUBCATEGORY_DISTRIBUTION = {"backend": 1.0}
CATEGORIZATION_STATUS = "completed"


class FakeMembershipSink:
    """Intercepts every call backfill_memberships makes, dispatching on the
    query text for reads. Records every write for the generator to inspect.

    NOT a BaseMetricsSink subclass -- that ABC declares ~30 unrelated
    write_*() abstract methods (daily/testops/investment families this job
    never touches), so subclassing it would mean stubbing all of them for no
    reason. create_sink is fully patched below, so nothing ever isinstance-
    checks this object against the ABC; Python's duck typing is what
    backfill_memberships actually relies on.
    """

    def __init__(self) -> None:
        self.membership_rows: list[WorkUnitMembershipRecord] = []
        self.run_record: WorkUnitMembershipRunRecord | None = None
        self.scoped_run_records: list[WorkUnitScopedMembershipRunRecord] = []
        self.prune_calls: list[tuple[str, int]] = []

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_edges" in query:
            return list(EDGE_ROWS)
        if "FROM work_unit_investments" in query:
            work_unit_ids = parameters.get("work_unit_ids") or []
            # Only the FIRST discovered unit id in sorted order carries a row
            # in this fixture -- deliberately not both, so the run exercises
            # the skip branch. Which id that is depends on the live
            # work_unit_id hash, so key on whichever id is passed rather than
            # a hardcoded string: the fixture's own generator picks the first
            # id lexically, matching a stable, re-derivable rule instead of a
            # magic constant nobody could regenerate independently.
            if not work_unit_ids:
                return []
            matched_id = sorted(work_unit_ids)[0]
            return [
                {
                    "work_unit_id": matched_id,
                    "theme_distribution_json": THEME_DISTRIBUTION,
                    "subcategory_distribution_json": SUBCATEGORY_DISTRIBUTION,
                    "categorization_status": CATEGORIZATION_STATUS,
                }
            ]
        raise AssertionError(f"unexpected query in fixture generation: {query[:120]}")

    def write_work_unit_memberships(self, rows: list[WorkUnitMembershipRecord]) -> None:
        self.membership_rows.extend(rows)

    def write_membership_run(self, record: WorkUnitMembershipRunRecord) -> None:
        self.run_record = record

    def write_scoped_membership_runs(
        self, records: list[WorkUnitScopedMembershipRunRecord]
    ) -> None:
        self.scoped_run_records.extend(records)

    def prune_membership_runs(self, org_id: str, *, keep: int = 2) -> int:
        self.prune_calls.append((org_id, keep))
        return 0

    def ensure_schema(self) -> None:
        pass

    def close(self) -> None:
        pass

    @property
    def backend_type(self) -> str:
        return "fake"


def encode_record(record: WorkUnitMembershipRecord) -> dict[str, Any]:
    return {
        "org_id": record.org_id,
        "node_type": record.node_type,
        "node_id": record.node_id,
        "work_unit_id": record.work_unit_id,
        "category_kind": record.category_kind,
        "category": record.category,
        "weight": record.weight,
        "is_dominant": record.is_dominant,
        "categorization_status": record.categorization_status,
        # computed_at/run_id deliberately excluded -- see module docstring.
    }


def build() -> dict[str, Any]:
    sink = FakeMembershipSink()
    with patch(
        "dev_health_ops.work_graph.investment.backfill.create_sink",
        return_value=sink,
    ):
        stats = backfill_memberships(
            MembershipBackfillConfig(dsn="fake://unused", org_id=ORG_ID)
        )

    if sink.run_record is None:
        raise AssertionError("org-wide run must publish the completion marker")
    if sink.scoped_run_records:
        raise AssertionError("an org-wide run must not write scoped markers")
    if sink.prune_calls != [(ORG_ID, 2)]:
        raise AssertionError(
            f"expected exactly one keep=2 prune call, got {sink.prune_calls}"
        )

    return {
        "generator": "tests/fixtures/generate_membership_backfill_golden.py",
        "python_version": sys.version.split()[0],
        "org_id": ORG_ID,
        "edges": EDGE_ROWS,
        # Ordered [category, weight] PAIR LISTS, not dicts: sort_keys=True
        # below sorts every dict key recursively, which would silently
        # destroy the very insertion order this fixture exists to pin (see
        # module docstring / units.Distribution's own doc comment). A list
        # is immune to sort_keys by construction.
        # [k, v] lists, not tuples: json.load never reconstructs a tuple, so
        # comparing a freshly-built list-of-tuples against the reloaded
        # list-of-lists in --check mode would report every run as stale.
        "theme_distribution_pairs": [list(pair) for pair in THEME_DISTRIBUTION.items()],
        "subcategory_distribution_pairs": [
            list(pair) for pair in SUBCATEGORY_DISTRIBUTION.items()
        ],
        "categorization_status": CATEGORIZATION_STATUS,
        "stats": stats,
        "membership_rows": [encode_record(r) for r in sink.membership_rows],
        "run_record": {
            "org_id": sink.run_record.org_id,
            # completed_at excluded (wall-clock stamped).
        },
    }


def main(argv: list[str]) -> int:
    document = build()
    if len(argv) > 1 and argv[1] == "--check":
        with open(argv[2], encoding="utf-8") as handle:
            existing = json.load(handle)
        payload_keys = (
            "org_id",
            "edges",
            "theme_distribution_pairs",
            "subcategory_distribution_pairs",
            "categorization_status",
            "stats",
            "membership_rows",
            "run_record",
        )
        for key in payload_keys:
            if existing.get(key) != document[key]:
                sys.stderr.write(
                    f"membership backfill golden is STALE in {key!r}: the live "
                    "producer no longer returns the recorded values\n"
                )
                return 1
        sys.stdout.write("MEMBERSHIP_BACKFILL_GOLDEN_CURRENT\n")
        return 0
    sys.stdout.write(json.dumps(document, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
