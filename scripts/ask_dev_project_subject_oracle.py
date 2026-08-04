#!/usr/bin/env python3
"""Differential oracle: every referenced project is a resolvable subject.

Two independent producers write the two sides:

* the **work-item sync** writes ``work_items.project_id`` / ``project_key`` /
  ``project_name`` (``providers/<provider>/normalize.py``), and
* the **team auto-import** writes ``projects`` rows
  (``workers/team_autoimport_<provider>.py``, CHAOS-3365/CHAOS-3380).

Nothing in the schema forces them to agree, and no type checker or code index
can tell you whether they do — so this compares them by execution. The
invariant: **every project id any work item currently points at must exist in
the catalog, under the same name, and be resolvable.** A gap here is exactly
the Ask Dev symptom — a user names a real project and gets
``NO_AUTHORIZED_MATCH``.

READ-ONLY. It issues a single ``SELECT``; it never writes, and it is safe to
point at the dev ``default`` database.

Two providers, two id spaces (CHAOS-3380 round 2, matching
``native_status_change._project_identity_match``): Linear's catalog ``id`` IS
the raw ``work_items.project_id`` (a project's own id never changes), so the
join below matches directly. Jira's and GitLab's catalog ``id`` is prefixed/
opaque (``{org}:<provider>:<native_id>``, immutable) because their own native
project identity is a KEY (Jira) or a MUTABLE path (GitLab) rather than a
stable id work_items itself carries — for those, the join instead matches
``work_items.project_id`` (which stays the raw key/path, unchanged from
before) against the catalog row's own ``project_key`` column. A project id
this oracle cannot join by either arm is a genuine gap, not a false negative
of the join itself.

GitLab epics are explicitly EXCLUDED from the reference side (a named,
tested carve-out, not silence): ``providers/gitlab/normalize.
gitlab_epic_to_work_item`` sets ``work_items.project_id`` to the GROUP path,
not a PROJECT path — a wholly different identity space with, deliberately,
no catalog entity of its own in this ticket (CHAOS-3380 covers GitLab
PROJECTS; group/epic subjects are an explicit follow-up). Comparing a group
path against the project catalog would report a permanent, un-closeable
"missing" gap for every org with epics enabled — a false signal about a
carve-out, not a real defect.

What it deliberately does NOT check, so nobody reads more into a green run than
is there: it does not execute the async resolver itself (the equivalent
``EXACT_MATCH`` assertion lives in
``tests/test_ask_dev_linear_project_subject_live.py`` /
``tests/test_ask_dev_gitlab_project_subject_live.py``), and it has no catalog
freshness floor — a row that stopped being refreshed still reads as present.

Usage::

    python scripts/ask_dev_project_subject_oracle.py \\
        --dsn clickhouse://user:pass@localhost:8123/default \\
        --org-id 70d529e0-3c06-4597-8480-794fd02328b6

Exit codes: ``0`` no gaps, ``1`` gaps found, ``2`` the measurement could not be
taken (empty reference side, or an acceptance-set project that must be
rediscovered every run was absent).
"""

from __future__ import annotations

import argparse
import dataclasses
import sys
from dataclasses import dataclass
from typing import Any

from dev_health_ops.metrics.schemas import ProjectRecord

#: Acceptance set — known-real projects the oracle must keep rediscovering on
#: the reference side. If one stops being referenced the oracle has silently
#: stopped measuring what it was built to measure, which is a failure, not a
#: pass. ``Ask Dev`` in the Fullchaos workspace is the CHAOS-3365 case itself.
DEFAULT_ACCEPTANCE_PROJECT_IDS = ("13e65c04-40ec-4a95-8216-f7c2ce233244",)

#: The catalog-side fields this oracle compares. Asserted against the real
#: ``ProjectRecord`` so a rename breaks the oracle loudly instead of quietly
#: comparing nothing. ``project_key`` (CHAOS-3380 round 2) is the join arm
#: Jira/GitLab resolve through -- comparing it too, not just reading it.
_COMPARED_PROJECT_FIELDS = frozenset(
    {"id", "name", "is_active", "provider", "org_id", "project_key"}
)

# ``work_items`` is a ReplacingMergeTree(last_synced) keyed
# (org_id, repo_id, work_item_id). A plain DISTINCT would resurrect the OLD
# project of any item that moved between projects, inventing references that
# no longer exist — hence the per-item argMax, which is the full key inside one
# org, followed by a per-project argMax to pick the name from the most recently
# synced item that still points at it.
#
# ``type != 'epic'`` excludes GitLab's group-path epic references (see module
# docstring) — a no-op for every other provider, none of which ever write
# ``type = 'epic'`` (only ``providers/gitlab/normalize.py`` does).
#
# The emptiness test on ``catalog_id`` is unambiguous because ``referenced``
# excludes ``project_id = ''``: the join key is never the empty string, so a
# LEFT JOIN can never bind to a catalog row whose id is genuinely empty, and an
# empty ``catalog_id`` can only mean "no match".
#
# The invariant is deliberately one-directional: every REFERENCED project must
# be in the catalog. A catalog project with no work items is not a gap.
_GAP_QUERY = """
WITH latest_items AS (
    SELECT
        argMax(project_id, last_synced) AS project_id,
        argMax(project_name, last_synced) AS project_name,
        max(last_synced) AS synced_at
    FROM work_items
    WHERE org_id = {org_id:String} AND provider = {provider:String}
      AND type != 'epic'
    GROUP BY repo_id, work_item_id
),
referenced AS (
    SELECT project_id, argMax(project_name, synced_at) AS project_name
    FROM latest_items
    WHERE project_id != ''
    GROUP BY project_id
),
catalog AS (
    SELECT id, name, is_active, ifNull(project_key, '') AS project_key
    FROM projects FINAL
    WHERE org_id = {org_id:String} AND provider = {provider:String}
),
active_labels AS (
    SELECT lowerUTF8(name) AS label, count() AS label_rows
    FROM projects FINAL
    WHERE org_id = {org_id:String} AND is_active = 1
    GROUP BY label
)
SELECT
    r.project_id AS project_id,
    r.project_name AS project_name,
    c.id AS catalog_id,
    c.name AS catalog_name,
    c.is_active AS catalog_is_active,
    l.label_rows AS active_label_rows
FROM referenced AS r
LEFT JOIN catalog AS c
  ON r.project_id = c.id
  OR (c.project_key != '' AND r.project_id = c.project_key)
LEFT JOIN active_labels AS l ON lowerUTF8(r.project_name) = l.label
ORDER BY project_id
"""


@dataclass(frozen=True)
class ProjectGap:
    project_id: str
    referenced_name: str
    kind: str
    detail: str


class OracleNotMeasured(RuntimeError):
    """The comparison did not happen — never report this as a pass."""


def _assert_compared_fields_exist() -> None:
    actual = {field.name for field in dataclasses.fields(ProjectRecord)}
    missing = _COMPARED_PROJECT_FIELDS - actual
    if missing:
        raise OracleNotMeasured(
            f"ProjectRecord no longer has {sorted(missing)}; this oracle is "
            "comparing fields that do not exist and would pass vacuously."
        )


def _rows(client: Any, org_id: str, provider: str) -> list[dict[str, Any]]:
    result = client.query(
        _GAP_QUERY, parameters={"org_id": org_id, "provider": provider}
    )
    return [
        dict(zip(result.column_names, row, strict=True)) for row in result.result_rows
    ]


def project_subject_gaps(
    client: Any,
    *,
    org_id: str,
    provider: str = "linear",
    acceptance_project_ids: tuple[str, ...] = (),
) -> list[ProjectGap]:
    """Compare the two producers and return every way the catalog falls short.

    Raises :class:`OracleNotMeasured` when the reference side is empty or an
    acceptance-set project is not referenced — an empty comparison is not a
    clean bill of health.
    """

    _assert_compared_fields_exist()
    rows = _rows(client, org_id, provider)
    if not rows:
        raise OracleNotMeasured(
            f"no {provider} work item in org {org_id} carries a project_id; there "
            "is nothing to compare, so this run proves nothing."
        )

    referenced_ids = {str(row["project_id"]) for row in rows}
    absent = [pid for pid in acceptance_project_ids if pid not in referenced_ids]
    if absent:
        raise OracleNotMeasured(
            f"acceptance-set project(s) {absent} are not referenced by any work "
            "item; the oracle stopped measuring the case it was built for."
        )

    gaps: list[ProjectGap] = []
    for row in rows:
        project_id = str(row["project_id"])
        referenced_name = str(row["project_name"])
        if not str(row["catalog_id"]):
            gaps.append(
                ProjectGap(
                    project_id=project_id,
                    referenced_name=referenced_name,
                    kind="missing",
                    detail="no projects row; the project cannot resolve as a subject",
                )
            )
            continue
        if str(row["catalog_name"]) != referenced_name:
            gaps.append(
                ProjectGap(
                    project_id=project_id,
                    referenced_name=referenced_name,
                    kind="name_mismatch",
                    detail=f"catalog name is {str(row['catalog_name'])!r}",
                )
            )
        if int(row["catalog_is_active"]) != 1:
            gaps.append(
                ProjectGap(
                    project_id=project_id,
                    referenced_name=referenced_name,
                    kind="inactive",
                    detail="is_active != 1, so scope_catalog filters it out",
                )
            )
        # Presence is not resolvability. ``scope_service.resolve_mention``
        # returns AMBIGUOUS_CANDIDATES, not EXACT_MATCH, when a second ACTIVE
        # catalog row carries the same label — and the catalog's project query
        # is not provider-scoped, so the competing row can come from anywhere.
        # Without this the oracle prints OK for a name the user cannot resolve.
        if int(row["active_label_rows"] or 0) > 1:
            gaps.append(
                ProjectGap(
                    project_id=project_id,
                    referenced_name=referenced_name,
                    kind="name_ambiguous",
                    detail=(
                        f"{int(row['active_label_rows'])} active catalog rows share "
                        "this label; the name resolves AMBIGUOUS, not EXACT"
                    ),
                )
            )
    return gaps


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True, help="ClickHouse DSN (read-only use)")
    parser.add_argument("--org-id", required=True)
    parser.add_argument("--provider", default="linear")
    parser.add_argument(
        "--acceptance-project-id",
        action="append",
        default=None,
        help="Project id that must stay referenced; repeatable. "
        "Pass --acceptance-project-id '' to disable the default set.",
    )
    args = parser.parse_args(argv)

    acceptance = args.acceptance_project_id
    if acceptance is None:
        acceptance = list(DEFAULT_ACCEPTANCE_PROJECT_IDS)
    acceptance = tuple(value for value in acceptance if value)

    import clickhouse_connect

    client = clickhouse_connect.get_client(dsn=args.dsn)
    try:
        gaps = project_subject_gaps(
            client,
            org_id=args.org_id,
            provider=args.provider,
            acceptance_project_ids=acceptance,
        )
    except OracleNotMeasured as exc:
        print(f"NOT MEASURED: {exc}", file=sys.stderr)
        return 2
    finally:
        client.close()

    if not gaps:
        print(
            f"OK: every {args.provider} project referenced by org {args.org_id} "
            "resolves as an active catalog subject."
        )
        return 0

    print(f"{len(gaps)} gap(s) between referenced projects and the subject catalog:")
    for gap in gaps:
        print(f"  [{gap.kind}] {gap.project_id} {gap.referenced_name!r} — {gap.detail}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
