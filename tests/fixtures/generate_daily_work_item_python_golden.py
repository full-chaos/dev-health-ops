"""Generate/verify the frozen work_item + work_item_estimate Python golden (CHAOS-4283).

Mirrors tests/fixtures/generate_daily_wellbeing_python_golden.py's shape for
two families at once, because they share one input corpus exactly as
job_daily.py does (it loads work items ONCE and calls both computes over the
same list, :1550 and :1570):

  * compute_work_item_metrics_daily         (compute_work_items.py:1075)
  * compute_estimate_coverage_metrics_daily (compute_work_items.py:1425)

Both are the production Python this repo is porting to Go
(internal/jobs/metrics/workitemmetrics). This generator is the single source
both the frozen golden and the live rot guard
(internal/jobs/metrics/workitemmetrics/golden_rot_guard_test.go) render from,
so those two can never independently drift from each other -- only the frozen
file can drift from a CHANGED production Python, which the rot guard exists to
catch.

WHY THE ATTRIBUTION ROWS ARE IN THE GOLDEN TOO
----------------------------------------------
The Go daily executors do NOT re-run the 9-source resolve_team_attribution
cascade; they read its already-materialised output from
work_item_team_attributions (the CHAOS-4278 ruling). So this generator also
calls compute_work_item_team_attributions -- the SAME function, over the SAME
items, with the SAME resolvers, that writes that table in production -- and
emits its primary rows. The Go parity test feeds those rows in as its
resolver's answers. That makes the oracle reproduce the real production
seam (Python cascade -> table -> Go compute) instead of quietly assuming the
two agree.

`work_scope_id` is emitted per item as well, so the Go side's own
work_scope_id derivation (workItemStateWorkItem.workScopeID) is pinned against
WorkItem.work_scope_id rather than merely being believed.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dev_health_ops.metrics.compute_work_items import (
    compute_estimate_coverage_metrics_daily,
    compute_work_item_metrics_daily,
    compute_work_item_team_attributions,
)
from dev_health_ops.models.work_items import WorkItem, WorkItemStatusTransition
from dev_health_ops.providers.teams import ProjectKeyTeamResolver

OUTPUT = Path(__file__).with_name("daily_work_item_python_golden.json")

DAY = date(2026, 8, 24)
COMPUTED_AT = datetime(2026, 8, 25, 6, 30, 15, 123456, tzinfo=timezone.utc)
START = datetime(2026, 8, 24, tzinfo=timezone.utc)
END = START + timedelta(days=1)

# issue_project is one of the two cascade sources this fleet's real data
# actually exercises (CHAOS-4278's measurement), and ProjectKeyTeamResolver is
# directly constructible, so the corpus drives attribution through it.
PROJECT_TEAM_RESOLVER = ProjectKeyTeamResolver(
    project_key_to_team={
        "acme/platform": ("team-platform", "Platform"),
        "acme/payments": ("team-payments", "Payments"),
        "PROJ": ("team-jira", "Jira Team"),
    }
)


def _item(
    work_item_id: str,
    *,
    provider: str = "github",
    item_type: str = "task",
    status: str = "in_progress",
    project_id: str | None = "acme/platform",
    project_key: str | None = None,
    project_name: str | None = None,
    native_team_key: str | None = None,
    assignees: list[str] | None = None,
    created_at: datetime,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    closed_at: datetime | None = None,
    story_points: float | None = None,
) -> WorkItem:
    return WorkItem(
        work_item_id=work_item_id,
        provider=provider,  # type: ignore[arg-type]
        title=work_item_id,
        type=item_type,  # type: ignore[arg-type]
        status=status,  # type: ignore[arg-type]
        status_raw=None,
        project_id=project_id,
        project_key=project_key,
        project_name=project_name,
        native_team_key=native_team_key,
        assignees=list(assignees or []),
        created_at=created_at,
        updated_at=created_at,
        started_at=started_at,
        completed_at=completed_at,
        closed_at=closed_at,
        story_points=story_points,
        org_id="org-golden",
    )


def _transition(
    work_item_id: str, occurred_at: datetime, from_status: str, to_status: str
) -> WorkItemStatusTransition:
    return WorkItemStatusTransition(
        work_item_id=work_item_id,
        provider="github",
        occurred_at=occurred_at,
        from_status=from_status,  # type: ignore[arg-type]
        to_status=to_status,  # type: ignore[arg-type]
        from_status_raw=None,
        to_status_raw=None,
        actor=None,
        org_id="org-golden",
    )


def _hour(hour: int, minute: int = 0, second: int = 0, micro: int = 0) -> datetime:
    return datetime(2026, 8, 24, hour, minute, second, micro, tzinfo=timezone.utc)


def _corpus() -> tuple[list[WorkItem], list[WorkItemStatusTransition]]:
    """One corpus, deliberately covering every branch the Go port can take.

    Each entry names the axis it exists for; an axis with no case here is an
    axis the differential oracle cannot speak for.
    """
    items = [
        # created today, never started -> new_items/new_bugs only
        _item("gh:1", item_type="bug", status="backlog", created_at=_hour(1)),
        # created today, not started, NOT a bug -> new_items only
        _item("gh:2", status="todo", created_at=_hour(2)),
        # started today, still open at end of day -> items_started + WIP
        _item("gh:3", created_at=_hour(0), started_at=_hour(3), assignees=["alice"]),
        # started AND completed today, with transitions -> full flow breakdown
        _item(
            "gh:4",
            status="done",
            created_at=datetime(2026, 8, 20, 9, tzinfo=timezone.utc),
            started_at=_hour(4),
            completed_at=_hour(17, 31, 8, 107259),
            assignees=["bob"],
            story_points=3.5,
        ),
        # completed today with NO transitions -> 100%-active fallback
        _item(
            "gh:5",
            status="done",
            created_at=datetime(2026, 8, 22, tzinfo=timezone.utc),
            started_at=_hour(6),
            completed_at=_hour(9, 17, 33, 987654),
            assignees=["alice"],
            story_points=1.25,
        ),
        # completed today, NEVER started -> lead time but no cycle time
        _item(
            "gh:6",
            status="done",
            created_at=datetime(2026, 8, 23, 5, tzinfo=timezone.utc),
            completed_at=_hour(11),
        ),
        # UNASSIGNED (assignees empty) completed today -> *_unassigned counters
        _item(
            "gh:7",
            item_type="bug",
            status="done",
            created_at=datetime(2026, 8, 21, tzinfo=timezone.utc),
            started_at=_hour(7),
            completed_at=_hour(12),
        ),
        # assignee present but EMPTY STRING -> user_identity "unassigned",
        # yet the *_unassigned counters must NOT fire (`assignee is None`).
        _item(
            "gh:8",
            status="done",
            created_at=datetime(2026, 8, 21, tzinfo=timezone.utc),
            started_at=_hour(8),
            completed_at=_hour(13),
            assignees=[""],
        ),
        # completed BEFORE the window, created before it -> irrelevant entirely
        _item(
            "gh:9",
            status="done",
            created_at=datetime(2026, 8, 10, tzinfo=timezone.utc),
            started_at=datetime(2026, 8, 11, tzinfo=timezone.utc),
            completed_at=datetime(2026, 8, 12, tzinfo=timezone.utc),
        ),
        # completed exactly 7 days before window end -> weekly-throughput edge
        _item(
            "gh:10",
            status="done",
            created_at=datetime(2026, 8, 10, tzinfo=timezone.utc),
            started_at=datetime(2026, 8, 11, tzinfo=timezone.utc),
            completed_at=END - timedelta(days=7),
            assignees=["carol"],
        ),
        # terminal via closed_at, NOT completed_at -> _earliest_utc arm, and
        # excluded from estimate coverage's backlog
        _item(
            "gh:11",
            status="canceled",
            created_at=datetime(2026, 8, 19, tzinfo=timezone.utc),
            started_at=_hour(5),
            closed_at=_hour(10),
        ),
        # SECOND work scope, same provider -> proves grouping splits by scope
        _item(
            "gh:12",
            project_id="acme/payments",
            status="done",
            created_at=datetime(2026, 8, 23, tzinfo=timezone.utc),
            started_at=_hour(2),
            completed_at=_hour(14),
            assignees=["dave"],
            story_points=8.0,
        ),
        # UNMAPPED scope -> unassigned/Unassigned normalisation
        _item(
            "gh:13",
            project_id="acme/unmapped",
            created_at=_hour(1),
            started_at=_hour(1),
        ),
        # story_points 0.0 (falsy but NOT None) -> estimated, not unestimated
        _item(
            "gh:14",
            status="done",
            created_at=datetime(2026, 8, 23, tzinfo=timezone.utc),
            started_at=_hour(3),
            completed_at=_hour(15),
            story_points=0.0,
            assignees=["alice"],
        ),
        # three fractional story points on ONE bucket, chosen so a
        # Neumaier-compensated sum differs from the sequential one Python
        # actually performs (compute_work_items.py uses `+=`, never sum()).
        _item(
            "gh:15",
            project_id="acme/fma",
            status="done",
            created_at=datetime(2026, 8, 23, tzinfo=timezone.utc),
            started_at=_hour(3),
            completed_at=_hour(15),
            story_points=1e16,
            assignees=["erin"],
        ),
        _item(
            "gh:16",
            project_id="acme/fma",
            status="done",
            created_at=datetime(2026, 8, 23, tzinfo=timezone.utc),
            started_at=_hour(3),
            completed_at=_hour(16),
            story_points=1.0,
            assignees=["erin"],
        ),
        _item(
            "gh:17",
            project_id="acme/fma",
            status="done",
            created_at=datetime(2026, 8, 23, tzinfo=timezone.utc),
            started_at=_hour(3),
            completed_at=_hour(17),
            story_points=-1e16,
            assignees=["erin"],
        ),
        # jira: work_scope_id comes from project_key, not project_id
        _item(
            "jira:PROJ-1",
            provider="jira",
            project_id=None,
            project_key="PROJ",
            status="done",
            created_at=datetime(2026, 8, 23, tzinfo=timezone.utc),
            started_at=_hour(4),
            completed_at=_hour(18),
            assignees=["frank"],
            story_points=2.0,
        ),
        # jira with BOTH project_key AND project_id, set to DIFFERENT values.
        # This is the only shape that DISCRIMINATES the jira arm of
        # work_scope_id: with only project_key set, the final project_key
        # fallback returns the same answer, so removing the jira arm entirely
        # would still produce the right value (measured -- mutation M6 survived
        # against a corpus that had only the single-field shape).
        _item(
            "jira:PROJ-2",
            provider="jira",
            project_id="10042",
            project_key="PROJ",
            status="done",
            created_at=datetime(2026, 8, 23, tzinfo=timezone.utc),
            started_at=_hour(4),
            completed_at=_hour(19),
            assignees=["heidi"],
        ),
        # project_name is the THIRD fallback and no other case reaches it:
        # project_id empty, project_name set, native_team_key set to a
        # different value so the ordering between them is discriminated too.
        _item(
            "gh:24",
            project_id=None,
            project_name="acme/by-name",
            native_team_key="NAMEFALLBACK",
            created_at=_hour(1),
            started_at=_hour(1),
        ),
        # linear team-only: work_scope_id falls back to native_team_key
        _item(
            "linear:TEAM-1",
            provider="linear",
            project_id=None,
            native_team_key="TEAM",
            created_at=_hour(1),
            started_at=_hour(1),
        ),
        # EMPTY work_scope_id (no project fields at all)
        _item(
            "gh:18",
            project_id=None,
            created_at=_hour(1),
            started_at=_hour(1),
        ),
        # sub-microsecond cycle time -> total_seconds() vs Duration.Hours()
        _item(
            "gh:19",
            status="done",
            created_at=datetime(2026, 8, 23, tzinfo=timezone.utc),
            started_at=_hour(20, 0, 0, 0),
            completed_at=_hour(20, 0, 0, 1),
            assignees=["grace"],
        ),
        # MULTI-ASSIGNEE: first and last identities DIFFER. Without this,
        # `assignees[0]` and `assignees[-1]` are indistinguishable -- every
        # other corpus item has zero or one assignee, so a port taking the LAST
        # assignee passes both the frozen corpus and the live rot guard
        # (codex r1 P2, proved with `jq '[.items[]|select((.assignees|length)>1)]|length'` -> 0).
        # Drives user_identity in work_item_user_metrics_daily and assignee in
        # work_item_cycle_times.
        _item(
            "gh:25",
            project_id="acme/multi",
            status="done",
            created_at=datetime(2026, 8, 22, tzinfo=timezone.utc),
            started_at=_hour(2),
            completed_at=_hour(20),
            assignees=["ivan", "judy", "ken"],
            story_points=2.0,
        ),
        # DUAL TERMINAL, STRADDLING the window end -- the only shape that
        # actually discriminates _earliest_utc.
        #
        # Measured, not assumed: a first version put both timestamps INSIDE the
        # day (closed 09:00, completed 21:00) and the mutation "return the first
        # non-nil instead of the earliest" SURVIVED, because both values are
        # before `end`, so terminal_at lands on the same side of every
        # comparison either way. Reaching the arm is not discriminating it --
        # the same lesson as M6, hit a second time.
        #
        # closed_at is INSIDE the window and completed_at is OUTSIDE it, so:
        #   earliest      -> closed_at  (before end) -> item counts as terminal
        #   first non-nil -> completed_at (after end) -> item counts as OPEN
        # which flips wip_count_end_of_day and the estimate-coverage backlog.
        _item(
            "gh:26",
            project_id="acme/dual",
            status="done",
            created_at=datetime(2026, 8, 21, tzinfo=timezone.utc),
            started_at=_hour(3),
            completed_at=datetime(2026, 8, 25, 6, tzinfo=timezone.utc),
            closed_at=_hour(9),
        ),
        # DUAL TERMINAL, the REVERSE ordering (completed_at earlier). Both
        # orderings are needed: with only one, a port that always picks
        # `closed_at` (or always `completed_at`) still matches on the single
        # case present.
        _item(
            "gh:27",
            project_id="acme/dual",
            status="done",
            created_at=datetime(2026, 8, 21, tzinfo=timezone.utc),
            started_at=_hour(4),
            completed_at=_hour(8),
            closed_at=_hour(22),
        ),
        # OPEN items carrying story_points, in their own scope, so estimate
        # coverage produces a real FRACTION (1/3) rather than only the 0.0 and
        # None a corpus of terminal-or-unestimated items can reach.
        _item(
            "gh:21",
            project_id="acme/backlog",
            status="todo",
            created_at=datetime(2026, 8, 20, tzinfo=timezone.utc),
            story_points=5.0,
        ),
        _item(
            "gh:22",
            project_id="acme/backlog",
            status="todo",
            created_at=datetime(2026, 8, 20, tzinfo=timezone.utc),
        ),
        _item(
            "gh:23",
            project_id="acme/backlog",
            status="backlog",
            created_at=datetime(2026, 8, 20, tzinfo=timezone.utc),
        ),
        # a fifth completed item in the platform scope so its cycle-hour list
        # has >2 entries and the p50/p90 interpolation is a real lerp, not an
        # endpoint pick.
        _item(
            "gh:20",
            status="done",
            created_at=datetime(2026, 8, 18, tzinfo=timezone.utc),
            started_at=_hour(1, 13, 47, 654321),
            completed_at=_hour(22, 41, 3, 271828),
            assignees=["bob"],
        ),
    ]
    transitions = [
        # gh:4 walks in_progress -> blocked -> in_review -> done inside the
        # window, so both wait and active seconds accumulate.
        _transition("gh:4", _hour(2), "todo", "in_progress"),
        _transition("gh:4", _hour(6), "in_progress", "blocked"),
        _transition("gh:4", _hour(11, 5, 30), "blocked", "in_review"),
        _transition("gh:4", _hour(16), "in_review", "done"),
        # a transition BEFORE started_at seeds current_status (and is skipped)
        _transition("gh:20", _hour(0, 30), "todo", "backlog"),
        # ...and one AFTER completed_at, which must be ignored
        _transition("gh:20", _hour(23, 30), "done", "canceled"),
        # gh:7 sits in a wait status for its whole cycle -> flow_efficiency 0
        _transition("gh:7", _hour(7, 30), "in_progress", "waiting_for_review"),
        # a transition for an item with no cycle time -> never consulted
        _transition("gh:6", _hour(10), "todo", "done"),
    ]
    return items, transitions


def _encode(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "__dataclass_fields__"):
        return {
            name: _encode(getattr(value, name)) for name in value.__dataclass_fields__
        }
    if isinstance(value, (list, tuple)):
        return [_encode(entry) for entry in value]
    return value


def render() -> str:
    items, transitions = _corpus()
    resolvers: dict[str, Any] = {
        "team_resolver": None,
        "project_key_resolver": PROJECT_TEAM_RESOLVER,
        "linked_issue_resolver": None,
        "attribution_context": None,
    }

    group_rows, user_rows, cycle_rows = compute_work_item_metrics_daily(
        day=DAY,
        work_items=items,
        transitions=transitions,
        computed_at=COMPUTED_AT,
        **resolvers,
    )
    coverage_rows = compute_estimate_coverage_metrics_daily(
        day=DAY,
        work_items=items,
        computed_at=COMPUTED_AT,
        **resolvers,
    )
    attribution_rows = compute_work_item_team_attributions(
        work_items=items,
        computed_at=COMPUTED_AT,
        **resolvers,
    )

    payload = {
        "day": DAY.isoformat(),
        "computed_at": _encode(COMPUTED_AT),
        "items": [
            {
                "work_item_id": item.work_item_id,
                "provider": item.provider,
                "type": item.type,
                "status": item.status,
                "work_scope_id": item.work_scope_id,
                "project_id": item.project_id,
                "project_key": item.project_key,
                "project_name": item.project_name,
                "native_team_key": item.native_team_key,
                "assignees": list(item.assignees),
                "created_at": _encode(item.created_at),
                "started_at": _encode(item.started_at),
                "completed_at": _encode(item.completed_at),
                "closed_at": _encode(item.closed_at),
                "story_points": item.story_points,
            }
            for item in items
        ],
        "transitions": [
            {
                "work_item_id": transition.work_item_id,
                "occurred_at": _encode(transition.occurred_at),
                "from_status": transition.from_status,
                "to_status": transition.to_status,
            }
            for transition in transitions
        ],
        # Only the PRIMARY rows, and only the columns the Go read consumes --
        # this mirrors LoadWorkItemPrimaryTeamAttributions' own projection
        # (is_primary = 1, team_id/team_name), not the whole candidate set.
        "primary_attributions": [
            {
                "work_item_id": row.work_item_id,
                "team_id": row.team_id,
                "team_name": row.team_name,
                "source": row.source,
            }
            for row in attribution_rows
            if row.is_primary
        ],
        "work_item_metrics_daily": [_encode(row) for row in group_rows],
        "work_item_user_metrics_daily": [_encode(row) for row in user_rows],
        "work_item_cycle_times": [_encode(row) for row in cycle_rows],
        "estimate_coverage_metrics_daily": [_encode(row) for row in coverage_rows],
    }
    return json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--stdout",
        action="store_true",
        help=(
            "Render to stdout instead of writing the checked-in file. The live "
            "rot guard (internal/jobs/metrics/workitemmetrics) uses this to "
            "compare what TODAY's production Python produces against the frozen "
            "file, so a drift is reported as a diff rather than a bare exit code."
        ),
    )
    args = parser.parse_args()
    rendered = render()
    if args.stdout:
        print(rendered, end="")
        return 0
    if args.check:
        return 0 if OUTPUT.read_text() == rendered else 1
    OUTPUT.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
