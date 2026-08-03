#!/usr/bin/env python3
"""Live acceptance smoke: the CHAOS-3297 stack-3 newly-wired intents, run
against the real Compose stack after ops #1383 (s2)/#1387 (s3) merged
``WAVE_3_1_PLANS_BY_INTENT`` into ``production_runtime.py``'s live
``plan_registry``.

Four scenarios, each its own artifact:

- ``team_health`` (TEAM_HEALTH, a named TEAM subject) -- proves
  health.team.v1 is reachable and answers for a real team.
- ``team_workload_balance`` (TEAM_WORKLOAD_BALANCE, a named TEAM subject)
  -- proves balance.team_workload.v1 is reachable and answers.
- ``operational_deficiency_team`` (OPERATIONAL_DEFICIENCY_INVENTORY, a
  named TEAM subject -- the plan also supports PROJECT, but this fixture
  profile seeds zero PROJECT-kind rows, the same constraint already
  documented on positive-control.real-project-status) -- proves
  deficiency.operational.v1 is reachable and answers.
- ``portfolio_status_gap`` (PORTFOLIO_STATUS) -- proves the OPPOSITE: this
  intent is deliberately NOT wired (wave_3_1_plans.py's own module
  docstring -- StepContext carries exactly one DevScope, and
  PortfolioStatusService needs several), so a portfolio-shaped question
  must fall back LOUDLY (ask_dev.orchestrator.plan_registry_gap WARNING +
  ASK_DEV_PLAN_REGISTRY_GAP_TOTAL counter) to the legacy model-round loop
  and still complete as a non-error answer -- never terminate, per
  orchestrator.py's team-lead-ratified rule. This smoke cannot observe the
  log line or counter directly (no public API surface for either), so it
  proves the externally-observable half of that contract: the run
  completes rather than hanging or crashing. The WARNING/counter half is
  covered separately at the unit level -- see the manifest row's evidence.

None of these plans use conditional model-tool-call rounds (each mandatory
step calls its canonical service directly per wave_3_1_plans.py's module
docstring), but answer synthesis still runs through the scripted provider,
so this is exercised the same way as the other live scenarios.

Codex finding (HIGH, 2026-08-03, round 4): scope+commit+non-error is not
proof the CLAIMED plan executed -- a legacy-loop answer (the exact
fallback PORTFOLIO_STATUS uses) would pass every one of those checks too
if a plan-registry entry were silently missing. The trustworthy signal is
the same one used diagnosing CHAOS-3332: a direct ``dev_runs`` row read
(``preflight_outcome`` + ``plan_step_partition``), not the scripted
provider's intent-blind narrative. Each team-scoped scenario now reads its
own row (via ``docker exec`` into the acceptance Postgres container -- the
same technique, not a new one) and asserts ``preflight_outcome ==
"proceeded_committed_subject"`` AND that the row's OWN mandatory step name
(``health_evaluation`` / ``workload_evaluation`` / ``deficiency_
evaluation`` -- these differ per plan in wave_3_1_plans.py, which is what
lets this assertion distinguish the three plans from each other, not only
from the legacy loop) appears in ``plan_step_partition["completed"]``.

Negative-control demonstration performed once (2026-08-03, not automated
as a permanent test -- it needs temporarily editing the production plan
registry, not something safe to run as part of a normal test suite):
``_HEALTH_TEAM`` was removed from ``WAVE_3_1_PLANS_BY_INTENT`` in
wave_3_1_plans.py, the api container rebuilt, and team_health re-run. The
old assertions (scope resolved, named team committed, a real non-error
answer) ALL STILL PASSED -- the legacy loop silently absorbed the missing
plan exactly as codex predicted. Only the new plan_step_partition
assertion failed (``health_evaluation`` absent from the completed-steps
list). The change was then reverted, the container rebuilt again, and the
scenario re-run to confirm it passes cleanly -- see the commit message for
both runs' recorded observation.

Writes one machine-checkable execution artifact per scenario to
``tests/acceptance/artifacts/<scenario_id>.json`` -- see
``acceptance_artifact.py``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dev_health_ops.api.dev.contracts import DevStreamEvent, StreamEventType
from scripts.acceptance.acceptance_artifact import AcceptanceFailure, ScenarioRecorder
from scripts.acceptance.prepare_ask_dev_acceptance import AcceptanceApi

NAMED_TEAM = "Core"
_WAVE_3_1_FEATURE_KEY = "ask_dev_wave_3_1"
_POSTGRES_CONTAINER = "ask-dev-acceptance-postgres"

_ARTIFACT_DIR = (
    Path(__file__).resolve().parents[2] / "tests" / "acceptance" / "artifacts"
)


@dataclass(frozen=True, slots=True)
class Scenario:
    scenario_id: str
    question: str
    question_class: str
    #: A SINGULAR-cardinality question needs exactly one entity mention in
    #: surface_context.entity_refs to resolve a named TEAM subject; an
    #: ORGANIZATION-WIDE one (portfolio_status_gap) needs none.
    team_scoped: bool
    #: The plan's own mandatory step name this scenario's plan_step_
    #: partition must show completed -- None for portfolio_status_gap,
    #: which deliberately has no plan to check (that is its own claim).
    expected_plan_step: str | None = None


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        scenario_id="team_health",
        question=f"Is the {NAMED_TEAM} team healthy?",
        question_class="status",
        team_scoped=True,
        expected_plan_step="health_evaluation",
    ),
    Scenario(
        scenario_id="team_workload_balance",
        question=f"Is the {NAMED_TEAM} team overburdened?",
        question_class="status",
        team_scoped=True,
        expected_plan_step="workload_evaluation",
    ),
    Scenario(
        scenario_id="operational_deficiency_team",
        question=f"What operational deficiencies does the {NAMED_TEAM} team have?",
        question_class="status",
        team_scoped=True,
        expected_plan_step="deficiency_evaluation",
    ),
    Scenario(
        scenario_id="portfolio_status_gap",
        question="What's the status of all our projects across the portfolio?",
        question_class="status",
        team_scoped=False,
    ),
)


def _read_dev_run(run_id: str) -> dict[str, Any]:
    """Read one ``dev_runs`` row directly from Postgres via ``docker exec``
    -- the acceptance profile publishes no host port for Postgres (see
    tests/acceptance/compose.ask-dev.yml's ``ports: !reset []``), and this
    mirrors the exact technique used live-diagnosing CHAOS-3332 rather than
    inventing a new DB-access path for this one check.

    Returns ``{"preflight_outcome": str | None, "plan_step_partition": ...}``
    -- a single-row JSON object built server-side so there is no fragile
    tab/column parsing of psql's tabular output.
    """

    query = (
        "SELECT jsonb_build_object("
        "'preflight_outcome', preflight_outcome, "
        "'plan_step_partition', plan_step_partition"
        f") FROM dev_runs WHERE id = '{run_id}'"
    )
    result = subprocess.run(
        [
            "docker",
            "exec",
            _POSTGRES_CONTAINER,
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-t",
            "-A",
            "-c",
            query,
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        raise AcceptanceFailure(
            f"dev_runs lookup for run {run_id} failed: {result.stderr.strip()}"
        )
    stdout = result.stdout.strip()
    if not stdout:
        raise AcceptanceFailure(f"dev_runs has no row for run {run_id}")
    parsed = json.loads(stdout)
    if not isinstance(parsed, dict):
        raise AcceptanceFailure(
            f"dev_runs lookup for run {run_id} returned a non-object: {parsed!r}"
        )
    return parsed


def _authenticate(
    api: AcceptanceApi, recorder: ScenarioRecorder, *, email: str, password: str
) -> str:
    login = api.request(
        "POST", "/api/v1/auth/login", {"email": email, "password": password}
    )
    recorder.check("login_response_is_object", isinstance(login, dict), str(login))
    token = login.get("access_token")
    user = login.get("user")
    recorder.check(
        "login_returned_access_token",
        isinstance(token, str) and bool(token),
        "login returned no access token",
    )
    recorder.check(
        "login_returned_user", isinstance(user, dict), "login returned no user"
    )
    org_id = user.get("org_id")
    recorder.check(
        "login_returned_org_id",
        isinstance(org_id, str) and bool(org_id),
        "login returned no org_id",
    )
    api.token = token
    return org_id


def _enable_wave_3_1(
    api: AcceptanceApi, recorder: ScenarioRecorder, *, org_id: str
) -> None:
    flags = api.request("GET", "/api/v1/admin/feature-flags")
    recorder.check(
        "feature_flags_is_list", isinstance(flags, list), "response was not a list"
    )
    flag = next(
        (
            item
            for item in flags
            if isinstance(item, dict) and item.get("key") == _WAVE_3_1_FEATURE_KEY
        ),
        None,
    )
    recorder.check(
        "wave_3_1_flag_registered",
        flag is not None,
        f"feature flag {_WAVE_3_1_FEATURE_KEY} is not registered",
    )
    assert flag is not None
    override_path = f"/api/v1/admin/orgs/{org_id}/feature-overrides"
    overrides = api.request("GET", override_path)
    recorder.check(
        "feature_overrides_is_list",
        isinstance(overrides, list),
        "response was not a list",
    )
    existing = next(
        (
            item
            for item in overrides
            if isinstance(item, dict)
            and item.get("feature_key") == _WAVE_3_1_FEATURE_KEY
        ),
        None,
    )
    if existing is None:
        created = api.request(
            "POST",
            override_path,
            {
                "feature_id": flag["id"],
                "is_enabled": True,
                "reason": "CHAOS-3300 stack-3 newly-wired intents",
            },
        )
        recorder.check(
            "wave_3_1_override_created",
            isinstance(created, dict) and created.get("is_enabled") is True,
            f"failed to enable {_WAVE_3_1_FEATURE_KEY}",
        )
    elif existing.get("is_enabled") is not True:
        override_id = existing.get("id")
        recorder.check(
            "wave_3_1_override_has_id",
            isinstance(override_id, str),
            f"{_WAVE_3_1_FEATURE_KEY} override has no id",
        )
        updated = api.request(
            "PATCH",
            f"{override_path}/{override_id}",
            {
                "is_enabled": True,
                "reason": "CHAOS-3300 stack-3 newly-wired intents",
            },
        )
        recorder.check(
            "wave_3_1_override_updated",
            isinstance(updated, dict) and updated.get("is_enabled") is True,
            f"failed to enable {_WAVE_3_1_FEATURE_KEY}",
        )


def _scope(org_id: str, *, team_scoped: bool) -> dict[str, Any]:
    now = datetime.now(UTC).replace(microsecond=0)
    current_start = now - timedelta(days=14)
    comparison_start = current_start - timedelta(days=14)

    def time_range(start: datetime, end: datetime) -> dict[str, str]:
        return {
            "start": start.isoformat().replace("+00:00", "Z"),
            "end": end.isoformat().replace("+00:00", "Z"),
            "timezone": "UTC",
        }

    return {
        "schema_version": "dev_scope.v1",
        "organization_id": org_id,
        "direct_scope": "organization",
        "repositories": [],
        "entity_refs": [],
        "team_ids": [],
        "time_range": time_range(current_start, now),
        "comparison_range": time_range(comparison_start, current_start),
        "surface_context": {
            "route_id": "diagnose_overview",
            "entity_refs": [],
            "filter_fingerprint": "stack3_intents_acceptance",
        },
    }


def _sse_request(
    api: AcceptanceApi, recorder: ScenarioRecorder, path: str, payload: dict[str, Any]
) -> list[DevStreamEvent]:
    recorder.check(
        "sse_request_authenticated",
        api.token is not None,
        "SSE request requires authentication",
    )
    request = Request(
        f"{api.base_url}{path}",
        data=json.dumps(payload, separators=(",", ":")).encode(),
        headers={
            "Accept": "text/event-stream",
            "Authorization": f"Bearer {api.token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=60) as response:  # noqa: S310
            content_type = response.headers.get_content_type()
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise AcceptanceFailure(
            f"POST {path} returned HTTP {exc.code}: {detail}"
        ) from exc
    except URLError as exc:
        raise AcceptanceFailure(f"POST {path} failed: {exc.reason}") from exc

    recorder.check(
        "sse_content_type",
        content_type == "text/event-stream",
        f"unexpected SSE type {content_type}",
    )
    events: list[DevStreamEvent] = []
    for frame in body.split("\n\n"):
        if not frame.strip():
            continue
        event_name: str | None = None
        data_lines: list[str] = []
        for line in frame.splitlines():
            if line.startswith("event: "):
                event_name = line.removeprefix("event: ")
            elif line.startswith("data: "):
                data_lines.append(line.removeprefix("data: "))
        recorder.check(
            "sse_frame_has_event_name",
            event_name is not None,
            "SSE frame omitted event name",
        )
        recorder.check(
            "sse_frame_has_data",
            bool(data_lines),
            f"SSE {event_name} frame omitted data",
        )
        raw = json.loads("\n".join(data_lines))
        events.append(DevStreamEvent.model_validate(raw))
    return events


def _run_scenario(
    api: AcceptanceApi, recorder: ScenarioRecorder, scenario: Scenario, *, org_id: str
) -> str:
    scope = _scope(org_id, team_scoped=scenario.team_scoped)
    conversation = api.request(
        "POST",
        "/api/v1/dev/conversations",
        {
            "current_scope": scope,
            "retention_days": 30,
            "title": f"CHAOS-3300 stack-3 acceptance: {scenario.scenario_id}",
        },
    )
    recorder.check(
        "conversation_response_is_object",
        isinstance(conversation, dict),
        "conversation response was not an object",
    )
    conversation_id = conversation.get("conversation_id")
    recorder.check(
        "conversation_id_present",
        isinstance(conversation_id, str) and bool(conversation_id),
        "conversation response returned no conversation_id",
    )
    events = _sse_request(
        api,
        recorder,
        f"/api/v1/dev/conversations/{conversation_id}/messages",
        {
            "schema_version": "dev_message_request.v1",
            "request_id": str(uuid.uuid4()),
            "client_message_id": str(uuid.uuid4()),
            "conversation_id": conversation_id,
            "question": scenario.question,
            "question_class": scenario.question_class,
            "scope": scope,
        },
    )
    # Codex finding (HIGH, 2026-08-03, round 3): scope resolution was only
    # checked for team_scoped scenarios -- portfolio_status_gap's row claims
    # "committed scope" as part of its contract too (a real scope.resolved
    # event, not merely "some answer came back"), and the prior code let
    # that go unasserted. Every scenario here resolves a real scope
    # (organization-wide for portfolio, team-committed for the other
    # three), so this check now runs unconditionally.
    scope_resolved = next(
        (e for e in events if e.event is StreamEventType.SCOPE_RESOLVED), None
    )
    recorder.check(
        "scope_resolved_event_present",
        scope_resolved is not None and scope_resolved.scope_resolution is not None,
        "expected a scope.resolved event",
    )
    if (
        scenario.team_scoped
        and scope_resolved is not None
        and scope_resolved.scope_resolution is not None
    ):
        authorized_entities = scope_resolved.scope_resolution.authorized_entity_ids
        recorder.check(
            "named_team_committed",
            len(authorized_entities) > 0,
            "the named team subject was never committed to the resolved scope",
        )

    detail = "no answer completed"
    completed = next(
        (e for e in events if e.event is StreamEventType.ANSWER_COMPLETED), None
    )
    if completed is None or completed.answer is None:
        failed = next((e for e in events if e.event is StreamEventType.ERROR), None)
        if failed is not None and failed.error is not None:
            detail = (
                f"Ask Dev run failed with {failed.error.code}: "
                f"{failed.error.safe_message}"
            )
    recorder.check(
        "answer_completed_event_present",
        completed is not None and completed.answer is not None,
        detail,
    )
    assert completed is not None and completed.answer is not None
    answer = completed.answer
    recorder.check(
        "answer_status_not_hard_error",
        answer.status.value != "error",
        f"expected a real (possibly degraded) answer, got status={answer.status.value!r}",
    )
    if scenario.expected_plan_step is not None:
        # Codex finding (HIGH, 2026-08-03, round 4): the checks above (a
        # committed scope, a real non-error answer) do NOT distinguish "the
        # claimed plan actually ran" from "the legacy fallback silently
        # absorbed a missing plan-registry entry" -- portfolio_status_gap's
        # own row proves the legacy loop produces exactly this shape of
        # answer. Read the persisted dev_runs row directly (never the
        # scripted provider's narrative, which is intent-blind) and assert
        # the plan-governed path specifically ran.
        dev_run = _read_dev_run(str(completed.run_id))
        recorder.check(
            "preflight_proceeded_committed_subject",
            dev_run.get("preflight_outcome") == "proceeded_committed_subject",
            f"expected preflight_outcome='proceeded_committed_subject', "
            f"got {dev_run.get('preflight_outcome')!r}",
        )
        partition = dev_run.get("plan_step_partition")
        completed_steps = (
            partition.get("completed", []) if isinstance(partition, dict) else []
        )
        recorder.check(
            "claimed_plan_step_completed",
            scenario.expected_plan_step in completed_steps,
            f"expected {scenario.expected_plan_step!r} in plan_step_partition"
            f"['completed'], got partition={partition!r} -- a legacy-loop "
            "fallback would pass every OTHER assertion in this scenario "
            "while never running this step",
        )
    recorder.check(
        "stream_terminated_as_answer",
        events[-1].terminal_kind == "answer",
        "stream did not terminate as answer -- a hang or non-answer terminal "
        "kind would mean the newly-wired plan or the deliberate portfolio "
        "fallback broke the run rather than completing it",
    )
    if scenario.scenario_id == "portfolio_status_gap":
        # Codex finding (HIGH, 2026-08-03, round 3): "not error" alone
        # passes for status in {complete, partial, degraded} -- the row's
        # own claim is specifically that the legacy-fallback answer lands
        # as PARTIAL (a provider-budget-exhaustion outcome from running
        # many progress rounds, per production_runtime.py's fallback
        # answer builder), not any other non-error status. Assert the
        # exact value the row claims, not a category that would also
        # accept a lucky "complete".
        recorder.check(
            "answer_status_is_exactly_partial",
            answer.status.value == "partial",
            f"expected status='partial' per the legacy-fallback contract, "
            f"got {answer.status.value!r}",
        )
        # Live-confirmed 2026-08-03: answer.warnings for this scenario are
        # ["Deterministic scripted acceptance response.", "Provider health
        # was measured through data_health.v1."] -- generic scripted-
        # provider boilerplate present on every scenario this harness runs,
        # NOT text tied to the plan_registry_gap signal. The WARNING stream
        # events mirror answer.warnings 1:1 (streaming.py's public_event
        # loop), so there is no client-observable signal distinguishing
        # "this run hit the plan_registry_gap fallback" from any other
        # reason the legacy loop's warnings might fire -- the structured
        # WARNING log record + ASK_DEV_PLAN_REGISTRY_GAP_TOTAL counter this
        # row's other half of the claim depends on are server-side only,
        # with no public API surface. Recorded here as an assertion so a
        # future change to that boilerplate text is visible, NOT as a
        # stand-in for the signal itself -- gate.plan-registry-gap-is-loud
        # proves the signal at the unit level, through the real
        # orchestrator seam, which is the only place it is observable.
        recorder.check(
            "warnings_present_but_not_a_plan_registry_gap_signal",
            len(answer.warnings) > 0,
            f"expected the scripted provider's boilerplate warnings, got "
            f"{answer.warnings!r}",
        )
    return conversation_id


def smoke(
    api: AcceptanceApi, *, email: str, password: str
) -> dict[str, dict[str, Any]]:
    bootstrap_recorder = ScenarioRecorder(
        scenario_id="stack3_bootstrap", script_path=Path(__file__).resolve()
    )
    org_id = _authenticate(api, bootstrap_recorder, email=email, password=password)
    _enable_wave_3_1(api, bootstrap_recorder, org_id=org_id)
    artifacts: dict[str, dict[str, Any]] = {}
    failures: list[str] = []
    for scenario in SCENARIOS:
        recorder = ScenarioRecorder(
            scenario_id=scenario.scenario_id, script_path=Path(__file__).resolve()
        )
        error_detail: str | None = None
        try:
            _run_scenario(api, recorder, scenario, org_id=org_id)
        except AcceptanceFailure as exc:
            error_detail = str(exc)
            failures.append(f"{scenario.scenario_id}: {error_detail}")
        artifact_path = _ARTIFACT_DIR / f"{scenario.scenario_id}.json"
        artifacts[scenario.scenario_id] = recorder.write(
            artifact_path, error=error_detail
        )
    if failures:
        raise AcceptanceFailure("; ".join(failures))
    return artifacts


def main() -> int:
    if os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1":
        print("ASK_DEV_LIVE_ACCEPTANCE=1 is required", file=sys.stderr)
        return 64
    api = AcceptanceApi(
        os.getenv("ASK_DEV_ACCEPTANCE_API_URL", "http://127.0.0.1:8000")
    )
    email = os.getenv("TEST_SUPERUSER_EMAIL", "admin@devhealth.example")
    password = os.getenv("TEST_SUPERUSER_PASSWORD", "devhealth123")
    try:
        artifacts = smoke(api, email=email, password=password)
    except AcceptanceFailure as exc:
        print(
            f"Ask Dev stack-3 intents acceptance smoke failed: {exc}", file=sys.stderr
        )
        return 1
    print("Ask Dev stack-3 intents acceptance smoke completed:")
    for scenario_id, artifact in artifacts.items():
        print(f"  {scenario_id}: status={artifact['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
