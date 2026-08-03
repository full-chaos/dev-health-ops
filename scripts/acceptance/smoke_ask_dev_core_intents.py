#!/usr/bin/env python3
"""Live acceptance smoke: organization-wide DATA_TRUST and REMAINING_WORK
questions, proven over the real public Ask Dev REST/SSE API.

Same shape as ``smoke_ask_dev_provider_profile.py`` and
``smoke_ask_dev_not_found.py`` -- no Playwright, no dev-health-web. Both
scenarios run organization-wide (no named subject), so they exercise the
same scripted-provider default flow the existing positive-control oracle
already proves safe, but for two more of CHAOS-3300's 6 wired core question
classes (``QuestionClass.DATA_TRUST`` / ``QuestionClass.REMAINING_WORK``)
rather than only the oracle's single ``observed_change`` question.

Writes one machine-checkable execution artifact per scenario --
``tests/acceptance/artifacts/data_trust_organization_wide.json`` and
``tests/acceptance/artifacts/remaining_work_organization_wide.json`` -- see
``acceptance_artifact.py``.
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dev_health_ops.api.dev.contracts import DevStreamEvent, StreamEventType
from dev_health_ops.llm.agent.scripted_openai_service import LIST_METRICS_QUESTION
from scripts.acceptance.acceptance_artifact import AcceptanceFailure, ScenarioRecorder
from scripts.acceptance.prepare_ask_dev_acceptance import AcceptanceApi

_ARTIFACT_DIR = (
    Path(__file__).resolve().parents[2] / "tests" / "acceptance" / "artifacts"
)


@dataclass(frozen=True, slots=True)
class Scenario:
    scenario_id: str
    question: str
    question_class: str


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        scenario_id="data_trust_organization_wide",
        question="Can we trust the data in this scope, or is anything stale or unconfigured?",
        question_class="data_trust",
    ),
    Scenario(
        scenario_id="remaining_work_organization_wide",
        question="What work remains in this scope right now?",
        question_class="remaining_work",
    ),
    # The literal CHAOS-3262 reproduction question -- scripted_openai_
    # service.py special-cases exactly this string to drive a deterministic
    # list_metrics.v1 tool call (see LIST_METRICS_QUESTION's docstring).
    Scenario(
        scenario_id="registered_metrics_organization_wide",
        question=LIST_METRICS_QUESTION,
        question_class="registered_statistics",
    ),
)


def _authenticate(
    api: AcceptanceApi, recorder: ScenarioRecorder, *, email: str, password: str
) -> str:
    login = api.request(
        "POST",
        "/api/v1/auth/login",
        {"email": email, "password": password},
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


def _scope(org_id: str) -> dict[str, Any]:
    now = datetime.now(UTC).replace(microsecond=0)
    current_start = now - timedelta(days=28)
    comparison_start = current_start - timedelta(days=28)

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
            "filter_fingerprint": "core_intents_acceptance",
        },
    }


def _sse_request(
    api: AcceptanceApi,
    recorder: ScenarioRecorder,
    path: str,
    payload: dict[str, Any],
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
        with urlopen(request, timeout=180) as response:  # noqa: S310
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
    api: AcceptanceApi,
    recorder: ScenarioRecorder,
    scenario: Scenario,
    *,
    org_id: str,
    scope: dict[str, Any],
) -> str:
    conversation = api.request(
        "POST",
        "/api/v1/dev/conversations",
        {
            "current_scope": scope,
            "retention_days": 30,
            "title": f"CHAOS-3300 core-intent acceptance: {scenario.scenario_id}",
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
    completed = next(
        (event for event in events if event.event is StreamEventType.ANSWER_COMPLETED),
        None,
    )
    detail = "no answer completed"
    if completed is None or completed.answer is None:
        failed = next(
            (event for event in events if event.event is StreamEventType.ERROR), None
        )
        if failed is not None and failed.error is not None:
            detail = (
                f"Ask Dev run failed with {failed.error.code}: "
                f"{failed.error.safe_message}"
            )
    # Codex finding (MED, 2026-08-02, round 2): unconditional so this
    # assertion can appear passed=True in a real successful artifact -- see
    # smoke_ask_dev_exact_commit.py's identical fix for the full rationale.
    recorder.check(
        "answer_completed_event_present",
        completed is not None and completed.answer is not None,
        detail,
    )
    assert completed is not None and completed.answer is not None
    answer = completed.answer
    recorder.check(
        "answer_status_not_error",
        answer.status.value != "error",
        "Ask Dev returned an error-status answer",
    )
    recorder.check(
        "answer_summary_not_empty",
        bool(answer.direct_summary.strip()),
        "answer summary was empty",
    )
    recorder.check(
        "stream_terminated_as_answer",
        events[-1].terminal_kind == "answer",
        "stream did not terminate as answer",
    )
    return conversation_id


def smoke(
    api: AcceptanceApi, *, email: str, password: str
) -> dict[str, dict[str, Any]]:
    """Runs every scenario, writing one artifact each, and returns
    ``{scenario_id: artifact}``. Does not stop at the first scenario's
    failure -- each scenario's recorder and artifact are independent, so one
    broken scenario does not prevent the others from being measured."""

    bootstrap_recorder = ScenarioRecorder(
        scenario_id="core_intents_bootstrap", script_path=Path(__file__).resolve()
    )
    org_id = _authenticate(api, bootstrap_recorder, email=email, password=password)
    scope = _scope(org_id)
    artifacts: dict[str, dict[str, Any]] = {}
    failures: list[str] = []
    for scenario in SCENARIOS:
        recorder = ScenarioRecorder(
            scenario_id=scenario.scenario_id, script_path=Path(__file__).resolve()
        )
        error_detail: str | None = None
        try:
            _run_scenario(api, recorder, scenario, org_id=org_id, scope=scope)
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
        print(f"Ask Dev core-intents acceptance smoke failed: {exc}", file=sys.stderr)
        return 1
    print("Ask Dev core-intents acceptance smoke completed:")
    for scenario_id, artifact in artifacts.items():
        print(f"  {scenario_id}: status={artifact['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
