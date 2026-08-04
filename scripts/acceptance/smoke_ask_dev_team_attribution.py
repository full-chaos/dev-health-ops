#!/usr/bin/env python3
"""Live acceptance smoke: the team-attribution attack, re-verified after the
CHAOS-3332 fix (ops #1382).

Before #1382, ANY status question naming a real team (tried core/growth/
platform) returned a terminal ERROR/internal_error, 100% reproducible, with
zero corresponding server-side log line -- see
attack.team-attribution.e2e-blocked-by-live-defect's original repro. #1382
fixed the crash; this script re-runs the exact same shape of question
(smoke_ask_dev_exact_commit.py's pattern, a named TEAM subject instead of a
repository) against the fixed code and asserts the honest outcome CHAOS-3333
predicts: the named team subject still commits correctly (scope.resolved
with a non-empty authorized_entity_ids), and the run completes as an
ANSWER (not error) -- but the answer's metrics come back empty
(query_metric.v1 unavailable for a TEAM-scoped request today, tracked
separately as CHAOS-3333) so the answer is honestly ``degraded``, not a
fabricated success. A crash is a failure here; a degraded-but-honest answer
is not.

Writes a machine-checkable execution artifact to
``tests/acceptance/artifacts/team_attribution.json`` -- see
``acceptance_artifact.py``.
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dev_health_ops.api.dev.contracts import DevStreamEvent, StreamEventType
from scripts.acceptance.acceptance_artifact import AcceptanceFailure, ScenarioRecorder
from scripts.acceptance.prepare_ask_dev_acceptance import AcceptanceApi

#: A real fixture-generated team (see docker exec ... clickhouse-client
#: --query "SELECT id, name FROM default.teams" -- core/growth/platform,
#: confirmed live 2026-08-02).
NAMED_TEAM = "Core"
TEAM_ATTRIBUTION_QUESTION = f"What's the status of the {NAMED_TEAM} team?"

_WAVE_3_1_FEATURE_KEY = "ask_dev_wave_3_1"

SCENARIO_ID = "team_attribution"
_ARTIFACT_PATH = (
    Path(__file__).resolve().parents[2]
    / "tests"
    / "acceptance"
    / "artifacts"
    / f"{SCENARIO_ID}.json"
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
                "reason": "CHAOS-3300 team-attribution re-verify (CHAOS-3332)",
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
                "reason": "CHAOS-3300 team-attribution re-verify (CHAOS-3332)",
            },
        )
        recorder.check(
            "wave_3_1_override_updated",
            isinstance(updated, dict) and updated.get("is_enabled") is True,
            f"failed to enable {_WAVE_3_1_FEATURE_KEY}",
        )


def _scope(org_id: str) -> dict[str, Any]:
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
            "filter_fingerprint": "team_attribution_acceptance",
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


def smoke(
    api: AcceptanceApi, recorder: ScenarioRecorder, *, email: str, password: str
) -> str:
    org_id = _authenticate(api, recorder, email=email, password=password)
    _enable_wave_3_1(api, recorder, org_id=org_id)
    scope = _scope(org_id)
    conversation = api.request(
        "POST",
        "/api/v1/dev/conversations",
        {
            "current_scope": scope,
            "retention_days": 30,
            "title": "CHAOS-3300 team-attribution re-verify",
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
            "question": TEAM_ATTRIBUTION_QUESTION,
            "question_class": "status",
            "scope": scope,
        },
    )
    scope_resolved = next(
        (event for event in events if event.event is StreamEventType.SCOPE_RESOLVED),
        None,
    )
    recorder.check(
        "scope_resolved_event_present",
        scope_resolved is not None and scope_resolved.scope_resolution is not None,
        "expected a scope.resolved event committing the named team subject",
    )
    assert scope_resolved is not None and scope_resolved.scope_resolution is not None
    authorized_entities = scope_resolved.scope_resolution.authorized_entity_ids
    recorder.check(
        "named_team_committed",
        len(authorized_entities) > 0,
        "the named team subject was never committed to the resolved scope",
    )

    # The pre-#1382 defect: a terminal ERROR/internal_error instead of any
    # answer. Assert directly that this specific failure mode is gone.
    error_event = next(
        (event for event in events if event.event is StreamEventType.ERROR), None
    )
    recorder.check(
        "no_internal_error_event",
        error_event is None
        or error_event.error is None
        or error_event.error.code != "internal_error",
        "CHAOS-3332 regression: team-subject question crashed to "
        f"internal_error again ({error_event.error if error_event else None!r})",
    )

    completed = next(
        (event for event in events if event.event is StreamEventType.ANSWER_COMPLETED),
        None,
    )
    recorder.check(
        "answer_completed_event_present",
        completed is not None and completed.answer is not None,
        "expected a real answer (possibly degraded) for a named team "
        "subject, not a blank/refused/error result",
    )
    assert completed is not None and completed.answer is not None
    answer = completed.answer
    recorder.check(
        "answer_status_not_hard_error",
        answer.status.value != "error",
        f"expected a degraded-but-honest answer, got status={answer.status.value!r}",
    )
    # Codex finding (MED, 2026-08-02, round 2): "not error" alone does not
    # prove the CHAOS-3333 characterization this row's description claims --
    # a status of "complete" or "partial" would also pass that check. Assert
    # the specific state CHAOS-3333 predicts: degraded (not some other
    # non-error status), metrics genuinely empty (not just an unrelated
    # partial answer), and the limitation named as an unavailable required
    # source rather than silently absent. This is the state the system
    # exists to reach for a TEAM subject today, not merely "did not crash";
    # if CHAOS-3333 later adds TEAM-scoped metric support, this assertion
    # SHOULD start failing -- that is the correct, honest outcome, and the
    # row's claim must be updated alongside the fix, not loosened in
    # advance to survive it.
    recorder.check(
        "answer_status_is_degraded",
        answer.status.value == "degraded",
        f"expected status='degraded' per CHAOS-3333, got {answer.status.value!r}",
    )
    recorder.check(
        "metrics_empty_for_team_scope",
        len(answer.metrics) == 0,
        f"expected no metrics for a TEAM-scoped answer, got {len(answer.metrics)}",
    )
    unavailable_sources = [str(s) for s in answer.coverage.unavailable_required_sources]
    recorder.check(
        "limitation_names_unavailable_metric_source",
        any("metric" in source.lower() for source in unavailable_sources),
        "expected coverage.unavailable_required_sources to name the "
        f"unavailable metric source, got {unavailable_sources!r}",
    )
    recorder.check(
        "stream_terminated_as_answer",
        events[-1].terminal_kind == "answer",
        "stream did not terminate as answer",
    )
    return conversation_id


def main() -> int:
    if os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1":
        print("ASK_DEV_LIVE_ACCEPTANCE=1 is required", file=sys.stderr)
        return 64
    api = AcceptanceApi(
        os.getenv("ASK_DEV_ACCEPTANCE_API_URL", "http://127.0.0.1:8000")
    )
    email = os.getenv("TEST_SUPERUSER_EMAIL", "admin@devhealth.example")
    password = os.getenv("TEST_SUPERUSER_PASSWORD", "devhealth123")
    recorder = ScenarioRecorder(
        scenario_id=SCENARIO_ID, script_path=Path(__file__).resolve()
    )
    error_detail: str | None = None
    conversation_id: str | None = None
    try:
        conversation_id = smoke(api, recorder, email=email, password=password)
    except AcceptanceFailure as exc:
        error_detail = str(exc)
    artifact = recorder.write(_ARTIFACT_PATH, error=error_detail)
    if error_detail is not None:
        print(
            f"Ask Dev team-attribution re-verify failed: {error_detail}",
            file=sys.stderr,
        )
        print(f"wrote {_ARTIFACT_PATH} (status={artifact['status']})", file=sys.stderr)
        return 1
    print(
        f"Ask Dev team-attribution re-verify completed (conversation={conversation_id})"
    )
    print(f"wrote {_ARTIFACT_PATH} (status={artifact['status']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
