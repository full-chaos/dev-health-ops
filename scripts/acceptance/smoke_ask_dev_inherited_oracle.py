#!/usr/bin/env python3
"""Live acceptance smoke: re-verify the inherited positive-control oracle.

``tests/acceptance/ask-dev-oracle.v1.json`` predates CHAOS-3300 and is the
question the Playwright leg of ``run_ask_dev_compose.sh`` asks through the
web UI. This script asks the SAME exact question over the real Ask Dev
REST/SSE API instead (no Playwright/web dependency) and asserts the same
three claims the oracle file encodes: which metric grounds the answer, which
evidence entity it cites, and what claim kind it makes.

This scenario is organization-wide (the oracle names no subject), so it does
NOT independently prove a named-project claim -- see
``smoke_ask_dev_project_status.py`` for that (codex finding HIGH,
2026-08-02: this script's prior use as evidence for
``positive-control.real-project-status``/``matrix.exact-project-complete``
overclaimed "exact project" from an org-wide run with no SCOPE_RESOLVED
assertion).

Writes a machine-checkable execution artifact to
``tests/acceptance/artifacts/inherited_oracle.json`` -- see
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

_ORACLE_PATH = (
    Path(__file__).resolve().parents[2]
    / "tests"
    / "acceptance"
    / "ask-dev-oracle.v1.json"
)

SCENARIO_ID = "inherited_oracle"
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


def _scope(org_id: str) -> dict[str, Any]:
    # 14+14, not 28+28: `dev-hops fixtures generate --days 28` (see
    # run_ask_dev_compose.sh) only backfills 28 days of history, so a
    # comparison window starting 28 days before the current window's start
    # falls entirely outside the seeded data and comparison_value comes back
    # null (observed live 2026-08-02 -- a test-scope bug, not a product one:
    # every metric in-window is real, there is just nothing seeded further
    # back to compare against).
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
            "filter_fingerprint": "inherited_oracle_acceptance",
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


def smoke(
    api: AcceptanceApi, recorder: ScenarioRecorder, *, email: str, password: str
) -> str:
    oracle = json.loads(_ORACLE_PATH.read_text(encoding="utf-8"))
    question = oracle["question"]
    expected_metric_id = oracle["expected_metric_id"]
    expected_evidence_fragment = oracle["expected_evidence_entity_fragment"]
    expected_claim_kind = oracle["expected_claim_kind"]

    org_id = _authenticate(api, recorder, email=email, password=password)
    scope = _scope(org_id)
    conversation = api.request(
        "POST",
        "/api/v1/dev/conversations",
        {
            "current_scope": scope,
            "retention_days": 30,
            "title": "CHAOS-3300 inherited-oracle re-verification",
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
            "question": question,
            "question_class": "observed_change",
            "scope": scope,
            "requested_metric_ids": [expected_metric_id],
        },
    )
    completed = next(
        (event for event in events if event.event is StreamEventType.ANSWER_COMPLETED),
        None,
    )
    if completed is None or completed.answer is None:
        failed = next(
            (event for event in events if event.event is StreamEventType.ERROR), None
        )
        detail = "no answer completed"
        if failed is not None and failed.error is not None:
            detail = (
                f"Ask Dev run failed with {failed.error.code}: "
                f"{failed.error.safe_message}"
            )
        recorder.check("answer_completed_event_present", False, detail)
        raise AcceptanceFailure(detail)  # pragma: no cover - recorder already raised
    answer = completed.answer
    recorder.check(
        "answer_status_not_error",
        answer.status.value != "error",
        "Ask Dev returned an error-status answer",
    )
    recorder.check(
        "expected_metric_present",
        any(metric.metric_id == expected_metric_id for metric in answer.metrics),
        f"expected metric {expected_metric_id!r} not present in the answer",
    )
    recorder.check(
        "expected_evidence_fragment_present",
        any(
            expected_evidence_fragment in str(item.entity_id)
            for item in answer.evidence
        ),
        f"expected evidence entity fragment {expected_evidence_fragment!r} not "
        "present in the answer",
    )
    recorder.check(
        "expected_claim_kind_present",
        any(claim.kind.value == expected_claim_kind for claim in answer.claims),
        f"expected claim kind {expected_claim_kind!r} not present in the answer",
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
            f"Ask Dev inherited-oracle re-verification failed: {error_detail}",
            file=sys.stderr,
        )
        print(f"wrote {_ARTIFACT_PATH} (status={artifact['status']})", file=sys.stderr)
        return 1
    print(
        "Ask Dev inherited-oracle re-verification completed "
        f"(conversation={conversation_id})"
    )
    print(f"wrote {_ARTIFACT_PATH} (status={artifact['status']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
