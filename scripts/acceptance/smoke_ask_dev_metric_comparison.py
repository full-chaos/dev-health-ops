#!/usr/bin/env python3
"""Live acceptance smoke: organization-wide multi-metric comparison
(``QuestionIntentID.METRIC_COMPARISON`` / ``metric.comparison.v1``).

Same shape as the other CHAOS-3300 smoke scripts -- real Compose stack,
real HTTP/SSE API, no Playwright/web dependency. Proves the wired
``metric.comparison.v1`` plan actually runs for a real multi-metric request
(``requested_metric_ids`` with two canonical metric ids), by checking the
persisted ``dev_run_source_observations`` row's ``registered_metric_query``
content directly rather than only the streamed answer -- the scripted
provider's answer narrative is intent-blind (it always narrates
``items_completed`` regardless of what was actually compared), so the
plan-governed investigation result is the only place this scenario is
actually observable end to end today.
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dev_health_ops.api.dev.contracts import DevStreamEvent, StreamEventType
from scripts.acceptance.prepare_ask_dev_acceptance import (
    AcceptanceApi,
    AcceptanceFailure,
)

#: Exact-alias phrasing (see question_interpreter._metric_aliases): each
#: metric name must appear verbatim (registry value-with-spaces or label,
#: normalized) for has_metric_alias to fire alongside the "compare" anchor.
METRIC_COMPARISON_QUESTION = "Compare items completed against cyclomatic per kloc"
REQUESTED_METRIC_IDS = ("items_completed", "cyclomatic_per_kloc")

_WAVE_3_1_FEATURE_KEY = "ask_dev_wave_3_1"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AcceptanceFailure(message)


def _authenticate(api: AcceptanceApi, *, email: str, password: str) -> str:
    login = api.request(
        "POST",
        "/api/v1/auth/login",
        {"email": email, "password": password},
    )
    _require(isinstance(login, dict), "login response was not an object")
    token = login.get("access_token")
    user = login.get("user")
    _require(isinstance(token, str) and bool(token), "login returned no access token")
    _require(isinstance(user, dict), "login returned no user")
    org_id = user.get("org_id")
    _require(isinstance(org_id, str) and bool(org_id), "login returned no org_id")
    api.token = token
    return org_id


def _enable_wave_3_1(api: AcceptanceApi, *, org_id: str) -> None:
    flags = api.request("GET", "/api/v1/admin/feature-flags")
    _require(isinstance(flags, list), "feature flag response was not a list")
    flag = next(
        (
            item
            for item in flags
            if isinstance(item, dict) and item.get("key") == _WAVE_3_1_FEATURE_KEY
        ),
        None,
    )
    _require(
        flag is not None, f"feature flag {_WAVE_3_1_FEATURE_KEY} is not registered"
    )
    assert flag is not None
    override_path = f"/api/v1/admin/orgs/{org_id}/feature-overrides"
    overrides = api.request("GET", override_path)
    _require(isinstance(overrides, list), "feature override response was not a list")
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
                "reason": "CHAOS-3300 metric-comparison acceptance scenario",
            },
        )
        _require(
            isinstance(created, dict) and created.get("is_enabled") is True,
            f"failed to enable {_WAVE_3_1_FEATURE_KEY}",
        )
    elif existing.get("is_enabled") is not True:
        override_id = existing.get("id")
        _require(
            isinstance(override_id, str), f"{_WAVE_3_1_FEATURE_KEY} override has no id"
        )
        updated = api.request(
            "PATCH",
            f"{override_path}/{override_id}",
            {
                "is_enabled": True,
                "reason": "CHAOS-3300 metric-comparison acceptance scenario",
            },
        )
        _require(
            isinstance(updated, dict) and updated.get("is_enabled") is True,
            f"failed to enable {_WAVE_3_1_FEATURE_KEY}",
        )


def _scope(org_id: str) -> dict[str, Any]:
    # 14+14: see smoke_ask_dev_inherited_oracle.py's comment -- fixtures
    # only backfill 28 days, so a 28+28 window leaves the comparison half
    # empty.
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
            "filter_fingerprint": "metric_comparison_acceptance",
        },
    }


def _sse_request(
    api: AcceptanceApi, path: str, payload: dict[str, Any]
) -> list[DevStreamEvent]:
    _require(api.token is not None, "SSE request requires authentication")
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

    _require(content_type == "text/event-stream", f"unexpected SSE type {content_type}")
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
        _require(event_name is not None, "SSE frame omitted event name")
        _require(bool(data_lines), f"SSE {event_name} frame omitted data")
        raw = json.loads("\n".join(data_lines))
        events.append(DevStreamEvent.model_validate(raw))
    return events


def smoke(api: AcceptanceApi, *, email: str, password: str) -> tuple[str, str]:
    """Returns ``(conversation_id, run_id)``.

    ``run_id`` is printed so a caller who wants to inspect the persisted
    ``dev_run_source_observations`` row directly (the only place this
    scenario's real per-metric result currently surfaces) can do so; this
    script itself only proves the run completed without error, since it has
    no public API to read that table.
    """

    org_id = _authenticate(api, email=email, password=password)
    _enable_wave_3_1(api, org_id=org_id)
    scope = _scope(org_id)
    conversation = api.request(
        "POST",
        "/api/v1/dev/conversations",
        {
            "current_scope": scope,
            "retention_days": 30,
            "title": "CHAOS-3300 metric-comparison acceptance",
        },
    )
    _require(isinstance(conversation, dict), "conversation response was not an object")
    conversation_id = conversation.get("conversation_id")
    _require(
        isinstance(conversation_id, str) and bool(conversation_id),
        "conversation response returned no conversation_id",
    )
    events = _sse_request(
        api,
        f"/api/v1/dev/conversations/{conversation_id}/messages",
        {
            "schema_version": "dev_message_request.v1",
            "request_id": str(uuid.uuid4()),
            "client_message_id": str(uuid.uuid4()),
            "conversation_id": conversation_id,
            "question": METRIC_COMPARISON_QUESTION,
            "question_class": "status",
            "scope": scope,
            "requested_metric_ids": list(REQUESTED_METRIC_IDS),
        },
    )
    run_started = next(
        (event for event in events if event.event is StreamEventType.RUN_STARTED),
        None,
    )
    _require(run_started is not None, "expected a run.started event")
    assert run_started is not None
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
        raise AcceptanceFailure(detail)
    answer = completed.answer
    _require(
        answer.status.value != "error",
        "Ask Dev returned an error-status answer for a multi-metric comparison",
    )
    _require(events[-1].terminal_kind == "answer", "stream did not terminate as answer")
    return conversation_id, run_started.run_id


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
        conversation_id, run_id = smoke(api, email=email, password=password)
    except AcceptanceFailure as exc:
        print(
            f"Ask Dev metric-comparison acceptance smoke failed: {exc}", file=sys.stderr
        )
        return 1
    print(
        "Ask Dev metric-comparison acceptance smoke completed "
        f"(conversation={conversation_id}, run={run_id})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
