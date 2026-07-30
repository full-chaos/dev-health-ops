#!/usr/bin/env python3
"""Exercise a live platform provider through the public Ask Dev REST/SSE API."""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dev_health_ops.api.dev.contracts import (
    DevStreamEvent,
    StreamEventType,
    validate_stream,
)
from scripts.acceptance.prepare_ask_dev_acceptance import (
    AcceptanceApi,
    AcceptanceFailure,
)


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
            "filter_fingerprint": "provider_profile_acceptance",
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
        try:
            raw = json.loads("\n".join(data_lines))
            event = DevStreamEvent.model_validate(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            raise AcceptanceFailure(f"invalid SSE {event_name} frame: {exc}") from exc
        _require(
            event.event.value == event_name, "SSE event name disagreed with payload"
        )
        events.append(event)
    try:
        validate_stream(events)
    except ValueError as exc:
        raise AcceptanceFailure(f"invalid bounded Ask Dev stream: {exc}") from exc
    return events


def smoke(
    api: AcceptanceApi,
    *,
    email: str,
    password: str,
    expected_provider: str,
) -> tuple[str, str]:
    org_id = _authenticate(api, email=email, password=password)
    scope = _scope(org_id)
    conversation = api.request(
        "POST",
        "/api/v1/dev/conversations",
        {
            "current_scope": scope,
            "retention_days": 30,
            "title": "Live provider acceptance",
        },
    )
    _require(isinstance(conversation, dict), "conversation response was not an object")
    conversation_id = conversation.get("conversation_id")
    _require(
        isinstance(conversation_id, str) and bool(conversation_id),
        "conversation response returned no conversation_id",
    )
    request_id = str(uuid.uuid4())
    events = _sse_request(
        api,
        f"/api/v1/dev/conversations/{conversation_id}/messages",
        {
            "schema_version": "dev_message_request.v1",
            "request_id": request_id,
            "client_message_id": str(uuid.uuid4()),
            "conversation_id": conversation_id,
            "question": (
                "For this acceptance request, call only query_metric.v1 for "
                "items_completed with comparison enabled and limit 5. Make exactly one tool call. "
                "After that metric result is present, do not call another tool; return "
                "a grounded final answer summarizing what changed in completed work."
            ),
            "question_class": "observed_change",
            "scope": scope,
            "requested_metric_ids": ["items_completed"],
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
        raise AcceptanceFailure(detail)
    answer = completed.answer
    _require(
        answer.model.provider_source == "platform", "answer did not use platform LLM"
    )
    _require(
        answer.model.provider_family == expected_provider,
        (
            f"answer provider was {answer.model.provider_family!r}, expected "
            f"{expected_provider!r}"
        ),
    )
    _require(bool(answer.direct_summary.strip()), "answer summary was empty")
    _require(answer.status.value != "error", "Ask Dev returned an error answer")
    _require(events[-1].terminal_kind == "answer", "stream did not terminate as answer")
    return conversation_id, answer.answer_id


def main() -> int:
    if os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1":
        print("ASK_DEV_LIVE_ACCEPTANCE=1 is required", file=sys.stderr)
        return 64
    expected_provider = os.getenv("ASK_DEV_ACCEPTANCE_EXPECTED_PROVIDER", "").strip()
    expected_model = os.getenv("ASK_DEV_ACCEPTANCE_EXPECTED_MODEL", "").strip()
    if not expected_provider or not expected_model:
        print(
            "ASK_DEV_ACCEPTANCE_EXPECTED_PROVIDER and "
            "ASK_DEV_ACCEPTANCE_EXPECTED_MODEL are required",
            file=sys.stderr,
        )
        return 64
    api = AcceptanceApi(
        os.getenv("ASK_DEV_ACCEPTANCE_API_URL", "http://127.0.0.1:8000")
    )
    email = os.getenv("TEST_SUPERUSER_EMAIL", "admin@devhealth.example")
    password = os.getenv("TEST_SUPERUSER_PASSWORD", "devhealth123")
    try:
        conversation_id, answer_id = smoke(
            api,
            email=email,
            password=password,
            expected_provider=expected_provider,
        )
    except AcceptanceFailure as exc:
        print(f"Ask Dev live provider smoke failed: {exc}", file=sys.stderr)
        return 1
    print(
        "Ask Dev live provider smoke completed "
        f"(provider={expected_provider}, model={expected_model}, "
        f"conversation={conversation_id}, answer={answer_id})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
