#!/usr/bin/env python3
"""Live acceptance smoke: the Ask Dev "exact commit" original defect
reproduction.

Runs purely over the public Ask Dev REST/SSE API (no Playwright, no
dev-health-web), same shape as ``smoke_ask_dev_not_found.py``. Complements
the not-found scenario: seeding a real, authorized, named subject (the
fixture-generated repository ``meridian/web-app``) and asking about it must
commit that subject before investigation and return a substantive,
non-error answer -- the positive control ``smoke_ask_dev_not_found.py``'s
negative control needs to hold against.
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

#: The fixture-generated repository this Compose profile's
#: `dev-hops fixtures generate --repo-name meridian/web-app` always seeds
#: (see run_ask_dev_compose.sh) -- a real, authorized, named subject.
EXACT_COMMIT_QUESTION = "What's the status of meridian/web-app?"

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
                "reason": "CHAOS-3300 exact-commit acceptance scenario",
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
                "reason": "CHAOS-3300 exact-commit acceptance scenario",
            },
        )
        _require(
            isinstance(updated, dict) and updated.get("is_enabled") is True,
            f"failed to enable {_WAVE_3_1_FEATURE_KEY}",
        )


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
            "filter_fingerprint": "exact_commit_acceptance",
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


def smoke(api: AcceptanceApi, *, email: str, password: str) -> str:
    org_id = _authenticate(api, email=email, password=password)
    _enable_wave_3_1(api, org_id=org_id)
    scope = _scope(org_id)
    conversation = api.request(
        "POST",
        "/api/v1/dev/conversations",
        {
            "current_scope": scope,
            "retention_days": 30,
            "title": "CHAOS-3300 exact-commit acceptance",
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
            "question": EXACT_COMMIT_QUESTION,
            "question_class": "status",
            "scope": scope,
        },
    )
    scope_resolved = next(
        (event for event in events if event.event is StreamEventType.SCOPE_RESOLVED),
        None,
    )
    _require(
        scope_resolved is not None and scope_resolved.scope_resolution is not None,
        "expected a scope.resolved event committing the named repository subject",
    )
    assert scope_resolved is not None and scope_resolved.scope_resolution is not None
    authorized_repos = scope_resolved.scope_resolution.authorized_repository_ids
    _require(
        len(authorized_repos) > 0,
        "the named repository subject was never committed to the resolved scope",
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
        answer.status.value != "error",
        "Ask Dev returned an error-status answer for a real authorized subject",
    )
    _require(bool(answer.direct_summary.strip()), "answer summary was empty")
    _require(events[-1].terminal_kind == "answer", "stream did not terminate as answer")
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
    try:
        conversation_id = smoke(api, email=email, password=password)
    except AcceptanceFailure as exc:
        print(f"Ask Dev exact-commit acceptance smoke failed: {exc}", file=sys.stderr)
        return 1
    print(
        f"Ask Dev exact-commit acceptance smoke completed (conversation={conversation_id})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
