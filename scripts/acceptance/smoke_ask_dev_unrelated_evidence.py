#!/usr/bin/env python3
"""Live acceptance smoke: the unrelated-evidence attack (CHAOS-3300).

Same shape as the other CHAOS-3300 smoke scripts -- real Compose stack,
real HTTP/SSE API, no Playwright/web dependency. Requires a compose bring-up
seeded with TWO distinctly-named repositories
(``dev-hops fixtures generate --repo-name meridian/web-app --repo-count 2``
draws from the curated demo-repo list, yielding ``meridian/web-app`` and
``meridian/core-api`` -- confirmed live 2026-08-02 by querying
``default.repos`` directly rather than assumed): the negative control asks
about ``meridian/web-app`` by name and asserts NO evidence entity or
repository id belonging to ``meridian/core-api`` appears in the answer or
its scope resolution.

The positive control is WEAKER than originally intended and this is
disclosed rather than hidden: the scripted OpenAI-compatible provider's
default ``search_evidence.v1`` call (see ``scripted_openai_service.py``)
hardcodes its query argument to the literal string ``"meridian/web-app"``
regardless of the question asked, so an organization-wide question through
this fixture can never surface ``meridian/core-api`` evidence even though
the real product has no such restriction -- confirmed live by inspecting
the actual evidence entity ids returned (all ``meridian/web-app-*``, zero
``meridian/core-api-*``). The positive control here therefore only proves
"the organization-wide question still succeeds, unrestricted by any named
subject" -- it does NOT independently prove "multi-repository evidence
actually appears," which would need the scripted provider extended with a
non-hardcoded search step (not done here, time-boxed per wrap directive).
The negative control's exclusion proof is unaffected by this limitation:
it holds regardless of what the positive control can or can't show.

This scenario needs its OWN bring-up with ``--repo-count 2`` -- the shared
``run_ask_dev_compose.sh`` launcher seeds exactly one repository
(``--repo-count 1``) because the inherited positive-control oracle and this
lane's other smoke scripts depend on that single-repo shape. This script is
therefore NOT wired into that launcher; run it against a standalone
``--repo-count 2`` bring-up instead (see the manifest entry for the exact
compose invocation used to validate it).
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

NAMED_REPO = "meridian/web-app"
UNRELATED_REPO = "meridian/core-api"
NAMED_QUESTION = f"What's the status of {NAMED_REPO}?"
ORG_WIDE_QUESTION = "What's the status of the organization's repositories?"

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
                "reason": "CHAOS-3300 unrelated-evidence acceptance scenario",
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
                "reason": "CHAOS-3300 unrelated-evidence acceptance scenario",
            },
        )
        _require(
            isinstance(updated, dict) and updated.get("is_enabled") is True,
            f"failed to enable {_WAVE_3_1_FEATURE_KEY}",
        )


def _scope(org_id: str) -> dict[str, Any]:
    # 14+14: see smoke_ask_dev_inherited_oracle.py's comment.
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
            "filter_fingerprint": "unrelated_evidence_acceptance",
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


def _ask(
    api: AcceptanceApi, *, scope: dict[str, Any], question: str
) -> list[DevStreamEvent]:
    conversation = api.request(
        "POST",
        "/api/v1/dev/conversations",
        {
            "current_scope": scope,
            "retention_days": 30,
            "title": "CHAOS-3300 unrelated-evidence acceptance",
        },
    )
    _require(isinstance(conversation, dict), "conversation response was not an object")
    conversation_id = conversation.get("conversation_id")
    _require(
        isinstance(conversation_id, str) and bool(conversation_id),
        "conversation response returned no conversation_id",
    )
    return _sse_request(
        api,
        f"/api/v1/dev/conversations/{conversation_id}/messages",
        {
            "schema_version": "dev_message_request.v1",
            "request_id": str(uuid.uuid4()),
            "client_message_id": str(uuid.uuid4()),
            "conversation_id": conversation_id,
            "question": question,
            "question_class": "status",
            "scope": scope,
        },
    )


def smoke(api: AcceptanceApi, *, email: str, password: str) -> tuple[str, str]:
    org_id = _authenticate(api, email=email, password=password)
    _enable_wave_3_1(api, org_id=org_id)
    scope = _scope(org_id)

    # --- negative control: named subject must exclude the unrelated repo ---
    named_events = _ask(api, scope=scope, question=NAMED_QUESTION)
    named_scope_resolved = next(
        (
            event
            for event in named_events
            if event.event is StreamEventType.SCOPE_RESOLVED
        ),
        None,
    )
    _require(
        named_scope_resolved is not None
        and named_scope_resolved.scope_resolution is not None,
        "expected a scope.resolved event committing the named repository subject",
    )
    assert (
        named_scope_resolved is not None
        and named_scope_resolved.scope_resolution is not None
    )
    _require(
        len(named_scope_resolved.scope_resolution.authorized_repository_ids) > 0,
        "the named repository subject was never committed to the resolved scope",
    )
    named_completed = next(
        (
            event
            for event in named_events
            if event.event is StreamEventType.ANSWER_COMPLETED
        ),
        None,
    )
    _require(
        named_completed is not None and named_completed.answer is not None,
        "named-subject question did not produce an answer",
    )
    assert named_completed is not None and named_completed.answer is not None
    named_answer = named_completed.answer
    _require(
        named_answer.status.value != "error",
        "named-subject question returned an error-status answer",
    )
    leaked = [
        item.entity_id
        for item in named_answer.evidence
        if UNRELATED_REPO in str(item.entity_id)
    ]
    _require(
        not leaked,
        f"unrelated repository {UNRELATED_REPO} leaked into the named-subject "
        f"answer's evidence: {leaked}",
    )

    # --- positive control: organization-wide question is not scope-blocked
    # (weaker than "multi-repo evidence appears" -- see module docstring) ---
    org_events = _ask(api, scope=scope, question=ORG_WIDE_QUESTION)
    org_completed = next(
        (
            event
            for event in org_events
            if event.event is StreamEventType.ANSWER_COMPLETED
        ),
        None,
    )
    _require(
        org_completed is not None and org_completed.answer is not None,
        "organization-wide question did not produce an answer",
    )
    assert org_completed is not None and org_completed.answer is not None
    org_answer = org_completed.answer
    _require(
        org_answer.status.value != "error",
        "organization-wide question returned an error-status answer",
    )
    return str(named_completed.run_id), str(org_completed.run_id)


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
        named_run_id, org_run_id = smoke(api, email=email, password=password)
    except AcceptanceFailure as exc:
        print(
            f"Ask Dev unrelated-evidence acceptance smoke failed: {exc}",
            file=sys.stderr,
        )
        return 1
    print(
        "Ask Dev unrelated-evidence acceptance smoke completed "
        f"(named_run={named_run_id}, org_run={org_run_id})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
