"""CHAOS-3219 Phase 1 Lane 1b conformance suite.

Proves, unit-level and in-process against the real scripted HTTP server (and,
for the leak-safety claim, the real production ``OpenAICompatibleAgentProvider``
adapter too -- never a stub of either):

1. every enabled Ask Dev provider role has a ``role-<role>.json`` script file,
   and every role this repo's code does not yet certify is independently
   proven not-yet-enabled (grounded in ``role_readiness.py``, not asserted);
2. every case id a role script references exists in the frozen registry;
3. every one of the 6 fault types is scripted and provably produces its
   fault;
4. an unmapped case tag (unknown id, or a valid id with no script for the
   active role) fails loud -- RED-verified: the response is never a 200.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

from dev_health_ops.api.dev.contracts_v2.validators import scan_public_text
from dev_health_ops.api.dev.no_match_terminal import INTERNAL_TOKEN_DENYLIST
from dev_health_ops.llm.agent import provider_scripts
from dev_health_ops.llm.agent.contracts import AgentMessage, AgentMessageRole
from dev_health_ops.llm.agent.errors import AgentProviderError
from dev_health_ops.llm.agent.openai_compatible import OpenAICompatibleAgentProvider
from dev_health_ops.llm.agent.role_readiness import RoleReadinessService
from dev_health_ops.llm.agent.roles import AgentRole
from dev_health_ops.llm.agent.scripted_openai_service import ScriptedOpenAIServer

_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS_DIR = (
    _ROOT / "tests" / "acceptance" / "world" / "ask-dev-world.v1" / "provider-scripts"
)
_REGISTRY_PATH = _SCRIPTS_DIR / "registry-ids.v1.json"
_API_KEY = "provider-roles-conformance-key"

#: The only role with a working, production-representative certification
#: probe today -- see ``role_readiness.py`` / ``probes/legacy_agent.py``.
#: ``test_only_legacy_agent_role_is_currently_enabled`` grounds this in code
#: rather than letting it silently go stale.
_ENABLED_ROLES = (AgentRole.LEGACY_AGENT,)
_NOT_YET_ENABLED_ROLES = tuple(role for role in AgentRole if role not in _ENABLED_ROLES)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def scripted_openai_server() -> Iterator[ScriptedOpenAIServer]:
    server = ScriptedOpenAIServer(_API_KEY)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _post(
    server: ScriptedOpenAIServer,
    question: str,
    *,
    tools: list[dict[str, Any]] | None = None,
    prior_tool_results: list[dict[str, Any]] | None = None,
) -> httpx.Response:
    host, port = cast(tuple[str, int], server.server_address)
    messages: list[dict[str, Any]] = [
        {"role": "user", "content": json.dumps({"question": question})}
    ]
    for result in prior_tool_results or []:
        messages.append({"role": "tool", "content": json.dumps(result)})
    payload = {
        "model": "ask-dev-scripted-v1",
        "messages": messages,
        "tools": tools or [],
        "tool_choice": "auto",
    }
    return httpx.post(
        f"http://{host}:{port}/v1/chat/completions",
        headers={"Authorization": f"Bearer {_API_KEY}"},
        json=payload,
        timeout=15,
    )


def _tool_offer(wire_name: str) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": wire_name,
            "description": "d",
            "parameters": {"type": "object", "properties": {}},
        },
    }


def _tagged(question: str, case_id: str) -> str:
    return f"{question} [[case:{case_id}]]"


# ---------------------------------------------------------------------------
# 1. Enabled-role inventory
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("role", _NOT_YET_ENABLED_ROLES)
async def test_only_legacy_agent_role_is_currently_enabled(role: AgentRole) -> None:
    """Grounds the "only legacy_agent is enabled" claim in the actual
    certification path, rather than asserting it as prose: every other
    ``AgentRole`` member has no working probe yet
    (``role_readiness.RoleReadinessService.certify_role`` raises
    ``NotImplementedError`` before touching its store or provider argument at
    all, so ``None`` is a safe stand-in for both here).
    """

    service = RoleReadinessService(store=cast(Any, None))
    with pytest.raises(NotImplementedError):
        await service.certify_role(
            role, cast(Any, None), certification_key="irrelevant"
        )


@pytest.mark.parametrize("role", _ENABLED_ROLES)
def test_every_enabled_role_has_a_script_file(role: AgentRole) -> None:
    path = _SCRIPTS_DIR / f"role-{role.value}.json"
    assert path.is_file(), f"missing {path}"
    script = provider_scripts.load_role_script(role.value, scripts_dir=_SCRIPTS_DIR)
    assert script.role == role.value
    assert script.cases, "role script must declare at least one case"


# ---------------------------------------------------------------------------
# 2. Referential integrity: every scripted case id is a real registry id
# ---------------------------------------------------------------------------


def test_registry_ids_file_totals_match_the_frozen_registry() -> None:
    payload = json.loads(_REGISTRY_PATH.read_text(encoding="utf-8"))
    ids = provider_scripts.load_registry_ids(scripts_dir=_SCRIPTS_DIR)
    assert len(ids) == payload["total"] == 134
    for group in payload["groups"].values():
        assert len(group["ids"]) == group["count"]
        assert len(set(group["ids"])) == group["count"], "duplicate id within a group"


@pytest.mark.parametrize("role", _ENABLED_ROLES)
def test_every_script_referenced_case_id_exists_in_the_registry(
    role: AgentRole,
) -> None:
    registry_ids = provider_scripts.load_registry_ids(scripts_dir=_SCRIPTS_DIR)
    script = provider_scripts.load_role_script(role.value, scripts_dir=_SCRIPTS_DIR)
    referenced = set(script.cases)
    missing = referenced - registry_ids
    assert not missing, f"role-{role.value}.json references unknown case ids: {missing}"


def test_no_registry_case_id_or_tag_trips_the_real_public_text_denylist() -> None:
    """Direct proof (not prose) for the leak-safety argument in
    ``provider_scripts.py``'s module docstring: feed every frozen registry
    case id, and the exact tag shape this module emits, through the REAL
    production denylist scanner (``contracts_v2.validators.scan_public_text``,
    the function ``validate_no_internal_leakage`` runs over every public copy
    field) and require zero hits. Also asserts the denylist itself is still
    exclusively underscore-shaped -- if a future change ever added a
    hyphenated or bare-word token to it, this would catch the assumption
    breaking instead of the leak-safety argument silently going stale.
    """

    assert all("_" in token for token in INTERNAL_TOKEN_DENYLIST)

    # Positive control: the scanner is genuinely live, not a no-op that would
    # make every assertion below vacuous.
    assert scan_public_text("this text leaks forbidden_or_not_found verbatim")

    registry_ids = provider_scripts.load_registry_ids(scripts_dir=_SCRIPTS_DIR)
    for case_id in sorted(registry_ids):
        assert scan_public_text(case_id) == [], (
            f"case id {case_id!r} trips the denylist"
        )
        tagged = f"Some rendered prose. [[case:{case_id}]]"
        assert scan_public_text(tagged) == [], f"tag for {case_id!r} trips the denylist"


# ---------------------------------------------------------------------------
# 3. Fault matrix coverage + provable faults
# ---------------------------------------------------------------------------


def test_all_six_fault_types_are_scripted_for_legacy_agent() -> None:
    raw = json.loads(
        (_SCRIPTS_DIR / "role-legacy_agent.json").read_text(encoding="utf-8")
    )
    fault_types_present = {
        entry["fault"]["type"]
        for entry in raw["cases"].values()
        if entry.get("kind") == "fault"
    }
    assert fault_types_present == provider_scripts.FAULT_TYPES


@pytest.mark.asyncio
async def test_fail_before_frame_never_issues_a_tool_call_and_fails_loud(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    response = _post(
        scripted_openai_server,
        _tagged("What is the status?", "provider-fail.before-frame"),
        tools=[_tool_offer("query_metric_v1")],
    )
    assert response.status_code != 200
    body = response.json()
    assert body["error"]["code"] == "scripted_fault_fail_before_frame"

    # Same fault through the real production adapter: must raise, never
    # return a decision (a tool_call OR a final_answer would both be wrong --
    # "before any frame" means nothing was ever committed).
    provider = OpenAICompatibleAgentProvider(
        api_key=_API_KEY,
        model="ask-dev-scripted-v1",
        base_url=_base_url(scripted_openai_server),
    )
    try:
        with pytest.raises(AgentProviderError):
            await provider.decide(
                [
                    AgentMessage(
                        AgentMessageRole.USER,
                        json.dumps(
                            {
                                "question": _tagged(
                                    "What is the status?", "provider-fail.before-frame"
                                )
                            }
                        ),
                    )
                ],
                [],
                {"type": "object"},
                5,
                128,
            )
    finally:
        await provider.aclose()


def test_fail_after_frame_succeeds_first_round_then_fails_second(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    question = _tagged("What is the status?", "provider-fail.after-frame")
    first = _post(
        scripted_openai_server, question, tools=[_tool_offer("query_metric_v1")]
    )
    assert first.status_code == 200
    tool_call = first.json()["choices"][0]["message"]["tool_calls"][0]
    assert tool_call["function"]["name"] == "query_metric_v1"

    second = _post(
        scripted_openai_server,
        question,
        prior_tool_results=[{"tool_id": "query_metric.v1", "status": "success"}],
    )
    assert second.status_code != 200
    assert second.json()["error"]["code"] == "scripted_fault_fail_after_frame"


def _base_url(server: ScriptedOpenAIServer) -> str:
    host, port = cast(tuple[str, int], server.server_address)
    return f"http://{host}:{port}/v1"


@pytest.mark.asyncio
async def test_unsafe_error_text_leaks_raw_but_production_adapter_never_surfaces_it(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    """RED/GREEN differential proof for the ``unsafe-error-text`` fault:

    RED -- the scripted server's raw HTTP body genuinely contains the
    denylisted/secret-shaped token (proves the fault actually fired, not a
    vacuous no-op).

    GREEN -- the real ``OpenAICompatibleAgentProvider.decide()`` /
    ``safe_agent_provider_error`` path (production code, unmodified) never
    surfaces that token: it always raises with one of ``errors.py``'s fixed
    ``_SAFE_MESSAGES`` strings.
    """

    question = _tagged("What is the status?", "adv.unsafe-error-text.provider")
    raw = _post(scripted_openai_server, question)
    assert raw.status_code >= 400
    raw_body = raw.text
    assert "sk-scriptedFAKEsecretDONOTUSE0000001" in raw_body
    assert "forbidden_or_not_found" in raw_body

    provider = OpenAICompatibleAgentProvider(
        api_key=_API_KEY,
        model="ask-dev-scripted-v1",
        base_url=_base_url(scripted_openai_server),
    )
    try:
        with pytest.raises(AgentProviderError) as exc_info:
            await provider.decide(
                [
                    AgentMessage(
                        AgentMessageRole.USER,
                        json.dumps({"question": question}),
                    )
                ],
                [],
                {"type": "object"},
                5,
                128,
            )
    finally:
        await provider.aclose()

    safe_text = str(exc_info.value)
    assert "sk-scripted" not in safe_text
    assert "forbidden_or_not_found" not in safe_text
    assert "internal-audit-org" not in safe_text
    # And it is one of the fixed, pre-declared safe messages -- not a
    # passthrough of anything provider-authored.
    from dev_health_ops.llm.agent.errors import _SAFE_MESSAGES

    assert safe_text in _SAFE_MESSAGES.values()


def test_oversized_output_exceeds_the_provable_floor(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    question = _tagged("What is the status?", "adv.oversized.result")
    response = _post(scripted_openai_server, question)
    assert response.status_code == 200
    value = json.loads(response.json()["choices"][0]["message"]["content"])["value"]
    assert (
        len(value["direct_summary"].encode("utf-8"))
        >= provider_scripts.MIN_OVERSIZED_BYTES
    )


def test_slow_response_honors_its_configured_delay(tmp_path: Path) -> None:
    """Uses a throwaway scripts directory with a short delay instead of the
    checked-in 4000ms case, so this test stays fast without weakening the
    checked-in script's production-representative (real-timeout-shaped)
    delay."""

    scripts_dir = _fast_slow_response_scripts_dir(tmp_path)
    server = ScriptedOpenAIServer(_API_KEY)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        import os

        os.environ[provider_scripts.SCRIPTS_DIR_ENV] = str(scripts_dir)
        try:
            started = time.monotonic()
            response = _post(server, _tagged("q", "deg.timeout.status"))
            elapsed = time.monotonic() - started
        finally:
            del os.environ[provider_scripts.SCRIPTS_DIR_ENV]
        assert response.status_code == 200
        assert elapsed >= 0.25, f"slow-response fault fired too fast ({elapsed}s)"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _fast_slow_response_scripts_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "provider-scripts"
    directory.mkdir()
    (directory / "registry-ids.v1.json").write_text(
        json.dumps(
            {
                "schema_version": "ask_dev_corpus_registry_ids.v1",
                "total": 1,
                "groups": {"9": {"count": 1, "ids": ["deg.timeout.status"]}},
            }
        ),
        encoding="utf-8",
    )
    (directory / "role-legacy_agent.json").write_text(
        json.dumps(
            {
                "schema_version": provider_scripts.SCRIPT_SCHEMA_VERSION,
                "role": "legacy_agent",
                "cases": {
                    "deg.timeout.status": {
                        "kind": "fault",
                        "fault": {
                            "type": "slow-response",
                            "delay_ms": 300,
                            "decision": {
                                "type": "final_answer",
                                "value": {
                                    "status": "degraded",
                                    "direct_summary": "fast test fixture",
                                },
                            },
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    return directory


def test_retry_storm_trigger_returns_a_retryable_status_every_round(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    question = _tagged("q", "adv.abuse.retry-storm")
    for _ in range(3):
        response = _post(scripted_openai_server, question)
        assert response.status_code == 429
        assert response.json()["error"]["code"] == "scripted_fault_retry_storm"
        assert response.headers.get("retry-after") is not None


# ---------------------------------------------------------------------------
# 4. Unknown/unscripted case tags fail loud (RED-verified) -- never a canned 200
# ---------------------------------------------------------------------------


def test_unknown_case_id_fails_loud_never_a_canned_200(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    response = _post(
        scripted_openai_server, _tagged("q", "this.id.does-not-exist-in-the-registry")
    )
    assert response.status_code == 422
    body = response.json()
    assert body["error"]["type"] == "scripted_provider_unmapped_case"
    assert body["error"]["code"] == "unknown_case_id"


def test_registry_case_id_with_no_script_entry_fails_loud_never_a_canned_200(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    registry_ids = provider_scripts.load_registry_ids(scripts_dir=_SCRIPTS_DIR)
    script = provider_scripts.load_role_script(
        AgentRole.LEGACY_AGENT.value, scripts_dir=_SCRIPTS_DIR
    )
    unscripted = sorted(registry_ids - set(script.cases))
    assert unscripted, "expected at least one registry id with no script entry"
    response = _post(scripted_openai_server, _tagged("q", unscripted[0]))
    assert response.status_code == 422
    body = response.json()
    assert body["error"]["type"] == "scripted_provider_unmapped_case"
    assert body["error"]["code"] == "case_not_scripted"


def test_scripted_tool_call_requesting_an_unoffered_tool_fails_loud(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    """provider-fail.after-frame's pre-fault decision requests
    query_metric_v1; offering only an unrelated tool must fail loud rather
    than silently substitute or drop the call."""

    response = _post(
        scripted_openai_server,
        _tagged("q", "provider-fail.after-frame"),
        tools=[_tool_offer("data_health_v1")],
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "scripted_tool_not_offered"


def test_scripts_directory_unavailable_fails_loud_only_for_tagged_requests(
    scripted_openai_server: ScriptedOpenAIServer, tmp_path: Path
) -> None:
    import os

    missing_dir = tmp_path / "does-not-exist"
    os.environ[provider_scripts.SCRIPTS_DIR_ENV] = str(missing_dir)
    try:
        tagged = _post(
            scripted_openai_server, _tagged("q", "status.single-project.exact-subject")
        )
        assert tagged.status_code == 422
        assert tagged.json()["error"]["code"] == "scripts_directory_unavailable"

        # Laziness proof: an UNTAGGED request must be completely unaffected
        # by the same missing directory -- the pre-CHAOS-3219 default
        # heuristic never touches provider_scripts at all.
        untagged = _post(
            scripted_openai_server,
            "What is the status of meridian/web-app?",
            tools=[_tool_offer("readiness_echo_v1")],
        )
        assert untagged.status_code == 200
        assert (
            untagged.json()["choices"][0]["message"]["tool_calls"][0]["function"][
                "name"
            ]
            == "readiness_echo_v1"
        )
    finally:
        del os.environ[provider_scripts.SCRIPTS_DIR_ENV]


# ---------------------------------------------------------------------------
# 5. Backward compatibility: untagged + delegate_default behavior unchanged
# ---------------------------------------------------------------------------


def test_untagged_request_is_unaffected_by_provider_scripts(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    response = _post(
        scripted_openai_server,
        "What is the status of meridian/web-app?",
        tools=[_tool_offer("readiness_echo_v1")],
    )
    assert response.status_code == 200
    tool_call = response.json()["choices"][0]["message"]["tool_calls"][0]
    assert tool_call["function"]["name"] == "readiness_echo_v1"


def test_delegate_default_case_matches_the_untagged_grounded_flow(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    """status.single-project.positive-control-v1 is scripted as
    delegate_default -- tagging it must not change the Wave 3.1 inherited
    oracle's grounded tool sequence at all."""

    question_untagged = "How did completed work change in this scope?"
    question_tagged = _tagged(
        question_untagged, "status.single-project.positive-control-v1"
    )

    def first_tool_call(question: str) -> str:
        response = _post(
            scripted_openai_server,
            question,
            tools=[_tool_offer("query_metric_v1"), _tool_offer("readiness_echo_v1")],
        )
        assert response.status_code == 200
        return str(
            response.json()["choices"][0]["message"]["tool_calls"][0]["function"][
                "name"
            ]
        )

    assert first_tool_call(question_untagged) == first_tool_call(question_tagged)


def test_case_tag_is_never_echoed_into_a_final_answer_value(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    """Structural guard for the leak-safety argument in provider_scripts.py's
    docstring: no scripted decision's serialized JSON contains the literal
    tag delimiter, for any scripted case that produces a final_answer/
    disambiguation/refusal directly."""

    for case_id, question in (
        ("scope.ambiguous", "Which Atlas did you mean?"),
        ("scope.unsupported-request", "Refactor this function for me"),
        ("scope.prohibited-write", "Update the ticket status to Done"),
    ):
        response = _post(scripted_openai_server, _tagged(question, case_id))
        assert response.status_code == 200
        content = response.json()["choices"][0]["message"]["content"]
        assert "[[case:" not in content
        assert case_id not in content
