"""CHAOS-3219 Phase 1 Lane 1b conformance suite.

Proves, unit-level and in-process against the real scripted HTTP server, the
real production ``OpenAICompatibleAgentProvider`` adapter, and (for the
full-surface leak claim) the real ``DevOrchestrator`` + ``DevPersistenceService``
+ ``PersistenceRunRecorder`` stack over a real database -- never a stub of
any of them:

1. every enabled Ask Dev provider role has a ``role-<role>.json`` script
   file, and every role this repo's code does not yet certify is
   independently proven not-yet-enabled (grounded in ``role_readiness.py``,
   not asserted);
2. every case id a role script references exists in the frozen registry, and
   a role script that lets two cases collide on the same question fingerprint
   fails to load (RED-verified);
3. every one of the 6 fault types is scripted and provably produces its
   fault;
4. the retired ``[[case:`` marker fails loud in every shape (well-formed,
   malformed, truncated, duplicated) -- RED-verified, never a fallthrough;
5. a question matching no scripted case is indistinguishable from an
   ordinary one and falls through to the untagged default heuristic,
   unchanged (backward compatibility);
6. nothing acceptance-specific -- no marker, no case id -- ever appears in
   the REAL persisted transcript (user message, assistant message, or the
   real production adapter's own error text) for a properly-addressed
   scripted case, driven through the real orchestrator/persistence stack,
   not just the provider in isolation.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevContractVersions,
    DevMessageRequest,
    DevScopeResolution,
    DevToolRequest,
    DevToolResult,
    ToolID,
)
from dev_health_ops.api.dev.contracts_v2.validators import scan_public_text
from dev_health_ops.api.dev.no_match_terminal import INTERNAL_TOKEN_DENYLIST
from dev_health_ops.api.dev.orchestrator import DevOrchestrator, RunState
from dev_health_ops.api.dev.orchestrator_persistence import PersistenceRunRecorder
from dev_health_ops.api.dev.persistence.service import DevPersistenceService
from dev_health_ops.api.dev.tool_registry import AskDevToolRegistry
from dev_health_ops.llm.agent import provider_scripts
from dev_health_ops.llm.agent.contracts import AgentMessage, AgentMessageRole
from dev_health_ops.llm.agent.errors import (
    _SAFE_MESSAGES,  # noqa: PLC2701
    AgentProviderError,
)
from dev_health_ops.llm.agent.openai_compatible import OpenAICompatibleAgentProvider
from dev_health_ops.llm.agent.role_readiness import RoleReadinessService
from dev_health_ops.llm.agent.roles import AgentRole
from dev_health_ops.llm.agent.scripted_openai_service import ScriptedOpenAIServer
from dev_health_ops.models.dev_persistence import (
    DevConversation,
    DevConversationTombstone,
    DevFeedback,
    DevMessage,
    DevRun,
    DevToolCall,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

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

#: The Wave 3.1 inherited oracle's exact question -- scripted as
#: ``delegate_default`` under case id ``status.single-project.positive-control-v1``.
_POSITIVE_CONTROL_QUESTION = (
    "How did completed work change in this scope during the selected time "
    "range, and what evidence supports it?"
)

_TABLES = tables_of(
    User,
    Organization,
    DevConversation,
    DevMessage,
    DevRun,
    DevToolCall,
    DevFeedback,
    DevConversationTombstone,
)


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


@pytest_asyncio.fixture
async def persisted_dev_stack(
    tmp_path: Path,
) -> AsyncIterator[tuple[Any, uuid.UUID, uuid.UUID]]:
    """A real SQLite-backed ``DevPersistenceService`` database (same pattern
    as ``tests/api/dev/test_persistence.py``), seeded with one organization
    and one user, for the full-surface persistence leak test."""

    database = tmp_path / "provider-roles-persistence.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database}")

    @event.listens_for(engine.sync_engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_TABLES
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    async with maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug="ask-dev-provider-roles", name="Ask Dev"),
                User(id=user_id, email="ask-dev-provider-roles@example.com"),
            ]
        )
        await session.commit()
    try:
        yield maker, org_id, user_id
    finally:
        await engine.dispose()


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
# 2. Referential integrity + question-fingerprint collision guard + denylist
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


def test_every_scripted_case_question_is_pairwise_distinct_after_normalization() -> (
    None
):
    script = provider_scripts.load_role_script(
        AgentRole.LEGACY_AGENT.value, scripts_dir=_SCRIPTS_DIR
    )
    assert len(script.by_fingerprint) == len(script.cases), (
        "a fingerprint collision would silently drop a case from the routing "
        "index -- load_role_script must have already raised ValueError "
        "before this point if one existed"
    )


def test_a_question_fingerprint_collision_fails_to_load(tmp_path: Path) -> None:
    """RED: two different case ids that normalize to the identical question
    must not silently pick one -- the whole role script fails to load."""

    directory = tmp_path / "provider-scripts"
    directory.mkdir()
    (directory / "role-legacy_agent.json").write_text(
        json.dumps(
            {
                "schema_version": provider_scripts.SCRIPT_SCHEMA_VERSION,
                "role": "legacy_agent",
                "cases": {
                    "scope.no-match": {
                        "question": "  Is this   collided?  ",
                        "kind": "decisions",
                        "decisions": [
                            {
                                "type": "refusal",
                                "code": "unsupported_request",
                                "message": "m",
                            }
                        ],
                    },
                    "scope.ambiguous": {
                        "question": "IS THIS COLLIDED?",
                        "kind": "decisions",
                        "decisions": [
                            {
                                "type": "refusal",
                                "code": "unsupported_request",
                                "message": "m",
                            }
                        ],
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="question fingerprint"):
        provider_scripts.load_role_script("legacy_agent", scripts_dir=directory)


def test_no_registry_case_id_or_scripted_question_trips_the_real_public_text_denylist() -> (
    None
):
    """Direct proof (not prose) for the leak-safety argument in
    ``provider_scripts.py``'s module docstring: feed every frozen registry
    case id, and every literal question this role actually scripts, through
    the REAL production denylist scanner
    (``contracts_v2.validators.scan_public_text``, the function
    ``validate_no_internal_leakage`` runs over every public copy field) and
    require zero hits. Also asserts the denylist itself is still exclusively
    underscore-shaped -- if a future change ever added a hyphenated or
    bare-word token to it, this would catch the assumption breaking instead
    of the leak-safety argument silently going stale.
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

    script = provider_scripts.load_role_script(
        AgentRole.LEGACY_AGENT.value, scripts_dir=_SCRIPTS_DIR
    )
    raw = json.loads(
        (_SCRIPTS_DIR / "role-legacy_agent.json").read_text(encoding="utf-8")
    )
    for case_id in script.cases:
        question = raw["cases"][case_id]["question"]
        assert scan_public_text(question) == [], (
            f"case {case_id!r}'s scripted question trips the denylist: {question!r}"
        )


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
    question = "Has anything changed in meridian/web-app during the selected period?"
    response = _post(
        scripted_openai_server, question, tools=[_tool_offer("query_metric_v1")]
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
                        AgentMessageRole.USER, json.dumps({"question": question})
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
    question = (
        "What changed in meridian/web-app during the selected period, and is "
        "the in-flight work complete?"
    )
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

    question = "How trustworthy is the data behind meridian/web-app's status?"
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
                        AgentMessageRole.USER, json.dumps({"question": question})
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
    assert safe_text in _SAFE_MESSAGES.values()


def test_oversized_output_exceeds_the_provable_floor(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    question = "Give me a comprehensive breakdown of meridian/web-app's status."
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
            response = _post(
                server, "Is the status snapshot for meridian/web-app ready now?"
            )
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
                        "question": "Is the status snapshot for meridian/web-app ready now?",
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
    question = "What's the status of meridian/web-app, checked repeatedly?"
    for _ in range(3):
        response = _post(scripted_openai_server, question)
        assert response.status_code == 429
        assert response.json()["error"]["code"] == "scripted_fault_retry_storm"
        assert response.headers.get("retry-after") is not None


# ---------------------------------------------------------------------------
# 4. The retired [[case: marker fails loud in every shape (RED-verified) --
#    never a fallthrough. Exhausted scripts/pre-fault-decisions also fail loud.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "question",
    [
        pytest.param(
            "hi [[case:status.single-project.exact-subject]]", id="well-formed"
        ),
        pytest.param("hi [[case:not-closed", id="malformed-no-close"),
        pytest.param("hi [[case:", id="truncated"),
        pytest.param("hi [[case:a.b]] and again [[case:c.d]]", id="duplicate"),
        pytest.param(
            f"{_POSITIVE_CONTROL_QUESTION} [[case:status.single-project.positive-control-v1]]",
            id="well-formed-now-unsupported-on-a-real-scripted-question",
        ),
    ],
)
def test_legacy_marker_in_any_shape_fails_loud_never_a_canned_200(
    scripted_openai_server: ScriptedOpenAIServer, question: str
) -> None:
    response = _post(scripted_openai_server, question)
    assert response.status_code != 200
    body = response.json()
    assert body["error"]["type"] == "scripted_provider_unmapped_case"
    assert body["error"]["code"] == "legacy_case_tag_marker_present"


def test_scripted_decisions_exhausted_fails_loud(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    """``scope.prohibited-write`` scripts exactly one decision (round 0) --
    asking for round 1 (a second call, as if the client kept the
    conversation going past what was scripted) must fail loud, never repeat
    or silently degrade to the default heuristic."""

    question = "Update the ticket status to Done"
    first = _post(scripted_openai_server, question)
    assert first.status_code == 200

    second = _post(
        scripted_openai_server,
        question,
        prior_tool_results=[{"tool_id": "query_metric.v1", "status": "success"}],
    )
    assert second.status_code != 200
    assert second.json()["error"]["code"] == "script_exhausted"


def test_fault_pre_decisions_exhausted_fails_loud(tmp_path: Path) -> None:
    """A synthetic script whose fault fires at round 2 but only scripts one
    pre-fault decision (round 0) -- round 1 has nothing to serve and must
    fail loud rather than silently repeating round 0 or falling through."""

    directory = tmp_path / "provider-scripts"
    directory.mkdir()
    (directory / "registry-ids.v1.json").write_text(
        json.dumps(
            {
                "schema_version": "ask_dev_corpus_registry_ids.v1",
                "total": 1,
                "groups": {"9": {"count": 1, "ids": ["provider-fail.after-frame"]}},
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
                    "provider-fail.after-frame": {
                        "question": "Synthetic two-round-gap fault probe question?",
                        "kind": "fault",
                        "fault": {
                            "type": "fail-after-frame",
                            "fires_from_round": 2,
                            "pre_fault_decisions": [
                                {
                                    "type": "tool_call",
                                    "tool": "query_metric_v1",
                                    "arguments": {},
                                }
                            ],
                            "http_error": {
                                "status": 503,
                                "code": "scripted_fault_fail_after_frame",
                                "message": "m",
                            },
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    server = ScriptedOpenAIServer(_API_KEY)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        import os

        os.environ[provider_scripts.SCRIPTS_DIR_ENV] = str(directory)
        try:
            question = "Synthetic two-round-gap fault probe question?"
            round0 = _post(server, question, tools=[_tool_offer("query_metric_v1")])
            assert round0.status_code == 200
            round1 = _post(
                server,
                question,
                prior_tool_results=[
                    {"tool_id": "query_metric.v1", "status": "success"}
                ],
            )
            assert round1.status_code != 200
            assert round1.json()["error"]["code"] == "fault_pre_decisions_exhausted"
        finally:
            del os.environ[provider_scripts.SCRIPTS_DIR_ENV]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_scripted_tool_call_requesting_an_unoffered_tool_fails_loud(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    """provider-fail.after-frame's pre-fault decision requests
    query_metric_v1; offering only an unrelated tool must fail loud rather
    than silently substitute or drop the call."""

    question = (
        "What changed in meridian/web-app during the selected period, and is "
        "the in-flight work complete?"
    )
    response = _post(
        scripted_openai_server, question, tools=[_tool_offer("data_health_v1")]
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "scripted_tool_not_offered"


def test_scripts_directory_unavailable_degrades_to_default_heuristic_not_a_fail(
    scripted_openai_server: ScriptedOpenAIServer, tmp_path: Path
) -> None:
    """A broken/missing scripts directory is NOT distinguishable, at the
    wire, from "this question was never meant to be scripted" -- see
    provider_scripts.py's module docstring. It must not turn ordinary
    traffic red; the static conformance tests above are what catch a broken
    scripts directory."""

    import os

    missing_dir = tmp_path / "does-not-exist"
    os.environ[provider_scripts.SCRIPTS_DIR_ENV] = str(missing_dir)
    try:
        response = _post(
            scripted_openai_server,
            "What is the status of meridian/web-app?",
            tools=[_tool_offer("readiness_echo_v1")],
        )
        assert response.status_code == 200
        assert (
            response.json()["choices"][0]["message"]["tool_calls"][0]["function"][
                "name"
            ]
            == "readiness_echo_v1"
        )
    finally:
        del os.environ[provider_scripts.SCRIPTS_DIR_ENV]


# ---------------------------------------------------------------------------
# 5. Backward compatibility: an unmatched (ordinary) question, and
#    delegate_default, are indistinguishable from the pre-CHAOS-3219 path.
# ---------------------------------------------------------------------------


def test_ordinary_unmatched_question_is_unaffected_by_provider_scripts(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    """None of the pre-existing smoke/oracle/probe questions match any of
    this role's scripted literal questions -- this is the routine,
    majority-case outcome, and must fall straight through, not fail loud."""

    response = _post(
        scripted_openai_server,
        "What is the status of meridian/web-app?",
        tools=[_tool_offer("readiness_echo_v1")],
    )
    assert response.status_code == 200
    tool_call = response.json()["choices"][0]["message"]["tool_calls"][0]
    assert tool_call["function"]["name"] == "readiness_echo_v1"


def test_delegate_default_case_matches_the_default_heuristic_with_scripts_disabled(
    scripted_openai_server: ScriptedOpenAIServer, tmp_path: Path
) -> None:
    """status.single-project.positive-control-v1 is scripted as
    delegate_default -- sending its exact question must produce the
    identical first tool call whether or not the scripts directory is even
    reachable, proving delegate_default changes nothing about the default
    heuristic's behavior for this question."""

    def first_tool_call() -> str:
        response = _post(
            scripted_openai_server,
            _POSITIVE_CONTROL_QUESTION,
            tools=[_tool_offer("query_metric_v1"), _tool_offer("readiness_echo_v1")],
        )
        assert response.status_code == 200
        return str(
            response.json()["choices"][0]["message"]["tool_calls"][0]["function"][
                "name"
            ]
        )

    with_scripts = first_tool_call()

    import os

    os.environ[provider_scripts.SCRIPTS_DIR_ENV] = str(tmp_path / "does-not-exist")
    try:
        without_scripts = first_tool_call()
    finally:
        del os.environ[provider_scripts.SCRIPTS_DIR_ENV]

    assert with_scripts == without_scripts


# ---------------------------------------------------------------------------
# 6. Full-surface leak proof: the REAL orchestrator + persistence stack,
#    not just the provider.
# ---------------------------------------------------------------------------


async def _execute_registered_tool(
    executed: list[DevToolRequest], _context: Any, request: DevToolRequest
) -> DevToolResult:
    executed.append(request)
    payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
    payload.update(
        {
            "run_id": request.run_id,
            "tool_call_id": request.tool_call_id,
            "tool_id": request.tool_id.value,
        }
    )
    if request.tool_id is ToolID.QUERY_METRIC:
        payload["evidence"] = []
    elif request.tool_id is ToolID.SEARCH_EVIDENCE:
        payload["metrics"] = []
        payload["metric_definitions"] = []
        payload["evidence"][0]["entity_id"] = "meridian/web-app-100"
    elif request.tool_id is ToolID.DATA_HEALTH:
        payload["metrics"] = []
        payload["metric_definitions"] = []
        payload["evidence"] = []
        payload["data_health"] = [
            {
                "source_system": "work_items",
                "freshness": "fresh",
                "last_successful_at": positive_fixtures()["dev_evidence_ref.v1"][
                    "observed_at"
                ],
                "coverage": 1.0,
                "warning": None,
            }
        ]
    else:
        raise AssertionError(f"unexpected scripted tool request: {request.tool_id}")
    return DevToolResult.model_validate(payload)


async def _resolve_fixture_scope(**_values: Any) -> DevScopeResolution:
    return DevScopeResolution.model_validate(
        positive_fixtures()["dev_scope_resolution.v1"]
    )


async def _run_through_real_stack(
    persisted_dev_stack: tuple[Any, uuid.UUID, uuid.UUID],
    server: ScriptedOpenAIServer,
    question: str,
) -> tuple[RunState, uuid.UUID, DevPersistenceService, AsyncSession]:
    """Drive ``question`` through the REAL ``DevPersistenceService`` (a real
    aiosqlite database) -> real ``PersistenceRunRecorder`` -> real
    ``DevOrchestrator`` -> real scripted HTTP provider. Returns the run's
    terminal state, its conversation id, and the still-open service/session
    for the caller to read persisted rows back from (same session the write
    happened on -- ``expire_on_commit=False`` so committed rows remain
    readable).
    """

    maker, org_id, user_id = persisted_dev_stack
    session = maker()
    service = DevPersistenceService(session)
    conversation = await service.create_conversation(
        org_id=org_id, user_id=user_id, current_scope={}
    )
    accepted = await service.append_user_message_and_run(
        org_id=org_id,
        user_id=user_id,
        conversation_id=conversation.id,
        client_message_id=uuid.uuid4(),
        question=question,
        scope_snapshot={},
    )
    run_id = accepted.run.id
    await session.commit()

    recorder = PersistenceRunRecorder(
        service,
        org_id=org_id,
        user_id=user_id,
        conversation_id=conversation.id,
        run_id=run_id,
        provider_source="platform",
    )
    provider = OpenAICompatibleAgentProvider(
        api_key=_API_KEY,
        model="ask-dev-scripted-v1",
        base_url=_base_url(server),
    )
    executed: list[DevToolRequest] = []
    orchestrator = DevOrchestrator(
        provider=provider,
        provider_source="platform",
        provider_family="openai",
        registry=AskDevToolRegistry(
            {
                tool_id: (lambda ctx, req: _execute_registered_tool(executed, ctx, req))
                for tool_id in ToolID
            }
        ),
        scope_resolver=_resolve_fixture_scope,
        versions=DevContractVersions.model_validate(
            positive_fixtures()["dev_answer.v1"]["versions"]
        ),
        recorder=recorder,
    )
    request = DevMessageRequest.model_validate(
        positive_fixtures()["dev_message_request.v1"] | {"question": question}
    )
    try:
        result = await orchestrator.run(
            request=request,
            org_id="org_fullchaos",
            user_id="user_01",
            permission_fingerprint="permissions_01",
            run_id=str(run_id),
            conversation_id=str(conversation.id),
            answer_id=str(uuid.uuid4()),
            cancellation=asyncio.Event(),
        )
    finally:
        await provider.aclose()
    await session.commit()
    return result.state, conversation.id, service, session


@pytest.mark.asyncio
async def test_scripted_case_leaves_no_acceptance_trace_in_the_persisted_transcript(
    persisted_dev_stack: tuple[Any, uuid.UUID, uuid.UUID],
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    """Full-surface proof (Codex round-2 requirement): drives the exact
    scripted positive-control question through the REAL orchestrator +
    persistence stack (not just the provider), then reads the persisted rows
    back and asserts:

    (a) the persisted user message is byte-for-byte identical to the
        question sent -- nothing was appended, wrapped, or stripped, because
        nothing acceptance-specific was ever added to it in the first place;
    (b) the persisted assistant answer payload contains neither the retired
        marker nor any registry case id fragment.
    """

    state, conversation_id, service, session = await _run_through_real_stack(
        persisted_dev_stack, scripted_openai_server, _POSITIVE_CONTROL_QUESTION
    )
    try:
        assert state is RunState.COMPLETED

        user_message = (
            await session.execute(
                select(DevMessage).where(
                    DevMessage.conversation_id == conversation_id,
                    DevMessage.role == "user",
                )
            )
        ).scalar_one()
        assert user_message.content == _POSITIVE_CONTROL_QUESTION
        assert provider_scripts.LEGACY_CASE_TAG_MARKER not in user_message.content

        assistant_message = (
            await session.execute(
                select(DevMessage).where(
                    DevMessage.conversation_id == conversation_id,
                    DevMessage.role == "assistant",
                )
            )
        ).scalar_one()
        answer_text = json.dumps(assistant_message.answer_payload)
        assert provider_scripts.LEGACY_CASE_TAG_MARKER not in answer_text
        registry_ids = provider_scripts.load_registry_ids(scripts_dir=_SCRIPTS_DIR)
        leaked = [case_id for case_id in registry_ids if case_id in answer_text]
        assert leaked == [], (
            f"registry case id(s) leaked into the persisted answer: {leaked}"
        )
    finally:
        await session.close()
        del service


@pytest.mark.asyncio
async def test_legacy_marker_question_is_persisted_verbatim_but_never_reaches_an_answer(
    persisted_dev_stack: tuple[Any, uuid.UUID, uuid.UUID],
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    """The adversarial/defensive half: a question a real end user typed
    themselves, which happens to contain the retired marker, is persisted
    exactly as they typed it (unavoidable and correct -- persistence always
    stores what the user actually asked; that is not a new leak). What must
    never happen is the run producing a fabricated, case-shaped ANSWER from
    it: the scripted provider fails loud and the run terminates in a
    provider error.

    CHAOS-3423 updated this test's own invariant (2026-08-05): a no-answer
    terminal like this one now correctly persists ONE assistant
    ``dev_messages`` row (``record_error_message``) -- the whole point of
    that ticket is that the conversation transcript is no longer silently
    incomplete for exactly this class of turn. What this test still must
    prove, and does below, is that the persisted row is never a real
    ``DevAnswer`` (schema_version dev_answer.v1/v2) -- only ever the
    error-shaped ``dev_error.v1`` row -- and that neither the retired
    marker nor a registry case id ever leaks into it, mirroring
    ``test_scripted_case_leaves_no_acceptance_trace_in_the_persisted_transcript``'s
    own leak checks for the completed-answer path.
    """

    question = f"{_POSITIVE_CONTROL_QUESTION} {provider_scripts.LEGACY_CASE_TAG_MARKER}status.single-project.positive-control-v1]]"
    state, conversation_id, service, session = await _run_through_real_stack(
        persisted_dev_stack, scripted_openai_server, question
    )
    try:
        assert state is not RunState.COMPLETED

        user_message = (
            await session.execute(
                select(DevMessage).where(
                    DevMessage.conversation_id == conversation_id,
                    DevMessage.role == "user",
                )
            )
        ).scalar_one()
        assert user_message.content == question

        assistant_messages = (
            (
                await session.execute(
                    select(DevMessage).where(
                        DevMessage.conversation_id == conversation_id,
                        DevMessage.role == "assistant",
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(assistant_messages) == 1, (
            "CHAOS-3423: this no-answer terminal must persist exactly one "
            f"assistant transcript row -- got {len(assistant_messages)}."
        )
        assistant_message = assistant_messages[0]
        assistant_payload = assistant_message.answer_payload
        assert assistant_payload is not None
        assert assistant_payload["schema_version"] == "dev_error.v1", (
            "a marker-containing question must never reach a persisted "
            "DevAnswer -- only the no-answer terminal's error row"
        )
        answer_text = json.dumps(assistant_payload)
        assert provider_scripts.LEGACY_CASE_TAG_MARKER not in answer_text
        registry_ids = provider_scripts.load_registry_ids(scripts_dir=_SCRIPTS_DIR)
        leaked = [case_id for case_id in registry_ids if case_id in answer_text]
        assert leaked == [], (
            f"registry case id(s) leaked into the persisted error row: {leaked}"
        )
    finally:
        await session.close()
        del service
