from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from typing import Any, cast

import pytest

from dev_health_ops.api.dev import production_runtime
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevScope, DevToolRequest, ToolID
from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
from dev_health_ops.api.dev.runtime import DevRuntimeUnavailable
from dev_health_ops.api.dev.tool_registry import (
    TOOL_CONTRACT_VERSION,
    ToolExecutionContext,
)
from dev_health_ops.llm.agent.budget_policy import BUDGET_POLICY_VERSION
from dev_health_ops.llm.agent.openai_compatible import READINESS_VERSION
from dev_health_ops.llm.agent.policy import AgentProviderCandidate, AgentProviderSource
from dev_health_ops.llm.agent.readiness import (
    PLATFORM_READINESS_SETTING_KEY,
    READINESS_SETTING_KEY,
)
from dev_health_ops.llm.agent.roles import (
    PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
    ROLE_CERTIFICATION_SETTING_KEY,
    AgentRole,
)
from dev_health_ops.llm.credentials import LLMCredentials


class FakeProvider:
    def __init__(self) -> None:
        self.closed = False

    async def decide(self, **_values):
        raise AssertionError("provider calls are outside this construction test")

    async def aclose(self) -> None:
        self.closed = True


class FakeSettingsService:
    values: dict[str, str] = {}

    def __init__(self, _session, _org_id: str) -> None:
        pass

    async def get(self, key: str, category: str, default=None):
        del category
        return self.values.get(key, default)


def _fingerprint(
    base_url: str = "",
    model: str = "certified-model",
    provider: str = "openai",
    role: AgentRole = AgentRole.LEGACY_AGENT,
    api_key: str = "test-key",
    organization: str = "",
    project: str = "",
    custom_headers: tuple[tuple[str, str], ...] = (),
    source: str = "platform",
) -> str:
    # CHAOS-3285: mirrors production_runtime._readiness_fingerprint's
    # extended formula. Every fixture in this file that builds a "current"
    # stored AgentReadinessRecord by hand must fold the same inputs the
    # real function now folds, or it would exercise a fingerprint formula
    # that no longer matches production and every "certification is
    # current" assertion below would be testing nothing. The canonical
    # contract digest (CHAOS-3285 round 2) is reused directly from the real
    # producer rather than re-derived here -- hand-duplicating a digest
    # computation is exactly the kind of drift that got the bare
    # PROMPT_VERSION constant caught in the first place. ``api_key``
    # defaults to "test-key" -- most fixtures in this file monkeypatch
    # OPENAI_API_KEY to that value; the "local" provider fixtures (whose
    # resolved credentials.api_key is genuinely "") pass api_key="" instead
    # (CHAOS-3285 round 5). organization/project/custom_headers default to
    # "empty" -- most fixtures never configure them; CHAOS-3285 round 6's
    # own tests pass real values. _credential_fingerprint (the real
    # producer) is reused directly here, not re-derived, for the same
    # anti-drift reason.
    return hashlib.sha256(
        "\0".join(
            (
                source,
                provider,
                model,
                base_url,
                production_runtime._credential_fingerprint(
                    LLMCredentials(
                        api_key=api_key,
                        organization=organization,
                        project=project,
                        custom_headers=custom_headers,
                    )
                ),
                READINESS_VERSION,
                TOOL_CONTRACT_VERSION,
                BUDGET_POLICY_VERSION,
                role.value,
                production_runtime._canonical_contract_digest(),
                production_runtime._wire_request_digest(model),
            )
        ).encode()
    ).hexdigest()[:24]


def _role_certification_setting(
    *,
    key_prefix: str = PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
    role: AgentRole = AgentRole.LEGACY_AGENT,
    certification_key: str,
    state: str = "compatible",
) -> tuple[str, str]:
    """(settings_key, json_value) for one role's row under
    SettingsRoleCertificationStore's per-role key format (CHAOS-3285)."""

    return (
        f"{key_prefix}:{role.value}",
        json.dumps(
            {
                "version": "ask-dev-role-certification.v1",
                "record": {
                    "role": role.value,
                    "certification_key": certification_key,
                    "readiness_version": READINESS_VERSION,
                    "checked_at": "2026-07-29T12:00:00+00:00",
                    "state": state,
                    "safe_error_code": None,
                },
            }
        ),
    )


def test_readiness_fingerprint_changes_when_source_changes() -> None:
    credentials = LLMCredentials(base_url="https://models.example.com/v1")
    platform = AgentProviderCandidate(
        provider="openai",
        model="certified-model",
        credentials=credentials,
        source=AgentProviderSource.PLATFORM,
    )
    byo = AgentProviderCandidate(
        provider="openai",
        model="certified-model",
        credentials=credentials,
        source=AgentProviderSource.BYO,
    )

    assert production_runtime._readiness_fingerprint(
        platform
    ) != production_runtime._readiness_fingerprint(byo)


def test_readiness_fingerprint_changes_when_role_changes() -> None:
    """CHAOS-3285: certification is now per-role -- a fingerprint computed
    for one role must never collide with another role's fingerprint for the
    otherwise-identical candidate, or a legacy_agent certification could be
    misread as covering intent_classification/answer_frame_narrative too."""

    credentials = LLMCredentials(base_url="https://models.example.com/v1")
    candidate = AgentProviderCandidate(
        provider="openai",
        model="certified-model",
        credentials=credentials,
        source=AgentProviderSource.PLATFORM,
    )

    fingerprints = {
        role: production_runtime._readiness_fingerprint(candidate, role=role)
        for role in AgentRole
    }
    assert len(set(fingerprints.values())) == len(AgentRole)
    # The default (no explicit role) must be legacy_agent -- production's
    # existing single-role selection path calls _readiness_fingerprint
    # without a role argument, and it is exactly the legacy_agent shape
    # (full tool registry, full DevAnswer grammar) that path exercises.
    assert (
        production_runtime._readiness_fingerprint(candidate)
        == fingerprints[AgentRole.LEGACY_AGENT]
    )


def test_readiness_fingerprint_invalidates_pre_chaos_3285_stored_records() -> None:
    """CHAOS-3285 migration semantics: a fingerprint computed under the old
    (pre-PR3) formula -- which folded only source/provider/model/base_url/
    READINESS_VERSION -- must never equal the new formula's output. This is
    the mechanism that makes every previously stored AgentReadinessRecord
    read as stale rather than silently still-current after this change
    (see the docstring on _readiness_fingerprint)."""

    credentials = LLMCredentials(base_url="https://models.example.com/v1")
    candidate = AgentProviderCandidate(
        provider="openai",
        model="certified-model",
        credentials=credentials,
        source=AgentProviderSource.PLATFORM,
    )
    pre_change_fingerprint = hashlib.sha256(
        "\0".join(
            (
                candidate.source.value,
                candidate.provider,
                candidate.model,
                candidate.credentials.base_url,
                READINESS_VERSION,
            )
        ).encode()
    ).hexdigest()[:24]

    assert pre_change_fingerprint != production_runtime._readiness_fingerprint(
        candidate
    )


def test_canonical_contract_digest_folds_the_legacy_prompt_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3285 round 2 (Codex HIGH): the fingerprint previously folded
    only PROMPT_VERSION -- the committed-subject prompt shape
    (subject_committed=True). PromptComposer also produces
    LEGACY_PROMPT_VERSION's shape (uncommitted subject / the flag-off
    path), which the legacy_agent probe never exercises (it forces
    subject_committed=True unconditionally) -- so a text-only change to
    ONLY that prompt shape never invalidated any certification at all,
    since LEGACY_PROMPT_VERSION was never folded anywhere. Prove the
    canonical contract digest now folds it."""

    production_runtime._canonical_contract_digest.cache_clear()
    try:
        baseline = production_runtime._canonical_contract_digest()

        monkeypatch.setattr(
            production_runtime, "LEGACY_PROMPT_VERSION", "changed-legacy-version"
        )
        production_runtime._canonical_contract_digest.cache_clear()
        changed = production_runtime._canonical_contract_digest()

        assert baseline != changed
    finally:
        production_runtime._canonical_contract_digest.cache_clear()


def test_canonical_contract_digest_folds_the_real_run_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DevRunLimits value changing (e.g. the per-call output cap) must
    invalidate certification even if nobody remembers to bump a version
    string for it."""
    from dev_health_ops.api.dev.orchestrator import DevRunLimits

    production_runtime._canonical_contract_digest.cache_clear()
    try:
        baseline = production_runtime._canonical_contract_digest()

        smaller_limits = DevRunLimits(max_output_tokens_per_call=2_048)
        monkeypatch.setattr(production_runtime, "DevRunLimits", lambda: smaller_limits)
        production_runtime._canonical_contract_digest.cache_clear()
        changed = production_runtime._canonical_contract_digest()

        assert baseline != changed
    finally:
        production_runtime._canonical_contract_digest.cache_clear()


def test_build_completion_request_differs_between_probe_round_shapes() -> None:
    """Round 1 (tools offered, no grammar yet) and round 2 (tools offered,
    grammar) are genuinely distinct wire request shapes -- tool_choice
    switches from "required" to "auto", and response_format only appears
    once a final answer is allowed."""

    from dev_health_ops.llm.agent.openai_compatible import build_completion_request

    round_1 = build_completion_request(
        model="certified-model",
        messages=production_runtime._wire_request_probe_messages(round_1=True),
        tools=production_runtime._probe_tools(production_runtime._probe_registry()),
        response_schema={"type": "object"},
        max_output_tokens=4096,
    )
    round_2 = build_completion_request(
        model="certified-model",
        messages=production_runtime._wire_request_probe_messages(round_1=False),
        tools=production_runtime._probe_tools(production_runtime._probe_registry()),
        response_schema={"type": "object"},
        max_output_tokens=4096,
    )

    assert round_1["tool_choice"] == "required"
    assert round_2["tool_choice"] == "auto"
    assert "response_format" not in round_1
    assert "response_format" in round_2


def test_wire_request_digest_changes_when_supports_temperature_toggles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3285 round 4 (Codex HIGH): before that fix, the readiness
    fingerprint never called any adapter capability-policy function at all
    -- only the bare candidate.model string, which does not change when a
    policy FUNCTION's behavior changes for an already-certified model (e.g.
    supports_temperature gaining a new excluded family on a future deploy).
    Prove the digest reacts to the policy itself, holding the model string
    fixed."""

    from dev_health_ops.llm.agent import openai_compatible

    production_runtime._wire_request_digest.cache_clear()
    try:
        baseline = production_runtime._wire_request_digest("certified-model")

        monkeypatch.setattr(
            openai_compatible, "supports_temperature", lambda _model: False
        )
        production_runtime._wire_request_digest.cache_clear()
        changed = production_runtime._wire_request_digest("certified-model")

        assert baseline != changed
    finally:
        production_runtime._wire_request_digest.cache_clear()


def test_wire_request_digest_changes_when_parallel_tool_calls_support_toggles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.llm.agent import openai_compatible

    production_runtime._wire_request_digest.cache_clear()
    try:
        baseline = production_runtime._wire_request_digest("certified-model")

        monkeypatch.setattr(
            openai_compatible, "supports_parallel_tool_calls", lambda _model: False
        )
        production_runtime._wire_request_digest.cache_clear()
        changed = production_runtime._wire_request_digest("certified-model")

        assert baseline != changed
    finally:
        production_runtime._wire_request_digest.cache_clear()


def test_wire_request_digest_changes_when_reasoning_effort_toggles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.llm.agent import openai_compatible

    production_runtime._wire_request_digest.cache_clear()
    try:
        baseline = production_runtime._wire_request_digest("certified-model")

        monkeypatch.setattr(
            openai_compatible,
            "chat_completion_reasoning_effort",
            lambda _model: "minimal",
        )
        production_runtime._wire_request_digest.cache_clear()
        changed = production_runtime._wire_request_digest("certified-model")

        assert baseline != changed
    finally:
        production_runtime._wire_request_digest.cache_clear()


def test_wire_request_digest_changes_when_the_max_token_policy_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3285 round 5 (Codex HIGH): decide() independently resolved
    max_completion_tokens via model_family_budget -- a field round 4's
    narrower wire_policy_kwargs never covered at all. build_completion_request
    now assembles it, so a DevRunLimits change (which flows into the
    max_output_tokens argument) must change the digest."""

    from dev_health_ops.api.dev.orchestrator import DevRunLimits

    production_runtime._wire_request_digest.cache_clear()
    try:
        baseline = production_runtime._wire_request_digest("certified-model")

        smaller_limits = DevRunLimits(max_output_tokens_per_call=2_048)
        monkeypatch.setattr(production_runtime, "DevRunLimits", lambda: smaller_limits)
        production_runtime._wire_request_digest.cache_clear()
        changed = production_runtime._wire_request_digest("certified-model")

        assert baseline != changed
    finally:
        production_runtime._wire_request_digest.cache_clear()


def test_wire_request_digest_is_sensitive_to_the_response_wrapper_and_tool_fields() -> (
    None
):
    """CHAOS-3285 round 5 (Codex HIGH): round 4's wire_policy_kwargs
    hand-duplicated the response_format wrapper's name/strict literals
    separately from decide()'s own assembly of them -- so a change to
    EITHER decide()'s real literals OR the full generated schema body, or
    to how a tool gets serialized, could drift from the hand-duplicated
    copy without the digest ever knowing. Now that build_completion_request
    is the single producer both decide() and the digest consume, there is
    no hand-duplicated copy left to drift -- but the digest's own
    serialize-then-hash step must still not silently drop any of these
    fields. Prove each one, mutating the SAME real producer's own output,
    changes what gets hashed."""

    from dev_health_ops.llm.agent.openai_compatible import build_completion_request

    registry = production_runtime._probe_registry()
    tools = production_runtime._probe_tools(registry)
    response_schema = {"type": "object", "properties": {"status": {"type": "string"}}}
    round_1 = build_completion_request(
        model="certified-model",
        messages=production_runtime._wire_request_probe_messages(round_1=True),
        tools=tools,
        response_schema=response_schema,
        max_output_tokens=4096,
    )
    round_2 = build_completion_request(
        model="certified-model",
        messages=production_runtime._wire_request_probe_messages(round_1=False),
        tools=tools,
        response_schema=response_schema,
        max_output_tokens=4096,
    )

    def _digest(
        round_1_payload: dict[str, Any], round_2_payload: dict[str, Any]
    ) -> str:
        canonical = json.dumps(
            {"round_1": round_1_payload, "round_2": round_2_payload},
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode()).hexdigest()[:24]

    baseline = _digest(round_1, round_2)

    mutated_name = json.loads(json.dumps(round_2))
    mutated_name["response_format"]["json_schema"]["name"] = "a_different_name"
    assert _digest(round_1, mutated_name) != baseline, "wrapper name change not hashed"

    mutated_strict = json.loads(json.dumps(round_2))
    mutated_strict["response_format"]["json_schema"]["strict"] = False
    assert _digest(round_1, mutated_strict) != baseline, "strict flag change not hashed"

    mutated_max_tokens = json.loads(json.dumps(round_2))
    mutated_max_tokens["max_completion_tokens"] = (
        mutated_max_tokens["max_completion_tokens"] + 1
    )
    assert _digest(round_1, mutated_max_tokens) != baseline, (
        "max_completion_tokens change not hashed"
    )

    mutated_tools = json.loads(json.dumps(round_2))
    mutated_tools["tools"][0]["function"]["description"] = "a different description"
    assert _digest(round_1, mutated_tools) != baseline, (
        "tool serialization change not hashed"
    )


def test_readiness_fingerprint_changes_when_wire_policy_toggles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The full fingerprint formula (not just the digest in isolation)
    reacts to a wire-policy change -- the exact codex repro: toggling
    supports_temperature changed the emitted request while
    _readiness_fingerprint stayed identical."""

    from dev_health_ops.llm.agent import openai_compatible

    credentials = LLMCredentials(base_url="https://models.example.com/v1")
    candidate = AgentProviderCandidate(
        provider="openai",
        model="certified-model",
        credentials=credentials,
        source=AgentProviderSource.PLATFORM,
    )

    production_runtime._wire_request_digest.cache_clear()
    try:
        baseline = production_runtime._readiness_fingerprint(candidate)

        monkeypatch.setattr(
            openai_compatible, "supports_temperature", lambda _model: False
        )
        production_runtime._wire_request_digest.cache_clear()
        changed = production_runtime._readiness_fingerprint(candidate)

        assert baseline != changed
    finally:
        production_runtime._wire_request_digest.cache_clear()


@pytest.mark.asyncio
async def test_wire_policy_change_invalidates_stored_certification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: a certification stored under today's wire policy must be
    invalidated once the policy changes for this model, even though the
    candidate itself (model/provider/base_url) never changed. Closes the loop
    codex asked for: fingerprint changes -> stored certification invalidated,
    not just a digest changing in isolation.

    CHAOS-3358: for the PLATFORM source that invalidation is now an
    operator-facing diagnostic rather than a runtime block, so the assertion
    is on the certification state itself; selection deliberately keeps
    working. The equivalent BYO invalidation still fails closed -- see
    test_byo_role_certification_is_still_a_hard_runtime_gate."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    stored_fingerprint = _fingerprint()
    role_key, role_value = _role_certification_setting(
        certification_key=stored_fingerprint
    )
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": stored_fingerprint,
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        role_key: role_value,
    }
    session = cast(Any, object())

    # Sanity: certified under today's real wire policy, selection succeeds.
    resolved = await production_runtime.resolve_production_provider(
        session, org_id="org_01"
    )
    assert resolved.model == "certified-model"
    assert await _platform_certification_is_current(session) is True

    from dev_health_ops.llm.agent import openai_compatible

    monkeypatch.setattr(openai_compatible, "supports_temperature", lambda _model: False)
    production_runtime._wire_request_digest.cache_clear()
    try:
        assert await _platform_certification_is_current(session) is False
        # Advisory, not a gate: the run still resolves.
        assert (
            await production_runtime.resolve_production_provider(
                session, org_id="org_01"
            )
        ).model == "certified-model"
    finally:
        production_runtime._wire_request_digest.cache_clear()


@pytest.mark.asyncio
async def test_credential_rotation_invalidates_certification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3285 round 5 (Codex HIGH), codex's exact repro: certify
    provider key A, then rotate to key B for the exact same
    provider/model/base_url -- the stored certification must not carry over.
    Before that fix, the certification key never depended on WHICH credential
    was actually tested, so B's rotation silently inherited A's certification
    while every real request already carried B's Authorization header, never
    having demonstrated B can handle the production request shape at all.

    CHAOS-3358: on the operator-owned PLATFORM source this now reports as a
    stale certification rather than a hard block."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "key-a")
    fingerprint_a = _fingerprint(api_key="key-a")
    role_key, role_value = _role_certification_setting(certification_key=fingerprint_a)
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": fingerprint_a,
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        role_key: role_value,
    }
    session = cast(Any, object())

    # Sanity: certified under key A, selection succeeds.
    resolved = await production_runtime.resolve_production_provider(
        session, org_id="org_01"
    )
    assert resolved.model == "certified-model"
    assert await _platform_certification_is_current(session) is True

    # Rotate to key B -- same provider/model/base_url, a different
    # credential -- without re-running preflight.
    monkeypatch.setenv("OPENAI_API_KEY", "key-b")
    assert await _platform_certification_is_current(session) is False


@pytest.mark.asyncio
async def test_organization_rotation_invalidates_certification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same guard as credential (api_key) rotation, for organization:
    certify under org A, flip OPENAI_ORG_ID to org B for the exact same
    provider/model/base_url/api_key -- the stored certification must not
    carry over (CHAOS-3358: advisory on the platform source)."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_ORG_ID", "org-a")
    fingerprint_a = _fingerprint(organization="org-a")
    role_key, role_value = _role_certification_setting(certification_key=fingerprint_a)
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": fingerprint_a,
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        role_key: role_value,
    }
    session = cast(Any, object())

    resolved = await production_runtime.resolve_production_provider(
        session, org_id="org_01"
    )
    assert resolved.model == "certified-model"
    assert await _platform_certification_is_current(session) is True

    monkeypatch.setenv("OPENAI_ORG_ID", "org-b")
    assert await _platform_certification_is_current(session) is False


@pytest.mark.asyncio
async def test_project_rotation_invalidates_certification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex's exact repro: flipping OPENAI_PROJECT_ID from one project to
    another for the exact same provider/model/base_url/api_key must
    invalidate certification -- previously this left the fingerprint
    byte-for-byte identical while the real OpenAI-Project wire header
    changed (CHAOS-3358: advisory on the platform source)."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_PROJECT_ID", "project-a")
    fingerprint_a = _fingerprint(project="project-a")
    role_key, role_value = _role_certification_setting(certification_key=fingerprint_a)
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": fingerprint_a,
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        role_key: role_value,
    }
    session = cast(Any, object())

    resolved = await production_runtime.resolve_production_provider(
        session, org_id="org_01"
    )
    assert resolved.model == "certified-model"
    assert await _platform_certification_is_current(session) is True

    monkeypatch.setenv("OPENAI_PROJECT_ID", "project-b")
    assert await _platform_certification_is_current(session) is False


@pytest.mark.asyncio
async def test_custom_header_rotation_invalidates_certification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same guard, for custom identity headers: changing
    OPENAI_CUSTOM_HEADERS for the exact same provider/model/base_url/
    api_key must invalidate certification (CHAOS-3358: advisory on the
    platform source)."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_CUSTOM_HEADERS", "X-Tenant: tenant-a")
    fingerprint_a = _fingerprint(custom_headers=(("X-Tenant", "tenant-a"),))
    role_key, role_value = _role_certification_setting(certification_key=fingerprint_a)
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": fingerprint_a,
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        role_key: role_value,
    }
    session = cast(Any, object())

    resolved = await production_runtime.resolve_production_provider(
        session, org_id="org_01"
    )
    assert resolved.model == "certified-model"
    assert await _platform_certification_is_current(session) is True

    monkeypatch.setenv("OPENAI_CUSTOM_HEADERS", "X-Tenant: tenant-b")
    assert await _platform_certification_is_current(session) is False


def test_credential_fingerprint_never_leaks_the_raw_api_key() -> None:
    """The credential fingerprint folded into the certification key must be
    non-reversible: the raw api_key string itself must never appear in the
    fingerprint's output."""

    fingerprint = production_runtime._credential_fingerprint(
        LLMCredentials(api_key="super-secret-key-value")
    )

    assert "super-secret-key-value" not in fingerprint
    assert len(fingerprint) == 24


def test_credential_fingerprint_changes_with_organization() -> None:
    baseline = production_runtime._credential_fingerprint(
        LLMCredentials(api_key="k", organization="org-a")
    )
    changed = production_runtime._credential_fingerprint(
        LLMCredentials(api_key="k", organization="org-b")
    )
    assert baseline != changed


def test_credential_fingerprint_changes_with_project() -> None:
    baseline = production_runtime._credential_fingerprint(
        LLMCredentials(api_key="k", project="project-a")
    )
    changed = production_runtime._credential_fingerprint(
        LLMCredentials(api_key="k", project="project-b")
    )
    assert baseline != changed


def test_credential_fingerprint_changes_with_custom_headers() -> None:
    baseline = production_runtime._credential_fingerprint(
        LLMCredentials(api_key="k", custom_headers=(("X-Tenant", "a"),))
    )
    changed = production_runtime._credential_fingerprint(
        LLMCredentials(api_key="k", custom_headers=(("X-Tenant", "b"),))
    )
    assert baseline != changed


def test_credential_fingerprint_has_no_concatenation_ambiguity() -> None:
    """CHAOS-3285 round 6 (Codex HIGH): folding identity fields via naive
    string concatenation (or a `\\0`-joined tuple, this function's own
    OUTER formula) can collide across different field boundaries -- e.g.
    organization="ab" + project="c" naively concatenates to the same
    string as organization="a" + project="bc". _credential_fingerprint
    uses a JSON-canonical structure internally (sort_keys, one key per
    field) specifically to avoid this; prove two genuinely different
    (organization, project) splits that share a naive concatenation do NOT
    collide."""

    split_one = production_runtime._credential_fingerprint(
        LLMCredentials(api_key="k", organization="ab", project="c")
    )
    split_two = production_runtime._credential_fingerprint(
        LLMCredentials(api_key="k", organization="a", project="bc")
    )
    assert split_one != split_two


@pytest.mark.asyncio
async def test_platform_certification_tracks_the_live_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The platform certification must follow the configuration it was taken
    against: changing the model, or storing a fingerprint from a different
    configuration, leaves it non-current. CHAOS-3358 makes that an operator
    diagnostic rather than a runtime block, so selection keeps working
    throughout."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime,
        "_provider",
        lambda _candidate: FakeProvider(),
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    role_key, role_value = _role_certification_setting(certification_key=_fingerprint())
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": _fingerprint(),
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        role_key: role_value,
    }

    session = cast(Any, object())
    resolved = await production_runtime.resolve_production_provider(
        session, org_id="org_01"
    )
    assert resolved.source is AgentProviderSource.PLATFORM
    assert resolved.family == "openai"
    assert resolved.model == "certified-model"
    assert await _platform_certification_is_current(session) is True

    monkeypatch.setenv("LLM_MODEL", "changed-model")
    assert await _platform_certification_is_current(session) is False
    assert (
        await production_runtime.resolve_production_provider(session, org_id="org_01")
    ).model == "changed-model"
    monkeypatch.setenv("LLM_MODEL", "certified-model")

    FakeSettingsService.values[PLATFORM_READINESS_SETTING_KEY] = json.dumps(
        {
            "fingerprint": "stale-fingerprint",
            "readiness_version": READINESS_VERSION,
            "checked_at": "2026-07-29T12:00:00+00:00",
            "outcome": "ready",
            "safe_error_code": None,
        }
    )
    assert await _platform_certification_is_current(session) is False
    assert (
        await production_runtime.resolve_production_provider(session, org_id="org_01")
    ).model == "certified-model"


@pytest.mark.asyncio
def _byo_settings(*, role_state: str | None) -> dict[str, str]:
    """A BYO organization whose binary readiness is CURRENT, parameterised on
    the legacy_agent role certification: absent (``None``) or present with the
    given state. Platform fallback is switched off so the BYO verdict is the
    only thing under test."""

    byo_fingerprint = _fingerprint(
        source="byo",
        provider="openai",
        model="gpt-5-mini",
        base_url="https://api.openai.com/v1",
        api_key="sk-org",
    )
    values = {
        "provider": "openai",
        "model": "gpt-5-mini",
        "api_key": "sk-org",
        "base_url": "https://api.openai.com/v1",
        "ask_dev_platform_fallback": "fail_closed",
        READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": byo_fingerprint,
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
    }
    if role_state is not None:
        role_key, role_value = _role_certification_setting(
            key_prefix=ROLE_CERTIFICATION_SETTING_KEY,
            certification_key=byo_fingerprint,
            state=role_state,
        )
        values[role_key] = role_value
    return values


@pytest.mark.asyncio
async def test_echo_only_certification_does_not_restore_live_byo_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3285 (Codex HIGH): the old binary AgentReadinessRecord being
    "ready" must NOT be sufficient for live selection on its own. Before the
    role-gate fix, an operator could re-certify through a route that only
    ever runs the old 512-token echo probe (or the new role probe simply
    never having run at all), the binary store would read current, and this
    candidate would become selectable for real traffic having never
    demonstrated it can handle the production request shape.

    CHAOS-3358 moved this gate off the operator-owned platform source, so the
    control now runs where it is still a runtime gate: a customer's BYO
    endpoint, whose binary readiness is current but which has NO legacy_agent
    role certification on record, must fail closed."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setattr(
        production_runtime, "attach_agent_budget_guard", lambda value, **_: value
    )
    FakeSettingsService.values = _byo_settings(role_state=None)

    session = cast(Any, object())
    with pytest.raises(DevRuntimeUnavailable) as exc_info:
        await production_runtime.resolve_production_provider(session, org_id="org_01")
    assert exc_info.value.code == "provider_not_configured"

    # The GREEN half: the same configuration with a compatible legacy_agent
    # role certification does resolve -- so the block above is the role gate
    # firing, not the fixture failing to configure BYO at all.
    FakeSettingsService.values = _byo_settings(role_state="compatible")
    resolved = await production_runtime.resolve_production_provider(
        session, org_id="org_01"
    )
    assert resolved.source is AgentProviderSource.BYO
    assert resolved.model == "gpt-5-mini"


@pytest.mark.asyncio
async def test_incompatible_legacy_agent_role_does_not_restore_live_byo_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same gate, with a role record present but INCOMPATIBLE (e.g. the
    production-sized probe reproduced output exhaustion) rather than absent
    -- an INCOMPATIBLE verdict must never be silently treated as good
    enough for live BYO selection either."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setattr(
        production_runtime, "attach_agent_budget_guard", lambda value, **_: value
    )
    FakeSettingsService.values = _byo_settings(role_state="incompatible")

    session = cast(Any, object())
    with pytest.raises(DevRuntimeUnavailable) as exc_info:
        await production_runtime.resolve_production_provider(session, org_id="org_01")
    assert exc_info.value.code == "provider_not_configured"


@pytest.mark.asyncio
async def test_platform_role_certification_is_advisory_not_a_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3358: the platform-side counterpart of the two BYO controls
    above -- binary readiness current, legacy_agent role certification absent
    -- reports as a stale certification to the operator while the run
    proceeds."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": _fingerprint(),
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        # Deliberately NO role-certification row at all.
    }

    session = cast(Any, object())
    assert await _platform_certification_is_current(session) is False
    resolved = await production_runtime.resolve_production_provider(
        session, org_id="org_01"
    )
    assert resolved.source is AgentProviderSource.PLATFORM
    assert resolved.model == "certified-model"


@pytest.mark.asyncio
async def test_platform_local_provider_uses_only_operator_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    candidates: list[Any] = []

    def provider(candidate):
        candidates.append(candidate)
        return FakeProvider()

    monkeypatch.setattr(production_runtime, "_provider", provider)
    attached: list[dict[str, Any]] = []

    def attach(value, **kwargs):
        attached.append(kwargs)
        return value

    monkeypatch.setattr(production_runtime, "attach_agent_budget_guard", attach)
    monkeypatch.setenv("LLM_PROVIDER", "local")
    monkeypatch.setenv("LLM_MODEL", "google/gemma-4-e4b")
    monkeypatch.setenv("LOCAL_LLM_MODEL", "google/gemma-4-e4b")
    monkeypatch.setenv("LOCAL_LLM_BASE_URL", "http://host.docker.internal:1234/v1")
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    monkeypatch.delenv("LOCAL_LLM_API_KEY", raising=False)
    local_fingerprint = _fingerprint(
        provider="local",
        model="google/gemma-4-e4b",
        base_url="http://host.docker.internal:1234/v1",
        api_key="",
    )
    role_key, role_value = _role_certification_setting(
        certification_key=local_fingerprint
    )
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": local_fingerprint,
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        role_key: role_value,
        # A complete organization BYO bundle remains database-owned and does
        # not overwrite the independently resolved platform candidate.
        "provider": "openai",
        "model": "gpt-5-mini",
        "api_key": "sk-org",
        "base_url": "https://api.openai.com/v1",
    }

    resolved = await production_runtime.resolve_production_provider(
        cast(Any, object()), org_id="org_01"
    )

    assert resolved.source is AgentProviderSource.PLATFORM
    assert resolved.family == "local"
    assert resolved.model == "google/gemma-4-e4b"
    assert len(candidates) == 1
    assert candidates[0].credentials.api_key == ""
    assert candidates[0].credentials.base_url == "http://host.docker.internal:1234/v1"
    assert attached == []


@pytest.mark.asyncio
async def test_explicit_fail_closed_prevents_platform_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "local")
    monkeypatch.setenv("LOCAL_LLM_MODEL", "local-agent-model")
    monkeypatch.setenv("LOCAL_LLM_BASE_URL", "http://host.docker.internal:1234/v1")
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": _fingerprint(
                    provider="local",
                    model="local-agent-model",
                    base_url="http://host.docker.internal:1234/v1",
                    api_key="",
                ),
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        "provider": "openai",
        "model": "gpt-5-mini",
        "api_key": "sk-org",
        "base_url": "https://api.openai.com/v1",
        "ask_dev_platform_fallback": "fail_closed",
    }

    with pytest.raises(DevRuntimeUnavailable) as exc_info:
        await production_runtime.resolve_production_provider(
            cast(Any, object()), org_id="org_01"
        )

    assert exc_info.value.code == "provider_not_configured"


@pytest.mark.asyncio
async def test_byo_provider_resolution_attaches_shared_budget_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    provider = FakeProvider()
    monkeypatch.setattr(production_runtime, "_provider", lambda _candidate: provider)
    attached: list[dict[str, Any]] = []

    def attach(value, **kwargs):
        attached.append(kwargs)
        return value

    monkeypatch.setattr(production_runtime, "attach_agent_budget_guard", attach)
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    FakeSettingsService.values = {
        "provider": "openai",
        "model": "gpt-5-mini",
        "api_key": "sk-org",
        "base_url": "https://api.openai.com/v1",
    }
    session = cast(Any, object())

    resolved = await production_runtime.resolve_certification_provider(
        session, org_id="org_01"
    )

    assert resolved.source is AgentProviderSource.BYO
    assert resolved.provider is provider
    assert attached == [
        {
            "session": session,
            "org_id": "org_01",
            "provider": "openai",
            "model": "gpt-5-mini",
            "base_url": "https://api.openai.com/v1",
        }
    ]


@pytest.mark.asyncio
async def test_production_runtime_wires_exactly_the_nine_registered_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def resolve_provider(_session, *, org_id: str):
        assert org_id == "org_01"
        return ProductionProviderResolution(
            provider=cast(Any, FakeProvider()),
            source=AgentProviderSource.PLATFORM,
            family="openai",
            model="certified-model",
            provider_label="OpenAI compatible",
            model_label="certified-model",
        )

    monkeypatch.setattr(
        production_runtime, "resolve_production_provider", resolve_provider
    )
    monkeypatch.setenv("JWT_SECRET_KEY", "test-evidence-signing-secret-32-bytes")

    runtime = await production_runtime.build_production_runtime(
        cast(Any, object()),
        org_id="org_01",
        permission_fingerprint="permissions_01",
        clickhouse=cast(Any, object()),
    )
    manifest = cast(Any, runtime.registry.manifest())
    assert {item["tool_id"] for item in manifest["tools"]} == {
        item.value for item in ToolID
    }
    assert len(manifest["tools"]) == 9

    scope = DevScope.model_validate(positive_fixtures()["dev_scope.v1"])
    execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.LIST_METRICS,
            scope=scope,
            limit=8,
        ),
        ToolExecutionContext(
            org_id=scope.organization_id,
            user_id="user_01",
            permission_fingerprint="permissions_01",
            authorized_scope=scope,
            cancellation=asyncio.Event(),
            remaining_seconds=5,
        ),
    )
    assert execution.result.warnings == []
    assert execution.result.metric_definitions
    assert execution.result.metric_definitions[0].description
    assert execution.result.metric_definitions[0].supported_time_grains
    await runtime.aclose()


async def _build_runtime_for_resolve_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> Any:
    async def resolve_provider(_session, *, org_id: str):
        return ProductionProviderResolution(
            provider=cast(Any, FakeProvider()),
            source=AgentProviderSource.PLATFORM,
            family="openai",
            model="certified-model",
            provider_label="OpenAI compatible",
            model_label="certified-model",
        )

    monkeypatch.setattr(
        production_runtime, "resolve_production_provider", resolve_provider
    )
    # Constructed at runtime, never a literal secret-shaped string in source.
    monkeypatch.setenv("JWT_SECRET_KEY", secrets.token_hex(32))
    return await production_runtime.build_production_runtime(
        cast(Any, object()),
        org_id="org_fullchaos",
        permission_fingerprint="permissions_01",
        clickhouse=cast(Any, object()),
    )


@pytest.mark.asyncio
async def test_resolve_scope_with_a_query_searches_the_authorized_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3256: a named-entity query must not re-resolve the caller's scope."""

    async def fake_query_dicts(_client, sql, params):
        if "FROM projects FINAL" in sql:
            assert params["org_id"] == "org_fullchaos"
            assert params["query"] == "ask dev"
            return [
                {
                    "canonical_id": "project-ask-dev",
                    "label": "Ask Dev",
                    "repository_id": None,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.query_dicts", fake_query_dicts
    )
    runtime = await _build_runtime_for_resolve_scope(monkeypatch)
    org_scope = DevScope.model_validate(
        {
            **positive_fixtures()["dev_scope.v1"],
            "organization_id": "org_fullchaos",
            "direct_scope": "organization",
            "repositories": [],
            "entity_refs": [],
            "surface_context": None,
        }
    )

    execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.RESOLVE_SCOPE,
            scope=org_scope,
            query="ask dev",
            limit=25,
        ),
        ToolExecutionContext(
            org_id="org_fullchaos",
            user_id="user_01",
            permission_fingerprint="permissions_01",
            authorized_scope=org_scope,
            cancellation=asyncio.Event(),
            remaining_seconds=5,
        ),
    )

    resolution = execution.result.scope_resolution
    assert resolution is not None
    assert resolution.outcome.value == "exact"
    assert resolution.resolved_scope is not None
    assert resolution.resolved_scope.direct_scope.value == "project"
    assert resolution.resolved_scope.entity_refs[0].entity_id == "project-ask-dev"
    await runtime.aclose()


@pytest.mark.asyncio
async def test_resolve_scope_without_a_query_keeps_resolving_the_current_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty/omitted query keeps the pre-existing re-authorization behavior."""

    async def fake_query_dicts(_client, sql, params):
        del sql, params
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.query_dicts", fake_query_dicts
    )
    runtime = await _build_runtime_for_resolve_scope(monkeypatch)
    org_scope = DevScope.model_validate(
        {
            **positive_fixtures()["dev_scope.v1"],
            "organization_id": "org_fullchaos",
            "direct_scope": "organization",
            "repositories": [],
            "entity_refs": [],
            "surface_context": None,
        }
    )

    execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.RESOLVE_SCOPE,
            scope=org_scope,
            limit=25,
        ),
        ToolExecutionContext(
            org_id="org_fullchaos",
            user_id="user_01",
            permission_fingerprint="permissions_01",
            authorized_scope=org_scope,
            cancellation=asyncio.Event(),
            remaining_seconds=5,
        ),
    )

    resolution = execution.result.scope_resolution
    assert resolution is not None
    # No connected repos in this fake catalog -> explicit insufficient
    # evidence, never a fabricated exact organization scope (CHAOS-3255).
    assert resolution.outcome.value == "unresolved"
    assert resolution.resolved_scope is None
    await runtime.aclose()


@pytest.mark.asyncio
async def test_runtime_construction_failure_closes_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = FakeProvider()

    async def resolve_provider(_session, *, org_id: str):
        assert org_id == "org_01"
        return ProductionProviderResolution(
            provider=cast(Any, provider),
            source=AgentProviderSource.PLATFORM,
            family="openai",
            model="certified-model",
            provider_label="OpenAI compatible",
            model_label="certified-model",
        )

    monkeypatch.setattr(
        production_runtime, "resolve_production_provider", resolve_provider
    )
    monkeypatch.delenv("JWT_SECRET_KEY", raising=False)

    with pytest.raises(DevRuntimeUnavailable):
        await production_runtime.build_production_runtime(
            cast(Any, object()),
            org_id="org_01",
            permission_fingerprint="permissions_01",
            clickhouse=cast(Any, object()),
        )
    assert provider.closed is True


# ---------------------------------------------------------------------------
# CHAOS-3358: the platform provider's stored certification is an operator
# diagnostic, not a runtime gate.
#
# The platform provider is operator-owned and assumed to work. Every
# READINESS_VERSION bump and every readiness-fingerprint format change
# invalidates the stored PLATFORM certification by construction, and before
# this change that invalidation hard-blocked Ask Dev for every organization
# without BYO ("No certified Ask Dev model is ready.") until a superadmin
# re-ran platform preflight. The stored record still drives the Platform Admin
# readiness badge and its "run preflight" copy -- it just no longer decides
# whether a run may select the platform endpoint. BYO gating is unchanged.
# ---------------------------------------------------------------------------


async def _platform_certification_is_current(session: Any) -> bool:
    """The operator-facing diagnostic on its own: is the stored platform
    certification still current for today's fingerprint?

    This is what the Platform Admin badge reports. Since CHAOS-3358 it is no
    longer the same question as "may a run use the platform provider", so the
    invalidation tests below assert it directly instead of inferring it from a
    runtime block that no longer happens.
    """

    readiness = await production_runtime._platform_readiness_store(session).load()
    role_profile = await production_runtime._platform_role_store(session).load()
    candidate, _ = production_runtime._platform_candidate(
        readiness=readiness, role_profile=role_profile
    )
    assert candidate is not None
    # The whole point of CHAOS-3358: a stale certification never costs the
    # candidate its usability.
    assert candidate.usable is True
    return candidate.readiness_current


def _certified_platform_settings(fingerprint: str) -> dict[str, str]:
    role_key, role_value = _role_certification_setting(certification_key=fingerprint)
    return {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": fingerprint,
                "readiness_version": READINESS_VERSION,
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        ),
        role_key: role_value,
    }


@pytest.mark.asyncio
async def test_stale_platform_certification_does_not_block_an_org_without_byo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3358 primary control: a platform certification stored under an
    older fingerprint/readiness version -- exactly what every READINESS_VERSION
    bump produces -- must not hard-block an organization that has no BYO
    configuration."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    FakeSettingsService.values = {
        PLATFORM_READINESS_SETTING_KEY: json.dumps(
            {
                "fingerprint": "fingerprint-from-the-previous-readiness-version",
                "readiness_version": "ask-dev-readiness.v0",
                "checked_at": "2026-07-29T12:00:00+00:00",
                "outcome": "ready",
                "safe_error_code": None,
            }
        )
    }
    session = cast(Any, object())

    resolved = await production_runtime.resolve_production_provider(
        session, org_id="org_01"
    )

    assert resolved.source is AgentProviderSource.PLATFORM
    assert resolved.family == "openai"
    assert resolved.model == "certified-model"
    # The operator diagnostic still reads "stale" -- the badge and the "run
    # preflight" copy are unchanged; only the runtime block is gone.
    assert await _platform_certification_is_current(session) is False


@pytest.mark.asyncio
async def test_absent_platform_certification_does_not_block_an_org_without_byo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same control for a platform provider that was never certified at
    all (fresh deploy, or the readiness slot cleared): still selectable."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    FakeSettingsService.values = {}
    session = cast(Any, object())

    resolved = await production_runtime.resolve_production_provider(
        session, org_id="org_01"
    )

    assert resolved.source is AgentProviderSource.PLATFORM
    assert resolved.model == "certified-model"
    assert await _platform_certification_is_current(session) is False


@pytest.mark.asyncio
async def test_stale_platform_certification_still_serves_the_byo_fallback_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An org WITH BYO whose own certification is stale, under the default
    platform-fallback policy, lands on the platform provider rather than
    hard-blocking -- the platform's own stale certification is irrelevant."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setattr(
        production_runtime, "attach_agent_budget_guard", lambda value, **_: value
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    FakeSettingsService.values = {
        # BYO is configured but has no certification on record.
        "provider": "openai",
        "model": "gpt-5-mini",
        "api_key": "sk-org",
        "base_url": "https://api.openai.com/v1",
        "ask_dev_platform_fallback": "platform",
    }
    session = cast(Any, object())

    resolved = await production_runtime.resolve_production_provider(
        session, org_id="org_01"
    )

    assert resolved.source is AgentProviderSource.PLATFORM
    assert resolved.model == "certified-model"


@pytest.mark.asyncio
async def test_byo_certification_is_still_a_hard_runtime_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Negative control: BYO gating is unchanged. An uncertified BYO
    configuration under an explicit fail-closed policy still blocks, and it
    blocks even though the PLATFORM candidate beside it is fully certified --
    so the block is demonstrably BYO's, not a side effect of platform state."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    FakeSettingsService.values = {
        **_certified_platform_settings(_fingerprint()),
        "provider": "openai",
        "model": "gpt-5-mini",
        "api_key": "sk-org",
        "base_url": "https://api.openai.com/v1",
        "ask_dev_platform_fallback": "fail_closed",
    }
    session = cast(Any, object())

    with pytest.raises(DevRuntimeUnavailable) as exc_info:
        await production_runtime.resolve_production_provider(session, org_id="org_01")
    assert exc_info.value.code == "provider_not_configured"


@pytest.mark.asyncio
async def test_platform_without_operator_credentials_is_still_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Negative control -- the gate that remains, observed failing: dropping
    the readiness conjunct must not admit a platform provider that has no
    operator credentials at all."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    for name in (
        "OPENAI_API_KEY",
        "LLM_API_KEY",
        "OPENAI_BASE_URL",
        "LLM_BASE_URL",
    ):
        monkeypatch.delenv(name, raising=False)
    FakeSettingsService.values = {}
    session = cast(Any, object())

    with pytest.raises(DevRuntimeUnavailable) as exc_info:
        await production_runtime.resolve_production_provider(session, org_id="org_01")
    assert exc_info.value.code == "provider_not_configured"


@pytest.mark.asyncio
async def test_operator_none_provider_is_still_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Negative control: the operator's explicit "no LLM" switch still
    disables Ask Dev outright."""

    monkeypatch.setattr(production_runtime, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(
        production_runtime, "_provider", lambda _candidate: FakeProvider()
    )
    monkeypatch.setenv("LLM_PROVIDER", "none")
    monkeypatch.setenv("LLM_MODEL", "certified-model")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    FakeSettingsService.values = {}
    session = cast(Any, object())

    with pytest.raises(DevRuntimeUnavailable) as exc_info:
        await production_runtime.resolve_production_provider(session, org_id="org_01")
    assert exc_info.value.code == "provider_not_configured"


def test_acceptance_candidate_still_fails_closed_without_current_readiness() -> None:
    """Negative control: the deterministic acceptance stack's pre-admitted
    endpoint keeps its strict readiness requirement -- it exists to fail
    closed, and CHAOS-3358 must not relax it via the shared base class."""

    def acceptance(*, readiness_current: bool) -> Any:
        return production_runtime._AcceptanceOpenAICandidate(
            provider="openai",
            model=production_runtime.ACCEPTANCE_OPENAI_MODEL,
            credentials=LLMCredentials(
                api_key="acceptance-key", base_url="http://127.0.0.1:8001/v1"
            ),
            source=AgentProviderSource.PLATFORM,
            readiness_current=readiness_current,
        )

    assert acceptance(readiness_current=False).usable is False
    assert acceptance(readiness_current=True).usable is True
