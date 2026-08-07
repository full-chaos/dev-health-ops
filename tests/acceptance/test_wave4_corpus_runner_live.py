"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: the pytest-based corpus runner (D5).

Drives real corpus cases from
``tests/acceptance/world/ask-dev-world.v1/corpus/*.json`` (Lane 2b) through
the real Ask Dev acceptance stack over `/api/v1/dev/**` HTTP/SSE, evaluates
each case's ``invariants`` via ``scripts.acceptance.corpus.invariants``, and
writes one ``wave4_case_result.v1`` receipt per case
(``scripts.acceptance.corpus.receipt``). ``write_session_summary``
(receipt.py) aggregates the per-case receipts this run writes into
``tests/acceptance/artifacts/wave4/session-summary.json`` -- invoked as a
separate CI step after this file's own pytest run completes (Phase 5c's
wiring), not from inside this module.

Session-start guards, in order:

1. armed-or-throw (``ASK_DEV_LIVE_ACCEPTANCE=1``) -- the ONE guard that
   legitimately SKIPS rather than fails when absent. This module is
   collected by the standard, always-on unit-tier gate (the whole
   ``tests/`` directory, unconditionally -- see ops/AGENTS.md), unlike the
   pre-CHAOS-3219 smoke-script convention where only the launcher ever
   invoked the live script directly. Failing loud merely because nobody
   booted the acceptance stack would break that gate for every ordinary
   local/CI run; ``test_live_openai_smoke.py`` documents the identical
   reasoning for its own opt-in live suite. See the ``_armed_or_throw``
   fixture docstring.
2. script-inventory preflight -- every collected case id must have a
   scripted-provider entry for the active role. Fails loud, never skips,
   once armed.
3. WORLD_DIGEST verification -- the live fixture database must match the
   pinned digest (ruling D2). Fails loud, never skips, once armed.
4. quota budget construction from the compose-configured ceiling.

KNOWN OPERATIONAL RISK (Codex round-1, MEDIUM, deliberately NOT fixed here
-- Phase 5c's territory, not Lane 2a's): because guard 1 skips rather than
fails, a CI acceptance JOB that forgets to export
``ASK_DEV_LIVE_ACCEPTANCE=1`` reports a green, entirely-skipped run --
indistinguishable at a glance from a real pass. This module cannot fix its
own invocation from the inside; Phase 5c's launcher/workflow wiring must
positively assert non-zero collected+executed tests for this file specifically
(e.g. parse the pytest summary, or run it with ``--no-cov -p no:cacheprovider
-rs`` and grep for an unexpected "skipped" line), the same way
``test_live_openai_smoke.py``'s own ``ASK_DEV_LIVE_GATE`` two-tier pattern
lets ITS caller distinguish "opted out" from "the gate forgot to opt in".

"Armed-or-THROW" describes guards 2-4: once armed, nothing downstream of
the arming check may skip -- only guard 1 itself is a legitimate skip.

VERIFICATION PLANE (team-lead ruling, 2026-08-06): product-facing case
execution (the SSE HTTP round-trip below) stays wire-only against the
public API, matching the existing smoke-script convention
(``prepare_ask_dev_acceptance.py``'s "public-API-only seeding" philosophy)
-- the acceptance Compose overlay (``compose.ask-dev.yml``) deliberately
exposes NO host port for postgres/clickhouse (``ports: !reset []``,
isolation-by-design), and that stays intact. Exactly three harness concerns
that genuinely need database access reach it through the CONTAINER boundary
instead, via ``docker compose exec -T`` (``scripts.acceptance.corpus.
db_verify``) -- never a host port, never a new product API endpoint:

* WORLD_DIGEST verification (guard 3, ruling D2) --
  ``db_verify.verify_world_digest_via_exec``.
* The CHAOS-3424 resolution ledger read for ``resolution_path`` derivation
  -- ``db_verify.query_resolution_ledger_via_exec``, keyed by the ``run_id``
  every ``DevStreamEvent`` carries.
* The CHAOS-3423 transcript read for ``terminal_persists_assistant_row``
  (2b codex round-1 addition, team-lead direction 2026-08-06) --
  ``db_verify.query_transcript_assistant_schema_versions_via_exec``, keyed
  by ``conversation_id``.

Both fail loud (``DbVerifyUnavailableError``) if the exec plane itself
cannot be reached at all (docker/compose missing, non-zero exit, unparseable
output) -- distinct from :func:`derive_resolution_path`'s own honest
``None``, which applies only once the exec plane genuinely reached the
ledger and found it empty (a non-subject-shaped case).

CLOSED (CHAOS-3462 B6): entries the exec plane returns still carry no
``mention_text`` -- ``DevResolutionEntry`` never persists the original span
-- so a single-shot ``exact_match`` used to be unclassifiable, which made
``deterministic-exact`` DEAD VOCABULARY for the whole corpus: ~46 cases were
red on ``resolution_path_classifiable`` no matter what their invariants or
profile said. The fix is the schema addition ``resolution_path.py``'s own
docstring anticipated: each case declares ``expected_mention_texts``, and
``attach_mention_texts`` threads them onto the entries by first-seen mention
order. The declared spans are DERIVED FROM THE PRODUCER (production's
``QuestionInterpreter``, whose ``normalized_lookup_text`` is exactly what
reached the resolver) and pinned against it by a corpus guard test, so a
question edited by one word fails the unit gate rather than the live run.
The two count mismatches are asymmetric: more observed mentions than
declared raises (a real mention would get no span), while fewer attaches
nothing and proceeds (a short ledger can only be a terminating
``ambiguous_candidates`` entry, which never needs a span).

Merge-order note: this repo's Lane 2a merges before Lane 2b, so
``tests/acceptance/world/ask-dev-world.v1/corpus/`` may not exist, or may be
empty, in this checkout. Case loading (``load_corpus_cases``) returns an
empty list for that -- not an error, matching ``case_schema.py``'s own
documented contract -- so the parametrized ``test_corpus_case`` below simply
collects zero cases in that state, while
``test_at_least_one_corpus_case_is_collected`` fails loud IF the run is
armed but still finds nothing (the actual false-green guard; it does not
fire merely from importing this module unarmed).
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev.contracts import (
    DevStreamEvent,
    StreamEventType,
    validate_stream,
)
from dev_health_ops.api.dev.terminal_frames import PUBLIC_OUTCOME_BY_ERROR_CODE
from dev_health_ops.llm.agent.provider_scripts import current_role, load_role_script
from scripts.acceptance.corpus.arming import (
    ArmedButScrubbedError,
    NotArmedError,
    require_armed,
)
from scripts.acceptance.corpus.case_schema import (
    CorpusCase,
    load_corpus_cases,
    load_resolution_profile,
    resolve_case_expectations,
)
from scripts.acceptance.corpus.compose_context import ComposeContext
from scripts.acceptance.corpus.db_verify import (
    DbVerifyUnavailableError,
    query_resolution_ledger_via_exec,
    query_transcript_assistant_schema_versions_via_exec,
    verify_world_digest_via_exec,
)
from scripts.acceptance.corpus.feature_flags import (
    enable_wave_3_1,
    verify_wave_3_1_enabled,
)
from scripts.acceptance.corpus.invariants import InvariantContext, evaluate_invariant
from scripts.acceptance.corpus.principals import (
    SEEDED_PROVISIONING_MARKER,
    PrincipalDirectory,
    PrincipalSession,
    PrincipalSessions,
)
from scripts.acceptance.corpus.quota import QuotaBudget, estimate_run_cost_microusd
from scripts.acceptance.corpus.receipt import (
    Wave4CaseRecorder,
    write_declared_blocked_receipt,
)
from scripts.acceptance.corpus.resolution_path import (
    ABSENCE_RUN_ID_NOT_OBSERVED,
    ResolutionPathError,
    absence_is_a_broken_measurement,
    attach_mention_texts,
    derive_resolution_path,
    resolution_path_absence_reason,
)
from scripts.acceptance.corpus.script_inventory import check_script_inventory
from scripts.acceptance.corpus.sse_client import SseFrame, parse_sse_events
from scripts.acceptance.corpus.world_digest_guard import (
    WorldDigestMismatchError,
    require_world_digest_match,
)
from scripts.acceptance.prepare_ask_dev_acceptance import (
    AcceptanceApi,
    AcceptanceFailure,
)

_ROOT = Path(__file__).resolve().parents[2]
_WORLD_DIR = _ROOT / "tests" / "acceptance" / "world" / "ask-dev-world.v1"
_CORPUS_DIR = _WORLD_DIR / "corpus"
_PROFILES_DIR = _WORLD_DIR / "resolution-profiles"
_RECEIPTS_DIR = _ROOT / "tests" / "acceptance" / "artifacts" / "wave4"

#: Internal-network connection strings -- meaningful only from INSIDE the
#: ask-dev-acceptance Compose network (docker compose exec), never from the
#: host. Match compose.yml's own `api` service DATABASE_URI/CLICKHOUSE_URI
#: exactly (direct to `postgres`, bypassing pgbouncer, for the same caution
#: compose.yml's own migration comment gives: a verification read should not
#: depend on pgbouncer's transaction-mode pooling behavior).
_CONTAINER_CLICKHOUSE_SINK = "clickhouse://ch:ch@clickhouse:8123/default"
_CONTAINER_POSTGRES_URI = (
    "postgresql+asyncpg://postgres:postgres@postgres:5432/postgres"
)
_CONTAINER_WORLD_MANIFEST = "/app/tests/acceptance/world/ask-dev-world.v1/world.json"

#: Computed at collection time -- pure JSON loading, no live infra, safe to
#: run unarmed (this is exactly what lets `pytest --collect-only` work
#: without touching the network, and what makes a malformed case file a
#: COLLECTION error rather than a runtime one).
_ALL_CASES: list[CorpusCase] = load_corpus_cases(_CORPUS_DIR)
#: Declared-blocked cases (team-lead direction 2026-08-06) never execute --
#: they get their own receipt via `test_declared_blocked_case` below,
#: entirely without touching the network/quota/script-inventory machinery
#: the executable cases need.
_CASES: list[CorpusCase] = [case for case in _ALL_CASES if not case.is_declared_blocked]
_BLOCKED_CASES: list[CorpusCase] = [
    case for case in _ALL_CASES if case.is_declared_blocked
]


def _load_resolution_profiles() -> dict[str, Any]:
    profiles: dict[str, Any] = {}
    if _PROFILES_DIR.is_dir():
        for path in sorted(_PROFILES_DIR.glob("*.json")):
            profile = load_resolution_profile(path)
            profiles[profile.profile_id] = profile
    return profiles


@pytest.fixture(scope="session", autouse=True)
def _armed_or_throw(scrubbed_ambient_env_names: tuple[str, ...]) -> None:
    """Skip when nobody asked for this run at all -- FAIL when they did.

    This module IS collected by the standard, always-on unit-tier gate
    (``ci/local_validate.sh`` runs the whole ``tests/`` directory
    unconditionally -- see ops/AGENTS.md) -- unlike the pre-CHAOS-3219
    smoke-script convention (a standalone ``.py`` script only the launcher
    ever invokes), this is a real pytest module every contributor's ordinary
    local/CI run collects. Failing loud here merely because
    ``ASK_DEV_LIVE_ACCEPTANCE`` is unset would break that gate for every
    contributor who has not booted the acceptance Compose stack -- exactly
    the same reasoning ``test_live_openai_smoke.py`` already documents for
    its own opt-in live suite ("nobody asked for the live gate to run
    here"). "Armed-or-THROW" still holds for everything downstream of this
    check: once armed, the script-inventory/world-digest/quota guards below
    never skip, only fail loud.

    CHAOS-3462 B1 -- THE THIRD STATE: the paragraph above was written as if
    there were only two ("armed" and "nobody asked"). The Phase 2 exit
    evidence run found a third and it was silently taking the skip branch.
    CHAOS-3402's ``tests/_env_isolation.py`` scrub deletes
    ``ASK_DEV_LIVE_ACCEPTANCE`` in ``pytest_configure``, before this fixture
    (or this module) exists, so a correctly-armed operator run reported
    ``144 skipped``, exit 0 -- a green session for a run that touched
    nothing. ``scrubbed_ambient_env_names`` (tests/conftest.py) is the
    scrub's own record of what it REMOVED, which is evidence of arming that
    survives the deletion of the arming variable itself; an
    ``ArmedButScrubbedError`` from that evidence is a hard FAIL, never a
    skip. ``scripts/acceptance/run_wave4_corpus.sh`` is the standing fix
    (it exports the ``DEV_HEALTH_TEST_ENV_ALLOW`` exemption); this branch is
    the belt to that braces, for a run invoked some other way.
    """

    try:
        require_armed(scrubbed_names=scrubbed_ambient_env_names)
    except ArmedButScrubbedError as exc:
        pytest.fail(str(exc), pytrace=False)
    except NotArmedError as exc:
        pytest.skip(str(exc))


@pytest.fixture(scope="session")
def role_script():
    role = current_role()
    return role, load_role_script(role)


@pytest.fixture(scope="session", autouse=True)
def _script_inventory_preflight(role_script) -> None:
    role, script = role_script
    check_script_inventory([case.id for case in _CASES], script.cases, role=role)


@pytest.fixture(scope="session")
def compose_context() -> ComposeContext:
    return ComposeContext.from_env()


def _resolve_world_digest_pin(context: ComposeContext) -> str:
    """Verify the live world matches WORLD_DIGEST via the exec verification
    plane, or let :class:`DbVerifyUnavailableError`/
    ``WorldDigestMismatchError`` propagate. Returns the verified live digest
    string for receipts to pin against.
    """

    verification = verify_world_digest_via_exec(
        context,
        manifest_path_in_container=_CONTAINER_WORLD_MANIFEST,
        sink=_CONTAINER_CLICKHOUSE_SINK,
        postgres_uri=_CONTAINER_POSTGRES_URI,
    )
    require_world_digest_match(verification, digest_path=_WORLD_DIR / "WORLD_DIGEST")
    return verification.live_digest


@pytest.fixture(scope="session", autouse=True)
def _world_digest_pin(compose_context: ComposeContext) -> str:
    try:
        return _resolve_world_digest_pin(compose_context)
    except (DbVerifyUnavailableError, WorldDigestMismatchError) as exc:
        pytest.fail(str(exc), pytrace=False)


@pytest.fixture(scope="session")
def resolution_profiles() -> dict[str, Any]:
    return _load_resolution_profiles()


@pytest.fixture(scope="session")
def quota_budget() -> QuotaBudget:
    return QuotaBudget.from_env()


def _api_base_url() -> str:
    return os.getenv("ASK_DEV_ACCEPTANCE_API_URL", "http://127.0.0.1:18080")


def _superuser_password() -> str:
    return os.getenv("TEST_SUPERUSER_PASSWORD", "devhealth123")


@pytest.fixture(scope="session")
def acceptance_api() -> tuple[AcceptanceApi, str]:
    """The ADMIN session -- used only to provision per-case principals.

    CHAOS-3462 B5: this is no longer the session cases execute under. It
    logs in as the acceptance superuser because that is the identity allowed
    to call ``POST /api/v1/admin/users/{id}/password``; the cases themselves
    run under ``principal_sessions`` below.
    """

    api = AcceptanceApi(_api_base_url())
    email = os.getenv("TEST_SUPERUSER_EMAIL", "admin@devhealth.example")
    password = _superuser_password()
    login = api.request(
        "POST", "/api/v1/auth/login", {"email": email, "password": password}
    )
    token = login.get("access_token") if isinstance(login, dict) else None
    if not token:
        raise AcceptanceFailure("login returned no access_token")
    api.token = token
    user = login.get("user") if isinstance(login, dict) else None
    org_id = user.get("org_id") if isinstance(user, dict) else None
    if not org_id:
        raise AcceptanceFailure("login returned no user.org_id")
    return api, org_id


@pytest.fixture(scope="session")
def principal_sessions(
    acceptance_api: tuple[AcceptanceApi, str],
    _world_digest_pin: str,
) -> PrincipalSessions:
    """Per-case principal selection (CHAOS-3462 B5).

    Before this existed, all 93 active cases ran as the acceptance superuser
    in one org, so the cross-tenant and entitlement families asserted
    nothing about the identities they name. Each case's ``org_alias`` /
    ``user_alias`` now resolves through ``world.json`` to a real principal,
    which the runner authenticates as. See ``principals.py`` for why this is
    a genuine login rather than ``/api/v1/admin/impersonate`` (the dev
    routers do not honor impersonation, and would silently evaluate
    entitlement and readiness against the superuser's org instead).

    THE ``_world_digest_pin`` DEPENDENCY IS STILL DECLARED, FOR A NARROWER
    REASON THAN IT ONCE HAD. It used to be load-bearing because selecting a
    principal MUTATED ``password_hash`` -- a column ``compute_world_digest``
    covers, since it hashes ``SELECT *`` from ``users`` with only
    ``_VOLATILE_COLUMNS`` excluded -- so the digest had to be read before
    the runner touched it. CHAOS-3463 removed that mutation: credentials are
    seeded at world generation, so nothing here provisions anything. The
    ordering is kept because authenticating against a world whose integrity
    has not been verified yet would produce evidence nobody can trust.

    NOT because this fixture is write-free, which would be the tempting and
    wrong way to put it: a successful login stamps ``users.last_login_at``.
    That column is excluded from the digest (``_VOLATILE_COLUMNS``, added by
    CHAOS-3463 after two boots both failed ``require_world_digest_match`` on
    ``postgres.users`` for exactly this reason), so it does not drift --
    which is a different statement from "nothing is written".

    That does retire the residual this fixture used to carry: a SECOND armed
    run against the same stack no longer reports a ``postgres.users`` digest
    mismatch caused by the first run's own provisioning, so re-runs no
    longer need a fresh seed/restore.

    ``acceptance_api`` is still requested although its client is no longer
    unpacked: it is what guarantees the stack is up and the superuser
    session exists before any principal login is attempted.
    """

    return PrincipalSessions(
        api_factory=lambda: AcceptanceApi(_api_base_url()),
        directory=PrincipalDirectory.from_world(_WORLD_DIR / "world.json"),
    )


@pytest.fixture(scope="session")
def wave_3_1_enabled_orgs(
    acceptance_api: tuple[AcceptanceApi, str],
    principal_sessions: PrincipalSessions,
) -> frozenset[str]:
    """Turn the CHAOS-3292 preflight ON for every org the corpus runs in.

    CHAOS-3219 Phase 2 exit, live-diagnosed: without this the corpus measured
    the pre-3292 legacy path for all 91 armed cases (see
    ``corpus/feature_flags.py`` for the full diagnosis and why
    ``prepare_ask_dev_acceptance.py`` is deliberately NOT the right place).

    Scoped to exactly the orgs the ACTIVE cases actually name, derived from
    the cases rather than hardcoded. That matters beyond tidiness: the world
    also contains a ``disabled`` org whose entitlement is off ON PURPOSE, and
    blanket-enabling every org in ``world.json`` would quietly destroy the
    disabled-entitlement cases' whole premise.

    Enable-then-read-back per org: a 200 on the write is the server echoing
    the request body, not proof the org resolves the flag as on.
    """

    admin_api, _admin_org_id = acceptance_api
    org_ids: set[str] = set()
    for org_alias, user_alias in _representative_alias_per_org():
        # Resolve the alias pair to its real org_id through the SAME principal
        # directory the cases authenticate with, so this can never enable the
        # flag on a different org than the one under test. `session_for_alias`
        # caches, so these are the same sessions the cases go on to reuse --
        # not extra logins against AUTH_LOGIN_IP_LIMIT.
        session = principal_sessions.session_for_alias(user_alias, org_alias=org_alias)
        enable_wave_3_1(admin_api, org_id=session.org_id)
        verify_wave_3_1_enabled(admin_api, org_id=session.org_id)
        org_ids.add(session.org_id)
    if not org_ids:
        raise AcceptanceFailure(
            "no active corpus case declares an org_alias -- cannot enable the "
            "wave 3.1 preflight, and running without it would silently "
            "measure the pre-3292 legacy path"
        )
    return frozenset(org_ids)


def _representative_alias_per_org() -> list[tuple[str, str]]:
    """One ``(org_alias, user_alias)`` pair per distinct org the ACTIVE cases
    name, in stable order.

    Read off ``case.raw`` rather than ``CorpusCase`` on purpose, and for the
    reason ``principals.principal_for`` documents: ``case_schema`` never
    rejects unknown extra fields so Lane 2b can add bookkeeping without a
    loader change, which is exactly why ``org_alias``/``user_alias`` live on
    the raw mapping and not on the dataclass. Cases missing either are skipped
    here rather than raising -- ``principal_for`` already fails loud on them
    at the point of use, and duplicating that check here would just move the
    error message somewhere less informative.
    """

    seen: dict[str, str] = {}
    for case in _CASES:
        raw = getattr(case, "raw", {}) or {}
        org_alias = raw.get("org_alias")
        user_alias = raw.get("user_alias")
        if not isinstance(org_alias, str) or not isinstance(user_alias, str):
            continue
        seen.setdefault(org_alias, user_alias)
    return sorted(seen.items())


def _scope(org_id: str) -> dict[str, Any]:
    now = datetime.now(UTC).replace(microsecond=0)
    current_start = now - timedelta(days=28)

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
        "comparison_range": time_range(
            current_start - timedelta(days=28), current_start
        ),
        "surface_context": None,
    }


def _post_sse(api: AcceptanceApi, path: str, payload: dict[str, Any]) -> list[SseFrame]:
    if api.token is None:
        raise AcceptanceFailure("SSE request requires authentication")
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
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise AcceptanceFailure(
            f"POST {path} returned HTTP {exc.code}: {detail}"
        ) from exc
    except URLError as exc:
        raise AcceptanceFailure(f"POST {path} failed: {exc.reason}") from exc
    return parse_sse_events(body)


def _run_id_from_frames(frames: list[SseFrame]) -> str | None:
    """Every ``DevStreamEvent`` carries ``run_id`` -- take it off the first
    validated frame."""

    return frames[0].data.get("run_id") if frames else None


def _public_outcome_from_events(validated_events: list[DevStreamEvent]) -> str | None:
    """Derive the wire-observable v2 ``PublicOutcome`` from a validated,
    lifecycle-checked stream.

    Codex round-2 finding (HIGH, confirmed): the original version read
    ``answer.status`` directly and called it ``public_outcome`` --
    ``AnswerStatus`` (v1: complete/partial/degraded/insufficient_evidence/
    refused/error) and ``PublicOutcome`` (v2: answered/answered_with_gaps/
    needs_clarification/not_found/temporarily_unavailable/unsupported/
    denied/failed) are DIFFERENT, non-overlapping vocabularies -- a probe
    of the real ``complete`` value against a profile-declared ``answered``
    expectation fails every time, silently inverting every correctly
    authored ``public_outcome_in`` assertion.

    For an error terminal, the real mapping is production's own
    ``PUBLIC_OUTCOME_BY_ERROR_CODE`` (``terminal_frames.py``) -- reused, not
    reimplemented. For a real ``DevAnswer`` terminal, today's stack
    ALWAYS maps to ``answered_with_gaps`` -- never plain ``answered`` --
    per ``terminal_frames.wrap_legacy_answer_as_frame``'s own explicit,
    unconditional documented behavior (the legacy v1 model-tool-choice loop
    never computes a real completion block, so the frame it wraps into is
    never eligible for plain ``answered``). A future v2-native orchestrator
    path would need its own mapping once one exists; not guessed at here.
    """

    for event in validated_events:
        if event.event is StreamEventType.ERROR and event.error is not None:
            return PUBLIC_OUTCOME_BY_ERROR_CODE.get(event.error.code)
    for event in validated_events:
        if event.event is StreamEventType.ANSWER_COMPLETED and event.answer is not None:
            return "answered_with_gaps"
    return None


def test_at_least_one_corpus_case_is_collected() -> None:
    """The case_count>0 false-green guard: an ARMED run that collected zero
    cases (executable OR declared-blocked) must fail loud, never silently
    report a vacuous, technically zero-failures green session."""

    assert len(_ALL_CASES) > 0, (
        f"zero corpus cases found under {_CORPUS_DIR} in an ARMED run -- "
        "Lane 2b's case content is missing from this checkout, or the "
        "directory moved; a live corpus run must never silently proceed "
        "with zero cases"
    )


@pytest.mark.parametrize(
    "case", _BLOCKED_CASES, ids=[case.id for case in _BLOCKED_CASES]
)
def test_declared_blocked_case(case: CorpusCase) -> None:
    """Declared-blocked cases never execute -- world-manifest discipline
    (``world.json``/``sources.json``'s own declared-blocked/blocked_by
    precedent) applied to the corpus receipt layer: loud, counted, never a
    silent drop, never mistaken for a pass. No network/quota/script-
    inventory fixtures needed -- this never touches the stack.
    """

    assert case.blocked_by is not None  # case_schema.py's own load-time guarantee
    artifact = write_declared_blocked_receipt(
        case_id=case.id,
        question=case.question,
        subject_class=case.subject_class,
        resolution_profile_ref=case.resolution_profile_ref,
        blocked_by=case.blocked_by,
        path=_RECEIPTS_DIR / f"{case.id}.json",
    )
    assert artifact["status"] == "declared-blocked"


@pytest.mark.parametrize("case", _CASES, ids=[case.id for case in _CASES])
def test_corpus_case(
    case: CorpusCase,
    acceptance_api: tuple[AcceptanceApi, str],
    principal_sessions: PrincipalSessions,
    resolution_profiles: dict[str, Any],
    quota_budget: QuotaBudget,
    compose_context: ComposeContext,
    _world_digest_pin: str,
    wave_3_1_enabled_orgs: frozenset[str],
) -> None:
    admin_api, _admin_org_id = acceptance_api
    # CHAOS-3462 B5: resolved BEFORE the quota reservation and any HTTP
    # traffic, so an unresolvable principal costs nothing and fails at the
    # earliest honest point. `session_for` never falls back to the admin
    # session -- an unknown/missing/incoherent alias raises.
    session: PrincipalSession = principal_sessions.session_for(case)
    api = session.api
    org_id = session.org_id
    expectations = resolve_case_expectations(case, resolution_profiles)
    cost = estimate_run_cost_microusd(input_tokens=8_000, output_tokens=3_000)
    quota_budget.reserve(case_id=case.id, requests=1, cost_microusd=cost)

    scope = _scope(org_id)
    try:
        conversation = api.request(
            "POST",
            "/api/v1/dev/conversations",
            {"current_scope": scope, "retention_days": 30, "title": f"wave4 {case.id}"},
        )
        conversation_id = (
            conversation.get("conversation_id")
            if isinstance(conversation, dict)
            else None
        )
        if not conversation_id:
            raise AcceptanceFailure(
                f"case {case.id!r}: conversation response returned no id"
            )
    except Exception:
        # Codex round-1 (MEDIUM) + round-2 (MEDIUM, confirmed) findings:
        # release is now scoped to ONLY this call. Conversation creation is
        # unambiguously pre-admission -- nothing has been sent to the
        # message endpoint yet. The message POST below is NOT wrapped the
        # same way: once that request may have reached the server, a
        # timeout/parse failure on OUR side does not prove the server never
        # admitted (and priced) it, so the reservation must be retained
        # rather than released against understated real spend.
        quota_budget.release(requests=1, cost_microusd=cost)
        raise

    frames = _post_sse(
        api,
        f"/api/v1/dev/conversations/{conversation_id}/messages",
        {
            "schema_version": "dev_message_request.v1",
            "request_id": str(uuid.uuid4()),
            "client_message_id": str(uuid.uuid4()),
            "conversation_id": conversation_id,
            "question": case.question,
            "question_class": "status",
            "scope": scope,
        },
    )

    # Codex round-1 (HIGH) + round-2 (HIGH, confirmed) findings: per-frame
    # validation alone still let an empty stream, a stream with no
    # terminal, or an envelope/payload event-name mismatch through.
    # `validate_stream` is the SAME production lifecycle contract the real
    # web client enforces (non-empty, ordered, one run id, starts with
    # run.started, exactly one terminal, ends with done) -- reused, not
    # reimplemented. The envelope/payload cross-check closes a smuggling
    # vector `no_internal_error` alone could not see: a payload whose OWN
    # validated `event` field disagrees with the SSE `event:` line this
    # runner's `events` list is keyed by.
    validated_events: list[DevStreamEvent] = []
    for frame in frames:
        try:
            validated = DevStreamEvent.model_validate(frame.data)
        except ValidationError as exc:
            raise AcceptanceFailure(
                f"case {case.id!r}: SSE frame failed dev_stream_event.v1 "
                f"validation: {exc}"
            ) from exc
        if validated.event.value != frame.event:
            raise AcceptanceFailure(
                f"case {case.id!r}: SSE envelope event {frame.event!r} does "
                f"not match the validated payload's own event "
                f"{validated.event.value!r}"
            )
        validated_events.append(validated)
    try:
        validate_stream(validated_events)
    except ValueError as exc:
        raise AcceptanceFailure(
            f"case {case.id!r}: stream failed dev_stream_event.v1 lifecycle "
            f"validation (non-empty/ordered/one-terminal/done): {exc}"
        ) from exc

    events = [{"event": frame.event, "data": frame.data} for frame in frames]
    public_outcome = _public_outcome_from_events(validated_events)

    run_id = _run_id_from_frames(frames)
    resolution_path: str | None
    ledger_classification_error: str | None = None
    if run_id is None:
        resolution_path = None
    else:
        ledger_entries = query_resolution_ledger_via_exec(
            compose_context, run_id=run_id
        )
        try:
            # CHAOS-3462 B6: the exec plane cannot return the mention span
            # (DevResolutionEntry never persists it), so the CASE supplies
            # it -- spans derived from production's own QuestionInterpreter
            # and pinned against it by the corpus guard test. Without this,
            # every single-shot exact_match was unclassifiable and
            # `deterministic-exact` was dead vocabulary for the whole
            # corpus. More observed mentions than declared raises; fewer
            # attaches nothing (a terminating ambiguous entry needs no span).
            resolution_path = derive_resolution_path(
                attach_mention_texts(ledger_entries, case.expected_mention_texts)
                if case.expected_mention_texts
                else ledger_entries
            )
        except ResolutionPathError as exc:
            # Still recorded as a named, failed assertion rather than
            # silently swallowed into a guessed path: after B6 this should
            # only fire on a real drift between the case's declared spans
            # and what the run actually resolved.
            resolution_path = None
            ledger_classification_error = str(exc)

    # CHAOS-3219 Phase 2 exit: `resolution_path is None` meant two different
    # things and the receipt recorded neither -- "the ledger was read and was
    # honestly empty" versus "no run_id, so nothing was ever read". All 143
    # receipts in exit run #3 said `resolution_path: null` and a reader could
    # not tell which had happened. Recorded explicitly from here on.
    resolution_path_absence = resolution_path_absence_reason(
        run_id=run_id,
        path=resolution_path,
        # Codex adversarial review (MEDIUM): a ledger that was queried and
        # could not be CLASSIFIED is not an empty ledger. Without this the
        # error path below reported `empty-resolution-ledger` -- an honest
        # absence -- for a measurement that actually broke.
        classification_failed=ledger_classification_error is not None,
    )

    # 2b codex round-1 addition (team-lead direction 2026-08-06): the third
    # harness concern allowed through the exec verification plane --
    # `terminal_persists_assistant_row` proves CHAOS-3423's own guarantee
    # from the corpus side. Queried unconditionally (cheap, and every case
    # gets the SAME context shape regardless of whether it declares this
    # invariant) rather than gated behind "does this case need it".
    assistant_schema_versions = (
        query_transcript_assistant_schema_versions_via_exec(
            compose_context, conversation_id=conversation_id
        )
        if conversation_id
        else []
    )

    context = InvariantContext(
        resolution_path=resolution_path,
        public_outcome=public_outcome,
        events=events,
        expectations=expectations,
        assistant_schema_versions=assistant_schema_versions,
    )

    recorder = Wave4CaseRecorder(
        case_id=case.id,
        question=case.question,
        subject_class=case.subject_class,
        resolution_profile_ref=case.resolution_profile_ref,
    )
    # CHAOS-3462 B5, recorded as a real checked assertion rather than left
    # implicit: this case executed under its OWN declared principal's token,
    # not the admin token used to provision it. Without this, a future
    # refactor that reintroduced the superuser fallback would leave every
    # receipt looking exactly the same as one from a correctly-scoped run.
    recorder.check(
        category="declared-principal",
        name="ran_as_declared_principal",
        condition=(
            api is not admin_api
            and api.token is not None
            and api.token != admin_api.token
        ),
        detail=(
            f"org_alias={session.principal.org_alias!r} "
            f"user_alias={session.principal.user_alias!r} "
            f"email={session.principal.email!r} org_id={org_id!r}"
        ),
    )
    # Adversarial round 3: the provisioning mode used to live only inside the
    # free text above, where deleting the fragment left every test green. It
    # is now its own NAMED, always-recorded check, so the receipt says which
    # credential path produced it in a field a reader can key on -- and a
    # test asserts the name is present for an executed case.
    #
    # The condition is a real assertion rather than a bare `True` (which it
    # was while two modes existed and either was acceptable). With the
    # admin-set-password bridge gone there is exactly ONE legitimate
    # credential path, so a run reporting any other value is a run whose
    # receipts mean something different from what a reader would assume --
    # and that must show up as a failed check, not as prose in a detail
    # string nobody diffs.
    recorder.check(
        category="provisioning-mode",
        name=f"provisioned_via_{principal_sessions.provisioning_mode}",
        condition=(principal_sessions.provisioning_mode == SEEDED_PROVISIONING_MARKER),
        detail=(
            "credentials came from the world seed "
            f"({principal_sessions.provisioning_mode})"
        ),
    )
    if ledger_classification_error is not None:
        recorder.check(
            category="subject-resolution",
            name="resolution_path_classifiable",
            condition=False,
            detail=ledger_classification_error,
        )
    # CHAOS-3219 Phase 2 exit: the run measured the CHAOS-3292 preflight path
    # rather than the pre-3292 legacy loop. Recorded as a NAMED, always-present
    # check on every receipt, not asserted once in a fixture -- exit runs #1-#3
    # were all invalidated by this being off, and a run-wide precondition that
    # leaves no per-receipt trace is exactly the kind that goes unnoticed for
    # three runs. A reader of any single receipt can now see which path it
    # measured.
    recorder.check(
        category="run-validity",
        name="measured_wave_3_1_preflight_path",
        condition=org_id in wave_3_1_enabled_orgs,
        detail=(
            f"org_id={org_id!r} wave_3_1 preflight enabled="
            f"{org_id in wave_3_1_enabled_orgs}"
        ),
    )
    # An absent resolution_path is only acceptable when the ledger was
    # genuinely read and genuinely empty. Both broken reasons -- "no run_id,
    # so nothing was read" and "read, non-empty, unclassifiable" -- must never
    # sit in a receipt looking like an honest absence.
    #
    # The detail is derived per reason rather than hardcoded: the first
    # version said "the ledger was never queried" for BOTH, which is false for
    # the unclassifiable case (it was queried, and was not empty) and would
    # have sent a reader hunting the wrong defect -- the same misdescription
    # this check exists to prevent, one layer up.
    if absence_is_a_broken_measurement(resolution_path_absence):
        broken_detail = (
            "the ledger was never queried"
            if resolution_path_absence == ABSENCE_RUN_ID_NOT_OBSERVED
            else "the ledger was queried and was NOT empty, but could not be "
            "classified (see resolution_path_classifiable)"
        )
        recorder.check(
            category="subject-resolution",
            name="resolution_path_measured",
            condition=False,
            detail=(
                f"resolution_path is absent for reason "
                f"{resolution_path_absence!r} -- {broken_detail}, so this "
                "case measured nothing trustworthy about subject resolution"
            ),
        )
    for entry in case.invariants:
        result = evaluate_invariant(entry, context)
        recorder.check(
            category=entry["category"],
            name=entry.get("name", entry["check"]),
            condition=result.passed,
            detail=result.detail,
        )
    recorder.set_resolution_path(resolution_path)
    # Never let a bare `resolution_path: null` stand alone again: the receipt
    # now always says WHY it is absent (or carries None when it is present).
    recorder.set_extra("resolution_path_absence", resolution_path_absence)
    recorder.set_world_digest(_world_digest_pin)
    artifact = recorder.write(_RECEIPTS_DIR / f"{case.id}.json")
    assert artifact["status"] == "passed", (
        f"case {case.id!r} failed one or more invariants -- see "
        f"{_RECEIPTS_DIR / f'{case.id}.json'}"
    )
