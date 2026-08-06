"""Unit coverage for ``scripts.acceptance.corpus.principals`` (CHAOS-3462 B5).

THE DEFECT: the corpus runner logged in once as the acceptance superuser and
ran all 93 active cases as that one identity in that one org, ignoring the
``org_alias`` / ``user_alias`` every case file declares. Cross-tenant and
entitlement cases were therefore not testing what their names claim -- a
cross-tenant probe run BY a superuser IN the primary org proves nothing
about what ``sibling.ordinary`` can see.

WHY A GENUINE LOGIN AND NOT IMPERSONATION: ``/api/v1/admin/impersonate``
exists and is session-backed, but the ``/api/v1/dev/**`` routers do not
honor it. ``_authenticated_user`` -> ``get_current_user`` ->
``get_authenticated_user`` builds ``AuthenticatedUser`` straight from the
JWT claims and never reads the impersonation contextvars (the GraphQL app
DOES read them -- ``api/graphql/authz.py`` -- which is what makes the REST
omission a real gap rather than a shared design). Worse for this lane
specifically: ``router.py`` derives the acting org from ``user.org_id`` (the
raw JWT org) for ``_owned_ids``, the entitlement check, and the readiness/
capability projection -- so an impersonated degraded-readiness or
cross-tenant case would still evaluate against the SUPERUSER's org, quietly
producing green results for the wrong principal. That is a worse failure
than the one B5 is fixing.

So the runner does the one thing that produces an authentically-scoped
token: a real ``POST /api/v1/auth/login`` as that user. The credential comes
from the world seed -- ``fixtures/world.py``'s ``_build_auth_fixture`` hashes
``password_for_alias(alias)`` for every world user at generation time
(CHAOS-3463) -- and the runner imports that same function rather than
keeping a password of its own, so the two sides cannot drift.

An earlier revision had to set a password first (``POST
/api/v1/admin/users/{id}/password``) because world users were seeded with
``password_hash=None`` and could not log in at all. That bridge wrote to a
digest-covered column and is gone.

Everything here runs against test doubles: no stack, no network.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.fixtures.world import password_for_alias
from scripts.acceptance.corpus.case_schema import load_corpus_case
from scripts.acceptance.corpus.principals import (
    SEEDED_PROVISIONING_MARKER,
    CasePrincipal,
    PrincipalDirectory,
    PrincipalError,
    PrincipalSessions,
)
from scripts.acceptance.prepare_ask_dev_acceptance import AcceptanceFailure

_WORLD_DIR = (
    Path(__file__).resolve().parents[2] / "acceptance" / "world" / "ask-dev-world.v1"
)
_REAL_MANIFEST = _WORLD_DIR / "world.json"
_REAL_CORPUS = _WORLD_DIR / "corpus"


def _write_case(path: Path, **overrides: Any) -> Path:
    payload: dict[str, Any] = {
        "id": "planted.case",
        "question": "q",
        "subject_class": "n/a",
        "org_alias": "primary",
        "user_alias": "primary.ordinary",
        "invariants": [{"category": "c", "check": "no_internal_error", "args": {}}],
    }
    payload.update(overrides)
    for key in [k for k, v in payload.items() if v is _ABSENT]:
        del payload[key]
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


class _Absent:
    pass


_ABSENT = _Absent()


class _FakeApi:
    """Records every call; returns canned responses keyed by (method, path)."""

    def __init__(self, responses: dict[tuple[str, str], Any] | None = None) -> None:
        self.base_url = "http://fake"
        self.token: str | None = None
        self.calls: list[tuple[str, str, Any]] = []
        self._responses = responses or {}

    def request(self, method: str, path: str, payload: Any = None) -> Any:
        self.calls.append((method, path, payload))
        key = (method, path)
        if key not in self._responses:
            raise AssertionError(f"unexpected call {method} {path}")
        response = self._responses[key]
        if isinstance(response, Exception):
            raise response
        return response


def _login_response(user_id: str, org_id: str, email: str) -> dict[str, Any]:
    return {
        "access_token": f"token-for-{email}",
        "user": {"id": user_id, "org_id": org_id, "email": email},
    }


# The old ``TestPrincipalPasswordMeetsProductionPolicy`` lived here. It
# asserted the runner's own password constant satisfied production's
# ``validate_password``, because a non-compliant value would have 422'd the
# admin set-password call the runner used to make. That call is gone -- the
# credential is seeded at world generation and only ever LOGGED IN with, and
# login does not re-run the policy -- so the test now guards nothing. The
# derivation's own properties (length, determinism, uniqueness per alias)
# are asserted where they belong, against ``password_for_alias`` itself, in
# ``tests/test_fixtures_world_runner.py``. Keeping a copy here would have
# read as coverage of a rule this module no longer depends on.


class TestPrincipalDirectoryAgainstTheRealWorldManifest:
    @pytest.fixture(scope="class")
    def directory(self) -> PrincipalDirectory:
        return PrincipalDirectory.from_world(_REAL_MANIFEST)

    def test_resolves_the_default_principal(
        self, directory: PrincipalDirectory, tmp_path: Path
    ) -> None:
        case = load_corpus_case(_write_case(tmp_path / "case-a.json"))
        principal = directory.principal_for(case)
        assert principal.user_alias == "primary.ordinary"
        assert principal.email == "ordinary@ask-dev-world-primary.example"
        assert principal.is_superuser is False

    def test_a_non_default_alias_resolves_to_a_different_org_and_identity(
        self, directory: PrincipalDirectory, tmp_path: Path
    ) -> None:
        """The whole point of B5: two cases with different declared aliases
        must not collapse onto one identity in one org."""

        primary = directory.principal_for(
            load_corpus_case(_write_case(tmp_path / "case-p.json"))
        )
        sibling = directory.principal_for(
            load_corpus_case(
                _write_case(
                    tmp_path / "case-s.json",
                    id="planted.sibling",
                    org_alias="sibling",
                    user_alias="sibling.ordinary",
                )
            )
        )
        assert sibling.org_id != primary.org_id
        assert sibling.user_id != primary.user_id
        assert sibling.email != primary.email

    def test_every_real_corpus_case_resolves(
        self, directory: PrincipalDirectory
    ) -> None:
        """The corpus this runner actually drives must resolve end to end --
        an alias typo in any case file is a load-time failure, not a
        surprise on case 87 of a live run."""

        from scripts.acceptance.corpus.case_schema import load_corpus_cases

        cases = load_corpus_cases(_REAL_CORPUS)
        assert cases, "no corpus cases found -- this test would measure nothing"
        for case in cases:
            principal = directory.principal_for(case)
            assert principal.email
            assert principal.org_id
            assert principal.user_id

    def test_the_corpus_exercises_more_than_one_principal(
        self, directory: PrincipalDirectory
    ) -> None:
        """Guards against a future 'simplification' that points every case at
        one alias -- which would silently restore exactly the B5 defect while
        every other test here kept passing."""

        from scripts.acceptance.corpus.case_schema import load_corpus_cases

        aliases = {
            directory.principal_for(case).user_alias
            for case in load_corpus_cases(_REAL_CORPUS)
        }
        assert len(aliases) > 1, (
            f"every corpus case resolves to the same principal {aliases!r} -- "
            "cross-tenant and entitlement families are not being exercised"
        )


class TestPrincipalDirectoryFailsLoud:
    @pytest.fixture(scope="class")
    def directory(self) -> PrincipalDirectory:
        return PrincipalDirectory.from_world(_REAL_MANIFEST)

    def test_unknown_user_alias_raises(
        self, directory: PrincipalDirectory, tmp_path: Path
    ) -> None:
        case = load_corpus_case(
            _write_case(tmp_path / "case-x.json", user_alias="primary.nobody")
        )
        with pytest.raises(PrincipalError, match="primary.nobody"):
            directory.principal_for(case)

    def test_unknown_org_alias_raises(
        self, directory: PrincipalDirectory, tmp_path: Path
    ) -> None:
        case = load_corpus_case(
            _write_case(tmp_path / "case-y.json", org_alias="nowhere")
        )
        with pytest.raises(PrincipalError, match="nowhere"):
            directory.principal_for(case)

    def test_missing_user_alias_raises(
        self, directory: PrincipalDirectory, tmp_path: Path
    ) -> None:
        """A case with no declared principal must NOT quietly inherit the
        superuser -- that silent fallback is the defect, not the remedy."""

        case = load_corpus_case(
            _write_case(tmp_path / "case-z.json", user_alias=_ABSENT)
        )
        with pytest.raises(PrincipalError, match="user_alias"):
            directory.principal_for(case)

    def test_missing_org_alias_raises(
        self, directory: PrincipalDirectory, tmp_path: Path
    ) -> None:
        case = load_corpus_case(
            _write_case(tmp_path / "case-w.json", org_alias=_ABSENT)
        )
        with pytest.raises(PrincipalError, match="org_alias"):
            directory.principal_for(case)

    def test_non_string_alias_raises(
        self, directory: PrincipalDirectory, tmp_path: Path
    ) -> None:
        case = load_corpus_case(_write_case(tmp_path / "case-n.json", user_alias=7))
        with pytest.raises(PrincipalError):
            directory.principal_for(case)

    def test_mismatched_org_and_user_alias_raises(
        self, directory: PrincipalDirectory, tmp_path: Path
    ) -> None:
        """``sibling.ordinary`` belongs to org ``sibling``. A case that pairs
        it with ``org_alias: primary`` is incoherent, and running it in
        EITHER org would silently answer a different question than the case
        asks."""

        case = load_corpus_case(
            _write_case(
                tmp_path / "case-m.json",
                org_alias="primary",
                user_alias="sibling.ordinary",
            )
        )
        with pytest.raises(PrincipalError, match="sibling"):
            directory.principal_for(case)


class TestPrincipalSessions:
    def _principal(self) -> CasePrincipal:
        directory = PrincipalDirectory.from_world(_REAL_MANIFEST)
        return directory.principal_by_alias("primary.ordinary")

    def test_logs_in_as_that_user_and_makes_no_other_call(self) -> None:
        principal = self._principal()
        principal_api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): _login_response(
                    str(principal.user_id), str(principal.org_id), principal.email
                )
            }
        )
        sessions = PrincipalSessions(
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
        )
        session = sessions.session_for_alias("primary.ordinary")

        assert session.api is principal_api
        assert principal_api.token == f"token-for-{principal.email}"
        assert session.org_id == str(principal.org_id)

        # Login is the ONLY call. The set-password bridge that used to run
        # first mutated `password_hash`, a digest-covered column, so a second
        # armed run against the same stack reported drift. Asserting the call
        # list exactly -- rather than just that login happened -- is what
        # would catch a re-introduced provisioning call.
        #
        # This is a claim about CALLS, not about the world being untouched:
        # the login itself stamps `users.last_login_at` server-side. That
        # column is excluded from the world digest (`_VOLATILE_COLUMNS`), so
        # it does not drift -- but "the runner writes nothing" would be an
        # overstatement, and an overstated guarantee is the kind a reader
        # stops checking.
        assert [(c[0], c[1]) for c in principal_api.calls] == [
            ("POST", "/api/v1/auth/login")
        ], "the runner made a call other than the login -- it must only read"

        login = principal_api.calls[0]
        assert login[2]["email"] == principal.email
        assert login[2]["password"] == password_for_alias("primary.ordinary")

    def test_sessions_are_cached_per_alias(self) -> None:
        principal = self._principal()
        principal_api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): _login_response(
                    str(principal.user_id), str(principal.org_id), principal.email
                )
            }
        )
        sessions = PrincipalSessions(
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
        )
        first = sessions.session_for_alias("primary.ordinary")
        second = sessions.session_for_alias("primary.ordinary")
        assert first is second
        assert len(principal_api.calls) == 1, "logged in again per case"

    def test_a_login_returning_the_wrong_org_fails_loud(self) -> None:
        """The single most dangerous silent outcome: a successful login that
        lands in a DIFFERENT org than the case declared. Every downstream
        assertion would then be evaluated against the wrong tenant and could
        pass while proving nothing."""

        principal = self._principal()
        principal_api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): _login_response(
                    str(principal.user_id),
                    "00000000-0000-0000-0000-000000000000",
                    principal.email,
                )
            }
        )
        sessions = PrincipalSessions(
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
        )
        with pytest.raises(PrincipalError, match="org"):
            sessions.session_for_alias("primary.ordinary")

    def test_a_login_returning_a_different_user_fails_loud(self) -> None:
        principal = self._principal()
        principal_api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): _login_response(
                    "11111111-1111-1111-1111-111111111111",
                    str(principal.org_id),
                    principal.email,
                )
            }
        )
        sessions = PrincipalSessions(
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
        )
        with pytest.raises(PrincipalError, match="user"):
            sessions.session_for_alias("primary.ordinary")

    def test_a_login_with_no_token_fails_loud(self) -> None:
        principal_api = _FakeApi({("POST", "/api/v1/auth/login"): {"user": {}}})
        sessions = PrincipalSessions(
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
        )
        with pytest.raises(PrincipalError, match="access_token"):
            sessions.session_for_alias("primary.ordinary")

    def test_a_failed_login_never_falls_back_to_another_session(self) -> None:
        """Rule 2, the guard observed failing: if authenticating a principal
        breaks, the run must STOP. Falling back to the superuser session --
        or handing back a session whose token was never installed -- would
        restore the exact B5 defect while reporting green.

        Retargeted from the admin set-password path, which no longer exists:
        login is now the only way this can fail.
        """

        principal_api = _FakeApi(
            {("POST", "/api/v1/auth/login"): RuntimeError("login returned HTTP 403")}
        )
        sessions = PrincipalSessions(
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
        )
        with pytest.raises(RuntimeError, match="403"):
            sessions.session_for_alias("primary.ordinary")
        assert principal_api.token is None, (
            "a token was installed despite the login failing"
        )

    def test_a_login_with_no_user_id_fails_loud(self) -> None:
        """Codex adversarial round-1, HIGH: the id check used to be skipped
        when the field was absent, so a response carrying a SUPERUSER token
        and only the expected org_id was accepted and installed. Because the
        dev routers trust the JWT's claims, the case would then have run as
        the wrong identity in the right org and reported success."""

        principal = self._principal()
        principal_api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): {
                    "access_token": "a-superuser-token",
                    "user": {"org_id": str(principal.org_id)},
                }
            }
        )
        sessions = PrincipalSessions(
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
        )
        with pytest.raises(PrincipalError, match="no.*'id'"):
            sessions.session_for_alias("primary.ordinary")
        assert principal_api.token is None, "an unverified token was installed"

    def test_a_failed_alias_is_not_retried_for_every_later_case(self) -> None:
        """Codex adversarial round-1, MEDIUM: only successes were cached, so
        a failing login was retried by every subsequent case using that
        alias. With 137 cases on one alias and a rate-limited login, one blip
        became a storm of 429s that buried the real error."""

        principal_api = _FakeApi(
            {("POST", "/api/v1/auth/login"): RuntimeError("login 503")}
        )
        sessions = PrincipalSessions(
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
        )
        with pytest.raises(RuntimeError, match="login 503"):
            sessions.session_for_alias("primary.ordinary")
        for _ in range(4):
            # Subsequent cases get a NEW error chained to the original, not
            # the same object re-raised (which would grow a traceback frame
            # per case and bury the real failure).
            with pytest.raises(PrincipalError, match="already failed") as caught:
                sessions.session_for_alias("primary.ordinary")
            assert isinstance(caught.value.__cause__, RuntimeError)
        assert len(principal_api.calls) == 1, (
            "the login endpoint was called again after the first failure "
            f"({len(principal_api.calls)} times) -- the rate limiter will "
            "bury the original error"
        )

    def test_an_unknown_alias_never_reaches_the_network(self) -> None:
        built: list[_FakeApi] = []

        def api_factory() -> _FakeApi:
            api = _FakeApi({})
            built.append(api)
            return api

        sessions = PrincipalSessions(
            api_factory=api_factory,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
        )
        with pytest.raises(PrincipalError, match="nobody"):
            sessions.session_for_alias("primary.nobody")
        assert built == [], (
            "an unresolvable alias built an API client -- resolution must "
            "fail before anything touches the network"
        )


class TestSeededCredentialsAreTheOnlyPath:
    """Replaces ``TestPasswordBridgeIsOptIn``, whose subject no longer exists.

    That class asserted the opt-in semantics of the temporary
    admin-set-password bridge. CHAOS-3463 seeds credentials at world
    generation, so the bridge, its env flag, and the runner-local password
    constant are all gone -- there is nothing left to opt into, and tests of
    an absent flag would be theatre.

    What carries over is the property that actually mattered and CAN still
    regress: authenticating a principal must not PROVISION anything.
    ``password_hash`` is inside the world digest, so a re-introduced write
    to it would make a second armed run report drift against the same stack.

    Stated exactly, because the loose version ("the runner writes nothing")
    is false: the login stamps ``users.last_login_at``, which is excluded
    from the digest by ``_VOLATILE_COLUMNS``. The guarantee is "no
    provisioning call, no digested column moved" -- not "no writes".
    """

    def _principal(self) -> CasePrincipal:
        return PrincipalDirectory.from_world(_REAL_MANIFEST).principal_by_alias(
            "primary.ordinary"
        )

    def _sessions(self, api: Any) -> PrincipalSessions:
        return PrincipalSessions(
            api_factory=lambda: api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
        )

    def test_authenticating_provisions_nothing(self) -> None:
        """``_FakeApi`` raises on any call it has no canned response for, so
        the login response being the ONLY one registered is what makes an
        admin set-password (or any other provisioning call) fail this test
        rather than pass unnoticed."""

        principal = self._principal()
        api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): _login_response(
                    str(principal.user_id), str(principal.org_id), principal.email
                )
            }
        )
        session = self._sessions(api).session_for_alias("primary.ordinary")

        assert session.api is api
        assert [(c[0], c[1]) for c in api.calls] == [("POST", "/api/v1/auth/login")], (
            "the runner issued a call beyond the login -- a provisioning "
            "call here mutates a digest-covered column and drifts the world"
        )

    def test_the_provisioning_mode_reports_seeded_credentials(self) -> None:
        """The receipt marker is stamped into every artifact. There is one
        credential path now, and the receipts must say WHICH one -- a receipt
        that omits it cannot later be told apart from one produced while the
        bridge still existed."""

        assert (
            self._sessions(_FakeApi({})).provisioning_mode == SEEDED_PROVISIONING_MARKER
        )
        assert SEEDED_PROVISIONING_MARKER == "world-seeded-credentials"

    def test_an_unseeded_world_names_the_remedy(self) -> None:
        """The remedy must reach the operator on the path a REAL unseeded
        account takes.

        Adversarial round 3: this test used to feed ``{"user": {}}`` -- a 200
        body the login route never returns for a bad credential -- so it
        passed while the remedy text sat on an unreachable branch. A world
        snapshot minted before seeded credentials has ``password_hash=None``,
        ``login.py`` answers 401, and the API client RAISES before any
        response-body branch runs. The double is that raise.

        The remedy itself changed with the bridge's removal: the operator is
        now told to re-mint the world snapshot, because provisioning a
        password at run time is no longer an option the runner offers.
        """

        api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): AcceptanceFailure(
                    "POST /api/v1/auth/login returned HTTP 401: "
                    '{"detail":"Invalid credentials"}'
                )
            }
        )
        with pytest.raises(
            PrincipalError, match="mint_ask_dev_world_snapshot"
        ) as caught:
            self._sessions(api).session_for_alias("primary.ordinary")
        assert "401" in str(caught.value), "the underlying cause must survive"
        assert "CHAOS-3463" in str(caught.value)

    def test_a_non_credential_login_failure_is_not_reframed(self) -> None:
        """Retargets ``test_a_bridged_run_does_not_swallow_the_real_login_
        error``, which used the bridge flag as its discriminator.

        A 503 is not a credential problem. Reframing it as one would send an
        operator off to re-mint a world that is perfectly fine, while the
        real fault -- an unwell API -- goes unmentioned. Without this, the
        remedy branch would swallow every login failure indiscriminately.
        """

        api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): AcceptanceFailure(
                    "POST /api/v1/auth/login returned HTTP 503: upstream down"
                )
            }
        )
        with pytest.raises(AcceptanceFailure, match="503") as caught:
            self._sessions(api).session_for_alias("primary.ordinary")
        assert "mint_ask_dev_world_snapshot" not in str(caught.value), (
            "a 503 was reframed as an unseeded-credential problem"
        )

    def test_a_server_error_whose_body_contains_401_is_not_reframed(self) -> None:
        """The failure mode of matching a bare status number.

        ``AcceptanceApi`` interpolates the raw response BODY into its message
        (``... returned HTTP {code}: {detail}``), so a substring test for
        ``"401"`` alone also fires on a 500 carrying ``401`` inside a trace
        id. The classifier keys on ``returned HTTP 401`` -- the producer's
        actual shape -- and this plants exactly the body that would defeat
        the looser version.
        """

        api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): AcceptanceFailure(
                    "POST /api/v1/auth/login returned HTTP 500: "
                    '{"detail":"internal error","trace_id":"req-a401bc93"}'
                )
            }
        )
        with pytest.raises(AcceptanceFailure, match="500") as caught:
            self._sessions(api).session_for_alias("primary.ordinary")
        assert "mint_ask_dev_world_snapshot" not in str(caught.value), (
            "a 500 was reframed as a credential problem because its body "
            "happened to contain '401'"
        )


class TestTheRunnerAuthenticatesWithSeededWorldCredentials:
    """CHAOS-3462 follow-up: log in with the credential the SEEDER wrote.

    A differential oracle across the two sides of the credential contract,
    because no type checker or code index can answer whether they agree.
    The seeding side hashes ``password_for_alias(alias)``
    (``fixtures/world.py::_build_auth_fixture``, CHAOS-3463). This asserts
    the LOGIN side sends a password that verifies against exactly that
    hash -- through the API's OWN ``_verify_password``, and against hashes
    produced by the REAL fixture builder rather than hand-authored ones.

    A runner-local shared constant cannot satisfy this: it is one value for
    every alias, while the seeded hashes are per-alias. That is what makes
    this fail RED before the swap rather than merely sit beside it.
    """

    def _login_password_for(self, alias: str) -> str:
        """Drive the real ``PrincipalSessions`` and capture what it sends."""

        directory = PrincipalDirectory.from_world(_REAL_MANIFEST)
        principal = directory.principal_by_alias(alias)
        api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): _login_response(
                    str(principal.user_id), str(principal.org_id), principal.email
                )
            }
        )
        sessions = PrincipalSessions(
            api_factory=lambda: api,
            directory=directory,
        )
        sessions.session_for_alias(alias)
        login = api.calls[0]
        assert login[1] == "/api/v1/auth/login"
        return login[2]["password"]

    def test_the_login_password_verifies_against_the_seeded_hash(self) -> None:
        from dev_health_ops.api.services.users import _verify_password
        from dev_health_ops.fixtures.world import (
            CORPUS_CONTRACT_USER_ALIASES,
            _build_auth_fixture,
            load_world_manifest,
        )

        manifest = load_world_manifest(_REAL_MANIFEST)
        by_alias = {u["alias"]: u for u in manifest.world["users"]}
        seeded = {u.email: u for u in _build_auth_fixture(manifest)["users"]}

        for alias in CORPUS_CONTRACT_USER_ALIASES:
            sent = self._login_password_for(alias)
            hashed = seeded[by_alias[alias]["email"]].password_hash
            assert _verify_password(sent, hashed), (
                f"the runner logs in as {alias!r} with a password the world "
                "seed's own hash rejects -- every case for that principal "
                "would 401 against a correctly seeded world"
            )

    def test_each_alias_gets_its_own_distinct_credential(self) -> None:
        """Rule 2: without this, a single constant that happened to verify
        for one alias would satisfy the oracle above for that alias alone.
        Credentials are per-alias by derivation, so a shared value is a
        detectable defect rather than a stylistic one."""

        from dev_health_ops.fixtures.world import CORPUS_CONTRACT_USER_ALIASES

        sent = {a: self._login_password_for(a) for a in CORPUS_CONTRACT_USER_ALIASES}
        assert len(set(sent.values())) == len(sent), (
            f"aliases share a login credential: {sent!r} -- the runner is not "
            "deriving per principal"
        )

    def test_the_credential_matches_productions_derivation(self) -> None:
        """The value contract, stated plainly.

        NOTE what this does NOT prove: a runner that hardcoded a copy of
        today's derived string would pass it. That gap is closed by the test
        below, not by this one -- claiming otherwise here would be worse than
        the gap, because a reader who sees "pinned to the function" stops
        checking.
        """

        assert self._login_password_for("primary.ordinary") == password_for_alias(
            "primary.ordinary"
        )

    def test_the_runner_calls_productions_derivation_rather_than_a_copy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Rule 2 applied to the import itself.

        Equality against ``password_for_alias(...)`` cannot distinguish "the
        runner calls production's function" from "someone pasted its current
        output into a constant" -- both agree until the derivation changes,
        which is exactly when the difference would start mattering and
        exactly when nobody would be looking. Rebinding the name the runner
        resolves at call time does distinguish them: only a real call picks
        up the sentinel.
        """

        import scripts.acceptance.corpus.principals as principals_mod

        monkeypatch.setattr(
            principals_mod,
            "password_for_alias",
            lambda alias: f"sentinel-for-{alias}",
        )
        assert (
            self._login_password_for("primary.ordinary")
            == "sentinel-for-primary.ordinary"
        ), "the runner did not go through production's derivation function"
