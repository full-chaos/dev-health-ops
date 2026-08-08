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


class _MultiPrincipalApi:
    """Like ``_FakeApi`` but answers a login with the principal that asked.

    Needed because every login hits the same ``(POST, /api/v1/auth/login)``
    key, so a single canned response cannot distinguish principals -- and
    "cannot distinguish principals" is precisely the defect the isolation
    test below exists to catch.
    """

    def __init__(self, directory: PrincipalDirectory, aliases: list[str]) -> None:
        self.base_url = "http://fake"
        self.token: str | None = None
        self.calls: list[tuple[str, str, Any]] = []
        self._by_email = {
            directory.principal_by_alias(a).email: directory.principal_by_alias(a)
            for a in aliases
        }

    def request(self, method: str, path: str, payload: Any = None) -> Any:
        self.calls.append((method, path, payload))
        if (method, path) != ("POST", "/api/v1/auth/login"):
            raise AssertionError(f"unexpected call {method} {path}")
        principal = self._by_email[payload["email"]]
        return _login_response(
            str(principal.user_id), str(principal.org_id), principal.email
        )

    @property
    def login_count(self) -> int:
        return sum(1 for c in self.calls if c[1] == "/api/v1/auth/login")


class TestLoginBudget:
    """CHAOS-3529 filed believing this runner logs in once per CASE, and
    prescribed per-principal session reuse as the fix. That reuse has existed
    since Lane 2a -- ``PrincipalSessions``' own docstring says it exists so
    re-login does not "put the login rate limiter in the path of a normal
    run". The premise was wrong, but nothing ASSERTED the real behaviour at
    corpus scale, which is how a load-bearing documented property becomes
    folklore that a ticket can contradict.

    The pre-existing ``test_sessions_are_cached_per_alias`` pins two calls on
    ONE alias. That is not the property that matters: the property is that a
    whole corpus spread over a principal POOL costs one login per POOL
    MEMBER, because the per-IP limiter is 20 per 15 minutes and the corpus is
    ~90 active cases.
    """

    ALIASES = ["primary.ordinary", "primary.ordinary-2", "primary.ordinary-3"]

    def _sessions(self) -> tuple[Any, Any]:
        directory = PrincipalDirectory.from_world(_REAL_MANIFEST)
        api = _MultiPrincipalApi(directory, self.ALIASES)
        return api, PrincipalSessions(api_factory=lambda: api, directory=directory)

    def test_n_cases_over_m_principals_costs_exactly_m_logins(self) -> None:
        api, sessions = self._sessions()
        # 90 "cases" round-robin over the pool, the real corpus shape.
        for index in range(90):
            sessions.session_for_alias(self.ALIASES[index % len(self.ALIASES)])

        assert api.login_count == len(self.ALIASES), (
            f"90 cases over {len(self.ALIASES)} principals cost "
            f"{api.login_count} logins; per-principal reuse is not holding, "
            "and the per-IP limiter (20/15min) is now in the path of a "
            "normal corpus run"
        )
        assert sessions.total_logins == len(self.ALIASES)
        for alias in self.ALIASES:
            assert sessions.login_count(alias) == 1

    def test_the_budget_scales_with_principals_not_cases(self) -> None:
        """The discriminator. If reuse silently broke, login count would
        track CASE count -- so run the same pool at two very different case
        counts and require the cost not to move."""

        api_small, sessions_small = self._sessions()
        for index in range(6):
            sessions_small.session_for_alias(self.ALIASES[index % len(self.ALIASES)])
        api_large, sessions_large = self._sessions()
        for index in range(300):
            sessions_large.session_for_alias(self.ALIASES[index % len(self.ALIASES)])

        assert api_small.login_count == api_large.login_count == len(self.ALIASES)


class TestCrossPrincipalIsolation:
    """The security-shaped one. A cache keyed wrongly -- on org, on a
    constant, on the first principal seen -- would hand principal A's bearer
    token to principal B. Every cross-tenant case in the corpus would then be
    evaluated as the wrong identity and could PASS while proving the
    opposite of what it claims.
    """

    ALIASES = ["primary.ordinary", "primary.ordinary-2", "sibling.ordinary"]

    def test_each_principal_gets_its_own_session_and_never_anothers(self) -> None:
        directory = PrincipalDirectory.from_world(_REAL_MANIFEST)
        api = _MultiPrincipalApi(directory, self.ALIASES)
        sessions = PrincipalSessions(api_factory=lambda: api, directory=directory)

        got = {alias: sessions.session_for_alias(alias) for alias in self.ALIASES}

        # Distinct session objects...
        assert len({id(s) for s in got.values()}) == len(self.ALIASES)
        # ...each carrying ITS OWN identity, which is the half that matters:
        # two distinct objects both holding principal A would still be a leak.
        for alias, session in got.items():
            expected = directory.principal_by_alias(alias)
            assert session.principal.user_alias == alias
            assert session.principal.email == expected.email
            assert session.org_id == str(expected.org_id)

        # And a cross-ORG pair must not collapse: sibling.ordinary is a
        # different tenant entirely.
        assert got["primary.ordinary"].org_id != got["sibling.ordinary"].org_id

    def test_repeated_interleaved_access_never_swaps_identities(self) -> None:
        """A cache that returned the most-recently-authenticated session
        regardless of alias would pass a single-pass test and fail here."""

        directory = PrincipalDirectory.from_world(_REAL_MANIFEST)
        api = _MultiPrincipalApi(directory, self.ALIASES)
        sessions = PrincipalSessions(api_factory=lambda: api, directory=directory)

        for _ in range(10):
            for alias in self.ALIASES:
                assert sessions.session_for_alias(alias).principal.user_alias == alias


class TestSessionExpiryStory:
    """Access tokens carry a 60-minute TTL and sessions are cached for the
    whole run, so a run that outlives its token starts failing with 401s that
    look nothing like their cause.

    MEASURED rather than assumed: the 2026-08-07 armed run took **6.8
    minutes** wall across 139 receipts (04:55:03 -> 05:01:48) against that
    60-minute TTL -- roughly 8.8x headroom. So this is deliberately a cheap,
    explicit invalidation rather than background refresh machinery: it covers
    a slow or wedged run, not the normal one. Building a refresh loop for a
    scenario 8.8x away would be speculation dressed as rigour.
    """

    ALIASES = ["primary.ordinary", "primary.ordinary-2"]

    def _sessions(self) -> tuple[Any, Any]:
        directory = PrincipalDirectory.from_world(_REAL_MANIFEST)
        api = _MultiPrincipalApi(directory, self.ALIASES)
        return api, PrincipalSessions(api_factory=lambda: api, directory=directory)

    def test_invalidate_forces_exactly_one_relogin_and_records_it(self) -> None:
        api, sessions = self._sessions()
        first = sessions.session_for_alias("primary.ordinary")
        assert sessions.invalidate("primary.ordinary", reason="401 from api") is True

        second = sessions.session_for_alias("primary.ordinary")
        assert second is not first, "invalidate did not force a re-login"
        assert api.login_count == 2
        assert sessions.login_count("primary.ordinary") == 2
        # Recorded, not silent: a refresh nobody can see is indistinguishable
        # from never having needed one.
        assert sessions.relogin_events == (("primary.ordinary", "401 from api"),)

    def test_the_refreshed_session_is_still_the_same_principal(self) -> None:
        """A re-login must not become an identity change."""

        _api, sessions = self._sessions()
        before = sessions.session_for_alias("primary.ordinary")
        sessions.invalidate("primary.ordinary", reason="expired")
        after = sessions.session_for_alias("primary.ordinary")
        assert after.principal == before.principal
        assert after.org_id == before.org_id

    def test_invalidating_an_uncached_alias_reports_false(self) -> None:
        """So a caller cannot mistake 'nothing was cached' for 'a refresh
        happened' -- and cannot log a refresh that never occurred."""

        _api, sessions = self._sessions()
        assert sessions.invalidate("primary.ordinary", reason="expired") is False
        assert sessions.relogin_events == ()

    def test_invalidate_does_not_disturb_other_principals(self) -> None:
        api, sessions = self._sessions()
        kept = sessions.session_for_alias("primary.ordinary-2")
        sessions.session_for_alias("primary.ordinary")
        sessions.invalidate("primary.ordinary", reason="expired")

        # Re-request BOTH: the invalidated one must re-login, the other
        # must not. (An earlier draft of this test asserted 3 logins without
        # ever re-requesting the invalidated alias, so it was asserting
        # arithmetic rather than behaviour -- caught by it failing 2 != 3.)
        assert sessions.session_for_alias("primary.ordinary-2") is kept
        sessions.session_for_alias("primary.ordinary")

        assert sessions.login_count("primary.ordinary-2") == 1, (
            "untouched alias re-logged in"
        )
        assert sessions.login_count("primary.ordinary") == 2
        assert api.login_count == 3  # 2 initial + 1 re-login, none for -2
