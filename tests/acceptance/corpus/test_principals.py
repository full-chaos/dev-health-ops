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
token: the superuser sets a password on the target world-fixture user
(``POST /api/v1/admin/users/{id}/password``, which requires the caller's own
password) and then performs a real ``POST /api/v1/auth/login`` as that user.
This is necessary because ``fixtures/world.py``'s ``_build_auth_fixture``
seeds every world user with ``password_hash=None``, and ``login.py`` refuses
password login for such an account -- there is no password to use until one
is set.

Everything here runs against test doubles: no stack, no network.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.api.utils.password_policy import validate_password
from scripts.acceptance.corpus.case_schema import load_corpus_case
from scripts.acceptance.corpus.principals import (
    BRIDGED_PROVISIONING_MARKER,
    PRINCIPAL_PASSWORD,
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


class TestPrincipalPasswordMeetsProductionPolicy:
    """Rule 1: assert the state the system must reach, not that we wrote a
    plausible-looking constant. A password that fails ``validate_password``
    would make every principal provisioning call 422 at runtime -- and the
    runner would then fail every case for a reason that has nothing to do
    with the case."""

    def test_password_passes_the_real_policy(self) -> None:
        assert validate_password(PRINCIPAL_PASSWORD) == []


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

    def test_provisions_a_password_then_logs_in_as_that_user(self) -> None:
        principal = self._principal()
        admin = _FakeApi(
            {
                ("POST", f"/api/v1/admin/users/{principal.user_id}/password"): {
                    "ok": True
                }
            }
        )
        principal_api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): _login_response(
                    str(principal.user_id), str(principal.org_id), principal.email
                )
            }
        )
        sessions = PrincipalSessions(
            admin_api=admin,
            admin_password="devhealth123",
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
            allow_password_bridge=True,
        )
        session = sessions.session_for_alias("primary.ordinary")

        assert session.api is principal_api
        assert principal_api.token == f"token-for-{principal.email}"
        assert session.org_id == str(principal.org_id)

        set_password = admin.calls[0]
        assert set_password[0] == "POST"
        assert str(principal.user_id) in set_password[1]
        assert set_password[2]["admin_password"] == "devhealth123"
        assert set_password[2]["password"] == PRINCIPAL_PASSWORD

        login = principal_api.calls[0]
        assert login[1] == "/api/v1/auth/login"
        assert login[2]["email"] == principal.email
        assert login[2]["password"] == PRINCIPAL_PASSWORD

    def test_sessions_are_cached_per_alias(self) -> None:
        principal = self._principal()
        admin = _FakeApi(
            {
                ("POST", f"/api/v1/admin/users/{principal.user_id}/password"): {
                    "ok": True
                }
            }
        )
        principal_api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): _login_response(
                    str(principal.user_id), str(principal.org_id), principal.email
                )
            }
        )
        sessions = PrincipalSessions(
            admin_api=admin,
            admin_password="pw",
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
            allow_password_bridge=True,
        )
        first = sessions.session_for_alias("primary.ordinary")
        second = sessions.session_for_alias("primary.ordinary")
        assert first is second
        assert len(admin.calls) == 1, "password was re-provisioned per case"
        assert len(principal_api.calls) == 1, "logged in again per case"

    def test_a_login_returning_the_wrong_org_fails_loud(self) -> None:
        """The single most dangerous silent outcome: a successful login that
        lands in a DIFFERENT org than the case declared. Every downstream
        assertion would then be evaluated against the wrong tenant and could
        pass while proving nothing."""

        principal = self._principal()
        admin = _FakeApi(
            {
                ("POST", f"/api/v1/admin/users/{principal.user_id}/password"): {
                    "ok": True
                }
            }
        )
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
            admin_api=admin,
            admin_password="pw",
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
            allow_password_bridge=True,
        )
        with pytest.raises(PrincipalError, match="org"):
            sessions.session_for_alias("primary.ordinary")

    def test_a_login_returning_a_different_user_fails_loud(self) -> None:
        principal = self._principal()
        admin = _FakeApi(
            {
                ("POST", f"/api/v1/admin/users/{principal.user_id}/password"): {
                    "ok": True
                }
            }
        )
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
            admin_api=admin,
            admin_password="pw",
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
            allow_password_bridge=True,
        )
        with pytest.raises(PrincipalError, match="user"):
            sessions.session_for_alias("primary.ordinary")

    def test_a_login_with_no_token_fails_loud(self) -> None:
        principal = self._principal()
        admin = _FakeApi(
            {
                ("POST", f"/api/v1/admin/users/{principal.user_id}/password"): {
                    "ok": True
                }
            }
        )
        principal_api = _FakeApi({("POST", "/api/v1/auth/login"): {"user": {}}})
        sessions = PrincipalSessions(
            admin_api=admin,
            admin_password="pw",
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
            allow_password_bridge=True,
        )
        with pytest.raises(PrincipalError, match="access_token"):
            sessions.session_for_alias("primary.ordinary")

    def test_a_failed_provisioning_never_falls_back_to_the_admin_session(self) -> None:
        """Rule 2, the guard observed failing: if principal provisioning
        breaks, the run must STOP. Falling back to the superuser session
        would restore the exact B5 defect while reporting green."""

        principal = self._principal()
        boom = RuntimeError("admin set-password returned HTTP 403")
        admin = _FakeApi(
            {("POST", f"/api/v1/admin/users/{principal.user_id}/password"): boom}
        )
        principal_api = _FakeApi({})
        sessions = PrincipalSessions(
            admin_api=admin,
            admin_password="pw",
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
            allow_password_bridge=True,
        )
        with pytest.raises(RuntimeError, match="403"):
            sessions.session_for_alias("primary.ordinary")
        assert principal_api.calls == [], (
            "attempted a login despite provisioning failing"
        )

    def test_a_login_with_no_user_id_fails_loud(self) -> None:
        """Codex adversarial round-1, HIGH: the id check used to be skipped
        when the field was absent, so a response carrying a SUPERUSER token
        and only the expected org_id was accepted and installed. Because the
        dev routers trust the JWT's claims, the case would then have run as
        the wrong identity in the right org and reported success."""

        principal = self._principal()
        admin = _FakeApi(
            {
                ("POST", f"/api/v1/admin/users/{principal.user_id}/password"): {
                    "ok": True
                }
            }
        )
        principal_api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): {
                    "access_token": "a-superuser-token",
                    "user": {"org_id": str(principal.org_id)},
                }
            }
        )
        sessions = PrincipalSessions(
            admin_api=admin,
            admin_password="pw",
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
            allow_password_bridge=True,
        )
        with pytest.raises(PrincipalError, match="no.*'id'"):
            sessions.session_for_alias("primary.ordinary")
        assert principal_api.token is None, "an unverified token was installed"

    def test_a_failed_alias_is_not_retried_for_every_later_case(self) -> None:
        """Codex adversarial round-1, MEDIUM: only successes were cached, so
        a set-password that succeeded followed by a failing login made every
        subsequent case using that alias re-run the admin password mutation.
        With 137 cases on one alias and a per-hour rate limit, one blip
        became a storm of 429s that buried the real error."""

        principal = self._principal()
        admin = _FakeApi(
            {
                ("POST", f"/api/v1/admin/users/{principal.user_id}/password"): {
                    "ok": True
                }
            }
        )
        principal_api = _FakeApi(
            {("POST", "/api/v1/auth/login"): RuntimeError("login 503")}
        )
        sessions = PrincipalSessions(
            admin_api=admin,
            admin_password="pw",
            api_factory=lambda: principal_api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
            allow_password_bridge=True,
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
        assert len(admin.calls) == 1, (
            "the admin password endpoint was called again after the first "
            f"failure ({len(admin.calls)} times) -- shared state keeps being "
            "mutated and the rate limit will bury the original error"
        )

    def test_an_unknown_alias_never_reaches_the_network(self) -> None:
        admin = _FakeApi({})
        sessions = PrincipalSessions(
            admin_api=admin,
            admin_password="pw",
            api_factory=lambda: _FakeApi({}),
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
            allow_password_bridge=True,
        )
        with pytest.raises(PrincipalError, match="nobody"):
            sessions.session_for_alias("primary.nobody")
        assert admin.calls == []


class TestPasswordBridgeIsOptIn:
    """Team-lead ruling 2026-08-06: credentials are seeded at world-generation
    time and ``password_hash`` STAYS in the world digest.

    Setting a password at run time therefore MUTATES a digest-covered column.
    That is a real cost, so it is opt-in and it marks the receipts: a bridged
    run is not a clean run and must not be mistaken for one. The bridge is
    temporary -- it goes away when CHAOS-3463 lands seeded credentials.
    """

    def _principal(self) -> CasePrincipal:
        return PrincipalDirectory.from_world(_REAL_MANIFEST).principal_by_alias(
            "primary.ordinary"
        )

    def _sessions(self, *, bridge: bool, admin: Any, api: Any) -> PrincipalSessions:
        return PrincipalSessions(
            admin_api=admin,
            admin_password="pw",
            api_factory=lambda: api,
            directory=PrincipalDirectory.from_world(_REAL_MANIFEST),
            allow_password_bridge=bridge,
        )

    def test_without_the_opt_in_no_password_is_ever_set(self) -> None:
        principal = self._principal()
        admin = _FakeApi({})  # any call raises AssertionError
        api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): _login_response(
                    str(principal.user_id), str(principal.org_id), principal.email
                )
            }
        )
        session = self._sessions(bridge=False, admin=admin, api=api).session_for_alias(
            "primary.ordinary"
        )
        assert admin.calls == [], "a digest-covered column was mutated without opt-in"
        assert session.api is api

    def test_with_the_opt_in_the_password_is_set(self) -> None:
        principal = self._principal()
        admin = _FakeApi(
            {
                ("POST", f"/api/v1/admin/users/{principal.user_id}/password"): {
                    "ok": True
                }
            }
        )
        api = _FakeApi(
            {
                ("POST", "/api/v1/auth/login"): _login_response(
                    str(principal.user_id), str(principal.org_id), principal.email
                )
            }
        )
        self._sessions(bridge=True, admin=admin, api=api).session_for_alias(
            "primary.ordinary"
        )
        assert len(admin.calls) == 1

    def test_the_provisioning_mode_distinguishes_the_two(self) -> None:
        """The receipt marker is the whole point: without it, artifacts from a
        bridged run are indistinguishable from artifacts produced against
        properly seeded credentials."""

        fake = _FakeApi({})
        assert (
            self._sessions(bridge=True, admin=fake, api=fake).provisioning_mode
            == BRIDGED_PROVISIONING_MARKER
        )
        assert (
            self._sessions(bridge=False, admin=fake, api=fake).provisioning_mode
            == SEEDED_PROVISIONING_MARKER
        )
        assert BRIDGED_PROVISIONING_MARKER != SEEDED_PROVISIONING_MARKER

    def test_an_unseeded_credential_without_the_bridge_names_the_remedy(self) -> None:
        """The remedy must reach the operator on the path a REAL unseeded
        account takes.

        Adversarial round 3: this test used to feed ``{"user": {}}`` -- a 200
        body the login route never returns for a bad credential -- so it
        passed while the remedy text sat on an unreachable branch. An
        unseeded world user has ``password_hash=None``, ``login.py`` answers
        401, and the API client RAISES before any response-body branch runs.
        The double is now that raise.
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
            PrincipalError, match="ASK_DEV_ACCEPTANCE_ALLOW_PASSWORD_BRIDGE"
        ) as caught:
            self._sessions(bridge=False, admin=_FakeApi({}), api=api).session_for_alias(
                "primary.ordinary"
            )
        assert "401" in str(caught.value), "the underlying cause must survive"

    def test_a_bridged_run_does_not_swallow_the_real_login_error(self) -> None:
        """With the bridge ON, a 401 means something else is wrong and the
        original error must propagate unmasked -- the remedy text would be
        actively misleading there."""

        principal = self._principal()
        admin = _FakeApi(
            {
                (
                    "POST",
                    f"/api/v1/admin/users/{principal.user_id}/password",
                ): {"ok": True}
            }
        )
        api = _FakeApi(
            {("POST", "/api/v1/auth/login"): AcceptanceFailure("HTTP 401 boom")}
        )
        with pytest.raises(AcceptanceFailure, match="boom"):
            self._sessions(bridge=True, admin=admin, api=api).session_for_alias(
                "primary.ordinary"
            )
