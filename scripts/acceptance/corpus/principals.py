"""CHAOS-3462 B5: run each corpus case as the principal it declares.

THE DEFECT (CHAOS-3219 Phase 2 exit evidence run, comment 286cfa66): the
corpus runner logged in once as the acceptance superuser and drove all 93
active cases as that single identity in that single org, ignoring the
``org_alias`` / ``user_alias`` every case file carries. Cross-tenant and
entitlement families were therefore not testing what their names claim -- a
cross-tenant probe executed BY a superuser IN the primary org proves nothing
about what ``sibling.ordinary`` is allowed to see, and would go green either
way.

WHY A REAL LOGIN AND NOT IMPERSONATION
--------------------------------------
``POST /api/v1/admin/impersonate`` exists, is session-backed and TTL'd, and
looks like the obvious mechanism. It is the wrong one here, for a reason
that only shows up by tracing the dependency chain rather than reading the
endpoint:

* the ``/api/v1/dev/**`` routers resolve their caller through
  ``_authenticated_user`` -> ``get_current_user`` ->
  ``get_authenticated_user``, which builds ``AuthenticatedUser`` directly
  from the JWT claims and never consults the impersonation contextvars. The
  GraphQL app DOES consult them (``api/graphql/authz.py``), which is what
  makes this a REST-side gap rather than a platform-wide design.
* worse for this lane specifically, ``router.py`` derives the acting org
  from ``user.org_id`` -- the raw JWT org -- for ``_owned_ids``, for the
  ask-dev entitlement check, and for the readiness/capability projection.
  An "impersonated" degraded-readiness or cross-tenant case would therefore
  still be evaluated against the SUPERUSER's org and provider profile, and
  report green for the wrong principal. That is a subtler and more damaging
  failure than the one B5 exists to fix.

So the only mechanism that yields an authentically-scoped token is a genuine
login. World users cannot log in as seeded -- ``fixtures/world.py``'s
``_build_auth_fixture`` writes ``password_hash=None`` for every one of them,
and ``login.py`` explicitly refuses password login for such an account -- so
the superuser first sets a password via ``POST
/api/v1/admin/users/{id}/password`` (which verifies the CALLER's own
password, hence ``admin_password``), and the runner then performs a real
``POST /api/v1/auth/login``. Both are public API calls, keeping the
"public-API-only seeding" philosophy ``prepare_ask_dev_acceptance.py``
already follows -- no direct database writes, no test-only backdoor.

NO SILENT FALLBACK, ANYWHERE
----------------------------
Every failure in this module raises. A case with no declared principal, an
unknown alias, an org/user alias pair that disagrees with ``world.json``, a
login that succeeds but lands in a different org than declared -- all of
them stop the run. Falling back to the superuser session in any of those
cases would restore the exact defect this module removes, while reporting
green.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from dev_health_ops.fixtures.world import derive_id

__all__ = [
    "PRINCIPAL_PASSWORD",
    "CasePrincipal",
    "PrincipalDirectory",
    "PrincipalError",
    "PrincipalSession",
    "PrincipalSessions",
]

#: The password the runner sets on every world principal before logging in
#: as it. Must satisfy production's real ``validate_password`` (>= 12 chars,
#: at least one letter and one digit, not in the common-password list) --
#: ``tests/acceptance/corpus/test_principals.py`` asserts that against the
#: real policy function rather than trusting this comment, because a
#: non-compliant value would 422 every provisioning call and fail every case
#: for a reason unrelated to the case.
PRINCIPAL_PASSWORD = "ask-dev-world-acceptance-4242"

_ADMIN_SET_PASSWORD_PATH = "/api/v1/admin/users/{user_id}/password"
_LOGIN_PATH = "/api/v1/auth/login"


class PrincipalError(Exception):
    """A case's declared principal cannot be resolved or authenticated."""


class _Api(Protocol):
    token: str | None

    def request(
        self, method: str, path: str, payload: Mapping[str, Any] | None = ...
    ) -> Any: ...


@dataclass(frozen=True, slots=True)
class CasePrincipal:
    org_alias: str
    user_alias: str
    email: str
    user_id: uuid.UUID
    org_id: uuid.UUID
    is_superuser: bool


@dataclass(frozen=True, slots=True)
class PrincipalSession:
    """An authenticated API client acting as one world principal."""

    api: Any
    principal: CasePrincipal
    org_id: str


class PrincipalDirectory:
    """``world.json``-backed resolution of case aliases to real principals.

    Reads the manifest directly rather than going through ``WorldManifest``:
    that class also requires ``subjects.json`` / ``sources.json``, which this
    lookup has no use for. The id derivation is NOT reimplemented -- it
    imports production's own :func:`derive_id`, so a runner-side id can
    never drift from the id the seeder actually wrote.
    """

    def __init__(self, *, master_seed: int, orgs: list[Any], users: list[Any]) -> None:
        self._master_seed = master_seed
        self._orgs = {org["alias"]: org for org in orgs}
        self._users = {user["alias"]: user for user in users}

    @classmethod
    def from_world(cls, manifest_path: Path) -> PrincipalDirectory:
        import json

        try:
            world = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise PrincipalError(
                f"cannot read the world manifest at {manifest_path}: {exc}"
            ) from exc
        try:
            return cls(
                master_seed=int(world["master_seed"]),
                orgs=list(world["orgs"]),
                users=list(world["users"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise PrincipalError(
                f"{manifest_path} is missing master_seed/orgs/users: {exc}"
            ) from exc

    def principal_by_alias(
        self, user_alias: str, *, org_alias: str | None = None
    ) -> CasePrincipal:
        """Resolve one principal, or raise.

        When ``org_alias`` is supplied it must AGREE with the org the user
        belongs to in ``world.json``. A case pairing ``sibling.ordinary``
        with ``org_alias: primary`` is incoherent, and honoring either half
        of it would silently answer a different question than the case asks.
        """

        user = self._users.get(user_alias)
        if user is None:
            raise PrincipalError(
                f"world.json declares no user with alias {user_alias!r} "
                f"(known: {sorted(self._users)!r})"
            )
        declared_org = user["org_alias"]
        if org_alias is not None and org_alias != declared_org:
            raise PrincipalError(
                f"user alias {user_alias!r} belongs to org {declared_org!r} in "
                f"world.json, but the case declares org_alias {org_alias!r} -- "
                "refusing to guess which half is right"
            )
        org = self._orgs.get(declared_org)
        if org is None:
            raise PrincipalError(
                f"world.json declares no org with alias {declared_org!r} "
                f"(known: {sorted(self._orgs)!r})"
            )
        return CasePrincipal(
            org_alias=declared_org,
            user_alias=user_alias,
            email=user["email"],
            user_id=derive_id(self._master_seed, user["id_seed"]),
            org_id=derive_id(self._master_seed, org["id_seed"]),
            is_superuser=bool(user.get("is_superuser", False)),
        )

    def principal_for(self, case: Any) -> CasePrincipal:
        """Resolve the principal a corpus case declares, or raise.

        ``org_alias`` / ``user_alias`` live on the raw case mapping rather
        than on ``CorpusCase``: ``case_schema`` deliberately never rejects an
        unknown extra field, so Lane 2b can add bookkeeping without a loader
        change. Requiring them HERE, at the point of use, keeps that contract
        intact while still making a case with no declared principal a hard
        failure -- it must never quietly inherit the superuser.
        """

        raw = getattr(case, "raw", case)
        case_id = getattr(case, "id", raw.get("id", "<unknown>"))
        org_alias = raw.get("org_alias")
        user_alias = raw.get("user_alias")
        for name, value in (("org_alias", org_alias), ("user_alias", user_alias)):
            if not isinstance(value, str) or not value.strip():
                raise PrincipalError(
                    f"case {case_id!r} has no usable {name!r} (got {value!r}) -- "
                    "a case with no declared principal must fail loud, never "
                    "silently run as the acceptance superuser"
                )
        return self.principal_by_alias(user_alias, org_alias=org_alias)


class PrincipalSessions:
    """Lazily provisions and caches one authenticated session per alias.

    Cached because provisioning is two HTTP calls and the corpus runs ~137
    cases as the same ``primary.ordinary`` principal; re-logging-in per case
    would add ~274 pointless calls and, worse, put the admin
    set-password rate limit (``ADMIN_PASSWORD_LIMIT``) in the path of a
    normal run.
    """

    def __init__(
        self,
        *,
        admin_api: Any,
        admin_password: str,
        api_factory: Callable[[], Any],
        directory: PrincipalDirectory,
    ) -> None:
        self._admin_api = admin_api
        self._admin_password = admin_password
        self._api_factory = api_factory
        self._directory = directory
        self._sessions: dict[str, PrincipalSession] = {}
        self._failures: dict[str, BaseException] = {}

    def session_for(self, case: Any) -> PrincipalSession:
        principal = self._directory.principal_for(case)
        return self._session(principal)

    def session_for_alias(
        self, user_alias: str, *, org_alias: str | None = None
    ) -> PrincipalSession:
        principal = self._directory.principal_by_alias(user_alias, org_alias=org_alias)
        return self._session(principal)

    def _session(self, principal: CasePrincipal) -> PrincipalSession:
        cached = self._sessions.get(principal.user_alias)
        if cached is not None:
            return cached
        # NEGATIVE caching, deliberately (Codex adversarial round-1, MEDIUM):
        # only successes were cached before, so a set-password that succeeded
        # followed by a transient login failure left nothing cached -- and
        # every later case using that alias re-ran the admin password
        # mutation. With 137 cases on `primary.ordinary` and the endpoint
        # rate-limited to a handful per hour, one login blip turned into a
        # storm of 429s that both buried the original error and kept mutating
        # shared world state. The first failure is now final for that alias
        # and is re-raised verbatim, so the run fails on the REAL cause.
        previous = self._failures.get(principal.user_alias)
        if previous is not None:
            raise previous
        try:
            session = self._authenticate(principal)
        except BaseException as exc:
            self._failures[principal.user_alias] = exc
            raise
        self._sessions[principal.user_alias] = session
        return session

    def _authenticate(self, principal: CasePrincipal) -> PrincipalSession:
        # Step 1: give the world-seeded user a password. Any failure here
        # propagates untouched -- there is deliberately no except/fallback.
        self._admin_api.request(
            "POST",
            _ADMIN_SET_PASSWORD_PATH.format(user_id=principal.user_id),
            {
                "admin_password": self._admin_password,
                "password": PRINCIPAL_PASSWORD,
            },
        )

        # Step 2: a genuine login, so the JWT's own sub/org_id/role are the
        # principal's -- which is what /api/v1/dev/** actually reads.
        api = self._api_factory()
        login = api.request(
            "POST",
            _LOGIN_PATH,
            {"email": principal.email, "password": PRINCIPAL_PASSWORD},
        )
        if not isinstance(login, Mapping):
            raise PrincipalError(
                f"login as {principal.user_alias!r} returned "
                f"{type(login).__name__}, expected an object"
            )
        token = login.get("access_token")
        if not token:
            raise PrincipalError(
                f"login as {principal.user_alias!r} ({principal.email}) returned "
                "no access_token"
            )
        user = login.get("user")
        if not isinstance(user, Mapping):
            raise PrincipalError(
                f"login as {principal.user_alias!r} returned no user object"
            )

        org_id = user.get("org_id")
        if str(org_id) != str(principal.org_id):
            raise PrincipalError(
                f"login as {principal.user_alias!r} landed in org {org_id!r}, but "
                f"world.json derives org {principal.org_id} for org_alias "
                f"{principal.org_alias!r}. Every downstream assertion would be "
                "evaluated against the wrong tenant and could pass while "
                "proving nothing."
            )
        # REQUIRED, not best-effort (Codex adversarial round-1, HIGH): an
        # earlier version skipped this check when `id` was absent. Because
        # the dev routers trust the JWT's own claims, a response carrying a
        # superuser token and only the expected org_id would have been
        # accepted and installed -- the case would then run as the WRONG
        # identity in the RIGHT org and report success. Absence of the field
        # is unverifiable, which is exactly the state that must fail.
        returned_user_id = user.get("id")
        if returned_user_id is None:
            raise PrincipalError(
                f"login as {principal.email!r} returned a user object with no "
                f"'id' -- cannot confirm the token belongs to alias "
                f"{principal.user_alias!r} rather than some other account in "
                "the same org, and an unverifiable identity must never be "
                "accepted"
            )
        if str(returned_user_id) != str(principal.user_id):
            raise PrincipalError(
                f"login as {principal.email!r} returned user id "
                f"{returned_user_id!r}, but world.json derives "
                f"{principal.user_id} for alias {principal.user_alias!r} -- the "
                "email resolved to a different account than the one whose "
                "password was just set"
            )

        api.token = token
        return PrincipalSession(api=api, principal=principal, org_id=str(org_id))
