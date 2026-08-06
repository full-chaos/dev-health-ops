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
``POST /api/v1/auth/login``, and the credential it uses comes from the world
seed: ``fixtures/world.py::_build_auth_fixture`` hashes
``password_for_alias(alias)`` for EVERY world user at generation time
(CHAOS-3463). This module imports that same function rather than holding a
password of its own, so the seeding side and the login side cannot disagree
-- there is one derivation, not two copies that agree until one is edited.

An earlier revision of this runner could not rely on that: world users had
``password_hash=None``, so it set a password through ``POST
/api/v1/admin/users/{id}/password`` behind an explicit opt-in flag. That
bridge mutated ``password_hash``, a digest-covered column, so a second armed
run against the same stack reported users-digest drift. It is gone now that
credentials are seeded, along with the flag and the runner-local password
constant it needed.

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

from dev_health_ops.fixtures.world import derive_id, password_for_alias

__all__ = [
    "SEEDED_PROVISIONING_MARKER",
    "CasePrincipal",
    "PrincipalDirectory",
    "PrincipalError",
    "PrincipalSession",
    "PrincipalSessions",
]

#: Stamped into every receipt (see the runner's ``provisioning_mode`` check)
#: so the artifacts themselves say where the credentials came from. Kept as a
#: named marker now that the bridge is gone, because a receipt is evidence
#: and a reader keys on this field to know the run authenticated against
#: seeded world credentials rather than anything provisioned at run time.
SEEDED_PROVISIONING_MARKER = "world-seeded-credentials"

_LOGIN_PATH = "/api/v1/auth/login"


class PrincipalError(Exception):
    """A case's declared principal cannot be resolved or authenticated."""


def _is_credential_rejection(exc: BaseException) -> bool:
    """Whether a failed login means "wrong credential" or "the API is unwell".

    Deliberately narrow, and matched against the shape its producer actually
    emits rather than a bare status number. ``AcceptanceApi.request`` raises
    ``AcceptanceFailure(f"{method} {path} returned HTTP {exc.code}: {detail}")``
    -- and ``detail`` there is the raw RESPONSE BODY. Searching for ``"401"``
    anywhere in that string would therefore also match a 500 whose body
    happens to carry ``401`` inside a trace id or correlation token, and would
    reframe an unwell API as an unseeded credential -- sending an operator off
    to re-mint a world that is perfectly fine.

    Anything that is not a credential rejection -- 5xx, 429, a transport
    error -- must keep its own error rather than be reframed as one.
    """

    text = str(exc)
    return "returned HTTP 401" in text or "Invalid credentials" in text


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

    Cached because the corpus runs ~137 cases as the same
    ``primary.ordinary`` principal; re-logging-in per case would add ~137
    pointless round trips and put the login rate limiter in the path of a
    normal run.

    KNOWN LIMIT, stated rather than discovered later: the access token has a
    60-minute TTL (``api/services/auth.py``) and these sessions are cached
    for the whole run with no refresh and no 401 retry. A corpus run that
    takes longer than an hour will therefore start failing with 401s near
    the end -- loudly, not silently, but with an error that looks nothing
    like its cause. If a real armed run approaches that duration, the fix is
    a re-login on 401 keyed by alias; it is not built here because no
    end-to-end run has yet been observed (the Phase 2 exit run was blocked),
    so its real duration is unknown and building against a guess would be
    speculative.
    """

    def __init__(
        self,
        *,
        api_factory: Callable[[], Any],
        directory: PrincipalDirectory,
    ) -> None:
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
        # only successes were cached before, so a transient login failure left
        # nothing cached and every later case using that alias retried it.
        # With 137 cases on `primary.ordinary` against a rate-limited login,
        # one blip turned into a storm of 429s that buried the original error.
        # The first failure is now final for that alias and is re-raised
        # verbatim, so the run fails on the REAL cause.
        previous = self._failures.get(principal.user_alias)
        if previous is not None:
            # A NEW exception chained to the original, rather than re-raising
            # the same object: re-raising one instance ~90 times accumulates a
            # frame on its traceback each time, and the report for the last
            # case buries the first (real) failure under them.
            raise PrincipalError(
                f"principal {principal.user_alias!r} already failed to "
                f"authenticate earlier in this run and is not retried -- see "
                f"the original error: {previous!r}"
            ) from previous
        try:
            session = self._authenticate(principal)
        except BaseException as exc:
            self._failures[principal.user_alias] = exc
            raise
        self._sessions[principal.user_alias] = session
        return session

    @property
    def provisioning_mode(self) -> str:
        """Where this run's credentials came from -- stamped into receipts.

        One value now that the admin-set-password bridge is gone: the runner
        has exactly one credential path, the world seed. The property stays
        so receipts keep the field readers already key on, and so a future
        second path has an obvious place to declare itself.
        """

        return SEEDED_PROVISIONING_MARKER

    def _authenticate(self, principal: CasePrincipal) -> PrincipalSession:
        # A genuine login, so the JWT's own sub/org_id/role are the
        # principal's -- which is what /api/v1/dev/** actually reads. The
        # password is DERIVED, not stored: `password_for_alias` is the same
        # function `_build_auth_fixture` hashed when it seeded this account,
        # so login and seeding cannot drift apart (CHAOS-3463). Nothing here
        # writes to the world -- the earlier bridge mutated `password_hash`,
        # a digest-covered column, and made a second armed run report drift.
        api = self._api_factory()
        try:
            login = api.request(
                "POST",
                _LOGIN_PATH,
                {
                    "email": principal.email,
                    "password": password_for_alias(principal.user_alias),
                },
            )
        except Exception as exc:
            # A world that predates seeded credentials has password_hash=None
            # and login.py answers 401 -- which the API client raises on,
            # BEFORE any response-body branch below could run. The remedy
            # therefore belongs HERE and not on the no-access_token branch,
            # where an operator would never see it.
            #
            # ONLY for a credential rejection, though. A 503, a timeout or a
            # rate-limit means something else entirely is wrong, and the
            # seeded-credential remedy would be actively misleading -- it
            # would send an operator to re-mint a world that is fine. The
            # previous revision preserved that distinction via the bridge
            # flag (`test_a_bridged_run_does_not_swallow_the_real_login_
            # error` pinned it); with the flag gone the discriminator is the
            # error itself, which is the more honest one anyway.
            if not _is_credential_rejection(exc):
                raise
            raise PrincipalError(
                f"login as {principal.user_alias!r} ({principal.email}) "
                f"failed: {exc}. This runner requires the account to carry the "
                f"credential seeded at world-generation time (CHAOS-3463). A "
                f"world snapshot minted before seeded credentials has "
                f"password_hash=None and answers 401 -- re-mint the world "
                f"snapshot (scripts/acceptance/mint_ask_dev_world_snapshot.sh) "
                f"rather than provisioning a password at run time, which would "
                f"mutate a digest-covered column."
            ) from exc
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
