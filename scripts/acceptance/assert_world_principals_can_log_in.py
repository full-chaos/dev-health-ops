#!/usr/bin/env python3
"""CHAOS-3463: prove the world's principals can actually authenticate.

Runs from the HOST against the acceptance stack's public API, as the last
step of ``mint_ask_dev_world_snapshot.sh`` -- so a snapshot can only be
minted if the credentials it freezes really work.

Why this exists, concretely: before CHAOS-3463 every ``ask-dev-world.v1``
user was seeded with ``password_hash=None``, so no world principal could log
in at all and the corpus's cross-tenant / entitlement cases had no way to be
anyone but the superuser (found by the CHAOS-3462 runner lane). The fix is
generation-time credential seeding; this is the check that the fix is real.

It asserts three things per alias, not one:

1. the login SUCCEEDS with ``password_for_alias(alias)`` -- proving the
   seeded bcrypt hash is one ``/api/v1/auth/login`` actually accepts, not
   merely a non-NULL column;
2. the authenticated user's id is the one ``world.json`` derives for that
   alias -- proving we logged in as the intended principal and not as some
   other account that happens to share an email;
3. the org the login lands in is the one ``world.json`` derives for that
   alias's ``org_alias``. The runner hard-fails on any alias whose login
   lands in a different org, so an alias silently re-orged by a world edit
   must fail HERE, at mint time.

A negative control runs too: a deliberately wrong password must be REJECTED.
Without it, an API that accepted anything would pass every check above.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from dev_health_ops.fixtures.world import (  # noqa: E402
    CORPUS_CONTRACT_USER_ALIASES,
    load_world_manifest,
    password_for_alias,
)


class PrincipalLoginError(RuntimeError):
    """A contract principal could not log in as world.json describes it."""


def _post(url: str, payload: dict[str, Any], *, timeout: float = 30.0) -> Any:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
        return json.loads(response.read().decode())


def _login(api_url: str, email: str, password: str) -> dict[str, Any]:
    payload = _post(
        f"{api_url}/api/v1/auth/login", {"email": email, "password": password}
    )
    if not isinstance(payload, dict):
        raise PrincipalLoginError(
            f"login for {email} returned {payload!r}, not an object"
        )
    return payload


def check(api_url: str, manifest_path: str) -> list[str]:
    manifest = load_world_manifest(manifest_path)
    by_alias = {user["alias"]: user for user in manifest.world["users"]}

    missing = [a for a in CORPUS_CONTRACT_USER_ALIASES if a not in by_alias]
    if missing:
        raise PrincipalLoginError(
            f"world.json no longer defines corpus contract principal(s) {missing}. "
            "The Wave 4 corpus binds user_alias to these -- dropping one breaks "
            "its cross-tenant/entitlement cases. Fix world.json or amend "
            "CORPUS_CONTRACT_USER_ALIASES deliberately."
        )

    verified: list[str] = []
    negative_control_done = False
    for alias in CORPUS_CONTRACT_USER_ALIASES:
        user = by_alias[alias]
        expected_user_id = str(manifest.user_id(alias))
        expected_org_id = str(manifest.org_id(user["org_alias"]))

        try:
            login = _login(api_url, user["email"], password_for_alias(alias))
        except urllib.error.HTTPError as exc:  # pragma: no cover -- live only
            raise PrincipalLoginError(
                f"{alias} ({user['email']}) could not log in: HTTP {exc.code} "
                f"{exc.read().decode(errors='replace')[:200]}. The seeded "
                "credential is not one the API accepts."
            ) from exc

        if not login.get("access_token"):
            raise PrincipalLoginError(f"{alias} login returned no access token")

        actual = login.get("user")
        if not isinstance(actual, dict):
            raise PrincipalLoginError(f"{alias} login returned no user object")
        if str(actual.get("id")) != expected_user_id:
            raise PrincipalLoginError(
                f"{alias} logged in as user {actual.get('id')}, but world.json "
                f"derives {expected_user_id} for that alias"
            )
        if str(actual.get("org_id")) != expected_org_id:
            raise PrincipalLoginError(
                f"{alias} logged into org {actual.get('org_id')}, but world.json "
                f"derives {expected_org_id} (org_alias={user['org_alias']!r}). "
                "The corpus runner hard-fails on exactly this mismatch."
            )

        # Negative control: an API that accepted any password would satisfy
        # every assertion above without proving anything at all.
        #
        # Run ONCE for the whole check, not once per alias (CHAOS-3490). The
        # per-alias form was affordable at four contract principals and is not
        # at ten: production limits logins to AUTH_LOGIN_IP_LIMIT
        # ("20/15minutes", per IP -- api/middleware/rate_limit.py), so ten
        # aliases x two attempts consumed the entire IP budget and the next
        # caller (prepare_ask_dev_acceptance.py's superuser login) took a 429
        # and failed the boot. Observed live, not theorised.
        #
        # Repeating it per alias also bought no information: the property is
        # "this API rejects a wrong password", which belongs to the auth path,
        # not to an individual account. One execution establishes it; ten run
        # the same code ten times and additionally risk the DB-backed
        # per-email lockout in login_attempts.py.
        if not negative_control_done:
            try:
                _login(api_url, user["email"], password_for_alias(alias) + "-wrong")
            except urllib.error.HTTPError as exc:
                if exc.code not in (400, 401, 403):
                    raise PrincipalLoginError(
                        f"{alias}: a wrong password returned HTTP {exc.code}; "
                        "expected an auth rejection"
                    ) from exc
            else:
                raise PrincipalLoginError(
                    f"{alias}: a deliberately WRONG password was accepted. The "
                    "login checks here prove nothing -- stop and fix "
                    "authentication."
                )
            negative_control_done = True

        verified.append(f"{alias} -> user {expected_user_id} in org {expected_org_id}")

    # A negative control that silently never ran would leave every positive
    # login above unproven, which is the "measurement that did not happen"
    # failure mode. Absence must be loud.
    if not negative_control_done:
        raise PrincipalLoginError(
            "the wrong-password negative control never ran, so every positive "
            "login above is unproven -- an API that accepted anything would "
            "look identical. A check that cannot fail must not report success."
        )
    return verified


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args(argv)

    try:
        verified = check(args.api_url, args.manifest)
    except PrincipalLoginError as exc:
        print(f"mint: WORLD PRINCIPAL CHECK FAILED -- {exc}", file=sys.stderr)
        return 1

    for line in verified:
        print(f"mint: verified login {line}")
    print(f"mint: all {len(verified)} corpus contract principals authenticate")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
