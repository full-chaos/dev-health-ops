from __future__ import annotations

import os

_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}

AUTH_AUTO_CREATE_ORG_ENV = "AUTH_AUTO_CREATE_ORG_ON_REGISTER"


def auth_auto_create_org_on_register(default: bool = True) -> bool:
    """Whether self-registration auto-creates an Organization + owner Membership.

    Controlled by ``AUTH_AUTO_CREATE_ORG_ON_REGISTER`` (default ``True`` to
    preserve current production behavior). When ``False``, registration creates
    the user identity only and the verified user is routed into guided onboarding
    (CHAOS-2670 / CHAOS-2671 / CHAOS-2682). Unrecognized values fall back to the
    default so a typo never silently disables org creation in production.
    """
    raw = os.getenv(AUTH_AUTO_CREATE_ORG_ENV)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in _TRUTHY:
        return True
    if normalized in _FALSY:
        return False
    return default
