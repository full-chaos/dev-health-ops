from __future__ import annotations

from fastapi import Depends, HTTPException

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser, is_impersonating


async def require_admin(
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> AuthenticatedUser:
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


async def require_superuser(
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> AuthenticatedUser:
    if not current_user.is_superuser:
        raise HTTPException(status_code=403, detail="Superuser access required")
    return current_user


async def get_admin_org_id(
    current_user: AuthenticatedUser = Depends(require_admin),
) -> str:
    if not current_user.org_id:
        raise HTTPException(status_code=403, detail="Organization context required")
    return current_user.org_id


async def get_admin_user(
    current_user: AuthenticatedUser = Depends(require_admin),
) -> AuthenticatedUser:
    return current_user


def block_impersonated_write(
    user: AuthenticatedUser, *, detail: str | dict[str, str]
) -> None:
    """Reject a write while an operator is impersonating another identity.

    Shared by every admin write route (Ask Dev org settings, the new
    Platform Admin readiness route, and the new BYO readiness route) so the
    impersonation guard can't drift between them. Each call site supplies its
    own ``detail`` so the response keeps that router's existing message
    style (CHAOS-3265).

    Checks BOTH signals, not just one:
    - ``user.impersonated_by`` -- the static JWT claim (``impersonating_user_id``)
      stamped at token-issue time.
    - ``is_impersonating()`` -- the LIVE, per-request impersonation context set by
      ``ImpersonationMiddleware`` from the active Valkey-cached session (see
      ``dev_health_ops.api.middleware.impersonation``). This is the authoritative,
      real-time signal: a session started after the JWT was minted, or one whose
      cache entry has since expired/been revoked, is reflected here even when the
      JWT claim disagrees. Codex review (CHAOS-3265) found that checking only the
      JWT claim let an actively-impersonating superuser reach both new POST
      routes because ``impersonated_by`` was unset even though ``is_impersonating()``
      was True -- check both so neither signal alone is a bypass.
    """

    if user.impersonated_by or is_impersonating():
        raise HTTPException(status_code=403, detail=detail)
