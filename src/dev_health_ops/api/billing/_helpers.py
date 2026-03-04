from __future__ import annotations

import uuid

from fastapi import HTTPException

from dev_health_ops.api.services.auth import AuthenticatedUser


def _resolve_org_id(
    user: AuthenticatedUser, org_id_param: uuid.UUID | None
) -> uuid.UUID | None:
    if org_id_param is not None:
        return org_id_param

    if user.is_superuser:
        return None

    if user.org_id:
        try:
            return uuid.UUID(user.org_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid organization") from exc

    raise HTTPException(status_code=400, detail="Organization context required")
