from __future__ import annotations

import uuid
from typing import Any

from dev_health_ops.models.users import Organization


def organization_exists_sync(session: Any, org_id: str | None) -> bool:
    if not org_id or org_id == "default":
        return True
    try:
        org_uuid = uuid.UUID(str(org_id))
    except ValueError:
        return True
    return (
        session.query(Organization.id).filter(Organization.id == org_uuid).one_or_none()
        is not None
    )


__all__ = ["organization_exists_sync"]
