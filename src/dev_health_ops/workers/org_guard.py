from __future__ import annotations

import logging
import uuid
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from dev_health_ops.models.users import Organization

logger = logging.getLogger(__name__)


def organization_exists_sync(session: Any, org_id: str | None) -> bool:
    if not org_id or org_id == "default":
        return True
    try:
        org_uuid = uuid.UUID(str(org_id))
    except ValueError:
        return True
    try:
        exists = (
            session.query(Organization.id)
            .filter(Organization.id == org_uuid)
            .one_or_none()
            is not None
        )
        if not exists:
            logger.warning("Skipping sync for missing organization org_id=%s", org_id)
        return exists
    except SQLAlchemyError:
        # Fail open: this guard only skips work for already-deleted orgs. If
        # existence cannot be verified (DB error, or a narrow test/migration
        # context without the organizations table), do not block scheduled
        # work — OrganizationDeletionService remains authoritative for removal.
        return True


__all__ = ["organization_exists_sync"]
