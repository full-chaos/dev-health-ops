from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from fastapi import HTTPException

from dev_health_ops.api.services.auth import AuthenticatedUser

BillingTier = Literal["community", "team", "enterprise"]
RefundReason = Literal["duplicate", "fraudulent", "requested_by_customer"]


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


def require_uuid(value: object, field_name: str) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    if isinstance(value, str):
        try:
            return uuid.UUID(value)
        except ValueError as exc:
            raise ValueError(f"Invalid {field_name}: {value}") from exc
    raise ValueError(f"Invalid {field_name}: {value!r}")


def require_str(value: object, field_name: str) -> str:
    if isinstance(value, str):
        return value
    raise ValueError(f"Invalid {field_name}: {value!r}")


def optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def require_int(value: object, field_name: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise ValueError(f"Invalid {field_name}: {value!r}")


def require_bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"Invalid {field_name}: {value!r}")


def optional_datetime(value: object) -> datetime | None:
    return value if isinstance(value, datetime) else None


def ensure_str_dict(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, str] = {}
    for key, item in value.items():
        if isinstance(key, str) and isinstance(item, str):
            out[key] = item
    return out


def ensure_dict(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(key, str):
            out[key] = item
    return out


def ensure_str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def normalize_billing_tier(value: object, default: BillingTier = "team") -> BillingTier:
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "community":
            return "community"
        if lowered == "team":
            return "team"
        if lowered == "enterprise":
            return "enterprise"
    return default


def assign_attr(target: object, field_name: str, value: object) -> None:
    setattr(target, field_name, value)
