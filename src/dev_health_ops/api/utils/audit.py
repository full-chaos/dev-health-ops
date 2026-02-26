from __future__ import annotations

import uuid
from typing import Any

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.audit import AuditAction, AuditLog, AuditResourceType


def extract_request_metadata(request: Request) -> dict[str, str]:
    metadata: dict[str, str] = {}

    x_forwarded_for = request.headers.get("X-Forwarded-For")
    if x_forwarded_for:
        metadata["ip_address"] = x_forwarded_for.split(",", 1)[0].strip()
    elif request.client and request.client.host:
        metadata["ip_address"] = request.client.host

    user_agent = request.headers.get("User-Agent")
    if user_agent:
        metadata["user_agent"] = user_agent

    request_id = request.headers.get("X-Request-ID")
    if request_id:
        metadata["request_id"] = request_id

    return metadata


def emit_audit_log(
    db: AsyncSession,
    org_id: uuid.UUID,
    action: AuditAction,
    resource_type: AuditResourceType,
    resource_id: str,
    user_id: uuid.UUID | None = None,
    description: str | None = None,
    changes: dict[str, Any] | None = None,
    request: Request | None = None,
    status: str = "success",
    error_message: str | None = None,
) -> AuditLog:
    metadata = extract_request_metadata(request) if request else {}

    entry = AuditLog.create_entry(
        org_id=org_id,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        user_id=user_id,
        description=description,
        changes=changes,
        ip_address=metadata.get("ip_address"),
        user_agent=metadata.get("user_agent"),
        request_id=metadata.get("request_id"),
    )
    entry.status = status
    entry.error_message = error_message
    db.add(entry)
    return entry
