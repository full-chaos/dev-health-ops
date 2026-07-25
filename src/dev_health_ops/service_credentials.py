from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.internal_service_credential import InternalServiceCredential

_SERVICE_SCOPES = {
    "acr": frozenset({"entitlements:read"}),
    "worker-operator": frozenset({"workers:read", "workers:operate"}),
}
_SERVICE_CHOICES = tuple(sorted(_SERVICE_SCOPES))
_MAX_OVERLAP_SECONDS = 3600


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "service-credentials", help="Manage internal service credentials."
    )
    commands = parser.add_subparsers(dest="service_credentials_command", required=True)
    create = commands.add_parser("create", help="Create a service credential.")
    _add_credential_arguments(create)
    create.set_defaults(func=run_create)
    listing = commands.add_parser(
        "list", help="List credential metadata without secrets."
    )
    listing.add_argument("--service", default="acr", choices=_SERVICE_CHOICES)
    listing.set_defaults(func=run_list)
    rotate = commands.add_parser("rotate", help="Rotate a service credential.")
    rotate.add_argument("credential_id")
    _add_credential_arguments(rotate)
    rotate.add_argument("--overlap-seconds", type=int, default=0)
    rotate.set_defaults(func=run_rotate)
    revoke = commands.add_parser("revoke", help="Revoke a service credential.")
    revoke.add_argument("credential_id")
    revoke.set_defaults(func=run_revoke)


def _add_credential_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--service", default="acr", choices=_SERVICE_CHOICES)
    parser.add_argument("--scope", action="append", required=True)
    parser.add_argument("--expires-at")
    parser.add_argument("--created-by-user-id")


def _parse_expiry(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    value = datetime.fromisoformat(raw)
    if value.tzinfo is None:
        raise ValueError("--expires-at must include a timezone")
    expiry = value.astimezone(timezone.utc)
    if expiry <= datetime.now(timezone.utc):
        raise ValueError("--expires-at must be in the future")
    return expiry


def _parse_creator(raw: str | None) -> uuid.UUID | None:
    return uuid.UUID(raw) if raw else None


def _scopes(service: str, raw_scopes: list[str]) -> list[str]:
    scopes = sorted(set(raw_scopes))
    allowed = _SERVICE_SCOPES.get(service, frozenset())
    if not scopes or not set(scopes).issubset(allowed):
        raise ValueError("unsupported internal service credential scope")
    return scopes


async def run_create(ns: argparse.Namespace) -> int:
    credential, token = InternalServiceCredential.issue(
        service_name=ns.service,
        scopes=_scopes(ns.service, ns.scope),
        created_by_user_id=_parse_creator(ns.created_by_user_id),
        expires_at=_parse_expiry(ns.expires_at),
    )
    async with get_postgres_session() as session:
        session.add(credential)
        await session.commit()
    print(token)
    return 0


async def run_list(ns: argparse.Namespace) -> int:
    async with get_postgres_session() as session:
        result = await session.execute(
            select(InternalServiceCredential)
            .where(InternalServiceCredential.service_name == ns.service)
            .order_by(InternalServiceCredential.created_at)
        )
        metadata = [
            credential.public_metadata() for credential in result.scalars().all()
        ]
    print(json.dumps(metadata, sort_keys=True), file=sys.stdout)
    return 0


async def run_rotate(ns: argparse.Namespace) -> int:
    if ns.overlap_seconds < 0 or ns.overlap_seconds > _MAX_OVERLAP_SECONDS:
        raise ValueError("--overlap-seconds must be between 0 and 3600")
    credential_id = uuid.UUID(ns.credential_id)
    scopes = _scopes(ns.service, ns.scope)
    created_by_user_id = _parse_creator(ns.created_by_user_id)
    expires_at = _parse_expiry(ns.expires_at)
    async with get_postgres_session() as session:
        credential = await session.get(InternalServiceCredential, credential_id)
        if credential is None or credential.service_name != ns.service:
            raise ValueError("service credential not found")
        now = datetime.now(timezone.utc)
        if not credential.is_valid(now):
            raise ValueError("service credential is not active")
        credential.expires_at = now + timedelta(seconds=ns.overlap_seconds)
        replacement, token = InternalServiceCredential.issue(
            service_name=ns.service,
            scopes=scopes,
            created_by_user_id=created_by_user_id,
            expires_at=expires_at,
        )
        session.add(replacement)
        await session.commit()
    print(token)
    return 0


async def run_revoke(ns: argparse.Namespace) -> int:
    credential_id = uuid.UUID(ns.credential_id)
    async with get_postgres_session() as session:
        credential = await session.get(InternalServiceCredential, credential_id)
        if credential is None:
            raise ValueError("service credential not found")
        credential.revoked_at = datetime.now(timezone.utc)
        await session.commit()
    return 0
