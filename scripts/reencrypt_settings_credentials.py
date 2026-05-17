#!/usr/bin/env python
"""Re-encrypt legacy v0 credential ciphertexts to v1 in place.

This utility upgrades rows encrypted with the pre-v1 SHA-256-derived Fernet key.
It is idempotent: values already prefixed with ``v1:`` are skipped.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select

from dev_health_ops.core.encryption import is_v1_ciphertext, reencrypt_legacy_value
from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.settings import IntegrationCredential, Setting
from dev_health_ops.models.sso import SSOProvider


@dataclass
class ReencryptStats:
    scanned: int = 0
    upgraded: int = 0
    skipped_v1: int = 0
    failed: int = 0

    def merge(self, other: "ReencryptStats") -> None:
        self.scanned += other.scanned
        self.upgraded += other.upgraded
        self.skipped_v1 += other.skipped_v1
        self.failed += other.failed


def _upgrade_ciphertext(value: str | None) -> tuple[str | None, str]:
    if not value:
        return value, "empty"
    if is_v1_ciphertext(value):
        return value, "v1"
    return reencrypt_legacy_value(value), "upgraded"


async def _upgrade_settings(dry_run: bool) -> ReencryptStats:
    stats = ReencryptStats()
    async with get_postgres_session() as session:
        result = await session.execute(
            select(Setting).where(
                Setting.is_encrypted == True,  # noqa: E712
                Setting.value.is_not(None),
            )
        )
        for row in result.scalars():
            stats.scanned += 1
            try:
                upgraded, status = _upgrade_ciphertext(row.value)
            except ValueError:
                stats.failed += 1
                continue
            if status == "v1":
                stats.skipped_v1 += 1
            elif status == "upgraded":
                stats.upgraded += 1
                if not dry_run:
                    row.value = upgraded
        if not dry_run:
            await session.commit()
    return stats


async def _upgrade_integration_credentials(dry_run: bool) -> ReencryptStats:
    stats = ReencryptStats()
    async with get_postgres_session() as session:
        result = await session.execute(
            select(IntegrationCredential).where(
                IntegrationCredential.credentials_encrypted.is_not(None)
            )
        )
        for row in result.scalars():
            stats.scanned += 1
            try:
                upgraded, status = _upgrade_ciphertext(row.credentials_encrypted)
            except ValueError:
                stats.failed += 1
                continue
            if status == "v1":
                stats.skipped_v1 += 1
            elif status == "upgraded":
                stats.upgraded += 1
                if not dry_run:
                    row.credentials_encrypted = upgraded
        if not dry_run:
            await session.commit()
    return stats


def _upgrade_secret_map(value: dict[str, Any] | None, stats: ReencryptStats) -> bool:
    changed = False
    if not value:
        return changed
    for key, item in list(value.items()):
        if not isinstance(item, str):
            continue
        stats.scanned += 1
        try:
            upgraded, status = _upgrade_ciphertext(item)
        except ValueError:
            stats.failed += 1
            continue
        if status == "v1":
            stats.skipped_v1 += 1
        elif status == "upgraded":
            stats.upgraded += 1
            value[key] = upgraded
            changed = True
    return changed


async def _upgrade_sso_secrets(dry_run: bool) -> ReencryptStats:
    stats = ReencryptStats()
    async with get_postgres_session() as session:
        result = await session.execute(
            select(SSOProvider).where(SSOProvider.encrypted_secrets.is_not(None))
        )
        for row in result.scalars():
            secrets = dict(row.encrypted_secrets or {})
            changed = _upgrade_secret_map(secrets, stats)
            if changed and not dry_run:
                row.encrypted_secrets = secrets
        if not dry_run:
            await session.commit()
    return stats


async def _run(dry_run: bool) -> ReencryptStats:
    total = ReencryptStats()
    for upgrade in (
        _upgrade_settings,
        _upgrade_integration_credentials,
        _upgrade_sso_secrets,
    ):
        total.merge(await upgrade(dry_run))
    return total


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Re-encrypt legacy v0 settings/credential ciphertexts to v1."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write upgraded ciphertexts. Without this flag the command is dry-run only.",
    )
    args = parser.parse_args()

    stats = asyncio.run(_run(dry_run=not args.apply))
    mode = "apply" if args.apply else "dry-run"
    print(
        f"mode={mode} scanned={stats.scanned} upgraded={stats.upgraded} "
        f"skipped_v1={stats.skipped_v1} failed={stats.failed}"
    )
    if stats.failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
