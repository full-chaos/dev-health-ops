from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import yaml

from dev_health_ops.models.work_items import WorkItemProvider

DEFAULT_IDENTITY_MAPPING_PATH = Path("src/dev_health_ops/config/identity_mapping.yaml")


def _norm_key(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def _norm_email(email: str) -> str:
    return (email or "").strip().lower()


@dataclass(frozen=True)
class IdentityResolver:
    """
    Best-effort identity resolver for cross-provider user rollups.

    Strategy:
    - Prefer email when present.
    - Otherwise map known aliases (config-driven).
    - Otherwise fall back to provider-qualified username (e.g., github:octocat).
    """

    alias_to_canonical: Mapping[str, str]

    def resolve(
        self,
        *,
        provider: WorkItemProvider,
        email: str | None = None,
        username: str | None = None,
        account_id: str | None = None,
        display_name: str | None = None,
    ) -> str:
        if email:
            normalized = _norm_email(email)
            if normalized:
                # Email is already canonical enough for most orgs.
                return self.alias_to_canonical.get(_norm_key(normalized), normalized)

        candidates: Sequence[str] = tuple(
            c
            for c in [
                f"{provider}:{username}" if username else None,
                f"{provider}:accountid:{account_id}" if account_id else None,
                username,
                display_name,
            ]
            if c
        )
        for candidate in candidates:
            key = _norm_key(candidate)
            if not key:
                continue
            mapped = self.alias_to_canonical.get(key)
            if mapped:
                return mapped

        # Stable fallbacks.
        if username:
            return f"{provider}:{username}"
        if display_name:
            return display_name.strip() or "unknown"
        return "unknown"


def load_identity_resolver(path: Path | None = None) -> IdentityResolver:
    raw_path = os.getenv("IDENTITY_MAPPING_PATH")
    if raw_path:
        path = Path(raw_path)
    path = path or DEFAULT_IDENTITY_MAPPING_PATH

    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    except FileNotFoundError:
        payload = {}

    alias_to_canonical: dict[str, str] = {}
    for entry in payload.get("identities") or []:
        canonical = entry.get("canonical")
        if not canonical:
            continue
        canonical_norm = _norm_email(str(canonical)) or str(canonical).strip()
        if not canonical_norm:
            continue
        alias_to_canonical[_norm_key(canonical_norm)] = canonical_norm
        for alias in entry.get("aliases") or []:
            alias_norm = _norm_key(str(alias))
            if not alias_norm:
                continue
            alias_to_canonical[alias_norm] = canonical_norm

    return IdentityResolver(alias_to_canonical=alias_to_canonical)


def normalize_git_identity(
    email: str | None,
    display_name: str | None,
    resolver: IdentityResolver | None = None,
) -> str:
    """Normalize a Git author identity to a canonical string.

    Uses IdentityResolver if provided, otherwise falls back to email > name > "unknown".
    """
    if resolver is not None:
        if email:
            normalized = _norm_email(email)
            if normalized:
                return resolver.alias_to_canonical.get(
                    _norm_key(normalized), normalized
                )
        if display_name:
            display_norm = display_name.strip()
            if display_norm:
                return resolver.alias_to_canonical.get(
                    _norm_key(display_norm), display_norm
                )
        return "unknown"

    if email:
        normalized = email.strip()
        if normalized:
            return normalized
    if display_name:
        normalized = display_name.strip()
        if normalized:
            return normalized
    return "unknown"
