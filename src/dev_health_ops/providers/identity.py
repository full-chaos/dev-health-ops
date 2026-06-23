from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import yaml

from dev_health_ops.models.work_items import WorkItemProvider

DEFAULT_IDENTITY_MAPPING_PATH = Path("src/dev_health_ops/config/identity_mapping.yaml")


def _norm_key(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def _norm_email(email: str) -> str:
    return (email or "").strip().lower()


def provider_qualified_identity(
    provider: WorkItemProvider | str,
    *,
    username: str | None = None,
    account_id: str | None = None,
) -> str | None:
    """The provider-qualified identity for a user lacking a usable email.

    Single source of truth shared by two sides that MUST agree (CHAOS-2609):

    * the work-item assignee path — ``IdentityResolver.resolve`` returns this
      string as its stable fallback when no email/alias matches; and
    * team auto-import — writes this exact string into
      ``team_memberships.raw_provider_user_id`` (a facet the canonical ladder's
      ``member_by_identity`` indexes) and the ``teams.members`` roster (read by
      the secondary ``TeamResolver``).

    Priority mirrors ``resolve``: ``provider:username`` before
    ``provider:accountid:account_id`` (GitHub/GitLab carry a username; Jira
    carries only an accountId). Returns ``None`` when neither is present.
    """
    if username and str(username).strip():
        return f"{provider}:{str(username).strip()}"
    if account_id and str(account_id).strip():
        return f"{provider}:accountid:{str(account_id).strip()}"
    return None


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

        # Stable fallbacks. A provider-qualified identity (username, then
        # accountId) takes precedence over the unreliable display name so a
        # no-email assignee resolves to the SAME facet team auto-import writes
        # into team_memberships / teams.members — otherwise member-based
        # attribution silently misses for no-email assignees (CHAOS-2609). This
        # mirrors the candidate priority above (provider:accountid: outranks
        # display_name) and shares provider_qualified_identity as the single
        # derivation used by the auto-import writer.
        qualified = provider_qualified_identity(
            provider, username=username, account_id=account_id
        )
        if qualified:
            return qualified
        if display_name:
            return display_name.strip() or "unknown"
        return "unknown"

    def membership_facets(
        self,
        *,
        provider: WorkItemProvider | str,
        username: str | None = None,
        account_id: str | None = None,
    ) -> list[str]:
        """Identities to store on a team membership so a no-email assignee for
        this member matches on resolution — under THIS org's alias map.

        Returns the **alias-resolved** identity FIRST (the canonical string a
        no-email assignee resolves to via :meth:`resolve` with the same alias
        map, e.g. ``lead@example.com`` when ``github:lead`` is aliased), then the
        provider-qualified id (``github:lead``) as a robustness fallback,
        de-duplicated. The first element is the one that MUST match; auto-import
        stores it in ``team_memberships.raw_provider_user_id`` and the
        ``teams.members`` roster so BOTH attribution paths resolve aliased and
        non-aliased members alike (CHAOS-2609). ``display_name`` is intentionally
        omitted — it is never a stable membership facet.
        """
        primary = self.resolve(
            provider=cast(WorkItemProvider, provider),
            username=username,
            account_id=account_id,
        )
        qualified = provider_qualified_identity(
            provider, username=username, account_id=account_id
        )
        facets: list[str] = []
        for candidate in (primary, qualified):
            if candidate and candidate != "unknown" and candidate not in facets:
                facets.append(candidate)
        return facets


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
