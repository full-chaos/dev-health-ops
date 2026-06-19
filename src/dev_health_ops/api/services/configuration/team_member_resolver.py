from __future__ import annotations

from typing import Any


def members_by_team(identity_mappings: Any) -> dict[str, set[str]]:
    """Collect member identities per team from confirmed identity mappings.

    The membership-based ``TeamResolver`` matches work-item assignees
    (emails, provider logins/account ids) against ``teams.members`` in
    ClickHouse, so every confirmed identity facet is included: email,
    canonical_id, and all provider identities. ``display_name`` is used
    only when no email exists (mirrors the worker ``sync_teams`` path and
    avoids false-positive matches on common names).
    """
    members: dict[str, set[str]] = {}
    for ident in identity_mappings:
        identities: set[str] = set()
        email = getattr(ident, "email", None)
        if email:
            identities.add(str(email))
        canonical_id = getattr(ident, "canonical_id", None)
        if canonical_id:
            identities.add(str(canonical_id))
        for values in (getattr(ident, "provider_identities", None) or {}).values():
            if isinstance(values, list):
                identities.update(str(v) for v in values if v)
            elif values:
                identities.add(str(values))
        if not email:
            display_name = getattr(ident, "display_name", None)
            if display_name:
                identities.add(str(display_name))
        if not identities:
            continue
        for team_id in getattr(ident, "team_ids", None) or []:
            key = str(team_id).strip()
            if key:
                members.setdefault(key, set()).update(identities)
    return members
