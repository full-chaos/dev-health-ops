from __future__ import annotations

from typing import Any, Dict

from dev_health_ops.providers.identity import (
    load_identity_resolver as _load_identity_resolver,
)
from dev_health_ops.providers.teams import (
    load_team_resolver as _load_team_resolver,
    load_team_resolver_from_store,
)
from dev_health_ops.models.work_items import WorkItemProvider

_IDENTITY_RESOLVER = None
_TEAM_RESOLVER = None


def resolve_identity(
    provider: str,
    raw_user_fields: Dict[str, Any],
) -> str:
    """
    Resolve a canonical identity from raw provider fields.

    :param provider: 'github', 'gitlab', 'jira', etc.
    :param raw_user_fields: dict containing potential identity keys like:
           email, username, login, account_id, display_name
    :return: canonical identity string (e.g. email or provider:username)
    """
    global _IDENTITY_RESOLVER
    if _IDENTITY_RESOLVER is None:
        _IDENTITY_RESOLVER = _load_identity_resolver()

    # Normalize provider string to WorkItemProvider literal if possible, else str
    prov: WorkItemProvider = provider.lower()  # type: ignore

    return _IDENTITY_RESOLVER.resolve(
        provider=prov,
        email=raw_user_fields.get("email"),
        username=raw_user_fields.get("username") or raw_user_fields.get("login"),
        account_id=raw_user_fields.get("account_id")
        or raw_user_fields.get("accountId"),
        display_name=raw_user_fields.get("display_name") or raw_user_fields.get("name"),
    )


def load_team_map() -> Dict[str, str]:
    """
    Load mapping of identity_id -> team_id.

    :return: dict where key is canonical identity, value is team_id
    """
    global _TEAM_RESOLVER
    if _TEAM_RESOLVER is None:
        _TEAM_RESOLVER = _load_team_resolver()

    # provider.teams.TeamResolver stores (team_id, team_name) tuple.
    # We just want identity -> team_id map.
    return {
        member: team_id
        for member, (team_id, _) in _TEAM_RESOLVER.member_to_team.items()
    }


async def init_team_resolver(store: Any) -> None:
    """Initialize or update the global team resolver from a database store."""
    global _TEAM_RESOLVER
    _TEAM_RESOLVER = await load_team_resolver_from_store(store)


def get_team_resolver() -> Any:
    """Get the current global team resolver, initializing from dev_health_ops.config if needed."""
    global _TEAM_RESOLVER
    if _TEAM_RESOLVER is None:
        _TEAM_RESOLVER = _load_team_resolver()
    return _TEAM_RESOLVER
