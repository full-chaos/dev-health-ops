"""Shared private helpers for the settings services package.

* ``_CREDENTIAL_KEY_MAP`` + ``_normalize_credential_keys`` translate camelCase
  credential keys from frontend forms into the snake_case form used inside
  the backend.
* The ``_get_*_cls`` helpers perform lazy imports of admin schema classes to
  avoid a circular dependency between ``api.services.settings`` and
  ``api.admin.schemas``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dev_health_ops.api.admin.schemas import (
        DiscoveredMember,
        DiscoveredTeam,
        IdentityMappingResponse,
        MemberMatchResult,
    )


# Normalize camelCase credential keys from frontend forms to snake_case for backend consistency.
_CREDENTIAL_KEY_MAP: dict[str, dict[str, str]] = {
    "linear": {"apiKey": "api_key"},
    "jira": {"apiToken": "api_token", "baseUrl": "base_url"},
    "github": {"baseUrl": "base_url"},
    "gitlab": {"baseUrl": "base_url"},
    "atlassian": {"apiToken": "api_token", "cloudId": "cloud_id"},
}


def _normalize_credential_keys(
    provider: str, credentials: dict[str, Any]
) -> dict[str, Any]:
    """Normalize camelCase credential keys to snake_case based on provider."""
    key_map = _CREDENTIAL_KEY_MAP.get(provider.lower(), {})
    if not key_map:
        return credentials
    normalized: dict[str, Any] = {}
    for k, v in credentials.items():
        normalized[key_map.get(k, k)] = v
    return normalized


def _get_discovered_team_cls() -> type[DiscoveredTeam]:
    """Lazy import to avoid circular dependency with admin.schemas."""
    from dev_health_ops.api.admin.schemas import DiscoveredTeam as _DT

    return _DT


def _get_discovered_member_cls() -> type[DiscoveredMember]:
    from dev_health_ops.api.admin.schemas import DiscoveredMember as _DM

    return _DM


def _get_member_match_result_cls() -> type[MemberMatchResult]:
    from dev_health_ops.api.admin.schemas import MemberMatchResult as _MMR

    return _MMR


def _get_identity_mapping_response_cls() -> type[IdentityMappingResponse]:
    from dev_health_ops.api.admin.schemas import IdentityMappingResponse as _IMR

    return _IMR


def _get_jira_activity_schema_classes() -> tuple[type, type]:
    """Lazy import to avoid circular dependency with admin.schemas."""
    from dev_health_ops.api.admin.schemas import (
        ConfirmInferredMemberAction as _CIMA,
    )
    from dev_health_ops.api.admin.schemas import (
        InferredMember as _IM,
    )

    return _IM, _CIMA
