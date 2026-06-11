"""Configuration services package.

This package replaces what used to be a single 1.6k-line
``api/services/configuration.py`` module. The file was split by domain to keep
each service focused and reviewable; every public symbol that the old
module exposed is re-exported here so existing imports keep working
unchanged.

Naming note
-----------
This module is *not* Pydantic ``BaseSettings`` and goes far beyond generic key/value settings — it bundles integration
credentials, sync configuration, identity/team mappings, team discovery
and drift, team membership management, and Jira activity inference.
"""

from __future__ import annotations

# Re-export encryption helpers so existing
# ``from dev_health_ops.api.services.configuration import decrypt_value, encrypt_value``
# imports keep working (used by SSO router/service and tests).
from dev_health_ops.core.encryption import decrypt_value, encrypt_value

from ._helpers import (
    _CREDENTIAL_KEY_MAP,
    _get_discovered_member_cls,
    _get_discovered_team_cls,
    _get_identity_mapping_response_cls,
    _get_jira_activity_schema_classes,
    _get_member_match_result_cls,
    _normalize_credential_keys,
)
from .generic import SettingsService
from .identity_mapping import IdentityMappingService
from .integration_credentials import (
    AmbiguousCredentialError,
    IntegrationCredentialsService,
)
from .jira_activity_inference import JiraActivityInferenceService
from .sync_configuration import SyncConfigurationService
from .team_discovery import GitLabDiscoveryResult, TeamDiscoveryService
from .team_drift_sync import TeamDriftSyncService
from .team_mapping import TeamMappingService
from .team_membership import TeamMembershipService

__all__ = [
    "AmbiguousCredentialError",
    "IdentityMappingService",
    "IntegrationCredentialsService",
    "JiraActivityInferenceService",
    "SettingsService",
    "SyncConfigurationService",
    "GitLabDiscoveryResult",
    "TeamDiscoveryService",
    "TeamDriftSyncService",
    "TeamMappingService",
    "TeamMembershipService",
    "decrypt_value",
    "encrypt_value",
]
