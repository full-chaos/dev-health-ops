from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class SettingResponse(BaseModel):
    key: str
    value: str | None
    category: str
    is_encrypted: bool
    description: str | None

    model_config = ConfigDict(from_attributes=True)


class SettingCreate(BaseModel):
    key: str = Field(..., min_length=1, max_length=255)
    value: str | None = None
    category: str = "general"
    encrypt: bool = False
    description: str | None = None


class SettingUpdate(BaseModel):
    value: str | None = None
    encrypt: bool | None = None
    description: str | None = None


class SettingsListResponse(BaseModel):
    category: str
    settings: list[SettingResponse]


class IntegrationCredentialResponse(BaseModel):
    id: str
    provider: str
    name: str
    is_active: bool
    config: dict[str, Any]
    last_test_at: datetime | None
    last_test_success: bool | None
    last_test_error: str | None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class IntegrationCredentialCreate(BaseModel):
    provider: str = Field(..., min_length=1)
    name: str = Field(default="default", min_length=1)
    credentials: dict[str, Any] = Field(
        ..., description="Provider credentials (will be encrypted)"
    )
    config: dict[str, Any] | None = Field(
        default=None, description="Non-sensitive configuration"
    )


class IntegrationCredentialUpdate(BaseModel):
    credentials: dict[str, Any] | None = None
    config: dict[str, Any] | None = None
    is_active: bool | None = None


class TestConnectionRequest(BaseModel):
    provider: str
    name: str = "default"
    credential_id: str | None = Field(
        default=None,
        description="UUID of the stored credential to test. "
        "When provided (and no inline credentials), the credential is looked up by ID "
        "instead of by provider+name.",
    )
    credentials: dict[str, Any] | None = Field(
        default=None,
        description="Inline credentials to test without saving. "
        "When provided, these are used directly instead of looking up stored credentials.",
    )


class TestConnectionResponse(BaseModel):
    success: bool
    error: str | None = None
    details: dict[str, Any] | None = None


class SyncConfigResponse(BaseModel):
    id: str
    name: str
    provider: str
    credential_id: str | None
    sync_targets: list[str]
    sync_options: dict[str, Any]
    is_active: bool
    last_sync_at: datetime | None
    last_sync_success: bool | None
    last_sync_error: str | None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class SyncConfigCreate(BaseModel):
    name: str = Field(..., min_length=1)
    provider: str = Field(..., min_length=1)
    credential_id: str | None = None
    sync_targets: list[str] = Field(default_factory=list)
    sync_options: dict[str, Any] = Field(default_factory=dict)


class SyncConfigUpdate(BaseModel):
    sync_targets: list[str] | None = None
    sync_options: dict[str, Any] | None = None
    is_active: bool | None = None


class BackfillRequest(BaseModel):
    since: date
    before: date


JOB_RUN_STATUS_LABELS: dict[int, str] = {
    0: "pending",
    1: "running",
    2: "success",
    3: "failed",
    4: "cancelled",
}


class JobRunResponse(BaseModel):
    id: str
    job_id: str
    status: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: int | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    triggered_by: str
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class IdentityMappingResponse(BaseModel):
    id: str
    canonical_id: str
    display_name: str | None
    email: str | None
    provider_identities: dict[str, list[str]]
    team_ids: list[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class IdentityMappingCreate(BaseModel):
    canonical_id: str = Field(..., min_length=1)
    display_name: str | None = None
    email: str | None = None
    provider_identities: dict[str, list[str]] = Field(default_factory=dict)
    team_ids: list[str] = Field(default_factory=list)


class IdentityMappingUpdate(BaseModel):
    display_name: str | None = None
    email: str | None = None
    provider_identities: dict[str, list[str]] | None = None
    team_ids: list[str] | None = None


class TeamMappingResponse(BaseModel):
    id: str
    team_id: str
    name: str
    description: str | None
    repo_patterns: list[str]
    project_keys: list[str]
    extra_data: dict[str, Any]
    managed_fields: list[str]
    sync_policy: int
    flagged_changes: dict[str, Any] | None = None
    last_drift_sync_at: datetime | None = None
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TeamMappingCreate(BaseModel):
    team_id: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    description: str | None = None
    repo_patterns: list[str] = Field(default_factory=list)
    project_keys: list[str] = Field(default_factory=list)
    extra_data: dict[str, Any] = Field(default_factory=dict)
    managed_fields: list[str] = Field(default_factory=list)
    sync_policy: int = Field(default=1, ge=0, le=2)


class TeamMappingUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    repo_patterns: list[str] | None = None
    project_keys: list[str] | None = None
    extra_data: dict[str, Any] | None = None
    managed_fields: list[str] | None = None
    sync_policy: int | None = Field(default=None, ge=0, le=2)


class DiscoveredTeam(BaseModel):
    provider_type: str
    provider_team_id: str
    name: str
    description: str | None = None
    member_count: int | None = None
    associations: dict[str, Any] = Field(default_factory=dict)


class TeamDiscoverResponse(BaseModel):
    provider: str
    teams: list[DiscoveredTeam]
    total: int


class TeamImportRequest(BaseModel):
    teams: list[DiscoveredTeam]
    on_conflict: str = Field(default="skip", pattern="^(skip|merge)$")


class TeamImportResponse(BaseModel):
    imported: int
    skipped: int
    merged: int
    details: list[dict[str, Any]] = Field(default_factory=list)


class DiscoveredMember(BaseModel):
    provider_type: str
    provider_identity: str
    display_name: str | None = None
    email: str | None = None
    role: str | None = None


class MemberMatchResult(BaseModel):
    discovered: DiscoveredMember
    match_status: str = Field(pattern="^(matched|suggested|unmatched)$")
    matched_identity: IdentityMappingResponse | None = None
    confidence: float | None = None
    suggestion_reason: str | None = None


class TeamMembersDiscoverResponse(BaseModel):
    team_id: str
    provider: str
    members: list[MemberMatchResult]
    total: int


class ConfirmMemberLink(BaseModel):
    provider_identity: str
    provider: str = Field(pattern="^(github|gitlab|jira)$")
    canonical_id: str
    action: str = Field(pattern="^(link|create|skip)$")


class ConfirmMembersRequest(BaseModel):
    team_id: str
    links: list[ConfirmMemberLink]


class ConfirmMembersResponse(BaseModel):
    linked: int
    created: int
    skipped: int


class InferredMember(BaseModel):
    account_id: str
    display_name: str | None = None
    email: str | None = None
    activity_count: int = Field(ge=0)
    confidence: Literal["core", "active", "peripheral"]
    roles: list[Literal["assignee", "reporter", "commenter"]] = Field(
        default_factory=list
    )
    last_active: datetime | None = None


class JiraActivityInferenceResponse(BaseModel):
    team_id: str
    project_key: str
    window_days: int
    inferred_members: list[InferredMember]
    total: int


class ConfirmInferredMemberAction(BaseModel):
    account_id: str
    action: Literal["add", "skip"]
    canonical_id: str | None = None
    display_name: str | None = None
    email: str | None = None


class ConfirmInferredMembersRequest(BaseModel):
    team_id: str
    members: list[ConfirmInferredMemberAction] = Field(default_factory=list)


class ConfirmInferredMembersResponse(BaseModel):
    linked: int
    created: int
    skipped: int


class FlaggedChange(BaseModel):
    team_id: str
    team_name: str
    change_type: str
    field: str | None = None
    old_value: Any = None
    new_value: Any = None
    discovered_at: datetime


class PendingChangesResponse(BaseModel):
    changes: list[FlaggedChange]
    total: int


class ApproveChangesRequest(BaseModel):
    change_ids: list[str] = Field(default_factory=list)
    approve_all: bool = False


# ---- User schemas ----


class UserResponse(BaseModel):
    id: str
    email: str
    username: str | None
    full_name: str | None
    avatar_url: str | None
    auth_provider: str
    is_active: bool
    is_verified: bool
    is_superuser: bool
    last_login_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class UserCreate(BaseModel):
    email: str = Field(..., min_length=1)
    password: str | None = Field(default=None, min_length=8)
    username: str | None = None
    full_name: str | None = None
    auth_provider: str = "local"
    auth_provider_id: str | None = None
    is_verified: bool = False
    is_superuser: bool = False


class UserUpdate(BaseModel):
    email: str | None = None
    username: str | None = None
    full_name: str | None = None
    avatar_url: str | None = None
    is_active: bool | None = None
    is_verified: bool | None = None
    is_superuser: bool | None = None


class UserSetPassword(BaseModel):
    admin_password: str = Field(..., min_length=8)
    password: str = Field(..., min_length=8)


# ---- Organization schemas ----


class OrganizationResponse(BaseModel):
    id: str
    slug: str
    name: str
    description: str | None
    tier: str
    settings: dict[str, Any]
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class OrganizationCreate(BaseModel):
    name: str = Field(..., min_length=1)
    slug: str | None = None
    description: str | None = None
    tier: str = "community"
    settings: dict[str, Any] = Field(default_factory=dict)
    owner_user_id: str | None = None


class OrganizationUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    tier: str | None = None
    settings: dict[str, Any] | None = None
    is_active: bool | None = None


# ---- Membership schemas ----


class MembershipResponse(BaseModel):
    id: str
    org_id: str
    user_id: str
    role: str
    invited_by_id: str | None
    joined_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class MembershipCreate(BaseModel):
    user_id: str = Field(..., min_length=1)
    role: str = "member"
    invited_by_id: str | None = None


class MembershipUpdateRole(BaseModel):
    role: str = Field(..., min_length=1)


class OrgInviteCreate(BaseModel):
    email: str = Field(..., min_length=3)
    role: str = "member"


class OrgInviteResponse(BaseModel):
    id: str
    org_id: str
    email: str
    role: str
    invited_by_id: str | None
    status: str
    expires_at: datetime
    accepted_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class OwnershipTransfer(BaseModel):
    new_owner_user_id: str = Field(..., min_length=1)


class AuditLogResponse(BaseModel):
    id: str
    org_id: str
    user_id: str | None
    action: str
    resource_type: str
    resource_id: str
    description: str | None
    changes: dict[str, Any] | None
    request_metadata: dict[str, Any] | None
    status: str
    error_message: str | None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class AuditLogListResponse(BaseModel):
    items: list[AuditLogResponse]
    total: int
    limit: int
    offset: int


class AuditLogFilter(BaseModel):
    user_id: str | None = None
    action: str | None = None
    resource_type: str | None = None
    resource_id: str | None = None
    status: str | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None


class FeatureFlagResponse(BaseModel):
    id: str
    key: str
    name: str
    description: str | None
    category: str
    min_tier: str
    is_enabled: bool
    is_beta: bool
    is_deprecated: bool
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class FeatureOverrideCreate(BaseModel):
    feature_id: str
    is_enabled: bool = True
    expires_at: datetime | None = None
    config: dict[str, Any] | None = None
    reason: str | None = None


class FeatureOverrideResponse(BaseModel):
    id: str
    org_id: str
    feature_id: str
    feature_key: str
    is_enabled: bool
    expires_at: datetime | None
    config: dict[str, Any] | None
    reason: str | None
    created_by: str | None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class StartImpersonationRequest(BaseModel):
    target_user_id: str


class ImpersonateTargetUser(BaseModel):
    id: str
    email: str
    org_id: str
    role: str


class StartImpersonationResponse(BaseModel):
    status: str  # "active"
    target_user: ImpersonateTargetUser
    expires_at: datetime


class StopImpersonationResponse(BaseModel):
    status: str  # "stopped"


class ImpersonationStatusResponse(BaseModel):
    is_impersonating: bool
    target_user_id: str | None = None
    target_email: str | None = None
    target_org_id: str | None = None
    expires_at: datetime | None = None


# ---- IP Allowlist schemas (Enterprise feature: ip_allowlist) ----


class IPAllowlistResponse(BaseModel):
    id: str
    org_id: str
    ip_range: str
    description: str | None
    is_active: bool
    created_by_id: str | None
    created_at: datetime
    updated_at: datetime
    expires_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class IPAllowlistCreate(BaseModel):
    ip_range: str = Field(
        ..., description="IP address or CIDR range (e.g., '192.168.1.0/24')"
    )
    description: str | None = None
    expires_at: datetime | None = None


class IPAllowlistUpdate(BaseModel):
    ip_range: str | None = Field(None, description="IP address or CIDR range")
    description: str | None = None
    is_active: bool | None = None
    expires_at: datetime | None = None


class IPAllowlistListResponse(BaseModel):
    items: list[IPAllowlistResponse]
    total: int
    limit: int
    offset: int


class IPCheckRequest(BaseModel):
    ip_address: str = Field(..., description="IP address to check against allowlist")


class IPCheckResponse(BaseModel):
    allowed: bool
    ip_address: str


class RetentionPolicyResponse(BaseModel):
    id: str
    org_id: str
    resource_type: str
    retention_days: int
    description: str | None
    is_active: bool
    last_run_at: datetime | None
    last_run_deleted_count: int | None
    next_run_at: datetime | None
    created_by_id: str | None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class RetentionPolicyCreate(BaseModel):
    resource_type: str = Field(
        ..., description="Type of resource to apply retention to"
    )
    retention_days: int = Field(90, ge=1, description="Number of days to retain data")
    description: str | None = None


class RetentionPolicyUpdate(BaseModel):
    retention_days: int | None = Field(
        None, ge=1, description="Number of days to retain data"
    )
    description: str | None = None
    is_active: bool | None = None


class RetentionPolicyListResponse(BaseModel):
    items: list[RetentionPolicyResponse]
    total: int
    limit: int
    offset: int


class RetentionExecuteResponse(BaseModel):
    deleted_count: int
    error: str | None = None


class PlatformStatsResponse(BaseModel):
    total_organizations: int
    active_organizations: int
    total_users: int
    active_users: int
    superuser_count: int
    total_memberships: int
    tier_distribution: dict[str, int]
    total_sync_configs: int
    active_sync_configs: int
    recent_syncs_success: int
    recent_syncs_failed: int
