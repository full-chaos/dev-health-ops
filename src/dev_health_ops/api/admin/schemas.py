from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class SettingResponse(BaseModel):
    key: str
    value: Optional[str]
    category: str
    is_encrypted: bool
    description: Optional[str]

    class Config:
        from_attributes = True


class SettingCreate(BaseModel):
    key: str = Field(..., min_length=1, max_length=255)
    value: Optional[str] = None
    category: str = "general"
    encrypt: bool = False
    description: Optional[str] = None


class SettingUpdate(BaseModel):
    value: Optional[str] = None
    encrypt: Optional[bool] = None
    description: Optional[str] = None


class SettingsListResponse(BaseModel):
    category: str
    settings: list[SettingResponse]


class IntegrationCredentialResponse(BaseModel):
    id: str
    provider: str
    name: str
    is_active: bool
    config: dict[str, Any]
    last_test_at: Optional[datetime]
    last_test_success: Optional[bool]
    last_test_error: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class IntegrationCredentialCreate(BaseModel):
    provider: str = Field(..., min_length=1)
    name: str = Field(default="default", min_length=1)
    credentials: dict[str, Any] = Field(
        ..., description="Provider credentials (will be encrypted)"
    )
    config: Optional[dict[str, Any]] = Field(
        default=None, description="Non-sensitive configuration"
    )


class IntegrationCredentialUpdate(BaseModel):
    credentials: Optional[dict[str, Any]] = None
    config: Optional[dict[str, Any]] = None
    is_active: Optional[bool] = None


class TestConnectionRequest(BaseModel):
    provider: str
    name: str = "default"


class TestConnectionResponse(BaseModel):
    success: bool
    error: Optional[str] = None
    details: Optional[dict[str, Any]] = None


class SyncConfigResponse(BaseModel):
    id: str
    name: str
    provider: str
    credential_id: Optional[str]
    sync_targets: list[str]
    sync_options: dict[str, Any]
    is_active: bool
    last_sync_at: Optional[datetime]
    last_sync_success: Optional[bool]
    last_sync_error: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class SyncConfigCreate(BaseModel):
    name: str = Field(..., min_length=1)
    provider: str = Field(..., min_length=1)
    credential_id: Optional[str] = None
    sync_targets: list[str] = Field(default_factory=list)
    sync_options: dict[str, Any] = Field(default_factory=dict)


class SyncConfigUpdate(BaseModel):
    sync_targets: Optional[list[str]] = None
    sync_options: Optional[dict[str, Any]] = None
    is_active: Optional[bool] = None


class IdentityMappingResponse(BaseModel):
    id: str
    canonical_id: str
    display_name: Optional[str]
    email: Optional[str]
    provider_identities: dict[str, list[str]]
    team_ids: list[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class IdentityMappingCreate(BaseModel):
    canonical_id: str = Field(..., min_length=1)
    display_name: Optional[str] = None
    email: Optional[str] = None
    provider_identities: dict[str, list[str]] = Field(default_factory=dict)
    team_ids: list[str] = Field(default_factory=list)


class IdentityMappingUpdate(BaseModel):
    display_name: Optional[str] = None
    email: Optional[str] = None
    provider_identities: Optional[dict[str, list[str]]] = None
    team_ids: Optional[list[str]] = None


class TeamMappingResponse(BaseModel):
    id: str
    team_id: str
    name: str
    description: Optional[str]
    repo_patterns: list[str]
    project_keys: list[str]
    extra_data: dict[str, Any]
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class TeamMappingCreate(BaseModel):
    team_id: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    description: Optional[str] = None
    repo_patterns: list[str] = Field(default_factory=list)
    project_keys: list[str] = Field(default_factory=list)
    extra_data: dict[str, Any] = Field(default_factory=dict)


class TeamMappingUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    repo_patterns: Optional[list[str]] = None
    project_keys: Optional[list[str]] = None
    extra_data: Optional[dict[str, Any]] = None


# ---- User schemas ----


class UserResponse(BaseModel):
    id: str
    email: str
    username: Optional[str]
    full_name: Optional[str]
    avatar_url: Optional[str]
    auth_provider: str
    is_active: bool
    is_verified: bool
    is_superuser: bool
    last_login_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class UserCreate(BaseModel):
    email: str = Field(..., min_length=1)
    password: Optional[str] = Field(default=None, min_length=8)
    username: Optional[str] = None
    full_name: Optional[str] = None
    auth_provider: str = "local"
    auth_provider_id: Optional[str] = None
    is_verified: bool = False
    is_superuser: bool = False


class UserUpdate(BaseModel):
    email: Optional[str] = None
    username: Optional[str] = None
    full_name: Optional[str] = None
    avatar_url: Optional[str] = None
    is_active: Optional[bool] = None
    is_verified: Optional[bool] = None


class UserSetPassword(BaseModel):
    password: str = Field(..., min_length=8)


# ---- Organization schemas ----


class OrganizationResponse(BaseModel):
    id: str
    slug: str
    name: str
    description: Optional[str]
    tier: str
    settings: dict[str, Any]
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class OrganizationCreate(BaseModel):
    name: str = Field(..., min_length=1)
    slug: Optional[str] = None
    description: Optional[str] = None
    tier: str = "free"
    settings: dict[str, Any] = Field(default_factory=dict)
    owner_user_id: Optional[str] = None


class OrganizationUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    tier: Optional[str] = None
    settings: Optional[dict[str, Any]] = None
    is_active: Optional[bool] = None


# ---- Membership schemas ----


class MembershipResponse(BaseModel):
    id: str
    org_id: str
    user_id: str
    role: str
    invited_by_id: Optional[str]
    joined_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class MembershipCreate(BaseModel):
    user_id: str = Field(..., min_length=1)
    role: str = "member"
    invited_by_id: Optional[str] = None


class MembershipUpdateRole(BaseModel):
    role: str = Field(..., min_length=1)


class OwnershipTransfer(BaseModel):
    new_owner_user_id: str = Field(..., min_length=1)


class AuditLogResponse(BaseModel):
    id: str
    org_id: str
    user_id: Optional[str]
    action: str
    resource_type: str
    resource_id: str
    description: Optional[str]
    changes: Optional[dict[str, Any]]
    request_metadata: Optional[dict[str, Any]]
    status: str
    error_message: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class AuditLogListResponse(BaseModel):
    items: list[AuditLogResponse]
    total: int
    limit: int
    offset: int


class AuditLogFilter(BaseModel):
    user_id: Optional[str] = None
    action: Optional[str] = None
    resource_type: Optional[str] = None
    resource_id: Optional[str] = None
    status: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


# ---- IP Allowlist schemas (Enterprise feature: ip_allowlist) ----


class IPAllowlistResponse(BaseModel):
    id: str
    org_id: str
    ip_range: str
    description: Optional[str]
    is_active: bool
    created_by_id: Optional[str]
    created_at: datetime
    updated_at: datetime
    expires_at: Optional[datetime]

    class Config:
        from_attributes = True


class IPAllowlistCreate(BaseModel):
    ip_range: str = Field(
        ..., description="IP address or CIDR range (e.g., '192.168.1.0/24')"
    )
    description: Optional[str] = None
    expires_at: Optional[datetime] = None


class IPAllowlistUpdate(BaseModel):
    ip_range: Optional[str] = Field(None, description="IP address or CIDR range")
    description: Optional[str] = None
    is_active: Optional[bool] = None
    expires_at: Optional[datetime] = None


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
    description: Optional[str]
    is_active: bool
    last_run_at: Optional[datetime]
    last_run_deleted_count: Optional[int]
    next_run_at: Optional[datetime]
    created_by_id: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class RetentionPolicyCreate(BaseModel):
    resource_type: str = Field(
        ..., description="Type of resource to apply retention to"
    )
    retention_days: int = Field(90, ge=1, description="Number of days to retain data")
    description: Optional[str] = None


class RetentionPolicyUpdate(BaseModel):
    retention_days: Optional[int] = Field(
        None, ge=1, description="Number of days to retain data"
    )
    description: Optional[str] = None
    is_active: Optional[bool] = None


class RetentionPolicyListResponse(BaseModel):
    items: list[RetentionPolicyResponse]
    total: int
    limit: int
    offset: int


class RetentionExecuteResponse(BaseModel):
    deleted_count: int
    error: Optional[str] = None
