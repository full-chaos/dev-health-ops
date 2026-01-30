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
