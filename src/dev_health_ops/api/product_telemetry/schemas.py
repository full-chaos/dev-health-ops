from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

TelemetryEventName = Literal[
    "page_viewed",
    "feature_viewed",
    "filter_changed",
    "chart_interacted",
    "navigation_interacted",
    "guide_opened",
    "session_started",
    "session_ended",
    "client_error",
]


class ProductTelemetryEvent(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: TelemetryEventName
    schema_version: str = Field(alias="schemaVersion")
    event_id: str = Field(alias="eventId")
    ts: datetime
    session_id: str = Field(alias="sessionId")
    anonymous_user_id: str = Field(alias="anonymousUserId")
    org_id_hash: str | None = Field(default=None, alias="orgIdHash")
    route_pattern: str | None = Field(default=None, alias="routePattern")
    payload: dict[str, str | int | float | bool | None]


class ProductTelemetryBatch(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    org_id_hash: str | None = Field(default=None, alias="orgIdHash")
    source: Literal["dev-health-web"] = "dev-health-web"
    events: list[ProductTelemetryEvent] = Field(..., min_length=1, max_length=500)


class ProductTelemetryAccepted(BaseModel):
    ingestion_id: str
    status: Literal["accepted"] = "accepted"
    items_received: int
    stream: str
