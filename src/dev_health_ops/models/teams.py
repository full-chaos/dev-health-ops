from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

import uuid
from sqlalchemy import Column, Text, DateTime, JSON
from dev_health_ops.models.git import Base, GUID

class Team(Base):
    __tablename__ = "teams"

    id = Column(Text, primary_key=True, comment="Unique team identifier (slug)")
    team_uuid = Column(GUID, unique=True, default=uuid.uuid4, comment="Internal unique identifier")
    name = Column(Text, nullable=False, comment="Team display name")
    description = Column(Text, nullable=True, comment="Team description")
    members = Column(JSON, default=list, comment="List of member identities")
    updated_at = Column(
        DateTime(timezone=True), 
        default=lambda: datetime.now(timezone.utc), 
        onupdate=lambda: datetime.now(timezone.utc)
    )

    def __init__(
        self, 
        id: str, 
        name: str, 
        description: Optional[str] = None, 
        members: Optional[List[str]] = None, 
        updated_at: Optional[datetime] = None,
        team_uuid: Optional[uuid.UUID] = None
    ):
        self.id = id
        self.team_uuid = team_uuid or uuid.uuid4()
        self.name = name
        self.description = description
        self.members = members or []
        self.updated_at = updated_at or datetime.now(timezone.utc)