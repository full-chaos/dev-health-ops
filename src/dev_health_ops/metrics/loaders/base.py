"""Base utilities shared across all backend loaders."""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

from dev_health_ops.metrics.schemas import (
    CommitStatRow,
    DeploymentRow,
    IncidentRow,
    PipelineRunRow,
    PullRequestReviewRow,
    PullRequestRow,
)

if TYPE_CHECKING:
    from dev_health_ops.models.atlassian_ops import (
        AtlassianOpsAlert,
        AtlassianOpsIncident,
        AtlassianOpsSchedule,
    )
    from dev_health_ops.models.teams import JiraProjectOpsTeamLink


# Type aliases for internal data structures - using imports from dev_health_ops.metrics.schemas
# (Removed redundant definitions)


class DataLoader(Protocol):
    """Protocol defining the interface for data loaders."""

    async def load_git_rows(
        self,
        start: datetime,
        end: datetime,
        repo_id: Optional[uuid.UUID],
        repo_name: Optional[str] = None,
    ) -> Tuple[List[CommitStatRow], List[PullRequestRow], List[PullRequestReviewRow]]:
        """Load commit stats, pull requests, and reviews."""
        raise NotImplementedError()

    async def load_work_items(
        self,
        start: datetime,
        end: datetime,
        repo_id: Optional[uuid.UUID],
        repo_name: Optional[str] = None,
    ) -> Tuple[List[Any], List[Any]]:
        """Load work items and transitions."""
        raise NotImplementedError()

    async def load_cicd_data(
        self,
        start: datetime,
        end: datetime,
        repo_id: Optional[uuid.UUID],
        repo_name: Optional[str] = None,
    ) -> Tuple[List[PipelineRunRow], List[DeploymentRow]]:
        """Load CI pipeline runs and deployments."""
        raise NotImplementedError()

    async def load_incidents(
        self,
        start: datetime,
        end: datetime,
        repo_id: Optional[uuid.UUID],
        repo_name: Optional[str] = None,
    ) -> List[IncidentRow]:
        """Load incident records."""
        raise NotImplementedError()

    async def load_blame_concentration(
        self,
        repo_id: uuid.UUID,
        as_of: datetime,
    ) -> Dict[uuid.UUID, float]:
        """Load blame concentration stats for code ownership metrics."""
        raise NotImplementedError()

    async def load_atlassian_ops_incidents(
        self,
        start: datetime,
        end: datetime,
    ) -> List[AtlassianOpsIncident]:
        """Load Atlassian Ops incidents."""
        raise NotImplementedError()

    async def load_atlassian_ops_alerts(
        self,
        start: datetime,
        end: datetime,
    ) -> List[AtlassianOpsAlert]:
        """Load Atlassian Ops alerts."""
        raise NotImplementedError()

    async def load_atlassian_ops_schedules(
        self,
    ) -> List[AtlassianOpsSchedule]:
        """Load Atlassian Ops schedules."""
        raise NotImplementedError()

    async def load_jira_project_ops_team_links(
        self,
    ) -> List[JiraProjectOpsTeamLink]:
        """Load Jira project to Ops team mappings."""
        raise NotImplementedError()

    async def load_user_metrics_rolling_30d(
        self,
        as_of: date,
    ) -> List[Dict[str, Any]]:
        """Load 30-day rolling user metrics for IC landscape."""
        raise NotImplementedError()


def naive_utc(dt: datetime) -> datetime:
    """Convert a datetime to naive UTC (BSON/ClickHouse friendly)."""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def to_utc(dt: datetime) -> datetime:
    """Ensure datetime has UTC tzinfo."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_uuid(value: Any) -> Optional[uuid.UUID]:
    """Parse a value into a UUID, returning None on failure."""
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except Exception:
        return None


def safe_json_loads(value: Any) -> Any:
    """Safely parse JSON, returning None on failure."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return None


def chunked(values: Sequence[str], chunk_size: int) -> List[List[str]]:
    """Split a sequence into chunks of the given size."""
    return [list(values[i : i + chunk_size]) for i in range(0, len(values), chunk_size)]


def to_dataclass(cls: Any, row_map: Dict[str, Any]) -> Any:
    """Instantiate a dataclass from a dict, filtering unknown fields and parsing datetimes."""
    import dataclasses

    if not dataclasses.is_dataclass(cls):
        return cls(**row_map)

    field_names = {f.name for f in dataclasses.fields(cls)}
    filtered = {}
    for k, v in row_map.items():
        if k in field_names:
            if isinstance(v, str) and (
                k.endswith("_at") or k.endswith("_when") or k == "day"
            ):
                try:
                    v = datetime.fromisoformat(v.replace("Z", "+00:00"))
                    if v.tzinfo is None:
                        v = v.replace(tzinfo=timezone.utc)
                    else:
                        v = v.astimezone(timezone.utc)
                except Exception:
                    pass
            elif isinstance(v, datetime):
                # Ensure it has a timezone (assume UTC for naive DB times)
                if v.tzinfo is None:
                    v = v.replace(tzinfo=timezone.utc)
                else:
                    v = v.astimezone(timezone.utc)
            filtered[k] = v
    return cls(**filtered)


def clickhouse_query_dicts(
    client: Any, query: str, parameters: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Execute a ClickHouse query and return results as list of dicts."""
    result = client.query(query, parameters=parameters)
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]
