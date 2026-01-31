# Refactoring Plan: dev-health-ops

**Created**: 2026-01-30  
**Status**: Proposed  
**Estimated Effort**: 2-3 weeks  

---

## Executive Summary

This plan merges two independent analyses to create a comprehensive refactoring roadmap:
1. **User's plan**: API helper dedupe, DB URL normalization, processor unification, storage decomposition
2. **Codebase analysis**: Exception handling, provider normalization, test coverage gaps

**Goal**: Reduce technical debt while maintaining stability through incremental, well-tested changes.

---

## Phase 1A — Quick Wins: Utility Consolidation (Low Risk)

**Objective**: Extract duplicated helper functions into shared modules.  
**Effort**: 2-3 days  
**Risk**: Low (import path changes only)

### Task 1.1: DB URL Normalization → `metrics/db_utils.py`

**Problem**: `_normalize_sqlite_url()` and `_normalize_postgres_url()` duplicated in 5 files.

**Files to refactor**:
- `src/dev_health_ops/metrics/job_daily.py:59-62`
- `src/dev_health_ops/metrics/job_complexity.py:20-32`
- `src/dev_health_ops/metrics/sinks/sqlite.py:10-14`
- `src/dev_health_ops/metrics/sinks/postgres.py:21-28`
- `src/dev_health_ops/audit/rolling_aggregates.py:43-47`

**Action**: Create `src/dev_health_ops/metrics/db_utils.py`:
```python
"""Database URL normalization utilities."""

def normalize_sqlite_url(db_url: str) -> str:
    """Convert async SQLite URL to sync for clickhouse-connect compatibility."""
    if "sqlite+aiosqlite://" in db_url:
        return db_url.replace("sqlite+aiosqlite://", "sqlite://", 1)
    return db_url

def normalize_postgres_url(db_url: str) -> str:
    """Convert async Postgres URL to sync."""
    if "postgresql+asyncpg://" in db_url:
        return db_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    return db_url

def normalize_db_url(db_url: str) -> str:
    """Normalize any async DB URL to sync equivalent."""
    url = normalize_sqlite_url(db_url)
    url = normalize_postgres_url(url)
    return url
```

**Verification**: `pytest tests/test_metrics_sinks.py tests/test_storage.py tests/test_job_daily_rolling.py`

---

### Task 1.2: API Metric Configurations → `api/services/metric_registry.py`

**Problem**: `_METRICS`, `_PERSON_METRICS`, `_METRIC_CONFIG` define overlapping metric schemas.

**Files to refactor**:
- `src/dev_health_ops/api/services/home.py:32-108` (`_METRICS`)
- `src/dev_health_ops/api/services/people.py:60-200` (`_PERSON_METRICS`)
- `src/dev_health_ops/api/services/explain.py:15-96` (`_METRIC_CONFIG`)

**Action**: Create `src/dev_health_ops/api/services/metric_registry.py`:
```python
"""Centralized metric configuration registry."""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass

@dataclass
class MetricDefinition:
    metric: str
    label: str
    unit: str
    table: str
    column: str
    aggregator: str
    scope: str  # "team" | "repo" | "person"
    transform: Callable[[float], float] = lambda v: v
    group_by: Optional[str] = None
    identity_column: Optional[str] = None
    extra_where: Optional[str] = None

# Single source of truth for all metrics
METRIC_REGISTRY: Dict[str, MetricDefinition] = {
    "cycle_time": MetricDefinition(
        metric="cycle_time",
        label="Cycle Time",
        unit="days",
        table="work_item_metrics_daily",
        column="cycle_time_p50_hours",
        aggregator="avg",
        scope="team",
        transform=lambda v: v / 24.0,
        group_by="team_id",
    ),
    # ... additional metrics
}

def get_metric(name: str) -> MetricDefinition: ...
def list_metrics(scope: Optional[str] = None) -> List[MetricDefinition]: ...
```

**Verification**: `pytest tests/test_api_endpoints.py tests/test_people_endpoints.py`

---

## Phase 1B — Quick Wins: Code Hygiene (Low Risk)

**Objective**: Fix exception handling and abstract method patterns.  
**Effort**: 1-2 days  
**Risk**: Low (no behavioral changes)

### Task 1.3: Replace Bare Exception Handlers

**Problem**: 78 instances of `except Exception:` mask specific errors.

**Priority files** (highest instance count):
| File | Count | Action |
|------|-------|--------|
| `api/main.py` | 8 | Replace with specific types per handler |
| `processors/gitlab.py` | 4 | Use `(GitLabError, ConnectionError, TimeoutError)` |
| `storage.py` | 3 | Use `(SQLAlchemyError, ValueError)` |
| `processors/github.py` | 2 | Use `(GithubException, ConnectionError)` |

**Pattern to follow**:
```python
# Before
except Exception:
    pass

# After
except (ValueError, KeyError, json.JSONDecodeError) as e:
    logger.warning(f"Failed to parse response: {e}")
```

**Verification**: `pytest tests/ -x` (ensure no regressions)

---

### Task 1.4: Standardize Abstract Methods

**Problem**: 50 instances of bare `pass` in abstract methods; should use `...` (Ellipsis).

**Files to update**:
- `src/dev_health_ops/metrics/sinks/base.py` (14 instances)
- `src/dev_health_ops/connectors/base.py` (8 instances)
- `src/dev_health_ops/providers/base.py` (6 instances)

**Pattern**:
```python
# Before
@abstractmethod
def write_repo_metrics(self, rows: Sequence[RepoMetricsDailyRecord]) -> None:
    pass

# After
@abstractmethod
def write_repo_metrics(self, rows: Sequence[RepoMetricsDailyRecord]) -> None:
    ...
```

**Verification**: `python -c "from dev_health_ops.metrics.sinks.base import BaseMetricsSink"` (import check)

---

## Phase 1C — Quick Wins: Provider Normalization (Low Risk)

**Objective**: Extract duplicated datetime/normalization utilities from provider modules.  
**Effort**: 1-2 days  
**Risk**: Low

### Task 1.5: Provider Normalization → `providers/normalize_common.py`

**Problem**: `_to_utc()`, `_parse_datetime()`, similar functions duplicated across 3 files (2,495 LOC total).

**Files to refactor**:
- `src/dev_health_ops/providers/github/normalize.py` (944 lines)
- `src/dev_health_ops/providers/gitlab/normalize.py` (795 lines)
- `src/dev_health_ops/providers/jira/normalize.py` (756 lines)

**Action**: Create `src/dev_health_ops/providers/normalize_common.py`:
```python
"""Shared normalization utilities for all providers."""

from datetime import datetime, timezone
from typing import Any, Optional

def to_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Normalize datetime to UTC timezone."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def parse_datetime(value: Any) -> Optional[datetime]:
    """Parse various datetime formats to datetime object."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return to_utc(value)
    if isinstance(value, str):
        # Handle ISO format with various timezone suffixes
        try:
            return to_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            return None
    return None

def normalize_email(email: Optional[str]) -> Optional[str]:
    """Lowercase and strip email addresses."""
    return email.lower().strip() if email else None

def update_transitions_work_item_id(transitions: list, work_item_id: str) -> list:
    """Set work_item_id on all transitions."""
    for t in transitions:
        t.work_item_id = work_item_id
    return transitions
```

**Verification**: `pytest tests/test_github_provider.py tests/test_gitlab_provider.py tests/test_jira_extras.py`

---

## Phase 2 — Processor Unification (Medium Risk)

**Objective**: Reduce duplicated ingest orchestration between GitHub and GitLab processors.  
**Effort**: 3-4 days  
**Risk**: Medium (ingest behavior changes if not careful)

### Task 2.1: Extract Common Fetch Patterns → `processors/fetch_utils.py`

**Problem**: `github.py` (1407 lines) and `gitlab.py` (1362 lines) share:
- Pagination handling
- Rate limit backoff
- Batch processing patterns
- Progress reporting

**Action**: Create `src/dev_health_ops/processors/fetch_utils.py`:
```python
"""Common fetch/backfill utilities for processors."""

from typing import AsyncIterator, Callable, TypeVar
import asyncio
import logging

T = TypeVar("T")

async def fetch_with_backoff(
    fetch_fn: Callable[..., AsyncIterator[T]],
    max_retries: int = 3,
    initial_delay: float = 1.0,
) -> AsyncIterator[T]:
    """Fetch with exponential backoff on rate limits."""
    ...

async def batch_process(
    items: list,
    processor: Callable,
    batch_size: int = 100,
    progress_callback: Optional[Callable] = None,
) -> list:
    """Process items in batches with progress reporting."""
    ...

def paginate_all(
    fetch_page: Callable[[int], list],
    per_page: int = 100,
    max_pages: Optional[int] = None,
) -> Iterator:
    """Paginate through all results from a paginated API."""
    ...
```

**Verification**: `pytest tests/test_processors_pr_mr_rate_limit.py tests/test_github_connector.py tests/test_gitlab_connector.py`

---

### Task 2.2: Shared Base Processor Class

**Action**: Create `src/dev_health_ops/processors/base.py`:
```python
"""Base class for provider processors."""

from abc import ABC, abstractmethod
from typing import Any, Optional
from datetime import datetime

class BaseProcessor(ABC):
    """Abstract base for all provider processors."""
    
    def __init__(self, store: Any, since: Optional[datetime] = None):
        self.store = store
        self.since = since
    
    @abstractmethod
    async def process_commits(self, **kwargs) -> int:
        """Process and store commits. Returns count."""
        ...
    
    @abstractmethod
    async def process_pull_requests(self, **kwargs) -> int:
        """Process and store PRs/MRs. Returns count."""
        ...
    
    async def run_full_sync(self, **kwargs) -> dict:
        """Run complete sync pipeline."""
        results = {}
        results["commits"] = await self.process_commits(**kwargs)
        results["pull_requests"] = await self.process_pull_requests(**kwargs)
        return results
```

---

## Phase 3 — Storage/Sink Decomposition (Medium-High Risk)

**Objective**: Split monolithic storage files for maintainability.  
**Effort**: 5-7 days  
**Risk**: Medium-High (persistence layer changes)

### Task 3.1: Split `storage.py` (3,500 lines)

**Current structure**: Single file with SQLAlchemy, Mongo, ClickHouse implementations.

**Target structure**:
```
src/dev_health_ops/storage/
├── __init__.py          # Public API: create_store(), detect_db_type()
├── base.py              # Abstract DataStore interface
├── sqlalchemy.py        # SQLAlchemyStore (Postgres + SQLite)
├── mongo.py             # MongoStore
├── clickhouse.py        # ClickHouseStore
└── utils.py             # Shared utilities (_parse_date_value, etc.)
```

**Migration strategy**:
1. Create `storage/` package with re-exports in `__init__.py`
2. Move each store class to its own file
3. Keep `storage.py` as thin re-export layer for backward compatibility
4. Deprecation warning for direct `storage.py` imports

**Verification**: `pytest tests/test_storage.py tests/test_stores_contract.py`

---

### Task 3.2: Refactor `sqlalchemy_base.py` (2,667 lines)

**Problem**: Single class with 61 methods handling all metric types.

**Target structure**:
```
src/dev_health_ops/metrics/sinks/
├── sqlalchemy_base.py   # Base class + core methods only (~500 lines)
├── sqlalchemy_mixins/
│   ├── __init__.py
│   ├── repo_metrics.py      # write_repo_metrics, write_commit_metrics
│   ├── work_item_metrics.py # write_work_item_*, cycle times
│   ├── review_metrics.py    # write_review_edges, collaboration
│   ├── dora_metrics.py      # write_cicd_metrics, deploy, incidents
│   ├── complexity_metrics.py # write_file_complexity_*
│   └── investment_metrics.py # write_investment_*, work_unit_*
└── postgres.py          # PostgresMetricsSink(SQLAlchemyMetricsSink)
└── sqlite.py            # SQLiteMetricsSink(SQLAlchemyMetricsSink)
```

**Verification**: `pytest tests/test_metrics_sinks.py tests/test_mongo_work_unit_investments.py`

---

## Phase 4A — Resolve TODOs (Medium Risk)

**Objective**: Address explicit TODO comments in codebase.  
**Effort**: 2-3 days  
**Risk**: Medium (behavioral changes to metrics)

### Task 4.1: Pass Team Map in Daily Metrics

**Location**: `src/dev_health_ops/metrics/job_daily.py:411`
```python
team_map={},  # TODO: Pass actual team map if available
```

**Action**: 
1. Load team map from database at job start
2. Pass to `compute_ic_metrics_daily()` 
3. Use for identity→team resolution

**Verification**: `pytest tests/test_job_daily_rolling.py tests/test_work_item_metrics_compute.py`

---

### Task 4.2: Improve Work-Item ↔ Git Identity Mapping

**Location**: `src/dev_health_ops/metrics/compute_ic.py:52`
```python
# TODO: Find a way to map JIRA/LinearB/whatever to git commits more reliably.
```

**Action**:
1. Implement fuzzy email matching with Levenshtein distance
2. Add identity alias table lookup
3. Fall back to display name matching

**Verification**: `pytest tests/test_work_item_metrics_compute.py tests/test_work_item_state_durations_compute.py`

---

## Phase 4B — Test Coverage Gaps (Medium Risk)

**Objective**: Add tests for completely untested provider layer.  
**Effort**: 3-5 days  
**Risk**: Medium (no code changes, but reveals bugs)

### Task 4.3: Provider Layer Tests (18 Files, 0% Coverage)

**Critical untested files**:
| Provider | Files | Priority |
|----------|-------|----------|
| GitHub | `client.py`, `normalize.py`, `provider.py` | High |
| GitLab | `client.py`, `normalize.py`, `provider.py` | High |
| Jira | `client.py`, `normalize.py`, `provider.py`, `atlassian_compat.py` | High |
| Linear | `client.py`, `normalize.py`, `provider.py` | Medium |
| Base | `base.py`, `registry.py`, `identity.py`, `status_mapping.py`, `teams.py` | Medium |

**Action**: Create test files:
- `tests/providers/test_github_provider.py`
- `tests/providers/test_gitlab_provider.py`
- `tests/providers/test_jira_provider.py`
- `tests/providers/test_linear_provider.py`
- `tests/providers/test_base.py`

**Test patterns to use**:
```python
@pytest.fixture
def mock_github_client():
    with patch("dev_health_ops.providers.github.client.Github") as mock:
        yield mock

def test_normalize_github_issue(mock_github_client):
    # Test normalization of GitHub issue to WorkItem
    ...
```

**Verification**: `pytest tests/providers/ --cov=src/dev_health_ops/providers --cov-report=term-missing`

---

## Summary: Effort & Dependencies

```
┌─────────────────────────────────────────────────────────────────────┐
│                    REFACTORING DEPENDENCY GRAPH                     │
└─────────────────────────────────────────────────────────────────────┘

Phase 1A ──┬──> Phase 2 ──> Phase 3 ──> Phase 4A
Phase 1B ──┤                              │
Phase 1C ──┘                              v
                                      Phase 4B (can run in parallel)

Timeline:
  Week 1: Phase 1A + 1B + 1C (Quick wins)
  Week 2: Phase 2 (Processor unification)  
  Week 3: Phase 3 (Storage decomposition)
  Week 4: Phase 4A + 4B (TODOs + Tests)
```

| Phase | Effort | Risk | Files Changed | Tests Required |
|-------|--------|------|---------------|----------------|
| 1A | 2-3 days | Low | ~10 | Existing |
| 1B | 1-2 days | Low | ~15 | Existing |
| 1C | 1-2 days | Low | ~5 | Existing |
| 2 | 3-4 days | Medium | ~6 | Existing + New |
| 3 | 5-7 days | Medium-High | ~15 | Existing |
| 4A | 2-3 days | Medium | ~3 | Existing |
| 4B | 3-5 days | Medium | ~0 (tests only) | New |

**Total**: ~20-26 days (~3-4 weeks with buffer)

---

## Success Criteria

- [ ] All `_normalize_*_url()` functions consolidated to single module
- [ ] Metric configurations in single registry, not 3 files
- [ ] Zero bare `except Exception:` handlers
- [ ] Abstract methods use `...` not `pass`
- [ ] Provider normalization utilities in shared module
- [ ] Processor fetch patterns in shared module
- [ ] `storage.py` split into package with < 500 LOC per file
- [ ] `sqlalchemy_base.py` split into mixins with < 400 LOC per file
- [ ] Both TODOs resolved with tests
- [ ] Provider layer test coverage > 80%
- [ ] All existing tests passing after each phase
