"""Strict cross-language scope contract for dormant remaining-metrics jobs."""

from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SCOPE_VERSION = 1


def _date_text(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("must be an ISO calendar date") from exc
    if parsed.isoformat() != value:
        raise ValueError("must use canonical YYYY-MM-DD form")
    return value


def _uuid_text(value: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise ValueError("must be a UUID") from exc
    if str(parsed) != value:
        raise ValueError("must use canonical lowercase UUID form")
    return value


def _bounded_text(value: str, *, maximum: int) -> str:
    length = len(value.encode())
    if length == 0 or length > maximum:
        raise ValueError(f"must contain between 1 and {maximum} bytes")
    return value


class _StrictScope(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    version: Literal[1]
    _family: ClassVar[str]

    def canonical(self) -> str:
        return json.dumps(
            self.model_dump(mode="json", exclude_none=True, exclude_defaults=True),
            sort_keys=True,
            separators=(",", ":"),
        )


class CapacityScope(_StrictScope):
    _family = "capacity"

    team_id: str | None = None
    work_scope_id: str | None = None
    target_items: int | None = Field(default=None, gt=0, le=1_000_000)
    target_date: str | None = None
    history_days: int = Field(ge=1, le=365)
    simulations: int = Field(ge=100, le=100_000)
    all_teams: bool

    @field_validator("team_id", "work_scope_id")
    @classmethod
    def validate_uuid(cls, value: str | None) -> str | None:
        return None if value is None else _uuid_text(value)

    @field_validator("target_date")
    @classmethod
    def validate_date(cls, value: str | None) -> str | None:
        return None if value is None else _date_text(value)

    @model_validator(mode="after")
    def validate_selector(self) -> CapacityScope:
        if self.all_teams and (
            self.team_id is not None or self.work_scope_id is not None
        ):
            raise ValueError("all_teams cannot be combined with a team or work scope")
        if not self.all_teams and self.team_id is None and self.work_scope_id is None:
            raise ValueError("a team or work scope is required")
        return self


class ComplexityScope(_StrictScope):
    _family = "complexity"

    day: str
    backfill_days: Literal[1]
    repo_id: str | None = None
    search_pattern: str | None = None
    language_globs: list[str] = Field(default_factory=list)
    exclude_globs: list[str] = Field(default_factory=list)
    max_files: int | None = Field(default=None, gt=0, le=1_000_000)

    @field_validator("day")
    @classmethod
    def validate_day(cls, value: str) -> str:
        return _date_text(value)

    @field_validator("repo_id")
    @classmethod
    def validate_repo_id(cls, value: str | None) -> str | None:
        return None if value is None else _uuid_text(value)

    @field_validator("search_pattern")
    @classmethod
    def validate_pattern(cls, value: str | None) -> str | None:
        return None if value is None else _bounded_text(value, maximum=256)

    @field_validator("language_globs", "exclude_globs")
    @classmethod
    def validate_globs(cls, value: list[str]) -> list[str]:
        if len(value) > 32:
            raise ValueError("at most 32 globs are allowed")
        for item in value:
            _bounded_text(item, maximum=256)
        return value


class DoraScope(_StrictScope):
    _family = "dora"

    day: str
    backfill_days: int = Field(ge=1, le=90)
    repo_id: str | None = None
    repo_name: str | None = None
    sink: Literal["auto", "clickhouse"]
    metrics: str | None = None
    interval: Literal["daily", "weekly", "monthly"]

    @field_validator("day")
    @classmethod
    def validate_day(cls, value: str) -> str:
        return _date_text(value)

    @field_validator("repo_id")
    @classmethod
    def validate_repo_id(cls, value: str | None) -> str | None:
        return None if value is None else _uuid_text(value)

    @field_validator("repo_name", "metrics")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        return None if value is None else _bounded_text(value, maximum=256)


class ReleaseImpactScope(_StrictScope):
    _family = "release_impact"

    day: str
    backfill_days: int = Field(ge=1, le=90)
    recomputation_window_days: int = Field(ge=1, le=30)

    @field_validator("day")
    @classmethod
    def validate_day(cls, value: str) -> str:
        return _date_text(value)


class RecommendationsScope(_StrictScope):
    _family = "recommendations"

    window: int = Field(ge=1, le=90)
    team_id: str | None = None
    as_of: str | None = None

    @field_validator("team_id")
    @classmethod
    def validate_team_id(cls, value: str | None) -> str | None:
        return None if value is None else _uuid_text(value)

    @field_validator("as_of")
    @classmethod
    def validate_as_of(cls, value: str | None) -> str | None:
        return None if value is None else _date_text(value)


class MembershipBackfillScope(_StrictScope):
    _family = "membership_backfill"

    repo_ids: list[str] = Field(default_factory=list, max_length=256)

    @field_validator("repo_ids")
    @classmethod
    def validate_repo_ids(cls, value: list[str]) -> list[str]:
        canonical = [_uuid_text(item) for item in value]
        if len(set(canonical)) != len(canonical):
            raise ValueError("repo_ids must be unique")
        return canonical


class _DailyFamilyScope(_StrictScope):
    day: str
    backfill_days: int = Field(ge=1, le=30)
    repo_id: str | None = None
    repo_name: str | None = None
    sink: Literal["auto", "clickhouse"]
    provider: Literal["auto", "all", "jira", "github", "gitlab", "none"]

    @field_validator("day")
    @classmethod
    def validate_day(cls, value: str) -> str:
        return _date_text(value)

    @field_validator("repo_id")
    @classmethod
    def validate_repo_id(cls, value: str | None) -> str | None:
        return None if value is None else _uuid_text(value)

    @field_validator("repo_name")
    @classmethod
    def validate_repo_name(cls, value: str | None) -> str | None:
        return None if value is None else _bounded_text(value, maximum=256)


class ExtraMetricsScope(_DailyFamilyScope):
    _family = "extra_metrics"


class TeamMetricsScope(_DailyFamilyScope):
    _family = "team_metrics"


SCOPE_MODELS: dict[str, type[_StrictScope]] = {
    model._family: model
    for model in (
        CapacityScope,
        ComplexityScope,
        DoraScope,
        ReleaseImpactScope,
        RecommendationsScope,
        MembershipBackfillScope,
        ExtraMetricsScope,
        TeamMetricsScope,
    )
}


def parse_scope(family: str, value: Any) -> _StrictScope:
    """Validate a persisted scope without coercion or family inference."""

    try:
        model = SCOPE_MODELS[family]
    except KeyError as exc:
        raise ValueError("unknown remaining metrics family") from exc
    return model.model_validate(value)


def canonical_scope(family: str, value: Any) -> str:
    """Return the exact canonical JSON shared with the Go producer."""

    return parse_scope(family, value).canonical()
