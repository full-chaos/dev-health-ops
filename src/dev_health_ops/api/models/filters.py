from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field


class TimeFilter(BaseModel):
    range_days: int = 14
    compare_days: int = 14
    start_date: date | None = None
    end_date: date | None = None


class ScopeFilter(BaseModel):
    level: Literal["org", "team", "repo", "service", "developer"] = "org"
    ids: list[str] = Field(default_factory=list)


class WhoFilter(BaseModel):
    developers: list[str] | None = None
    roles: list[str] | None = None


class WhatFilter(BaseModel):
    repos: list[str] | None = None
    services: list[str] | None = None
    artifacts: list[Literal["pr", "issue", "commit", "pipeline"]] | None = None


class WhyFilter(BaseModel):
    work_category: list[str] | None = None
    issue_type: list[str] | None = None
    initiative: list[str] | None = None


class HowFilter(BaseModel):
    flow_stage: list[str] | None = None
    blocked: bool | None = None
    wip_state: list[str] | None = None


class MetricFilter(BaseModel):
    time: TimeFilter = Field(default_factory=TimeFilter)
    scope: ScopeFilter = Field(default_factory=ScopeFilter)
    who: WhoFilter = Field(default_factory=WhoFilter)
    what: WhatFilter = Field(default_factory=WhatFilter)
    why: WhyFilter = Field(default_factory=WhyFilter)
    how: HowFilter = Field(default_factory=HowFilter)


class HomeRequest(BaseModel):
    filters: MetricFilter


class ExplainRequest(BaseModel):
    metric: str
    filters: MetricFilter


class InvestmentExplainRequest(BaseModel):
    theme: str | None = None
    subcategory: str | None = None
    filters: MetricFilter = Field(default_factory=MetricFilter)
    llm_model: str | None = None


class InvestmentFlowRequest(BaseModel):
    filters: MetricFilter = Field(default_factory=MetricFilter)
    theme: str | None = None
    flow_mode: (
        Literal[
            "team_category_repo",
            "team_subcategory_repo",
            "team_category_subcategory_repo",
        ]
        | None
    ) = None
    drill_category: str | None = None
    top_n_repos: int = 12


class DrilldownRequest(BaseModel):
    filters: MetricFilter
    sort: str | None = None
    limit: int | None = None


class WorkUnitRequest(BaseModel):
    filters: MetricFilter
    limit: int | None = None
    include_textual: bool | None = None


class FilterOptionsResponse(BaseModel):
    teams: list[str]
    repos: list[str]
    services: list[str]
    developers: list[str]
    work_category: list[str]
    issue_type: list[str]
    flow_stage: list[str]


class SankeyContext(BaseModel):
    entity_id: str | None = None
    entity_label: str | None = None


class SankeyRequest(BaseModel):
    mode: Literal["investment", "expense", "state", "hotspot"]
    filters: MetricFilter
    context: SankeyContext | None = None
    window_start: date | None = None
    window_end: date | None = None
