from __future__ import annotations

from typing import List, Literal, Optional
from datetime import date

from pydantic import BaseModel, Field


class TimeFilter(BaseModel):
    range_days: int = 14
    compare_days: int = 14
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class ScopeFilter(BaseModel):
    level: Literal["org", "team", "repo", "service", "developer"] = "org"
    ids: List[str] = Field(default_factory=list)


class WhoFilter(BaseModel):
    developers: Optional[List[str]] = None
    roles: Optional[List[str]] = None


class WhatFilter(BaseModel):
    repos: Optional[List[str]] = None
    services: Optional[List[str]] = None
    artifacts: Optional[List[Literal["pr", "issue", "commit", "pipeline"]]] = None


class WhyFilter(BaseModel):
    work_category: Optional[List[str]] = None
    issue_type: Optional[List[str]] = None
    initiative: Optional[List[str]] = None


class HowFilter(BaseModel):
    flow_stage: Optional[List[str]] = None
    blocked: Optional[bool] = None
    wip_state: Optional[List[str]] = None


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
    theme: Optional[str] = None
    subcategory: Optional[str] = None
    filters: MetricFilter = Field(default_factory=MetricFilter)
    llm_model: Optional[str] = None


class InvestmentFlowRequest(BaseModel):
    filters: MetricFilter = Field(default_factory=MetricFilter)
    theme: Optional[str] = None
    flow_mode: Optional[
        Literal[
            "team_category_repo",
            "team_subcategory_repo",
            "team_category_subcategory_repo",
        ]
    ] = None
    drill_category: Optional[str] = None
    top_n_repos: int = 12


class DrilldownRequest(BaseModel):
    filters: MetricFilter
    sort: Optional[str] = None
    limit: Optional[int] = None


class WorkUnitRequest(BaseModel):
    filters: MetricFilter
    limit: Optional[int] = None
    include_textual: Optional[bool] = None


class FilterOptionsResponse(BaseModel):
    teams: List[str]
    repos: List[str]
    services: List[str]
    developers: List[str]
    work_category: List[str]
    issue_type: List[str]
    flow_stage: List[str]


class SankeyContext(BaseModel):
    entity_id: Optional[str] = None
    entity_label: Optional[str] = None


class SankeyRequest(BaseModel):
    mode: Literal["investment", "expense", "state", "hotspot"]
    filters: MetricFilter
    context: Optional[SankeyContext] = None
    window_start: Optional[date] = None
    window_end: Optional[date] = None
