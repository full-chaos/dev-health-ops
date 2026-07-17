from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


@dataclass(frozen=True, slots=True)
class CanonicalTask:
    number: int
    routes: tuple[str, ...]


def viewport_widths() -> tuple[int, int, int]:
    """Return the fixed capture widths for every canonical route."""
    return (375, 768, 1280)


def viewport_height() -> int:
    """Return the fixed capture height for every canonical route."""
    return 900


def canonical_tasks() -> tuple[CanonicalTask, ...]:
    """Return the immutable five-task user-guide evidence inventory."""
    return (
        CanonicalTask(
            number=7,
            routes=(
                "/user-guide/first-10-minutes/",
                "/user-guide/how-to-read-dev-health/",
                "/user-guide/glossary/",
                "/user-guide/journeys/investment-view/",
            ),
        ),
        CanonicalTask(
            number=8,
            routes=(
                "/user-guide/views/quadrants/",
                "/user-guide/views/flame-diagrams/",
                "/user-guide/views/code-hotspots/",
            ),
        ),
        CanonicalTask(
            number=9,
            routes=(
                "/user-guide/views/pr-flow/",
                "/user-guide/views/capacity-planning/",
                "/user-guide/views/work-graph/",
            ),
        ),
        CanonicalTask(
            number=10,
            routes=(
                "/user-guide/views/ai-impact/",
                "/user-guide/views/ai-review-load/",
                "/user-guide/views/ai-risk/",
                "/user-guide/views/ai-attribution/",
            ),
        ),
        CanonicalTask(
            number=11,
            routes=(
                "/user-guide/reports/",
                "/user-guide/metrics-interpretation/",
            ),
        ),
    )


class EvidenceModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Viewport(EvidenceModel):
    width: int
    height: int


class Browser(EvidenceModel):
    engine: str = Field(min_length=1)
    version: str = Field(min_length=1)


class Console(EvidenceModel):
    errors: int = Field(ge=0)
    page_errors: int = Field(ge=0)


class Network(EvidenceModel):
    failed_requests: int = Field(ge=0)
    http_error_responses: int = Field(ge=0)


class Accessibility(EvidenceModel):
    serious: int = Field(ge=0)
    critical: int = Field(ge=0)


class Sanitization(EvidenceModel):
    status: str = Field(min_length=1)
    notes: str = Field(min_length=1)


class Artifact(EvidenceModel):
    file: str = Field(min_length=1)
    route: str = Field(min_length=1)
    viewport: Viewport
    state: str = Field(min_length=1)
    browser: Browser
    captured_at: datetime
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    console: Console
    network: Network
    a11y: Accessibility
    sanitization: Sanitization


class Source(EvidenceModel):
    head_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    capture_started_at: datetime


class Manifest(EvidenceModel):
    task: int
    source: Source
    artifacts: tuple[Artifact, ...]


@dataclass(frozen=True, slots=True)
class SourceRevision:
    head_sha: str
    committed_at: datetime
    validated_at: datetime
