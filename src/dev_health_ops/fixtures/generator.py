"""Synthetic data generator composer.

Domain-specific generator methods live in :mod:`dev_health_ops.fixtures.generators`.
This module composes them into :class:`SyntheticDataGenerator` and keeps the
public import path stable (``from dev_health_ops.fixtures.generator import
SyntheticDataGenerator``).
"""

from __future__ import annotations

import random
import uuid

from dev_health_ops.fixtures.generators import (
    BaseGeneratorMixin,
    CommitsGeneratorMixin,
    IncidentsGeneratorMixin,
    InteractionsGeneratorMixin,
    InvestmentsGeneratorMixin,
    PipelinesGeneratorMixin,
    PrsGeneratorMixin,
    TeamsGeneratorMixin,
    WorkItemsGeneratorMixin,
)
from dev_health_ops.models.teams import Team

__all__ = ["SyntheticDataGenerator"]


class SyntheticDataGenerator(
    CommitsGeneratorMixin,
    PrsGeneratorMixin,
    PipelinesGeneratorMixin,
    IncidentsGeneratorMixin,
    WorkItemsGeneratorMixin,
    InteractionsGeneratorMixin,
    InvestmentsGeneratorMixin,
    TeamsGeneratorMixin,
    BaseGeneratorMixin,
):
    def __init__(
        self,
        repo_name: str = "acme/demo-app",
        repo_id: uuid.UUID | None = None,
        provider: str = "synthetic",
        seed: int | None = None,
        assigned_teams: list[Team] | None = None,
    ):
        self.repo_name = repo_name
        self.assigned_teams = assigned_teams
        if repo_id:
            self.repo_id = repo_id
        else:
            # Deterministic UUID based on repo name
            namespace = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
            self.repo_id = uuid.uuid5(namespace, repo_name)
        self.provider = provider
        seed_value = int(seed) if seed is not None else int(self.repo_id.int % (2**32))
        random.seed(seed_value)
        self.authors = [
            ("Alice Smith", "alice@example.com"),
            ("Bob Jones", "bob@example.com"),
            ("Charlie Brown", "charlie@example.com"),
            ("David White", "david@example.com"),
            ("Eve Black", "eve@example.com"),
            ("Frank Green", "frank@example.com"),
            ("Grace Hall", "grace@example.com"),
            ("Heidi Blue", "heidi@example.com"),
            ("Ivan Red", "ivan@example.com"),
            ("Judy Orange", "judy@example.com"),
            ("Kevin Purple", "kevin@example.com"),
            ("Liam Cyan", "liam@example.com"),
            ("Mia Magenta", "mia@example.com"),
            ("Noah Yellow", "noah@example.com"),
            ("Olivia Gray", "olivia@example.com"),
            ("Pat Lime", "pat@example.com"),
        ]
        # Randomize authors order to vary team composition
        random.shuffle(self.authors)
        self.unassigned_authors = [
            ("Unaffiliated One", "unassigned1@example.com"),
            ("Unaffiliated Two", "unassigned2@example.com"),
            ("Unaffiliated Three", "unassigned3@example.com"),
        ]
        self.repo_authors = self._resolve_repo_authors()
        self.files = [
            "src/main.py",
            "src/utils.py",
            "src/models.py",
            "src/api/routes.py",
            "src/api/auth.py",
            "src/api/dependencies.py",
            "src/api/health.py",
            "src/api/errors.py",
            "src/services/user_service.py",
            "src/services/metrics_service.py",
            "src/services/review_service.py",
            "src/db/session.py",
            "src/db/models/user.py",
            "src/db/models/repo.py",
            "src/db/models/work_item.py",
            "src/workflows/ingest.py",
            "src/workflows/compute.py",
            "src/workflows/publish.py",
            "src/utils/time.py",
            "src/utils/metrics.py",
            "src/utils/strings.py",
            "src/config/settings.py",
            "src/config/logging.py",
            "src/clients/github.py",
            "src/clients/gitlab.py",
            "src/clients/jira.py",
            "tests/test_main.py",
            "tests/test_api_routes.py",
            "tests/test_metrics_daily.py",
            "tests/test_hotspots.py",
            "tests/test_blame_loader.py",
            "README.md",
            "README_CONTRIBUTING.md",
            "docs/architecture.md",
            "docs/metrics.md",
            "docs/workflows.md",
            "docs/usage.md",
            "docker-compose.yml",
            "Dockerfile",
            ".github/workflows/ci.yml",
            ".github/workflows/release.yml",
        ]
