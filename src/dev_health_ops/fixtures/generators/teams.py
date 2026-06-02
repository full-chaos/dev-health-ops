"""Teams, repo, users, teams-config, and repo metrics fixture generators."""

from __future__ import annotations

import random
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from dev_health_ops.fixtures.demo_identity import DEMO_ORG_NAME
from dev_health_ops.fixtures.generators.base import BaseGeneratorMixin
from dev_health_ops.metrics.schemas import RepoMetricsDailyRecord
from dev_health_ops.models.git import Repo
from dev_health_ops.models.teams import Team


class TeamsGeneratorMixin(BaseGeneratorMixin):
    """Generates teams, repo, users, teams-config, and repo metrics records."""

    def generate_teams(self, count: int = 2) -> list[Team]:
        """
        Generate synthetic teams with members distributed among them.
        """
        return self.get_team_assignment(count)["teams"]

    def generate_repo(self) -> Repo:
        return Repo(
            id=self.repo_id,
            repo=self.repo_name,
            ref="main",
            provider="synthetic",
            settings={
                "source": "synthetic",
                "repo_id": str(self.repo_id),
            },
            tags=["demo", "synthetic"],
        )

    def generate_teams_config(self) -> dict[str, Any]:
        """
        Generate a team mapping configuration for the synthetic users.
        """
        # Split authors into two teams
        mid = len(self.authors) // 2
        team_alpha = self.authors[:mid]
        team_beta = self.authors[mid:]

        return {
            "teams": [
                {
                    "team_id": "team-alpha",
                    "team_name": "Team Alpha",
                    "members": [email for _, email in team_alpha],
                },
                {
                    "team_id": "team-beta",
                    "team_name": "Team Beta",
                    "members": [email for _, email in team_beta],
                },
            ]
        }

    def generate_repo_metrics_daily(
        self, days: int = 30
    ) -> list[RepoMetricsDailyRecord]:
        records = []
        end_date = datetime.now(timezone.utc).date()
        for i in range(days):
            day = end_date - timedelta(days=i)
            records.append(
                RepoMetricsDailyRecord(
                    repo_id=self.repo_id,
                    day=day,
                    commits_count=random.randint(1, 20),
                    total_loc_touched=random.randint(150, 3000),
                    avg_commit_size_loc=float(random.randint(10, 100)),
                    large_commit_ratio=random.uniform(0.0, 0.2),
                    prs_merged=random.randint(0, 5),
                    median_pr_cycle_hours=float(random.randint(4, 72)),
                    computed_at=datetime.now(timezone.utc),
                )
            )
        return records

    def generate_users(
        self,
        *,
        default_password: str = "devhealth123",
        include_admin: bool = True,
        org_id: str | None = None,
    ) -> dict[str, Any]:
        import bcrypt

        from dev_health_ops.licensing.types import LicenseTier
        from dev_health_ops.models.licensing import OrgLicense
        from dev_health_ops.models.users import Membership, Organization, User

        users = []
        orgs = []
        memberships = []
        licenses = []

        password_hash = bcrypt.hashpw(
            default_password.encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")

        _DEFAULT_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        # Resolve the target Postgres Organization identity from the supplied
        # CLI/sink-level org_id, so that seeded users/memberships/licenses live
        # in the SAME tenant as the analytics rows. Without this, the fixture
        # generator hardcoded "default-org" and broke multi-tenant scoping.
        if org_id:
            try:
                target_org_uuid = uuid.UUID(org_id)
                _slug_seed = f"fixture-{target_org_uuid.hex[:8]}"
            except ValueError:
                target_org_uuid = uuid.uuid5(_DEFAULT_NS, org_id)
                # Slug must satisfy uniqueness AND be deterministic per org_id.
                _safe = re.sub(r"[^a-z0-9-]+", "-", org_id.lower()).strip("-")
                _slug_seed = (_safe or f"fixture-{target_org_uuid.hex[:8]}")[:60]
            target_slug = _slug_seed
            target_name = DEMO_ORG_NAME
        else:
            target_org_uuid = uuid.uuid5(_DEFAULT_NS, "default-org")
            target_slug = "default-org"
            target_name = DEMO_ORG_NAME

        if include_admin:
            admin_user = User(
                id=uuid.uuid5(
                    _DEFAULT_NS,
                    "admin@devhealth.example",
                ),
                email="admin@devhealth.example",
                username="admin",
                password_hash=password_hash,
                full_name="Admin User",
                auth_provider="local",
                is_active=True,
                is_verified=True,
                is_superuser=True,
            )
            users.append(admin_user)

            admin_org = Organization(
                id=target_org_uuid,
                slug=target_slug,
                name=target_name,
                tier="enterprise",
                is_active=True,
            )
            orgs.append(admin_org)

            memberships.append(
                Membership(
                    id=uuid.uuid5(admin_user.id, str(admin_org.id)),
                    user_id=admin_user.id,
                    org_id=admin_org.id,
                    role="owner",
                    joined_at=datetime.now(timezone.utc),
                )
            )

            admin_license = OrgLicense(
                org_id=admin_org.id,
                tier=LicenseTier.ENTERPRISE.value,
                license_type="saas",
                licensed_users=None,
                licensed_repos=None,
                issued_at=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc) + timedelta(days=365),
            )
            admin_license.id = uuid.uuid5(admin_org.id, "org-license")
            licenses.append(admin_license)

        default_org_id = None
        if orgs:
            default_org_id = orgs[0].id

        for name, email in self.authors[:5]:
            user_id = uuid.uuid5(_DEFAULT_NS, email)
            user = User(
                id=user_id,
                email=email,
                username=email.split("@")[0],
                password_hash=password_hash,
                full_name=name,
                auth_provider="local",
                is_active=True,
                is_verified=True,
                is_superuser=False,
            )
            users.append(user)

            if default_org_id:
                memberships.append(
                    Membership(
                        id=uuid.uuid5(user_id, str(default_org_id)),
                        user_id=user_id,
                        org_id=default_org_id,
                        role="member",
                        joined_at=datetime.now(timezone.utc),
                    )
                )

        return {
            "users": users,
            "organizations": orgs,
            "memberships": memberships,
            "licenses": licenses,
            "default_password": default_password,
        }
