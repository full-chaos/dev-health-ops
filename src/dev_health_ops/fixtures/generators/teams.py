"""Teams, repo, users, teams-config, and repo metrics fixture generators."""

from __future__ import annotations

import random
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from dev_health_ops.fixtures.demo_identity import (
    DEMO_ORG_NAME,
    ONBOARDED_ADMIN_USER_EMAIL,
    ONBOARDED_ADMIN_USER_FULL_NAME,
    ONBOARDED_ADMIN_USER_USERNAME,
    ONBOARDING_ORGLESS_USER_EMAIL,
    ONBOARDING_ORGLESS_USER_FULL_NAME,
    ONBOARDING_ORGLESS_USER_USERNAME,
)
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

    def generate_team_ownership_edges(
        self,
        *,
        all_teams: list[Team],
        repo_team_assignments: list[list[Team]],
        repo_names: list[str],
        repo_ids: list[uuid.UUID],
        org_id: str,
        provider: str = "synthetic",
        as_of: datetime | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Build ``team_repo_ownership``, ``team_project_ownership``, and
        ``team_memberships`` edge rows from the SAME repo<->team assignment
        already used for ``teams.repo_patterns`` (CHAOS-4276), so ownership
        stays consistent with the pattern-resolver path.

        Fixtures audit 2026-08-26 / CHAOS-4338: before this, these three
        tables had zero writers anywhere in fixtures, starving 5-6 of the
        real attribution resolver's ~9 sources
        (``load_team_attribution_context``, ``compute_work_items.py``).

        Project ids are the repo's full name
        (``generators/projects.py``'s ``project_id_for_repo``: repo-backed
        projects are 1:1 with repos in this fixture world), so
        ``team_project_ownership`` reuses the identical assignment as
        ``team_repo_ownership``.

        CHAOS-4329 proof: every co-owning team (not just the primary owner
        at index 0) gets its own ownership row, so any team
        ``_build_repo_team_assignments`` gave >=2 repos naturally produces
        >=2 ``team_repo_ownership`` rows for that ``team_id`` -- no separate
        random assignment needed to keep in sync.

        Ambiguous-identity proof: the LAST member of the first team with
        members also gets a second ``team_memberships`` row into a
        DIFFERENT team (source='provider_access'), so a real attribution
        resolver run against this fixture data sees two distinct
        ``team_id``s for that one identity and correctly refuses to guess
        (per the documented rule: "if the person is mapped to two or more
        teams, we do not guess -- the item stays unassigned").

        Admin-override proof (CHAOS-4321, chris 2026-08-26 08:30 PT:
        "manual is override -- if the override exists, use it, else use
        attribution from providers"): a THIRD identity gets an
        ``identities`` row with ``team_ids`` pointing at one team AND that
        team's ``manual_members`` (mutated on the ``Team`` object in place,
        so the caller's subsequent ``store.insert_teams`` write carries it)
        -- the admin layer -- while the SAME identity also gets a
        conflicting ``team_memberships`` row (source='provider_access')
        into a DIFFERENT team -- the provider auto-import fallback layer.
        The two-layer resolver (``compute_work_items._resolve_membership``)
        must pick the admin team, never the provider-fallback one.
        """
        now = as_of or datetime.now(timezone.utc)
        # Comfortably in the past so every real `as_of <= now` compute-time
        # read sees these rows as already valid (valid_from <= as_of).
        valid_from = now - timedelta(days=len(repo_names) + 30)

        repo_rows: list[dict[str, Any]] = []
        project_rows: list[dict[str, Any]] = []
        for idx, owners in enumerate(repo_team_assignments):
            if not owners or idx >= len(repo_names) or idx >= len(repo_ids):
                continue
            repo_name = repo_names[idx]
            repo_id = repo_ids[idx]
            for owner_idx, team in enumerate(owners):
                is_primary = 1 if owner_idx == 0 else 0
                common = {
                    "org_id": org_id,
                    "provider": provider,
                    "team_id": team.id,
                    "source": "native",
                    "is_primary": is_primary,
                    "specificity": 100 if is_primary else 50,
                    "priority": 0,
                    "valid_from": valid_from,
                    "valid_to": None,
                    "updated_at": now,
                }
                repo_rows.append(
                    {
                        **common,
                        "repo_id": repo_id,
                        "repo_full_name": repo_name,
                        "match_type": "exact",
                    }
                )
                project_rows.append(
                    {
                        **common,
                        "project_id": repo_name,
                        "project_key": None,
                    }
                )

        membership_rows: list[dict[str, Any]] = []
        for team in all_teams:
            for member in team.members or []:
                member_key = str(member).strip().lower()
                membership_rows.append(
                    {
                        "org_id": org_id,
                        "provider": provider,
                        "team_id": team.id,
                        "member_id": member_key,
                        "raw_provider_user_id": None,
                        "raw_email": member_key,
                        "identity_facets": [],
                        "source": "native",
                        "is_primary": 1,
                        "specificity": 50,
                        "priority": 10,
                        "valid_from": valid_from,
                        "valid_to": None,
                        "updated_at": now,
                    }
                )

        teams_with_members = [team for team in all_teams if team.members]
        if len(teams_with_members) >= 2:
            primary_team, secondary_team = (
                teams_with_members[0],
                teams_with_members[1],
            )
            ambiguous_member = str((primary_team.members or [])[-1]).strip().lower()
            membership_rows.append(
                {
                    "org_id": org_id,
                    "provider": provider,
                    "team_id": secondary_team.id,
                    "member_id": ambiguous_member,
                    "raw_provider_user_id": None,
                    "raw_email": ambiguous_member,
                    "identity_facets": [],
                    "source": "provider_access",
                    "is_primary": 0,
                    "specificity": 50,
                    "priority": 10,
                    "valid_from": valid_from,
                    "valid_to": None,
                    "updated_at": now,
                }
            )

        identity_rows: list[dict[str, Any]] = []
        if len(teams_with_members) >= 3:
            admin_team, conflicting_team = (
                teams_with_members[2],
                teams_with_members[1],
            )
            override_member = str((admin_team.members or [])[0]).strip().lower()

            existing_manual = list(getattr(admin_team, "manual_members", None) or [])
            if override_member not in existing_manual:
                existing_manual.append(override_member)
            admin_team.manual_members = existing_manual

            identity_rows.append(
                {
                    "org_id": org_id,
                    "canonical_id": override_member,
                    "email": override_member,
                    "display_name": None,
                    "provider_identities": "{}",
                    "team_ids": [admin_team.id],
                    "is_active": 1,
                    "updated_at": now,
                }
            )

            # Conflicting provider-fallback signal for the SAME identity,
            # into a DIFFERENT team -- proves the admin layer wins, never
            # the reverse.
            membership_rows.append(
                {
                    "org_id": org_id,
                    "provider": provider,
                    "team_id": conflicting_team.id,
                    "member_id": override_member,
                    "raw_provider_user_id": None,
                    "raw_email": override_member,
                    "identity_facets": [],
                    "source": "provider_access",
                    "is_primary": 0,
                    "specificity": 50,
                    "priority": 10,
                    "valid_from": valid_from,
                    "valid_to": None,
                    "updated_at": now,
                }
            )

        return {
            "team_repo_ownership": repo_rows,
            "team_project_ownership": project_rows,
            "team_memberships": membership_rows,
            "identities": identity_rows,
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
        """Generate auth fixtures for the two supported first-run states.

        The seeded identities are deliberately minimal and purpose-specific:

        * one verified orgless user for onboarding/workspace tests;
        * one verified onboarded admin user with the demo workspace, owner
          membership, and enterprise license for admin/integration tests.

        End-to-end fixture users are not reused across these incompatible states.
        Synthetic authors remain analytics-only identities and are not created as
        login users here.
        """
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

        onboarding_user = User(
            id=uuid.uuid5(
                _DEFAULT_NS,
                ONBOARDING_ORGLESS_USER_EMAIL,
            ),
            email=ONBOARDING_ORGLESS_USER_EMAIL,
            username=ONBOARDING_ORGLESS_USER_USERNAME,
            password_hash=password_hash,
            full_name=ONBOARDING_ORGLESS_USER_FULL_NAME,
            auth_provider="local",
            is_active=True,
            is_verified=True,
            is_superuser=False,
        )
        users.append(onboarding_user)

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
                    ONBOARDED_ADMIN_USER_EMAIL,
                ),
                email=ONBOARDED_ADMIN_USER_EMAIL,
                username=ONBOARDED_ADMIN_USER_USERNAME,
                password_hash=password_hash,
                full_name=ONBOARDED_ADMIN_USER_FULL_NAME,
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

        return {
            "users": users,
            "organizations": orgs,
            "memberships": memberships,
            "licenses": licenses,
            "default_password": default_password,
        }
