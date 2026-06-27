"""CHAOS-2037: curated demo identity must never leak fixture-only identifiers.

These tests guard the seam where the synthetic fixture generator chooses the
org display name and repo names that ultimately render on demo / customer
surfaces (org switcher, churn paths, repo coverage). They assert that:

* the generated organization name is the curated brand, never
  ``Fixture Org (<uuid>)`` or ``Default Organization``;
* the default repo name and multi-repo naming draw from curated, believable
  names rather than ``acme/demo-app`` / ``acme/demo-app-1`` scaffolding.
"""

from __future__ import annotations

import re
import uuid

from dev_health_ops.fixtures.demo_identity import (
    DEFAULT_DEMO_REPO_NAME,
    DEFAULT_DEMO_TEAM,
    DEMO_ORG_NAME,
    DEMO_REPO_NAMES,
    DEMO_TEAMS,
    ONBOARDED_ADMIN_USER_EMAIL,
    ONBOARDING_ORGLESS_USER_EMAIL,
    demo_repo_name,
    demo_team_identity,
)
from dev_health_ops.fixtures.generator import SyntheticDataGenerator

# Identifiers that must never reach a customer-facing label.
_FORBIDDEN = ("Fixture Org", "Default Organization", "acme/demo-app")
_BARE_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def test_default_repo_name_is_curated_not_fixture():
    assert DEFAULT_DEMO_REPO_NAME == DEMO_REPO_NAMES[0]
    assert "acme/demo-app" not in DEFAULT_DEMO_REPO_NAME
    assert SyntheticDataGenerator().repo_name == DEFAULT_DEMO_REPO_NAME


def test_demo_repo_name_single_repo_returns_base():
    assert demo_repo_name(DEFAULT_DEMO_REPO_NAME, 0, 1) == DEFAULT_DEMO_REPO_NAME


def test_demo_repo_name_multi_repo_uses_curated_distinct_names():
    names = [demo_repo_name(DEFAULT_DEMO_REPO_NAME, i, 4) for i in range(4)]
    assert names == list(DEMO_REPO_NAMES[:4])
    # Distinct, believable, and never numeric-suffixed scaffolding.
    assert len(set(names)) == 4
    for name in names:
        assert not re.search(r"-\d+$", name)
        assert "acme/demo-app" not in name


def test_demo_repo_name_falls_back_to_suffix_when_exhausted():
    over = len(DEMO_REPO_NAMES) + 1
    last = demo_repo_name(DEFAULT_DEMO_REPO_NAME, over - 1, over)
    assert last == f"{DEFAULT_DEMO_REPO_NAME}-{over}"


def test_demo_repo_name_respects_explicit_base():
    # An explicit --repo-name keeps the legacy suffix scheme.
    assert demo_repo_name("custom/repo", 1, 3) == "custom/repo-2"


def test_generated_org_name_is_curated_for_uuid_org_id():
    org_id = str(uuid.uuid4())
    data = SyntheticDataGenerator(seed=1).generate_users(org_id=org_id)
    orgs = data["organizations"]
    assert orgs, "expected at least one seeded organization"
    for org in orgs:
        assert org.name == DEMO_ORG_NAME
        for forbidden in _FORBIDDEN:
            assert forbidden not in org.name
        assert not _BARE_UUID.match(org.name)


def test_generated_org_name_is_curated_without_org_id():
    data = SyntheticDataGenerator(seed=1).generate_users(org_id=None)
    for org in data["organizations"]:
        assert org.name == DEMO_ORG_NAME


def test_generated_auth_users_are_purpose_specific_journey_fixtures():
    data = SyntheticDataGenerator(seed=1).generate_users(org_id=str(uuid.uuid4()))

    users_by_email = {user.email: user for user in data["users"]}
    assert set(users_by_email) == {
        ONBOARDING_ORGLESS_USER_EMAIL,
        ONBOARDED_ADMIN_USER_EMAIL,
    }

    orgless_user = users_by_email[ONBOARDING_ORGLESS_USER_EMAIL]
    admin_user = users_by_email[ONBOARDED_ADMIN_USER_EMAIL]
    assert orgless_user.is_verified is True
    assert admin_user.is_superuser is True

    memberships_by_user_id = {membership.user_id for membership in data["memberships"]}
    assert orgless_user.id not in memberships_by_user_id
    assert admin_user.id in memberships_by_user_id
    assert len(data["memberships"]) == 1


def test_demo_team_identity_is_curated_and_distinct():
    names = [demo_team_identity(i) for i in range(len(DEMO_TEAMS))]
    assert names == list(DEMO_TEAMS)
    ids = [t[0] for t in DEMO_TEAMS]
    labels = [t[1] for t in DEMO_TEAMS]
    assert len(set(ids)) == len(ids)
    assert len(set(labels)) == len(labels)
    # Never the legacy generic scaffolding.
    for tid, label in DEMO_TEAMS:
        assert label not in ("Alpha Team", "Beta Team")
        assert not label.startswith("Team ")
        assert tid not in ("alpha", "beta")
    assert DEFAULT_DEMO_TEAM == DEMO_TEAMS[0]


def test_demo_team_identity_falls_back_past_curated_list():
    assert demo_team_identity(len(DEMO_TEAMS)) is None


def test_generated_team_names_are_curated_not_generic():
    teams = SyntheticDataGenerator(seed=3).generate_teams(count=4)
    rendered = [(t.id, t.name) for t in teams]
    assert rendered == list(DEMO_TEAMS[:4])
    for _, name in rendered:
        assert name not in ("Alpha Team", "Beta Team")
        assert not name.startswith("Team ")
