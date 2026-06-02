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
    DEMO_ORG_NAME,
    DEMO_REPO_NAMES,
    demo_repo_name,
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
