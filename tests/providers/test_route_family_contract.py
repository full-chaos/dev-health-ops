"""Differential oracle: emitted budget route families vs. the checked-in contract.

Recovered from ``tests/test_rate_limit_policy_doc.py`` (CHAOS-2757/CHAOS-2742). The
catalog it compared against used to be Markdown tables inside a prose page,
``.github/docs-legacy/providers/rate-limit-policy.md``, scoped by
``<!-- route-families:<provider> -->`` HTML comments; that page was deleted in
``e23ede618``. The catalog cites internal class names and CHAOS ticket IDs and had
exactly one consumer -- this test -- so it was a code contract mis-filed as prose. It
now lives at ``contracts/providers/v1/route-families.json`` and this module reads that
JSON instead of parsing Markdown.

Home: ``tests/providers/`` rather than the repo-root ``tests/`` docs area, because the
subject is the per-provider budget estimators (``providers/<provider>/budget.py``), not
a documentation build. It sits beside ``test_github_budget.py``,
``test_launchdarkly_budget_contract.py``, and the other per-provider budget tests, and
follows the ``test_*_contract.py`` naming already used for checked-in contracts
(``tests/workers/test_provider_matrix_contract.py``,
``tests/metrics/test_remaining_scope_contract.py``).

Assertion direction, preserved verbatim from the original: **emitted must be a subset of
catalogued**. The test fails when an estimator emits a ``route_family`` the contract does
not list. The reverse direction (a catalogued family no code emits) was NOT asserted by
the original and is NOT asserted here -- the catalog deliberately carries
modeled-but-not-yet-emitted families (LaunchDarkly ``projects``/``segments``/``members``
are declared in ``LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES`` with no client fetching them yet),
so a reverse assertion would fail by design.
"""

from __future__ import annotations

import dataclasses
import inspect
import json
import re
from collections import defaultdict
from pathlib import Path

import pytest

from dev_health_ops.sync import budget as budget_module
from dev_health_ops.sync.budget import estimate_provider_budget
from dev_health_ops.sync.budget_types import BudgetBucketKey, BudgetEstimate
from dev_health_ops.sync.datasets import supported_datasets
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONTRACT_PATH = _REPO_ROOT / "contracts" / "providers" / "v1" / "route-families.json"
_SCHEMA_PATH = _CONTRACT_PATH.with_name("route-families.schema.json")

# Enable every flag-gated route family so the estimators emit their full set
# (GitHub linked-PR expansion, Jira worklogs + AGG GraphQL enrichment, ...).
_ALL_FLAGS = {
    "sync_prs": True,
    "sync_git": True,
    "jira_fetch_worklogs": True,
    "fetch_worklogs": True,
    "atlassian_gql_enabled": True,
    "gql_enabled": True,
}

# Escape hatch for a dispatched provider that is knowingly not yet catalogued. Empty,
# and it should stay that way: an entry here is an admitted coverage gap, not coverage.
# ``pagerduty`` sat here briefly -- it shipped mid-way through the documentation move and
# the catalog, then a prose page, never gained its rows. It is fully catalogued now.
_UNCATALOGUED_PROVIDERS: frozenset[str] = frozenset()


def _dispatched_providers() -> frozenset[str]:
    """Providers ``estimate_provider_budget`` actually dispatches, read from its source.

    Derived from production rather than hardcoded so a newly dispatched provider cannot
    slip past this module's coverage check unnoticed.
    """
    source = inspect.getsource(budget_module.estimate_provider_budget)
    found = frozenset(
        re.findall(r'context\.provider\.lower\(\)\s*==\s*"(\w+)"', source)
    )
    # A regex that silently matched nothing would make every check below vacuous.
    assert len(found) >= 5, (
        "failed to parse dispatched providers out of estimate_provider_budget; the "
        f"dispatch shape changed. Parsed: {sorted(found)}"
    )
    return found


def _make_context(provider: str, dataset_key: str) -> SyncTaskContext:
    return SyncTaskContext(
        unit_id="unit-1",
        sync_run_id="run-1",
        org_id="org-1",
        integration_id="int-1",
        source_id="src-1",
        source_external_id="ext-1",
        provider=provider,
        dataset_key=dataset_key,
        cost_class="medium",
        mode="full",
        window_start=None,
        window_end=None,
        processor_flags=dict(_ALL_FLAGS),
        credential_id="cred-1",
        decrypted_credentials={"token": "x", "base_url": "https://example.test"},
        db_url="sqlite://",
    )


def _emitted_route_families(provider: str) -> set[str]:
    families: set[str] = set()
    for spec in supported_datasets(provider):
        for estimate in estimate_provider_budget(
            _make_context(provider, spec.dataset_key)
        ):
            families.add(estimate.route_family)
    return families


def _catalogued_route_families() -> dict[str, set[str]]:
    contract = json.loads(_CONTRACT_PATH.read_text())
    catalogued: dict[str, set[str]] = defaultdict(set)
    for row in contract["route_families"]:
        catalogued[row["provider"]].add(row["route_family"])
    return catalogued


def _catalogued_providers() -> list[str]:
    return sorted(json.loads(_CONTRACT_PATH.read_text())["providers"])


def test_contract_compares_the_fields_the_estimator_actually_emits():
    """Pin the compared field set to the production dataclasses, not a written list.

    ``route_family`` is what the oracle below compares and ``dimension`` is what the
    contract's ``dimensions`` column mirrors. If either is renamed or moved, the
    comparison would quietly stop covering it -- fail loudly here instead.
    """
    estimate_fields = {f.name for f in dataclasses.fields(BudgetEstimate)}
    bucket_fields = {f.name for f in dataclasses.fields(BudgetBucketKey)}

    assert "route_family" in estimate_fields, (
        f"BudgetEstimate no longer carries `route_family`: {sorted(estimate_fields)}"
    )
    assert "dimension" in bucket_fields, (
        f"BudgetBucketKey no longer carries `dimension`: {sorted(bucket_fields)}"
    )


def test_every_dispatched_provider_is_catalogued_or_a_declared_gap():
    """No provider may silently escape the oracle by simply not being listed."""
    dispatched = _dispatched_providers()
    covered = set(_catalogued_providers()) | _UNCATALOGUED_PROVIDERS
    uncovered = dispatched - covered
    assert not uncovered, (
        f"estimate_provider_budget dispatches {sorted(uncovered)}, which are neither "
        f"in {_CONTRACT_PATH.relative_to(_REPO_ROOT)} nor in the declared-gap set. Add "
        "their route families to the contract, or add them to _UNCATALOGUED_PROVIDERS "
        "with a reason."
    )


@pytest.mark.parametrize("provider", _catalogued_providers())
def test_emitted_route_families_are_catalogued(provider: str):
    catalogued = _catalogued_route_families()

    # Rows must exist for the provider, else the subset check below passes vacuously.
    assert catalogued.get(provider), (
        f"no route-family rows for {provider!r} in "
        f"{_CONTRACT_PATH.relative_to(_REPO_ROOT)}"
    )

    emitted = _emitted_route_families(provider)
    assert emitted, f"{provider} estimator emitted no route families"

    missing = emitted - catalogued[provider]
    assert not missing, (
        f"{provider} budget estimator emits route families that "
        f"{_CONTRACT_PATH.relative_to(_REPO_ROOT)} does not list: {sorted(missing)}. "
        f"Add them to the contract's `route_families` array with provider={provider!r}."
    )


def test_contract_conforms_to_its_own_schema() -> None:
    """The sibling schema file must actually constrain the contract.

    ``jsonschema`` is not a dependency here, so the schema's constraints are applied
    structurally. Without this the schema is decorative: a provider could be added to
    the data while its ``enum`` went stale, and nothing would notice.
    """
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    contract = json.loads(_CONTRACT_PATH.read_text(encoding="utf-8"))

    row_def = schema["$defs"]["route_family"]["properties"]
    provider_enum = set(schema["properties"]["providers"]["items"]["enum"])
    row_provider_enum = set(row_def["provider"]["enum"])
    dimension_enum = set(row_def["dimensions"]["items"]["enum"])
    family_pattern = re.compile(row_def["route_family"]["pattern"])
    required = set(schema["$defs"]["route_family"]["required"])

    assert provider_enum == row_provider_enum, (
        "the providers enum and the per-row provider enum disagree: "
        f"{sorted(provider_enum ^ row_provider_enum)}"
    )
    assert contract["schema_version"] == schema["properties"]["schema_version"]["const"]

    declared = set(contract["providers"])
    assert declared == provider_enum, (
        "contract `providers` and the schema enum disagree; a provider was added to "
        f"one but not the other: {sorted(declared ^ provider_enum)}"
    )

    rows = contract["route_families"]
    assert rows, "contract carries no route-family rows"
    row_providers = {row["provider"] for row in rows}
    assert row_providers == declared, (
        "every declared provider must carry rows and every row must belong to a "
        f"declared provider: {sorted(row_providers ^ declared)}"
    )

    for row in rows:
        where = f"{row.get('provider')}/{row.get('route_family')}"
        assert set(row) == required, (
            f"{where}: unexpected or missing keys {sorted(row)}"
        )
        assert row["provider"] in provider_enum, where
        assert family_pattern.fullmatch(row["route_family"]), where
        assert row["dimensions"], f"{where}: dimensions must not be empty"
        assert len(set(row["dimensions"])) == len(row["dimensions"]), (
            f"{where}: duplicate dimensions"
        )
        assert set(row["dimensions"]) <= dimension_enum, (
            f"{where}: unknown dimension "
            f"{sorted(set(row['dimensions']) - dimension_enum)}"
        )
        assert row["covers"].strip(), f"{where}: covers must not be empty"
        assert row["confidence"].strip(), f"{where}: confidence must not be empty"

    seen = [(row["provider"], row["route_family"]) for row in rows]
    assert len(seen) == len(set(seen)), "duplicate (provider, route_family) rows"
