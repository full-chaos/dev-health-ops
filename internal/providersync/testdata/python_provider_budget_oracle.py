#!/usr/bin/env python3
from __future__ import annotations

import ast
import json
import pathlib
import sys
from types import SimpleNamespace
from typing import Any


class _Value:
    def __init__(self, value: str) -> None:
        self.value = value


class _BudgetBucketKey:
    def __init__(self, **values: Any) -> None:
        self.dimension = values["dimension"]


class _BudgetEstimate:
    def __init__(
        self,
        *,
        bucket: _BudgetBucketKey,
        estimated_units: int,
        confidence: str,
        route_family: str,
        notes: tuple[str, ...] = (),
    ) -> None:
        self.bucket = bucket
        self.estimated_units = estimated_units
        self.confidence = confidence
        self.route_family = route_family
        self.notes = notes


DATASET_KEY = SimpleNamespace(
    WORK_ITEMS=_Value("work-items"),
    WORK_ITEM_LABELS=_Value("work-item-labels"),
    WORK_ITEM_PROJECTS=_Value("work-item-projects"),
    WORK_ITEM_HISTORY=_Value("work-item-history"),
    WORK_ITEM_COMMENTS=_Value("work-item-comments"),
    INCIDENTS=_Value("incidents"),
    FEATURE_FLAGS=_Value("feature-flags"),
)
BUDGET_DIMENSION = SimpleNamespace(
    REST_CORE="rest_core",
    SEARCH="search",
    GRAPHQL_COST="graphql_cost",
    SECONDARY_ABUSE_RISK="secondary_abuse_risk",
)


def _namespace(path: pathlib.Path, names: set[str]) -> dict[str, Any]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    selected: list[ast.stmt] = [
        ast.ImportFrom(module="__future__", names=[ast.alias("annotations")], level=0)
    ]
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names:
            selected.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in names
            for target in node.targets
        ):
            selected.append(node)
    namespace: dict[str, Any] = {
        "Any": Any,
        "DatasetKey": DATASET_KEY,
        "BudgetDimension": BUDGET_DIMENSION,
        "BudgetBucketKey": _BudgetBucketKey,
        "BudgetEstimate": _BudgetEstimate,
        "_host_from_credentials": lambda _credentials: "fixture.example",
        "_credential_fingerprint": lambda *_args, **_kwargs: "fingerprint",
        "_env_flag": lambda _name: False,
    }
    module = ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[]))
    exec(compile(module, str(path), "exec"), namespace)
    return namespace


def _render(estimates: tuple[_BudgetEstimate, ...]) -> list[dict[str, object]]:
    return sorted(
        (
            {
                "dimension": estimate.bucket.dimension,
                "units": estimate.estimated_units,
                "confidence": estimate.confidence,
                "route_family": estimate.route_family,
            }
            for estimate in estimates
        ),
        key=lambda value: (str(value["route_family"]), str(value["dimension"])),
    )


def main() -> int:
    if len(sys.argv) != 4:
        return 2
    linear = _namespace(
        pathlib.Path(sys.argv[1]),
        {
            "_CONFIDENCE_MEDIUM",
            "_CONFIDENCE_LOW",
            "_dataset_estimates",
            "_bucket_factory",
            "_estimate",
            "_scaled_units",
        },
    )
    jira = _namespace(
        pathlib.Path(sys.argv[2]),
        {
            "_CONFIDENCE_HIGH",
            "_CONFIDENCE_MEDIUM",
            "_CONFIDENCE_LOW",
            "_MAX_JSM_INCIDENT_ADMISSION_CANDIDATES",
            "_dataset_estimates",
            "_bucket_factory",
            "_estimate",
            "_scaled_units",
            "_flag_enabled",
        },
    )
    launchdarkly = _namespace(
        pathlib.Path(sys.argv[3]),
        {
            "_CONFIDENCE_MEDIUM",
            "_CONFIDENCE_LOW",
            "LaunchDarklyBudgetEstimator",
            "_bucket_factory",
            "_estimate",
        },
    )
    cases: list[dict[str, object]] = []
    work_item_datasets = (
        "work-items",
        "work-item-labels",
        "work-item-projects",
        "work-item-history",
        "work-item-comments",
    )
    for span_days in (1, 3):
        for dataset in work_item_datasets:
            cases.append(
                {
                    "provider": "linear",
                    "dataset": dataset,
                    "span_days": span_days,
                    "flags": {},
                    "estimates": _render(
                        linear["_dataset_estimates"](
                            dataset_key=dataset,
                            org_id="org",
                            host="fixture.example",
                            credential_fingerprint="fingerprint",
                            span_days=span_days,
                        )
                    ),
                }
            )
        for dataset in (*work_item_datasets, "incidents"):
            for flags in ({}, {"jira_fetch_worklogs": True, "gql_enabled": True}):
                cases.append(
                    {
                        "provider": "jira",
                        "dataset": dataset,
                        "span_days": span_days,
                        "flags": flags,
                        "estimates": _render(
                            jira["_dataset_estimates"](
                                dataset_key=dataset,
                                flags=flags,
                                org_id="org",
                                host="fixture.example",
                                credential_fingerprint="fingerprint",
                                span_days=span_days,
                            )
                        ),
                    }
                )
    context = SimpleNamespace(
        provider="launchdarkly",
        dataset_key="feature-flags",
        decrypted_credentials={},
        credential_id="credential",
        integration_id="integration",
        org_id="org",
    )
    cases.append(
        {
            "provider": "launchdarkly",
            "dataset": "feature-flags",
            "span_days": 1,
            "flags": {},
            "estimates": _render(
                launchdarkly["LaunchDarklyBudgetEstimator"]().estimate(context)
            ),
        }
    )
    json.dump(cases, sys.stdout, sort_keys=True, separators=(",", ":"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
