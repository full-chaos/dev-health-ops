#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import sys
from types import SimpleNamespace
from typing import Any

from python_oracle_loader import load_live_module


def _namespace(path: pathlib.Path, *, relative_path: str, module_name: str) -> Any:
    return load_live_module(path, relative_path=relative_path, module_name=module_name)


def _render(estimates: tuple[Any, ...]) -> list[dict[str, object]]:
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
        relative_path="src/dev_health_ops/providers/linear/budget.py",
        module_name="dev_health_ops.providers.linear.budget",
    )
    jira = _namespace(
        pathlib.Path(sys.argv[2]),
        relative_path="src/dev_health_ops/providers/jira/budget.py",
        module_name="dev_health_ops.providers.jira.budget",
    )
    launchdarkly = _namespace(
        pathlib.Path(sys.argv[3]),
        relative_path="src/dev_health_ops/providers/launchdarkly/budget.py",
        module_name="dev_health_ops.providers.launchdarkly.budget",
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
                        linear._dataset_estimates(
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
                            jira._dataset_estimates(
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
                launchdarkly.LaunchDarklyBudgetEstimator().estimate(context)
            ),
        }
    )
    json.dump(cases, sys.stdout, sort_keys=True, separators=(",", ":"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
