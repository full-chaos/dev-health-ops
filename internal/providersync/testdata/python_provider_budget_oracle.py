#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import subprocess
import sys
from types import SimpleNamespace
from typing import Any, cast

from python_oracle_loader import load_live_module

WORK_ITEM_DATASETS = (
    "work-items",
    "work-item-labels",
    "work-item-projects",
    "work-item-history",
    "work-item-comments",
)


def _namespace(path: pathlib.Path) -> Any:
    return load_live_module(path)


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


def _linear_cases(linear: Any) -> list[dict[str, object]]:
    cases: list[dict[str, object]] = []
    for span_days in (1, 3):
        for dataset in WORK_ITEM_DATASETS:
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
    return cases


def _jira_cases(jira: Any) -> list[dict[str, object]]:
    cases: list[dict[str, object]] = []
    for span_days in (1, 3):
        for dataset in (*WORK_ITEM_DATASETS, "incidents"):
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
    return cases


def _launchdarkly_cases(launchdarkly: Any) -> list[dict[str, object]]:
    context = SimpleNamespace(
        provider="launchdarkly",
        dataset_key="feature-flags",
        decrypted_credentials={},
        credential_id="credential",
        integration_id="integration",
        org_id="org",
    )
    return [
        {
            "provider": "launchdarkly",
            "dataset": "feature-flags",
            "span_days": 1,
            "flags": {},
            "estimates": _render(
                launchdarkly.LaunchDarklyBudgetEstimator().estimate(context)
            ),
        }
    ]


def _provider_cases(provider: str, source: pathlib.Path) -> list[dict[str, object]]:
    module = _namespace(source)
    if provider == "linear":
        return _linear_cases(module)
    if provider == "jira":
        return _jira_cases(module)
    if provider == "launchdarkly":
        return _launchdarkly_cases(module)
    raise ValueError(f"unexpected provider: {provider}")


def _provider_subprocess(
    provider: str, source: pathlib.Path
) -> list[dict[str, object]]:
    output = subprocess.check_output(
        [
            sys.executable,
            str(pathlib.Path(__file__).resolve()),
            "--provider",
            provider,
            str(source.resolve(strict=True)),
        ],
        text=True,
    )
    return cast(list[dict[str, object]], json.loads(output))


def main() -> int:
    if len(sys.argv) == 4 and sys.argv[1] == "--provider":
        child_cases = _provider_cases(sys.argv[2], pathlib.Path(sys.argv[3]))
        json.dump(child_cases, sys.stdout, sort_keys=True, separators=(",", ":"))
        return 0
    if len(sys.argv) != 4:
        return 2

    linear = _provider_subprocess("linear", pathlib.Path(sys.argv[1]))
    jira = _provider_subprocess("jira", pathlib.Path(sys.argv[2]))
    launchdarkly = _provider_subprocess("launchdarkly", pathlib.Path(sys.argv[3]))
    cases: list[dict[str, object]] = []
    for span_days in (1, 3):
        cases.extend(case for case in linear if case["span_days"] == span_days)
        cases.extend(case for case in jira if case["span_days"] == span_days)
    cases.extend(launchdarkly)
    json.dump(cases, sys.stdout, sort_keys=True, separators=(",", ":"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
