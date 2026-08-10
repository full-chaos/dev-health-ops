from __future__ import annotations

import pathlib
from dataclasses import asdict
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
CODE_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/code_client.py"
RATE_LIMIT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/ratelimit.py"


def _reflected_fields() -> frozenset[str]:
    return dataclass_field_names(
        RATE_LIMIT_SOURCE.read_text(), "GitLabRateLimitClassification"
    )


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    code_client = load_live_module(CODE_CLIENT_SOURCE)
    classification = code_client.classify_gitlab_status(
        status=int(case["status"]), headers=case.get("headers", {})
    )
    return asdict(classification)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/files/http-classification",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "reason": "Go's shared ProviderError exposes the stable rate-limit class but not GitLab's provider-local primary/secondary reason",
            "retry_after_seconds": "Go's shared ProviderError stores retry delay as a duration and this oracle pins classification, not provider-specific delay serialization",
        },
    )
)
