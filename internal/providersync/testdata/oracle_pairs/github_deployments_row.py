from __future__ import annotations

import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import class_annotated_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_CODE_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/github/code_client.py"
_BASE_GIT_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/base_git.py"
_RELEASE_REF_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/release_ref.py"
_MODEL_SOURCE = REPO_ROOT / "src/dev_health_ops/models/git.py"


def _reflected_fields() -> frozenset[str]:
    return class_annotated_field_names(_MODEL_SOURCE.read_text(), "Deployment")


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    code_client = load_live_module(_CODE_CLIENT_SOURCE)
    deployment = code_client._deployment_from_item(case["raw_deployment"])
    release_ref = load_live_module(_RELEASE_REF_SOURCE)
    enrichment = release_ref.get_release_ref_enrichment(
        deployment,
        "github",
        releases=[code_client._release_from_item(item) for item in case["releases"]],
    )
    pull_request_number, merged_at = code_client._choose_deployment_pull_request(
        case.get("pulls", []), deployment.sha
    )
    base_git = load_live_module(_BASE_GIT_SOURCE)
    row = base_git.build_deployment(
        repo_id=case["repo_id"],
        deployment_id=deployment.deployment_id,
        status=deployment.state,
        environment=deployment.environment,
        started_at=deployment.created_at,
        finished_at=None,
        deployed_at=deployment.created_at,
        merged_at=merged_at,
        pull_request_number=pull_request_number,
        release_ref=enrichment.release_ref,
        release_ref_confidence=enrichment.confidence,
    )
    return {key: value for key, value in vars(row).items() if not key.startswith("_")}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/deployments/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "org_id": "carried from the Go claim for tenant-scoped persistence",
            "last_synced": "stamped by the Go handler at its normalized collection instant",
        },
    )
)
