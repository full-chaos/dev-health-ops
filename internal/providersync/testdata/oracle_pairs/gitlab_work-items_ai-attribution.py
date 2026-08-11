"""Live Python oracle for GitLab MR AI attribution records."""

from __future__ import annotations

import contextlib
import dataclasses
import io
import pathlib
from typing import Any
from uuid import UUID

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
    object_from_case,
)

install_minimal_oracle_imports()

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.providers.gitlab.normalize import gitlab_mr_ai_attributions

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/ai_attribution.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    rows = gitlab_mr_ai_attributions(
        mr=object_from_case(case["raw_mr"]),
        project_full_path=case["repo_full_name"],
        org_id=UUID(case["org_id"]),
        repo_id=UUID(case["repo_id"]),
    )
    source = case["signal_source"]
    row = next(candidate for candidate in rows if candidate.source.value == source)
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/work-items/ai-attribution",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "AIAttributionRecord"
        ),
        excluded_fields={
            "ingested_at": (
                "the production constructor stamps wall-clock now and has no clock "
                "parameter; Go stamps the complete unit normalizedAt and its retry "
                "stability is asserted separately"
            ),
            "record_id": (
                "the Python producer assigns uuid4 while Go derives a stable retry "
                "identifier; non-zero and retry-stable Go IDs are asserted separately"
            ),
        },
    )
)
