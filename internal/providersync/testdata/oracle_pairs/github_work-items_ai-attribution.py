"""Live Python oracle for GitHub PR AI attribution records."""

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
    from dev_health_ops.models.ai_attribution import AIAttributionRecord
    from dev_health_ops.providers.github.normalize import (
        _to_utc,
        detect_pr_attributions,
    )

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/ai_attribution.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    pr = object_from_case(case["raw_pr"])
    signals = detect_pr_attributions(pr=pr)
    source = case["signal_source"]
    signal = next(
        candidate for candidate in signals if candidate.source.value == source
    )
    observed_at = _to_utc(pr.created_at)
    if observed_at is None:
        raise ValueError("production PR AI attribution case requires created_at")
    row = AIAttributionRecord.from_signal(
        signal,
        org_id=UUID(case["org_id"]),
        provider="github",
        subject_type="pull_request",
        subject_id=str(int(case["raw_pr"]["number"])),
        repo_id=UUID(case["repo_id"]),
        observed_at=observed_at,
    )
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/ai-attribution",
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
