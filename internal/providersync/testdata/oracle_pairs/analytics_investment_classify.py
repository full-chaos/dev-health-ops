"""Live Python oracle for the legacy rule-based investment classifier.

Compared surface: every field ``InvestmentClassification`` declares, reflected
from the production dataclass rather than hand-listed.

Nothing is reimplemented. The real ``InvestmentClassifier`` is constructed
against a real config FILE and its real ``classify`` is called; the only thing
the case chooses is WHICH file. Both sides resolve the same two names to the
same two paths, so neither engine can be handed rules the other never saw:

  "real"   -> src/dev_health_ops/config/investment_areas.yaml, the file
              production actually loads (job_work_items.py:448)
  "quirks" -> internal/providersync/testdata/investment_configs/quirks.yaml,
              for the matcher forms the real file never expresses

Pointing the primary cases at the REAL config is deliberate. A synthetic-only
oracle proves the two implementations agree about a file neither of them will
ever load in production, which is the same class of defect as a hand-typed
schema agreeing with itself.
"""

from __future__ import annotations

import contextlib
import io
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
)

install_minimal_oracle_imports()

from dev_health_ops.analytics.investment import (  # noqa: E402
    InvestmentClassifier,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
SOURCE = REPO_ROOT / "src/dev_health_ops/analytics/investment.py"

_CONFIGS = {
    "real": REPO_ROOT / "src/dev_health_ops/config/investment_areas.yaml",
    "quirks": REPO_ROOT
    / "internal/providersync/testdata/investment_configs/quirks.yaml",
    # A path that does not exist. Python logs a warning and classifies with an
    # EMPTY rule list rather than raising, so every artifact falls to the legacy
    # default. Reproduced rather than corrected.
    "missing": REPO_ROOT
    / "internal/providersync/testdata/investment_configs/nope.yaml",
}


def _fields() -> frozenset[str]:
    return dataclass_field_names(SOURCE.read_text(), "InvestmentClassification")


def _config_path(name: str) -> pathlib.Path:
    if name not in _CONFIGS:
        raise AssertionError(f"unknown investment config {name!r}")
    return _CONFIGS[name]


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    # The missing-config path calls logger.warning, and ANY byte written to the
    # captured streams lands in front of this oracle's JSON and breaks the
    # decode. The warning is production behaviour worth exercising, so it is
    # silenced here rather than avoided by dropping the case.
    with (
        contextlib.redirect_stdout(io.StringIO()),
        contextlib.redirect_stderr(io.StringIO()),
    ):
        classifier = InvestmentClassifier(_config_path(str(case["Config"])))
    artifact_case = case.get("Artifact") or {}
    # Exactly the keys the production call site supplies
    # (job_work_items.py:1377-1383), plus "paths", which that call site does
    # NOT supply and which the matcher nonetheless reads. Passing it only when
    # the case sets it keeps the default shape identical to production while
    # still allowing the path_prefix arm to be exercised deliberately.
    artifact: dict[str, Any] = {
        "labels": list(artifact_case.get("Labels") or []),
        "component": artifact_case.get("Component", ""),
        "title": artifact_case.get("Title", ""),
        "provider": artifact_case.get("Provider", "github"),
    }
    if artifact_case.get("Paths") is not None:
        artifact["paths"] = list(artifact_case["Paths"])
    result = classifier.classify(artifact)
    return {name: getattr(result, name) for name in sorted(_fields())}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="analytics/investment/classify",
        build_row=_build_row,
        reflected_fields=_fields,
    )
)
