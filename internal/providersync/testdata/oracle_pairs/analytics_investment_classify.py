"""Live Python oracle for the legacy rule-based investment classifier.

Compared surface: every field ``InvestmentClassification`` declares, reflected
from the production dataclass rather than hand-listed.

Nothing is reimplemented. The real ``InvestmentClassifier`` is constructed
against a real config FILE and its real ``classify`` is called; the only thing
the case chooses is WHICH file and WHICH artifact. Both sides resolve the same
names to the same paths (see _investment_helpers.config_path), so neither
engine can be handed rules the other never saw.

Pointing the primary cases at the REAL config is deliberate. A synthetic-only
oracle proves the two implementations agree about a file neither of them will
ever load in production, which is the same class of defect as a hand-typed
schema agreeing with itself.

This pair covers only the config shapes both engines CLASSIFY. The shapes on
which Python RAISES are the sibling pair, analytics_investment_refusal.py --
they cannot live here, because a raising case has no
``InvestmentClassification`` to compare and the registry's completeness check
correctly refuses a row missing the dataclass's fields.
"""

from __future__ import annotations

import contextlib
import io
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs import _investment_helpers
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
)

install_minimal_oracle_imports()

from dev_health_ops.analytics.investment import (  # noqa: E402
    InvestmentClassifier,
)

REPO_ROOT = _investment_helpers.REPO_ROOT
SOURCE = REPO_ROOT / "src/dev_health_ops/analytics/investment.py"


def _fields() -> frozenset[str]:
    return dataclass_field_names(SOURCE.read_text(), "InvestmentClassification")


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    # The missing-config path calls logger.warning, and ANY byte written to the
    # captured streams lands in front of this oracle's JSON and breaks the
    # decode. The warning is production behaviour worth exercising, so it is
    # silenced here rather than avoided by dropping the case.
    with (
        contextlib.redirect_stdout(io.StringIO()),
        contextlib.redirect_stderr(io.StringIO()),
    ):
        classifier = InvestmentClassifier(
            _investment_helpers.config_path(str(case["Config"]))
        )
    result = classifier.classify(_investment_helpers.artifact(case))
    return {name: getattr(result, name) for name in sorted(_fields())}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="analytics/investment/classify",
        build_row=_build_row,
        reflected_fields=_fields,
    )
)
