"""Live Python oracle asserting each DECLARED divergence still exists.

A divergence recorded only in a comment and a PR description is a claim, and a
claim decays silently: the day someone makes the two engines agree -- or the day
one of them changes and they stop agreeing in a NEW way -- the note stays behind
and reads as current. That is the same defect class as an inaccurate coverage
claim, and this repository already has the machinery to prevent it.

So each declared divergence is a CASE here, and what the case asserts is the
RELATIONSHIP between the engines rather than either engine's answer:

  Python CLASSIFIES this config, and the Go port REFUSES it, and the refusal is
  a DECLARED one (InvestmentConfigError.PythonException == "") rather than a
  mirrored raise.

Both halves are measured live on their own side. The compared field, ``relation``,
is a sentinel string each side emits only when its half holds, so the rows match
only when the whole relationship does. If the port is later taught to mirror one
of these shapes, its side stops emitting the sentinel and this fails loudly,
naming the case and telling the reader to delete the declaration -- instead of
leaving prose behind that no longer describes anything.

The classification fields are declared in ``excluded_fields`` rather than
compared, and that declaration is load-bearing in the harness's own terms:
Python's rows genuinely carry them (it classified), so the exclusion is
currently-effective rather than stale, and comparing them against the Go side's
nothing would just be restating the divergence as a diff.
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

SOURCE = _investment_helpers.REPO_ROOT / "src/dev_health_ops/analytics/investment.py"

# The exact string both sides must produce for the relationship to hold. Kept
# here, in the Python pair, and referenced by name from Go: one definition, so
# the two cannot drift into agreeing about different sentences.
DIVERGENCE_HOLDS = "python classifies; this port declares a divergence and refuses"


def _fields() -> frozenset[str]:
    return dataclass_field_names(SOURCE.read_text(), "InvestmentClassification")


_EXCLUSION_REASON = (
    "on a DECLARED-divergence shape Python classifies and the Go port refuses, "
    "so this field exists on the Python side only. Comparing it would restate "
    "the divergence as a field diff; the divergence itself is asserted by the "
    "'relation' field, which fails if either half stops holding. Every "
    "classification field is compared exhaustively by the "
    "analytics/investment/classify pair."
)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    with (
        contextlib.redirect_stdout(io.StringIO()),
        contextlib.redirect_stderr(io.StringIO()),
    ):
        try:
            classifier = InvestmentClassifier(
                _investment_helpers.config_path(str(case["Config"]))
            )
            result = classifier.classify(_investment_helpers.artifact(case))
        except Exception as exception:  # noqa: BLE001
            raise AssertionError(
                f"case {case['id']!r} declares that PYTHON CLASSIFIES this config "
                f"and the Go port refuses it, but Python itself raised "
                f"{type(exception).__name__}. If that is now correct, this is a "
                "mirrored refusal and the case belongs on the "
                "analytics/investment/refusal pair -- it must not stay here, "
                "where it would assert a divergence that no longer has the shape "
                "it claims."
            ) from exception
    row: dict[str, Any] = {name: getattr(result, name) for name in _fields()}
    row["relation"] = DIVERGENCE_HOLDS
    return row


oracle_registry.register(
    oracle_registry.PairSpec(
        id="analytics/investment/divergence",
        build_row=_build_row,
        reflected_fields=_fields,
        excluded_fields={name: _EXCLUSION_REASON for name in _fields()},
    )
)
