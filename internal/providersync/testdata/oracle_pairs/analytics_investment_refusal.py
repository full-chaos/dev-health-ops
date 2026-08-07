"""Live Python oracle for the config shapes the investment classifier REFUSES.

Compared surface: ``raises`` -- the NAME of the exception class the real
``InvestmentClassifier`` raises for a given config file and artifact.

Why this exists as its own pair rather than as more cases on
analytics/investment/classify: on these inputs Python produces no
``InvestmentClassification`` at all, and the classify pair's cases are all about
comparing one. Keeping them apart keeps two different questions apart -- "do
both engines produce the same classification" and "do both engines refuse, for
the same reason" -- and lets each fail with its own name.

The classification fields are still emitted here, as None on both sides, rather
than declared as excluded_fields: they are absent because NEITHER engine
produced a classification, which is itself the thing worth comparing.

Why the exception CLASS and not merely a "both refused" boolean: the direction
of a divergence is what D16 grades. A boolean would pass a Go engine that
refuses every malformed file for one blanket reason, which is indistinguishable
from a port that never learned which shapes Python actually rejects. The class
name is the coarsest value that still fails when the Go port refuses for the
wrong reason -- Python raises AttributeError where it dereferences a None as a
dict, and TypeError where it iterates or compares one, and those are two
genuinely different bugs in the same config file.

Every case here is EXECUTED against the real classifier: if a case's config
does not in fact raise, this fails loudly rather than reporting a tidy
"no exception".
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


# The reflected set stays tied to the production dataclass, so that ADDING a
# field to InvestmentClassification fails this pair too and forces a decision
# about what the refusal path owes it -- rather than letting the refusal pair
# quietly opt out of the contract the classify pair is held to. It is also what
# _build_row emits its all-None row from, so the two cannot drift.
def _fields() -> frozenset[str]:
    return dataclass_field_names(SOURCE.read_text(), "InvestmentClassification")


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    # A missing config logs a warning, and any byte on the captured streams
    # lands in front of this oracle's JSON and breaks the decode.
    try:
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            classifier = InvestmentClassifier(
                _investment_helpers.config_path(str(case["Config"]))
            )
            result = classifier.classify(_investment_helpers.artifact(case))
    except Exception as exception:  # noqa: BLE001 -- the exception IS the measurement
        # Every classification field is emitted, and emitted as None, rather
        # than declared as an excluded_field. They are not absent from this
        # boundary for a reason peculiar to the pair -- they are absent because
        # NEITHER engine produced one, and that is itself worth comparing:
        # a Go port that refused and still returned a classification would
        # differ here. Excluding them would also have been a stale exclusion,
        # since a declared exclusion must match a key some row actually carries.
        row: dict[str, Any] = {name: None for name in _fields()}
        row["raises"] = type(exception).__name__
        return row
    raise AssertionError(
        f"case {case['id']!r} is registered as a REFUSAL case, but the real "
        f"Python classifier returned {result!r} for config "
        f"{case['Config']!r}. Either the config file stopped being malformed "
        "or the case belongs on the analytics/investment/classify pair -- do "
        "not soften this into a reported 'no exception', which would let the "
        "Go side agree with a refusal Python never made."
    )


oracle_registry.register(
    oracle_registry.PairSpec(
        id="analytics/investment/refusal",
        build_row=_build_row,
        reflected_fields=_fields,
    )
)
