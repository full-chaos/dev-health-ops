from __future__ import annotations

import subprocess
import sys

import pytest

_OPERATIONAL_MODULE = "dev_health_ops.models.operational"
_ORDERING_MODULE = "dev_health_ops.models.operational_ordering"


@pytest.mark.parametrize(
    ("first_module", "second_module"),
    (
        (_OPERATIONAL_MODULE, _ORDERING_MODULE),
        (_ORDERING_MODULE, _OPERATIONAL_MODULE),
    ),
)
def test_operational_modules_import_in_both_orders_with_type_checking_edges(
    first_module: str,
    second_module: str,
) -> None:
    # Given: a fresh interpreter that evaluates analyzer-visible imports.
    script = f"""
import importlib
import typing

importlib.import_module("dev_health_ops.models")
typing.TYPE_CHECKING = True
importlib.import_module({first_module!r})
operational = importlib.import_module({_OPERATIONAL_MODULE!r})
ordering = importlib.import_module({_ORDERING_MODULE!r})
assert hasattr(operational, "CanonicalOperationalEntity")
assert hasattr(ordering, "build_entity_ordering")
importlib.import_module({second_module!r})
"""

    # When: the two operational modules are imported in the selected order.
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        check=False,
        text=True,
    )

    # Then: neither order observes a partially initialized public definition.
    assert result.returncode == 0, result.stderr
