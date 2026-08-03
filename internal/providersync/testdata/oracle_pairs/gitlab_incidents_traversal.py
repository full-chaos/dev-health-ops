from __future__ import annotations

from dataclasses import fields

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._gitlab_incidents_common import (
    TraversalTrace,
    build_traversal,
)

oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/incidents/traversal",
        build_row=build_traversal,
        reflected_fields=lambda: frozenset(
            field.name for field in fields(TraversalTrace)
        ),
    )
)
