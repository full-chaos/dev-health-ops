from __future__ import annotations

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._gitlab_deployments_common import (
    build_traversal,
    traversal_fields,
)

oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/deployments/trace",
        build_row=build_traversal,
        reflected_fields=traversal_fields,
    )
)
