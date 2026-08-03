from __future__ import annotations

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._gitlab_incidents_common import (
    OPERATIONAL,
    build_family,
    reflected,
)

oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/incidents/mapping",
        build_row=lambda case: build_family(case, "mapping"),
        reflected_fields=lambda: reflected(OPERATIONAL.ServiceRepositoryMapping),
    )
)
