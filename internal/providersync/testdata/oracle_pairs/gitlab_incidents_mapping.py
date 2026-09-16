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
        excluded_fields={
            "valid_from": "map_issue_incidents (the Python producer this oracle "
            "calls) never sets valid_from, leaving every mapping's validity "
            "window unbounded from the start; the Go port stamps it from the "
            "effect's own observation time instead, so every as-of reader over "
            "a Nullable valid_from column does not have to special-case a NULL "
            "row. Python is not changed to match: it is the retired plane for "
            "this producer.",
        },
    )
)
