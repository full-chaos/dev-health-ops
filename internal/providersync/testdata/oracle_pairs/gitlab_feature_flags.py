from __future__ import annotations

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._gitlab_feature_flags_common import (
    build_trace,
    trace_fields,
)

oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/feature/flags",
        build_row=build_trace,
        reflected_fields=trace_fields,
    )
)
