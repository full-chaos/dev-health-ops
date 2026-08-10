from __future__ import annotations

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._gitlab_files_common import (
    build_traversal,
    reflected_trace_fields,
)

oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/files/trace",
        build_row=build_traversal,
        reflected_fields=reflected_trace_fields,
    )
)
