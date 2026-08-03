from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._gitlab_tests_common import (
    build_coverage_rows,
    plain_row,
    reflected,
)

oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/tests/coverage",
        build_row=lambda case: plain_row(build_coverage_rows(case)[0]),
        reflected_fields=lambda: reflected("CoverageSnapshotRow"),
        excluded_fields={
            "last_synced": "stamped by the Go complete-route effect boundary"
        },
    )
)
