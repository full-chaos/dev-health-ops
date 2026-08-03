from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._gitlab_tests_common import (
    build_pipeline_rows,
    plain_row,
    reflected,
)


def _build(case):
    rows = build_pipeline_rows(case).acceptance_checks
    return plain_row(next(row for row in rows if row["check_name"] == "unit"))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/tests/acceptance",
        build_row=_build,
        reflected_fields=lambda: reflected("CIAcceptanceCheckRow"),
        excluded_fields={
            "last_synced": "stamped by the Go complete-route effect boundary"
        },
    )
)
