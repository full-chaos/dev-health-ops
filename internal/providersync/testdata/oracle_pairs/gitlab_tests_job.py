from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._gitlab_tests_common import (
    build_pipeline_rows,
    plain_row,
    reflected,
)

oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/tests/job",
        build_row=lambda case: plain_row(build_pipeline_rows(case).job_runs[0]),
        reflected_fields=lambda: reflected("JobRunRow"),
        excluded_fields={
            "last_synced": "stamped by the Go complete-route effect boundary"
        },
    )
)
