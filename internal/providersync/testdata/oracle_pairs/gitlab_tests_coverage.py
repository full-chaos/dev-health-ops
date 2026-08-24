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
            "last_synced": "stamped by the Go complete-route effect boundary",
            "snapshot_id": "CHAOS-4190: the Go producer scopes coverage "
            "SnapshotID to the artifact (GitLab job) a report came from, so "
            "two jobs of one pipeline with the same report path never "
            "collide as duplicate natural keys. This Python function is "
            "reachable only from the manual CLI (dev-hops sync --target "
            "tests|cicd), unscheduled and off the automated dispatch path "
            "-- River/Go owns 100% of worker-driven tests/cicd sync in "
            "production -- and was intentionally not changed to match.",
        },
    )
)
