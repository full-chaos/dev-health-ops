from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._github_tests_common import (
    build_rows,
    reflected,
)


def _build(case):
    _, _, coverage = build_rows(case)
    row = dict(coverage[0])
    row["repo_id"] = str(row["repo_id"])
    return row


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/tests/coverage",
        build_row=_build,
        reflected_fields=lambda: reflected("CoverageSnapshotRow"),
        excluded_fields={
            "snapshot_id": "CHAOS-4190: the Go producer scopes coverage "
            "SnapshotID to the artifact a report came from, so two "
            "artifacts of one run with the same report path never collide "
            "as duplicate natural keys. This Python function is reachable "
            "only from the manual CLI (dev-hops sync --target tests|cicd), "
            "unscheduled and off the automated dispatch path -- River/Go "
            "owns 100% of worker-driven tests/cicd sync in production -- "
            "and was intentionally not changed to match.",
        },
    )
)
