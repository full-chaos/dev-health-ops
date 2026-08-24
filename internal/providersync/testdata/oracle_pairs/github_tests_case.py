from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._github_tests_common import (
    build_rows,
    reflected,
)


def _build(case):
    _, cases, _ = build_rows(case)
    row = dict(cases[1])
    row["repo_id"] = str(row["repo_id"])
    return row


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/tests/case",
        build_row=_build,
        reflected_fields=lambda: reflected("TestCaseResultRow"),
        excluded_fields={
            "suite_id": "CHAOS-4190: inherited from the artifact-scoped "
            "SuiteID (see github/tests/suite's excluded_fields).",
            "case_id": "CHAOS-4190: CaseID hashes the artifact-scoped "
            "SuiteID plus the case name, so it diverges for the same "
            "reason. This Python function is reachable only from the "
            "manual CLI (dev-hops sync --target tests|cicd), unscheduled "
            "and off the automated dispatch path -- River/Go owns 100% of "
            "worker-driven tests/cicd sync in production -- and was "
            "intentionally not changed to match.",
        },
    )
)
