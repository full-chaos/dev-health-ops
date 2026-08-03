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
    )
)
