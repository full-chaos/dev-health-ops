from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._github_tests_common import (
    build_pipeline_rows,
    plain_row,
    reflected,
)


def _build(case):
    batch = build_pipeline_rows(case)
    return plain_row(batch.pipeline_runs[0])


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/tests/pipeline",
        build_row=_build,
        reflected_fields=lambda: reflected("PipelineRunExtendedRow"),
    )
)
