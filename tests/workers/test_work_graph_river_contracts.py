from dev_health_ops.workers.job_contracts import load_registry
from dev_health_ops.workers.work_graph_tasks import RIVER_CONTRACT_TARGETS


def test_every_current_work_graph_and_investment_entrypoint_has_one_river_target() -> (
    None
):
    # dispatch_investment_materialize_partitioned/run_investment_materialize_chunk/
    # finalize_investment_materialize_partitioned were REMOVED under
    # CHAOS-4438: their Go-side kinds (investment.dispatch/chunk/finalize)
    # were deleted outright (dead Go shells, zero producers), so there is no
    # longer any River target for these three Celery-only entrypoints to
    # claim.
    assert RIVER_CONTRACT_TARGETS == {
        "run_work_graph_build": "workgraph.build",
        "run_investment_materialize": "investment.materialize",
    }
    registry = load_registry()
    assert {contract.kind for contract in registry.contracts}.issuperset(
        RIVER_CONTRACT_TARGETS.values()
    )
