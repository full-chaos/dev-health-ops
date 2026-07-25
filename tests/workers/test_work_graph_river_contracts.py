from dev_health_ops.workers.job_contracts import load_registry
from dev_health_ops.workers.work_graph_tasks import RIVER_CONTRACT_TARGETS


def test_every_current_work_graph_and_investment_entrypoint_has_one_river_target() -> (
    None
):
    assert RIVER_CONTRACT_TARGETS == {
        "run_work_graph_build": "workgraph.build",
        "run_investment_materialize": "investment.materialize",
        "dispatch_investment_materialize_partitioned": "investment.dispatch",
        "run_investment_materialize_chunk": "investment.chunk",
        "finalize_investment_materialize_partitioned": "investment.finalize",
    }
    registry = load_registry()
    assert {contract.kind for contract in registry.contracts}.issuperset(
        RIVER_CONTRACT_TARGETS.values()
    )
