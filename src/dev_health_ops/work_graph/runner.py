import argparse
import logging
from datetime import datetime

from dev_health_ops.work_graph.investment.materialize import (
    MaterializeConfig,
    materialize_investments,
)


async def materialize_fixture_investments(
    *,
    db_url: str,
    from_ts: datetime,
    to_ts: datetime,
    repo_ids: list[str] | None = None,
    team_ids: list[str] | None = None,
    org_id: str | None = None,
    llm_concurrency: int = 5,
) -> dict[str, int]:
    """Materialize fixture investments using the mock LLM provider.

    ``llm_concurrency`` defaults to 5, matching ``MaterializeConfig``'s own
    default -- existing callers that don't pass it see no behavior change.
    CHAOS-3219 Codex adversarial review (HIGH-4, 2026-08-05): the multi-task
    LLM categorization path (this function's caller, ``materialize_
    investments``) iterates pending tasks via ``asyncio.as_completed``,
    recording each result in COMPLETION order -- with more than one
    concurrent task, real async scheduling (not the master seed) decides
    that order, and if any recorded result influences later random-draw-
    consuming work, the whole downstream generation for whatever runs next
    can desync between two otherwise-identical runs. `dev_health_ops.
    fixtures.world` (an explicitly authorized exception to this module's
    general concurrency behavior -- see its own call site) passes
    ``llm_concurrency=1`` to collapse this to strictly sequential
    completion order, closing the one remaining source of run-to-run
    WORLD_DIGEST drift identified by that review.
    """
    config = MaterializeConfig(
        dsn=db_url,
        from_ts=from_ts,
        to_ts=to_ts,
        repo_ids=repo_ids,
        llm_provider="mock",
        persist_evidence_snippets=True,
        llm_model=None,
        team_ids=team_ids,
        force=True,
        org_id=org_id,
        llm_concurrency=llm_concurrency,
    )
    logging.info(
        "Materializing fixture investments from %s to %s",
        config.from_ts,
        config.to_ts,
    )
    return await materialize_investments(config)


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    """No CLI verbs remain to register here.

    `dev-hops work-graph build` was deleted under CHAOS-4924 and `dev-hops
    investment materialize` was deleted under CHAOS-5173 -- both were
    direct-Python-compute CLI entry points, superseded by the native River
    kinds triggered via `dev-health-workerctl workgraph trigger` /
    `investment trigger`. This hook stays a no-op, kept as the extension
    point cli.py already calls, rather than also ripping out its wiring.
    """
