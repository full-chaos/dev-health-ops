import argparse
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

from dev_health_ops.work_graph.builder import BuildConfig, WorkGraphBuilder
from dev_health_ops.work_graph.investment.materialize import (
    MaterializeConfig,
    materialize_investments,
)


def _component_count(edges: list[tuple[str, str, str, str]]) -> int:
    """Count number of connected components in the graph."""
    adj: dict[tuple[str, str], list[tuple[str, str]]] = {}
    nodes: set[tuple[str, str]] = set()
    for source_type, source_id, target_type, target_id in edges:
        s = (source_type, source_id)
        t = (target_type, target_id)
        nodes.add(s)
        nodes.add(t)
        adj.setdefault(s, []).append(t)
        adj.setdefault(t, []).append(s)

    visited: set[tuple[str, str]] = set()
    count = 0
    for node in nodes:
        if node not in visited:
            count += 1
            stack = [node]
            visited.add(node)
            while stack:
                curr = stack.pop()
                for neighbor in adj.get(curr, []):
                    if neighbor not in visited:
                        visited.add(neighbor)
                        stack.append(neighbor)
    return count


def _llm_concurrency(value: object | None = None) -> int:
    raw = value if value is not None else os.getenv("INVESTMENT_LLM_CONCURRENCY", "5")
    try:
        concurrency = int(str(raw))
    except (TypeError, ValueError):
        concurrency = 5
    return max(1, concurrency)


def run_work_graph_build(ns: argparse.Namespace) -> int:
    # Parse dates
    from_date = None
    to_date = None

    if ns.from_date:
        from_date = datetime.fromisoformat(ns.from_date).replace(tzinfo=timezone.utc)
    else:
        from_date = datetime.now(timezone.utc) - timedelta(days=30)

    if ns.to_date:
        to_date = datetime.fromisoformat(ns.to_date).replace(tzinfo=timezone.utc)
    else:
        to_date = datetime.now(timezone.utc)

    # Parse repo_id if provided
    repo_id = None
    if ns.repo_id:
        repo_id = uuid.UUID(ns.repo_id)

    # Resolve the tenant scope. If --org was supplied it MUST flow into
    # BuildConfig so every read/write the builder performs is org-scoped: the
    # *_from_fast_path readers still in Python only apply the org filter (and
    # stamp org_id onto their written rows) when BuildConfig.org_id is set.
    # Dropping it here let `dev-hops work-graph build` scan all visible
    # PRs/commits and persist derived links under the empty org -- the intended
    # tenant would miss its links while cross-tenant linkage material
    # accumulated in the empty-org bucket (CHAOS-2375 round-3). PR->commit
    # derivation itself moved off this path entirely (CHAOS-5264, native Go
    # pre-step, internal/jobs/workgraph/prcommit) -- this CLI entry point does
    # NOT run it, same known gap as CHAOS-5256 for issue-PR links.
    org_raw = getattr(ns, "org", None)
    org_id = org_raw or None
    if org_raw is not None and not str(org_raw).strip():
        # An explicit but blank --org is almost certainly a mis-wired tenant
        # scope; fail closed rather than silently building under the empty org.
        logging.error(
            "FAIL: --org was provided but resolved empty; refusing to build an "
            "unscoped work graph. Pass a concrete org id or omit --org for a "
            "full rebuild."
        )
        return 2
    logging.info("Building work graph for org_id=%s", org_id)

    config = BuildConfig(
        dsn=ns.db,
        from_date=from_date,
        to_date=to_date,
        repo_id=repo_id,
        heuristic_days_window=ns.heuristic_window,
        heuristic_confidence=ns.heuristic_confidence,
        org_id=org_id or "",
    )

    logging.info(f"Building work graph from {config.from_date} to {config.to_date}")
    try:
        builder = WorkGraphBuilder(config)
    except ValueError as exc:
        parser = getattr(ns, "_leaf_parser", None)
        if parser is not None:
            parser.error(str(exc))
        logging.error("Work graph build failed: %s", exc)
        return 2
    try:
        result = builder.build()

        total_edges = sum(result.values())
        logging.info("Work graph build complete. Total edges: %d", total_edges)
        logging.info("  issue_issue_edges: %d", result.get("issue_issue_edges", 0))
        logging.info("  issue_pr_edges: %d", result.get("issue_pr_edges", 0))
        logging.info("  pr_commit_edges: %d", result.get("pr_commit_edges", 0))
        logging.info("  commit_file_edges: %d", result.get("commit_file_edges", 0))
        logging.info("  heuristic_edges: %d", result.get("heuristic_edges", 0))

        client = getattr(builder, "client", None)
        if client is None:
            client = getattr(getattr(builder, "sink", None), "client", None)
        if client is None:
            logging.error(
                "FAIL: Work graph builder did not expose a ClickHouse client."
            )
            return 1

        where_parts = [
            f"event_ts >= '{from_date.strftime('%Y-%m-%d %H:%M:%S')}'",
            f"event_ts <= '{to_date.strftime('%Y-%m-%d %H:%M:%S')}'",
        ]
        if repo_id:
            where_parts.append(f"repo_id = '{repo_id}'")
        # Verify against THIS tenant's edges only; otherwise a tenant-scoped build
        # could "pass" on another org's rows while writing nothing of its own.
        if org_id:
            where_parts.append(f"org_id = '{org_id}'")
        where_sql = " AND ".join(where_parts)

        # Check edge count in DB for verification
        edge_count = client.query(
            f"SELECT count() FROM work_graph_edges WHERE {where_sql}"
        ).result_rows[0][0]
        if int(edge_count or 0) == 0:
            logging.error(
                "FAIL: work_graph_edges is empty for the selected window. "
                "Prerequisites missing or build produced no edges."
            )
            return 1

        if ns.check_components:
            edge_rows = (
                client.query(
                    f"""
                SELECT source_type, source_id, target_type, target_id
                FROM work_graph_edges
                WHERE {where_sql}
                """
                ).result_rows
                or []
            )
            edge_list = [
                (
                    str(e[0]),
                    str(e[1]),
                    str(e[2]),
                    str(e[3]),
                )
                for e in edge_rows
            ]
            comp_count = _component_count(edge_list)
            if comp_count == 1 and not getattr(ns, "allow_degenerate", False):
                logging.error(
                    "FAIL: Work graph is degenerate (connected_components=1). "
                    "Re-run with --allow-degenerate to override."
                )
                return 1
            logging.info("Connected components in window: %d", comp_count)

        return 0
    except Exception as e:
        logging.error(f"Work graph build failed: {e}")
        return 1
    finally:
        builder.close()


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
    # ---- work-graph ----
    wg = subparsers.add_parser("work-graph", help="Work graph operations.")
    wg_sub = wg.add_subparsers(dest="work_graph_command", required=True)

    wg_build = wg_sub.add_parser("build", help="Build work graph edges from raw data.")
    wg_build.add_argument(
        "--db",
        required=True,
        help="ClickHouse connection string (clickhouse://user:pass@host:port/db).",
    )
    wg_build.add_argument(
        "--from",
        dest="from_date",
        type=str,
        help="Start date (YYYY-MM-DD). Defaults to 30 days ago.",
    )
    wg_build.add_argument(
        "--to",
        dest="to_date",
        type=str,
        help="End date (YYYY-MM-DD). Defaults to today.",
    )
    wg_build.add_argument(
        "--repo-id",
        type=str,
        help="Filter to specific repository UUID.",
    )
    wg_build.add_argument(
        "--heuristic-window",
        type=int,
        default=7,
        help="Days window for heuristic issue->PR matching (default: 7).",
    )
    wg_build.add_argument(
        "--heuristic-confidence",
        type=float,
        default=0.3,
        help="Confidence score for heuristic matches (default: 0.3).",
    )
    wg_build.add_argument(
        "--allow-degenerate",
        action="store_true",
        help="Allow single connected-component graphs (default: fail).",
    )
    wg_build.add_argument(
        "--check-components",
        action="store_true",
        default=True,
        help="Perform component analysis (enabled by default).",
    )
    wg_build.set_defaults(func=run_work_graph_build)
