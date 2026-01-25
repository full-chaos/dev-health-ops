import argparse
import asyncio
import logging
import os
import uuid
from datetime import date, datetime, time, timedelta, timezone

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


def run_work_graph_build(ns: argparse.Namespace) -> int:
    """Build work graph edges from raw data."""

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

    config = BuildConfig(
        dsn=ns.db,
        from_date=from_date,
        to_date=to_date,
        repo_id=repo_id,
        heuristic_days_window=ns.heuristic_window,
        heuristic_confidence=ns.heuristic_confidence,
    )

    logging.info(f"Building work graph from {config.from_date} to {config.to_date}")
    builder = WorkGraphBuilder(config)
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


def run_investment_materialization(ns: argparse.Namespace) -> int:
    """Materialize investment metrics for a given window."""

    now = datetime.now(timezone.utc)
    if ns.to_date:
        to_day = date.fromisoformat(ns.to_date)
        to_ts = datetime.combine(
            to_day + timedelta(days=1), time.min, tzinfo=timezone.utc
        )
    else:
        to_ts = now

    if ns.from_date:
        from_day = date.fromisoformat(ns.from_date)
        from_ts = datetime.combine(from_day, time.min, tzinfo=timezone.utc)
    else:
        window_days = max(1, int(ns.window_days or 30))
        from_ts = to_ts - timedelta(days=window_days)

    if from_ts >= to_ts:
        logging.error("--from must be before --to")
        return 2

    repo_ids = [repo_id for repo_id in (ns.repo_id or []) if repo_id]
    team_ids = [team_id for team_id in (ns.team_id or []) if team_id]

    config = MaterializeConfig(
        dsn=ns.db,
        from_ts=from_ts,
        to_ts=to_ts,
        repo_ids=repo_ids or None,
        llm_provider=getattr(ns, "llm_provider", "auto") or "auto",
        persist_evidence_snippets=getattr(ns, "persist_evidence_snippets", False),
        llm_model=getattr(ns, "model", None),
        team_ids=team_ids or None,
        force=getattr(ns, "force", False),
    )

    logging.info(f"Materializing investments from {config.from_ts} to {config.to_ts}")
    try:
        stats = asyncio.run(materialize_investments(config))
        logging.info(
            "Investment materialization complete. Components=%d Records=%d Quotes=%d",
            stats.get("components", 0),
            stats.get("records", 0),
            stats.get("quotes", 0),
        )
        return 0
    except Exception as e:
        logging.error(f"Investment materialization failed: {e}")
        return 1


async def materialize_fixture_investments(
    *,
    db_url: str,
    from_ts: datetime,
    to_ts: datetime,
    repo_ids: list[str] | None = None,
    team_ids: list[str] | None = None,
) -> dict[str, int]:
    """Materialize fixture investments using the mock LLM provider."""
    config = MaterializeConfig(
        dsn=db_url,
        from_ts=from_ts,
        to_ts=to_ts,
        repo_ids=repo_ids,
        llm_provider="mock",
        persist_evidence_snippets=False,
        llm_model=None,
        team_ids=team_ids,
        force=True,
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

    # ---- investment ----
    investment = subparsers.add_parser(
        "investment", help="Investment materialization operations."
    )
    investment_sub = investment.add_subparsers(dest="investment_command", required=True)
    investment_materialize = investment_sub.add_parser(
        "materialize",
        help="Materialize work unit investment categorization into sinks.",
    )
    investment_materialize.add_argument(
        "--db",
        default=os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL"),
        help="ClickHouse connection string (clickhouse://user:pass@host:port/db).",
    )
    investment_materialize.add_argument(
        "--from",
        dest="from_date",
        type=str,
        help="Start date (YYYY-MM-DD). Defaults to window-days before --to.",
    )
    investment_materialize.add_argument(
        "--to",
        dest="to_date",
        type=str,
        help="End date (YYYY-MM-DD). Defaults to now.",
    )
    investment_materialize.add_argument(
        "--window-days",
        type=int,
        default=30,
        help="Window size in days when --from is not set (default: 30).",
    )
    investment_materialize.add_argument(
        "--repo-id",
        action="append",
        default=[],
        help="Filter to specific repository UUID(s).",
    )
    investment_materialize.add_argument(
        "--team-id",
        action="append",
        default=[],
        help="Filter to specific team identifier(s).",
    )
    from dev_health_ops.llm.cli import add_llm_arguments

    add_llm_arguments(investment_materialize)
    investment_materialize.add_argument(
        "--persist-evidence-snippets",
        action="store_true",
        help="Persist extractive evidence quotes for work units.",
    )
    investment_materialize.add_argument(
        "--force", action="store_true", help="Force re-materialization."
    )
    investment_materialize.set_defaults(func=run_investment_materialization)
