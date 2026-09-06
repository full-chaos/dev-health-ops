import argparse
import asyncio
import logging
import os
from datetime import date, datetime, time, timedelta, timezone

from dev_health_ops.work_graph.investment.materialize import (
    MaterializeConfig,
    materialize_investments,
    resolve_llm_batch_min_items,
    resolve_llm_batch_mode,
    resolve_llm_batch_poll_interval_seconds,
    resolve_llm_batch_timeout_seconds,
)


def _llm_concurrency(value: object | None = None) -> int:
    raw = value if value is not None else os.getenv("INVESTMENT_LLM_CONCURRENCY", "5")
    try:
        concurrency = int(str(raw))
    except (TypeError, ValueError):
        concurrency = 5
    return max(1, concurrency)


def run_investment_materialization(ns: argparse.Namespace) -> int:
    analytics_db = str(
        getattr(ns, "analytics_db", None) or os.getenv("CLICKHOUSE_URI") or ""
    )
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

    org_id = getattr(ns, "org", None) or None

    config = MaterializeConfig(
        dsn=analytics_db,
        from_ts=from_ts,
        to_ts=to_ts,
        repo_ids=repo_ids or None,
        llm_provider=getattr(ns, "llm_provider", "auto") or "auto",
        persist_evidence_snippets=getattr(ns, "persist_evidence_snippets", True),
        llm_model=getattr(ns, "model", None),
        llm_api_key=str(getattr(ns, "llm_api_key", None) or ""),
        llm_base_url=str(getattr(ns, "llm_base_url", None) or ""),
        llm_concurrency=_llm_concurrency(getattr(ns, "llm_concurrency", None)),
        team_ids=team_ids or None,
        force=getattr(ns, "force", False),
        org_id=org_id,
        allow_unscoped=getattr(ns, "allow_unscoped", False),
        llm_batch_mode=resolve_llm_batch_mode(getattr(ns, "llm_batch_mode", None)),
        llm_batch_min_items=resolve_llm_batch_min_items(
            getattr(ns, "llm_batch_min_items", None)
        ),
        llm_batch_poll_interval_seconds=resolve_llm_batch_poll_interval_seconds(
            getattr(ns, "llm_batch_poll_interval_seconds", None)
        ),
        llm_batch_timeout_seconds=resolve_llm_batch_timeout_seconds(
            getattr(ns, "llm_batch_timeout_seconds", None)
        ),
    )

    # CHAOS-2433 round-4 finding #1: the materializer writes work_unit_investments
    # ONLY — membership rows + the completion marker are published EXCLUSIVELY by
    # the no-LLM full-coverage projection (the post-sync Celery chain runs it as a
    # 3rd step). This operator CLI entry point must do the same, or
    # `dev-hops investment materialize` would persist new investments WITHOUT
    # publishing a membership run/marker and GraphQL theme filters would keep
    # reading the stale/previous marker (or none). We therefore run the projection
    # synchronously after a successful materialization (a direct call, not a
    # Celery dispatch, so the CLI is complete on return).
    #
    # COVERAGE RULE (CHAOS-2433 finding #2, AMENDED by CHAOS-2776): the publish
    # gate is SCOPE, not window. ``backfill_memberships`` is ALWAYS full-coverage
    # BY CONSTRUCTION — it iterates the FULL current work graph and projects from
    # the latest persisted investments per unit (argMax(computed_at)), regardless
    # of the materialize window. --from/--to/--window-days only bound which units
    # get NEW LLM investment rows; they do NOT bound projection coverage. So an
    # UNSCOPED-but-WINDOWED materialize is safe to project: it republishes a
    # full-coverage marker at >= the newest investment clock and re-arms the
    # read-path stale-generation guard (CHAOS-2764). Previously we ALSO skipped
    # the projection for windowed runs, which — mirroring the post-sync Celery bug
    # this ticket fixes — left investments newer than the marker
    # (scope_mode='unscoped_fallback') until the daily 03:30 org-wide projection.
    # Only a repo/team-SCOPED run must NOT publish the org marker (a scoped
    # projection covers only in-scope units and would blank other repos for
    # unscoped reads); it relies on the org-wide daily projection to republish.
    is_org_wide = not repo_ids and not team_ids

    logging.info(f"Materializing investments from {config.from_ts} to {config.to_ts}")
    try:
        stats = asyncio.run(materialize_investments(config))
        logging.info(
            "Investment materialization complete. Components=%d Records=%d Quotes=%d",
            stats.get("components", 0),
            stats.get("records", 0),
            stats.get("quotes", 0),
        )
    except Exception as e:
        logging.error(f"Investment materialization failed: {e}")
        return 1

    if is_org_wide:
        # Publish a fresh full-coverage membership run + marker so theme filters
        # read the new investments. The projection is no-LLM and synchronous, and
        # its coverage is independent of any --from/--to/--window-days on this run
        # (CHAOS-2776).
        from dev_health_ops.work_graph.investment.backfill import (
            MembershipBackfillConfig,
            backfill_memberships,
        )

        logging.info(
            "Projecting work_unit_membership (no-LLM) to publish a full-coverage "
            "completion marker after org-wide materialization"
        )
        try:
            mstats = backfill_memberships(
                MembershipBackfillConfig(dsn=analytics_db, org_id=org_id, repo_ids=None)
            )
            logging.info(
                "Membership projection complete. Components=%d Matched=%d "
                "Memberships=%d",
                mstats.get("components", 0),
                mstats.get("matched", 0),
                mstats.get("memberships", 0),
            )
        except Exception as e:
            logging.error(f"Membership projection failed: {e}")
            return 1
    else:
        logging.info(
            "Scoped materialization (repos=%s teams=%s) — NOT publishing an "
            "org-wide membership marker; the org-wide daily projection "
            "republishes full coverage (CHAOS-2433/2776).",
            repo_ids or None,
            team_ids or None,
        )

    return 0


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
        "--analytics-db",
        "--db",
        dest="analytics_db",
        default=argparse.SUPPRESS,
        help="ClickHouse connection string (clickhouse://user:pass@host:port/db). "
        "Env: CLICKHOUSE_URI. Deprecated alias on this subcommand: --db.",
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
        default=None,
        help="Window size in days when --from is not set (default: 30). "
        "Bounds which WorkUnits get NEW LLM investment rows; it does NOT bound "
        "membership-projection coverage, so an unscoped windowed run still "
        "publishes a full-coverage org-wide membership marker (CHAOS-2776). "
        "Only --repo-id/--team-id scoping suppresses the org-wide marker "
        "(CHAOS-2433 coverage rule).",
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

    add_llm_arguments(investment_materialize, leaf_mode=True)
    investment_materialize.add_argument(
        "--persist-evidence-snippets",
        dest="persist_evidence_snippets",
        action="store_true",
        default=True,
        help="Persist extractive evidence quotes for work units.",
    )
    investment_materialize.add_argument(
        "--no-persist-evidence-snippets",
        dest="persist_evidence_snippets",
        action="store_false",
        help="Skip persisting extractive evidence quotes for work units.",
    )
    investment_materialize.add_argument(
        "--force", action="store_true", help="Force re-materialization."
    )
    investment_materialize.add_argument(
        "--llm-batch-mode",
        choices=("sync", "auto", "provider_batch"),
        default=argparse.SUPPRESS,
        help="LLM categorization execution mode. Env: INVESTMENT_LLM_BATCH_MODE. "
        "Default: sync.",
    )
    investment_materialize.add_argument(
        "--llm-batch-min-items",
        type=int,
        default=argparse.SUPPRESS,
        help="Minimum eligible LLM items before auto mode uses provider batch. "
        "Env: INVESTMENT_LLM_BATCH_MIN_ITEMS. Default: 25.",
    )
    investment_materialize.add_argument(
        "--llm-batch-poll-interval-seconds",
        type=float,
        default=argparse.SUPPRESS,
        help="Provider batch polling interval for CLI/worker completion waits. "
        "Env: INVESTMENT_LLM_BATCH_POLL_INTERVAL_SECONDS. Default: 30.",
    )
    investment_materialize.add_argument(
        "--llm-batch-timeout-seconds",
        type=float,
        default=argparse.SUPPRESS,
        help="Provider batch timeout for CLI/worker completion waits. "
        "Env: INVESTMENT_LLM_BATCH_TIMEOUT_SECONDS. Default: 3000.",
    )
    investment_materialize.add_argument(
        "--allow-unscoped",
        action="store_true",
        help="Allow real LLM materialization without --org, writing empty-org rows.",
    )
    investment_materialize.set_defaults(func=run_investment_materialization)
