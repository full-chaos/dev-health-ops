import argparse
import asyncio
import logging
import os
import random
from datetime import datetime, timedelta, timezone

from analytics.investment import InvestmentClassifier
from analytics.issue_types import IssueTypeNormalizer
from fixtures.generator import SyntheticDataGenerator
from metrics.compute import compute_daily_metrics
from metrics.compute_cicd import compute_cicd_metrics_daily
from metrics.compute_deployments import compute_deploy_metrics_daily
from metrics.compute_ic import compute_ic_landscape_rolling, compute_ic_metrics_daily
from metrics.compute_incidents import compute_incident_metrics_daily
from metrics.compute_wellbeing import compute_team_wellbeing_metrics_daily
from metrics.compute_work_item_state_durations import (
    compute_work_item_state_durations_daily,
)
from metrics.compute_work_items import compute_work_item_metrics_daily
from metrics.hotspots import compute_file_hotspots
from metrics.schemas import (
    DailyMetricsResult,
    InvestmentClassificationRecord,
    IssueTypeMetricsRecord,
)
from metrics.sinks.base import BaseMetricsSink
from providers.identity import load_identity_resolver
from providers.teams import TeamResolver
from storage import SQLAlchemyStore, resolve_db_type, run_with_store
from utils import BATCH_SIZE, MAX_WORKERS, REPO_ROOT


async def _insert_batches(
    insert_fn, items, batch_size: int = BATCH_SIZE, allow_parallel: bool = True
) -> None:
    if not items:
        return
    batches = [items[i : i + batch_size] for i in range(0, len(items), batch_size)]
    if not allow_parallel or MAX_WORKERS <= 1 or len(batches) == 1:
        for batch in batches:
            await insert_fn(batch)
        return

    insert_semaphore = asyncio.Semaphore(MAX_WORKERS)

    async def _run(batch):
        async with insert_semaphore:
            await insert_fn(batch)

    await asyncio.gather(*(_run(batch) for batch in batches))


def _write_metrics_result(sink: BaseMetricsSink, result: DailyMetricsResult) -> None:
    """Helper to write the DailyMetricsResult to any BaseMetricsSink implementation."""
    if result.repo_metrics:
        sink.write_repo_metrics(result.repo_metrics)
    if result.user_metrics:
        sink.write_user_metrics(result.user_metrics)
    if result.commit_metrics:
        sink.write_commit_metrics(result.commit_metrics)
    if result.team_metrics:
        sink.write_team_metrics(result.team_metrics)
    if result.file_metrics:
        sink.write_file_metrics(result.file_metrics)
    if result.work_item_metrics:
        sink.write_work_item_metrics(result.work_item_metrics)
    if result.work_item_user_metrics:
        sink.write_work_item_user_metrics(result.work_item_user_metrics)
    if result.work_item_cycle_times:
        sink.write_work_item_cycle_times(result.work_item_cycle_times)
    if result.work_item_state_durations:
        sink.write_work_item_state_durations(result.work_item_state_durations)
    if result.review_edges:
        sink.write_review_edges(result.review_edges)


async def run_fixtures_generation(ns: argparse.Namespace) -> int:
    now = datetime.now(timezone.utc)
    db_type = resolve_db_type(ns.db, ns.db_type)

    async def _handler(store):
        repo_count = max(1, ns.repo_count)
        base_name = ns.repo_name
        team_count = getattr(ns, "team_count", 8)
        team_assignment = SyntheticDataGenerator(
            repo_name=base_name, seed=ns.seed
        ).get_team_assignment(count=team_count)

        all_teams = team_assignment.get("teams", [])
        if hasattr(store, "insert_teams") and all_teams:
            await store.insert_teams(all_teams)
            logging.info("Inserted %d synthetic teams.", len(all_teams))

        allow_parallel_inserts = not isinstance(store, SQLAlchemyStore)

        for i in range(repo_count):
            r_name = base_name if repo_count == 1 else f"{base_name}-{i + 1}"
            logging.info(
                f"Generating fixture data for repo {i + 1}/{repo_count}: {r_name}"
            )
            seed_value = (int(ns.seed) + i) if ns.seed is not None else None

            # Pick 1-3 teams for this repo from the pool
            # Use deterministic choice based on seed
            pool_random = (
                random.Random(seed_value)
                if seed_value is not None
                else random.Random(i)
            )
            num_teams_for_repo = pool_random.randint(1, min(3, len(all_teams)))
            assigned_teams = pool_random.sample(all_teams, num_teams_for_repo)

            generator = SyntheticDataGenerator(
                repo_name=r_name,
                provider=ns.provider,
                seed=seed_value,
                assigned_teams=assigned_teams,
            )

            # 1. Repo
            repo = generator.generate_repo()
            await store.insert_repo(repo)

            # 2. Files
            files = generator.generate_files()
            await _insert_batches(
                store.insert_git_file_data, files, allow_parallel=allow_parallel_inserts
            )

            # 3. Commits & Stats
            commits = generator.generate_commits(
                days=ns.days, commits_per_day=ns.commits_per_day
            )
            await _insert_batches(
                store.insert_git_commit_data,
                commits,
                allow_parallel=allow_parallel_inserts,
            )
            stats = generator.generate_commit_stats(commits)
            await _insert_batches(
                store.insert_git_commit_stats,
                stats,
                allow_parallel=allow_parallel_inserts,
            )

            # 4. Work Items
            work_items = generator.generate_work_items(
                days=ns.days, provider=ns.provider
            )
            transitions = generator.generate_work_item_transitions(work_items)

            if hasattr(store, "insert_work_items"):
                await _insert_batches(
                    store.insert_work_items,
                    work_items,
                    allow_parallel=allow_parallel_inserts,
                )
            if hasattr(store, "insert_work_item_transitions"):
                await _insert_batches(
                    store.insert_work_item_transitions,
                    transitions,
                    allow_parallel=allow_parallel_inserts,
                )

            dependencies = generator.generate_work_item_dependencies(work_items)
            if hasattr(store, "insert_work_item_dependencies"):
                await _insert_batches(
                    store.insert_work_item_dependencies,
                    dependencies,
                    allow_parallel=allow_parallel_inserts,
                )

            issue_numbers = []
            for item in work_items:
                raw_id = str(getattr(item, "work_item_id", "") or "")
                if "#" in raw_id:
                    tail = raw_id.split("#")[-1]
                    if tail.isdigit():
                        issue_numbers.append(int(tail))
                        continue
                if "-" in raw_id:
                    tail = raw_id.split("-")[-1]
                    if tail.isdigit():
                        issue_numbers.append(int(tail))

            # 5. PRs & Reviews
            pr_data = generator.generate_prs(
                count=ns.pr_count, issue_numbers=issue_numbers
            )
            prs = [p["pr"] for p in pr_data]
            await _insert_batches(
                store.insert_git_pull_requests,
                prs,
                allow_parallel=allow_parallel_inserts,
            )

            all_reviews = []
            for p in pr_data:
                all_reviews.extend(p["reviews"])
            await _insert_batches(
                store.insert_git_pull_request_reviews,
                all_reviews,
                allow_parallel=allow_parallel_inserts,
            )

            pr_commit_links = generator.generate_pr_commits(prs, commits)
            if hasattr(store, "insert_work_graph_pr_commit"):
                await _insert_batches(
                    store.insert_work_graph_pr_commit,
                    pr_commit_links,
                    allow_parallel=allow_parallel_inserts,
                )

            issue_pr_links = generator.generate_issue_pr_links(
                work_items, prs, min_coverage=0.7
            )
            if hasattr(store, "insert_work_graph_issue_pr"):
                await _insert_batches(
                    store.insert_work_graph_issue_pr,
                    issue_pr_links,
                    allow_parallel=allow_parallel_inserts,
                )

            # 6. CI/CD + Deployments + Incidents
            pr_numbers = [pr.number for pr in prs]
            pipeline_runs = generator.generate_ci_pipeline_runs(days=ns.days)
            deployments = generator.generate_deployments(
                days=ns.days, pr_numbers=pr_numbers
            )
            incidents = generator.generate_incidents(days=ns.days)
            await _insert_batches(
                store.insert_ci_pipeline_runs,
                pipeline_runs,
                allow_parallel=allow_parallel_inserts,
            )
            await _insert_batches(
                store.insert_deployments,
                deployments,
                allow_parallel=allow_parallel_inserts,
            )
            await _insert_batches(
                store.insert_incidents, incidents, allow_parallel=allow_parallel_inserts
            )

            # 7. Blame
            blame_data = generator.generate_blame(commits)
            await _insert_batches(
                store.insert_blame_data,
                blame_data,
                allow_parallel=allow_parallel_inserts,
            )

            # 8. Metrics
            if ns.with_metrics:
                from metrics.job_daily import (
                    ClickHouseMetricsSink,
                    MongoMetricsSink,
                    PostgresMetricsSink,
                    SQLiteMetricsSink,
                )

                sink = None
                if db_type == "clickhouse":
                    sink = ClickHouseMetricsSink(ns.db)
                elif db_type == "sqlite":
                    from metrics.job_daily import _normalize_sqlite_url

                    sink = SQLiteMetricsSink(_normalize_sqlite_url(ns.db))
                elif db_type == "mongo":
                    sink = MongoMetricsSink(ns.db)
                elif db_type == "postgres":
                    sink = PostgresMetricsSink(ns.db)

                if sink:
                    if isinstance(sink, MongoMetricsSink):
                        sink.ensure_indexes()
                    else:
                        sink.ensure_tables()

                    comp_data = generator.generate_complexity_metrics(days=ns.days)
                    complexity_by_day = {}
                    for snapshot in comp_data["snapshots"]:
                        complexity_by_day.setdefault(snapshot.as_of_day, {})[
                            snapshot.file_path
                        ] = snapshot

                    if hasattr(sink, "write_file_complexity_snapshots"):
                        if comp_data["snapshots"]:
                            sink.write_file_complexity_snapshots(comp_data["snapshots"])
                        if comp_data["dailies"]:
                            sink.write_repo_complexity_daily(comp_data["dailies"])

                    blame_concentration = {}
                    if blame_data:
                        blame_by_file = {}
                        for row in blame_data:
                            author = (
                                getattr(row, "author_email", None)
                                or getattr(row, "author_name", None)
                                or "unknown"
                            )
                            path = getattr(row, "path", None)
                            if path:
                                blame_by_file.setdefault(path, {})[author] = (
                                    blame_by_file.setdefault(path, {}).get(author, 0)
                                    + 1
                                )
                        for path, counts in blame_by_file.items():
                            total = sum(counts.values())
                            if total:
                                blame_concentration[path] = max(
                                    counts.values()
                                ) / float(total)

                    investment_classifier = InvestmentClassifier(
                        REPO_ROOT / "config/investment_areas.yaml"
                    )
                    issue_type_normalizer = IssueTypeNormalizer(
                        REPO_ROOT / "config/issue_type_mapping.yaml"
                    )
                    identity_resolver = load_identity_resolver()
                    team_resolver = TeamResolver(
                        member_to_team=team_assignment["member_map"]
                    )
                    team_map = {
                        k: v[0] for k, v in team_assignment["member_map"].items()
                    }

                    commit_by_hash = {c.hash: c for c in commits}
                    commit_stat_rows = []
                    for stat in stats:
                        c = commit_by_hash.get(stat.commit_hash)
                        if c:
                            commit_stat_rows.append({
                                "repo_id": stat.repo_id,
                                "commit_hash": stat.commit_hash,
                                "author_email": c.author_email,
                                "author_name": c.author_name,
                                "committer_when": c.committer_when,
                                "file_path": stat.file_path,
                                "additions": stat.additions,
                                "deletions": stat.deletions,
                            })

                    pr_rows = []
                    for pr in prs:
                        pr_rows.append({
                            "repo_id": pr.repo_id,
                            "number": pr.number,
                            "author_email": pr.author_email,
                            "author_name": pr.author_name,
                            "created_at": pr.created_at,
                            "merged_at": pr.merged_at,
                            "additions": pr.additions,
                            "deletions": pr.deletions,
                            "changed_files": pr.changed_files,
                        })

                    review_rows = []
                    for rev in all_reviews:
                        review_rows.append({
                            "repo_id": rev.repo_id,
                            "number": rev.number,
                            "reviewer": rev.reviewer,
                            "submitted_at": rev.submitted_at,
                            "state": rev.state,
                        })

                    for d_idx in range(ns.days):
                        day = datetime.now(timezone.utc).replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ) - timedelta(days=d_idx)
                        day_date = day.date()
                        start_dt = day
                        end_dt = day + timedelta(days=1)
                        day_commit_stats = [
                            s
                            for s in commit_stat_rows
                            if start_dt <= s["committer_when"] < end_dt
                        ]
                        day_work_items = [
                            wi for wi in work_items if wi.created_at < end_dt
                        ]
                        day_transitions = [
                            t for t in transitions if t.occurred_at < end_dt
                        ]

                        day_prs = [
                            p
                            for p in pr_rows
                            if (p["merged_at"] and start_dt <= p["merged_at"] < end_dt)
                            or (
                                not p["merged_at"]
                                and start_dt <= p["created_at"] < end_dt
                            )
                        ]
                        day_reviews = [
                            r
                            for r in review_rows
                            if start_dt <= r["submitted_at"] < end_dt
                        ]

                        try:
                            # 1. Base Metrics (Repo, User, Commit)
                            metrics_result = compute_daily_metrics(
                                day=day_date,
                                commit_stat_rows=day_commit_stats,
                                pull_request_rows=day_prs,
                                pull_request_review_rows=day_reviews,
                                computed_at=now,
                                identity_resolver=identity_resolver,
                                team_resolver=team_resolver,
                            )
                            _write_metrics_result(sink, metrics_result)

                            # 2. Work Item Metrics
                            wi_recs, wi_user_recs, wi_cycle_recs = (
                                compute_work_item_metrics_daily(
                                    day=day_date,
                                    work_items=day_work_items,
                                    transitions=day_transitions,
                                    computed_at=now,
                                    team_resolver=team_resolver,
                                )
                            )
                            sink.write_work_item_metrics(wi_recs)
                            sink.write_work_item_user_metrics(wi_user_recs)
                            sink.write_work_item_cycle_times(wi_cycle_recs)

                            # 3. IC Metrics (Enriched User Metrics)
                            ic_metrics = compute_ic_metrics_daily(
                                git_metrics=metrics_result.user_metrics,
                                wi_metrics=wi_user_recs,
                                team_map=team_map,
                            )
                            sink.write_user_metrics(ic_metrics)  # Enriched ones

                            # 4. State Duration Metrics
                            sd_recs = compute_work_item_state_durations_daily(
                                day=day_date,
                                work_items=day_work_items,
                                transitions=day_transitions,
                                computed_at=now,
                                team_resolver=team_resolver,
                            )
                            sink.write_work_item_state_durations(sd_recs)

                            # 5. Issue Types (Simplified aggregation for fixtures)
                            it_recs = []
                            for wi in day_work_items:
                                norm_type = issue_type_normalizer.normalize(
                                    wi.provider, wi.type or "task", wi.labels or []
                                )
                                it_recs.append(
                                    IssueTypeMetricsRecord(
                                        repo_id=repo.id,
                                        day=day_date,
                                        provider=wi.provider,
                                        team_id=team_map.get(
                                            wi.assignees[0] if wi.assignees else ""
                                        )
                                        or "unassigned",
                                        issue_type_norm=norm_type,
                                        created_count=(
                                            1
                                            if start_dt <= wi.created_at < end_dt
                                            else 0
                                        ),
                                        completed_count=(
                                            1
                                            if wi.completed_at
                                            and start_dt <= wi.completed_at < end_dt
                                            else 0
                                        ),
                                        active_count=(
                                            1
                                            if wi.started_at
                                            and wi.started_at < end_dt
                                            and (
                                                not wi.completed_at
                                                or wi.completed_at >= end_dt
                                            )
                                            else 0
                                        ),
                                        cycle_p50_hours=0.0,
                                        cycle_p90_hours=0.0,
                                        lead_p50_hours=0.0,
                                        computed_at=now,
                                    )
                                )
                            if it_recs:
                                sink.write_issue_type_metrics(it_recs)

                            # 6. Investment
                            inv_recs = []
                            for wi in day_work_items:
                                area_result = investment_classifier.classify({
                                    "title": wi.title or "",
                                    "description": wi.description or "",
                                    "labels": wi.labels or [],
                                    "paths": [],  # We don't have easy path mapping here
                                    "component": "",
                                    "epic": "",
                                })
                                inv_recs.append(
                                    InvestmentClassificationRecord(
                                        repo_id=repo.id,
                                        day=day_date,
                                        artifact_type="work_item",
                                        artifact_id=wi.work_item_id,
                                        provider=wi.provider,
                                        investment_area=area_result.investment_area,
                                        project_stream=area_result.project_stream,
                                        confidence=area_result.confidence,
                                        rule_id=area_result.rule_id,
                                        computed_at=now,
                                    )
                                )
                            if inv_recs:
                                sink.write_investment_classifications(inv_recs)

                            # 7. Quality & Knowledge
                            hotspots = compute_file_hotspots(
                                repo_id=repo.id,
                                day=day_date,
                                window_stats=day_commit_stats,
                                computed_at=now,
                            )
                            # hotspots is List[FileMetricsRecord]
                            sink.write_file_metrics(hotspots)

                            # 8. CI/CD & Deployments
                            day_pipeline_runs = []
                            for p in pipeline_runs:
                                if p.started_at and start_dt <= p.started_at < end_dt:
                                    day_pipeline_runs.append({
                                        "repo_id": p.repo_id,
                                        "run_id": str(p.run_id),
                                        "status": p.status,
                                        "queued_at": p.queued_at,
                                        "started_at": p.started_at,
                                        "finished_at": p.finished_at,
                                    })
                            if day_pipeline_runs:
                                cicd_recs = compute_cicd_metrics_daily(
                                    day=day_date,
                                    pipeline_runs=day_pipeline_runs,
                                    computed_at=now,
                                )
                                sink.write_cicd_metrics(cicd_recs)

                            day_deployments = []
                            for d in deployments:
                                if d.started_at and start_dt <= d.started_at < end_dt:
                                    day_deployments.append({
                                        "repo_id": d.repo_id,
                                        "deployment_id": str(d.deployment_id),
                                        "status": d.status,
                                        "environment": d.environment,
                                        "started_at": d.started_at,
                                        "finished_at": d.finished_at,
                                        "deployed_at": d.deployed_at or d.finished_at,
                                    })
                            if day_deployments:
                                deploy_recs = compute_deploy_metrics_daily(
                                    day=day_date,
                                    deployments=day_deployments,
                                    computed_at=now,
                                )
                                sink.write_deploy_metrics(deploy_recs)

                            day_incidents = []
                            for inc in incidents:
                                if (
                                    inc.started_at
                                    and start_dt <= inc.started_at < end_dt
                                ):
                                    day_incident = {
                                        "repo_id": inc.repo_id,
                                        "incident_id": str(inc.incident_id),
                                        "status": inc.status,
                                        "started_at": inc.started_at,
                                        "resolved_at": inc.resolved_at,
                                    }
                                    day_incidents.append(day_incident)
                            if day_incidents:
                                incident_recs = compute_incident_metrics_daily(
                                    day=day_date,
                                    incidents=day_incidents,
                                    computed_at=now,
                                )
                                sink.write_incident_metrics(incident_recs)

                            # 9. Wellbeing
                            wb_recs = compute_team_wellbeing_metrics_daily(
                                day=day_date,
                                commit_stat_rows=day_commit_stats,
                                team_resolver=team_resolver,
                                computed_at=now,
                            )
                            sink.write_team_metrics(wb_recs)

                            # 11. IC Landscape (Rolling)
                            if d_idx == 0:
                                rolling_stats = sink.get_rolling_30d_user_stats(
                                    as_of_day=day_date, repo_id=repo.id
                                )
                                landscape_recs = compute_ic_landscape_rolling(
                                    as_of_day=day,
                                    rolling_stats=rolling_stats,
                                    team_map=team_map,
                                )
                                sink.write_ic_landscape_rolling(landscape_recs)
                        except Exception as e:
                            logging.warning(
                                "Failed to compute/write fixture metrics for day %s: %s",
                                day_date,
                                e,
                            )

                    logging.info("Generated fixtures metrics for %s", r_name)

    await run_with_store(ns.db, db_type, _handler)

    if ns.with_work_graph and db_type == "clickhouse":
        from work_graph.builder import BuildConfig, WorkGraphBuilder

        config = BuildConfig(
            dsn=ns.db, from_date=now - timedelta(days=ns.days), to_date=now
        )
        builder = WorkGraphBuilder(config)
        try:
            builder.build()
        finally:
            builder.close()
    return 0


def run_fixtures_validation(ns: argparse.Namespace) -> int:
    """Validate that fixture data is sufficient for work graph and investment."""
    import clickhouse_connect

    from work_graph.ids import parse_commit_from_id, parse_pr_from_id
    from work_graph.investment.constants import MIN_EVIDENCE_CHARS
    from work_graph.investment.evidence import build_text_bundle
    from work_graph.investment.queries import (
        fetch_commits,
        fetch_parent_titles,
        fetch_pull_requests,
        fetch_work_graph_edges,
        fetch_work_items,
    )

    db_url = ns.db
    if not db_url.startswith("clickhouse://"):
        logging.error("Validation only supported for ClickHouse currently.")
        return 1

    try:
        client = clickhouse_connect.get_client(dsn=db_url)
    except Exception as e:
        logging.error(f"Failed to connect to DB: {e}")
        return 1

    logging.info("Running fixture validation...")

    # 1. Check raw data counts
    try:
        wi_count = int(client.query("SELECT count() FROM work_items").result_rows[0][0])
        non_epic_wi_count = int(
            client.query(
                "SELECT count() FROM work_items WHERE type != 'epic'"
            ).result_rows[0][0]
        )
        pr_count = int(
            client.query("SELECT count() FROM git_pull_requests").result_rows[0][0]
        )
        commit_count = int(
            client.query("SELECT count() FROM git_commits").result_rows[0][0]
        )
        logging.info(
            f"Raw Counts: WI={wi_count}, PR={pr_count}, Commits={commit_count}"
        )

        if wi_count < 10 or pr_count < 5 or commit_count < 20:
            logging.error("FAIL: Insufficient raw data.")
            return 1
    except Exception as e:
        logging.error(f"FAIL: Could not query raw tables: {e}")
        return 1

    # 2. Check prerequisites
    try:
        pr_commit_count = int(
            client.query("SELECT count() FROM work_graph_pr_commit").result_rows[0][0]
        )
        issue_pr_count = int(
            client.query("SELECT count() FROM work_graph_issue_pr").result_rows[0][0]
        )
        logging.info(
            f"Prereqs: work_graph_pr_commit={pr_commit_count}, work_graph_issue_pr={issue_pr_count}"
        )
        if pr_commit_count == 0:
            logging.error(
                "FAIL: work_graph_pr_commit is empty (fixtures missing PR->commit prerequisites)."
            )
            return 1
        if issue_pr_count == 0:
            logging.error(
                "FAIL: work_graph_issue_pr is empty (fixtures missing issue->PR prerequisites)."
            )
            return 1

        linked_non_epic = int(
            client.query(
                """
                SELECT count(DISTINCT wi.repo_id, wi.work_item_id)
                FROM work_items wi
                INNER JOIN work_graph_issue_pr l
                  ON wi.repo_id = l.repo_id AND wi.work_item_id = l.work_item_id
                WHERE wi.type != 'epic'
                """
            ).result_rows[0][0]
        )
        coverage = (linked_non_epic / non_epic_wi_count) if non_epic_wi_count else 0.0
        if coverage < 0.7:
            logging.error(
                "FAIL: Issue->PR coverage too low (linked=%.1f%%, target>=70%%).",
                coverage * 100.0,
            )
            return 1

        prs_with_commits = int(
            client.query(
                "SELECT count(DISTINCT repo_id, pr_number) FROM work_graph_pr_commit"
            ).result_rows[0][0]
        )
        if prs_with_commits < pr_count:
            logging.error(
                "FAIL: Not all PRs have commits in work_graph_pr_commit (prs_with_commits=%d, prs=%d).",
                prs_with_commits,
                pr_count,
            )
            return 1
    except Exception as e:
        logging.error(f"FAIL: Could not validate prerequisites: {e}")
        return 1

    # 3. Check work_graph_edges + components
    try:
        edges = fetch_work_graph_edges(client)
        if not edges:
            logging.error(
                "FAIL: work_graph_edges is empty (run `cli.py work-graph build`)."
            )
            return 1

        adjacency: dict[tuple[str, str], list[tuple[str, str]]] = {}
        for edge in edges:
            source = (str(edge.get("source_type")), str(edge.get("source_id")))
            target = (str(edge.get("target_type")), str(edge.get("target_id")))
            adjacency.setdefault(source, []).append(target)
            adjacency.setdefault(target, []).append(source)

        visited: set[tuple[str, str]] = set()
        components: list[list[tuple[str, str]]] = []
        for node in adjacency:
            if node in visited:
                continue
            stack = [node]
            visited.add(node)
            group: list[tuple[str, str]] = []
            while stack:
                current = stack.pop()
                group.append(current)
                for neighbor in adjacency.get(current, []):
                    if neighbor in visited:
                        continue
                    visited.add(neighbor)
                    stack.append(neighbor)
            components.append(group)

        component_count = len(components)
        min_components = min(50, max(2, non_epic_wi_count // 2))
        logging.info(
            "WorkUnits (connected components): %d (min_required=%d)",
            component_count,
            min_components,
        )
        if component_count < min_components:
            logging.error(
                "FAIL: WorkUnits too low (components=%d, required>=%d).",
                component_count,
                min_components,
            )
            return 1

    except Exception as e:
        logging.error(f"FAIL: Could not validate work graph edges/components: {e}")
        return 1

    # 4. Evidence sanity (sample bundles)
    try:
        sample_needed = 10
        eligible = []
        for node_list in components:
            has_issue = any(nt == "issue" for nt, _ in node_list)
            has_pr = any(nt == "pr" for nt, _ in node_list)
            if has_issue and has_pr:
                eligible.append(node_list)
            if len(eligible) >= sample_needed:
                break

        if not eligible:
            logging.error(
                "FAIL: No WorkUnits with both issues and PRs; evidence bundles will be empty."
            )
            return 1

        for idx, node_list in enumerate(eligible, start=1):
            issue_ids = [
                node_id for node_type, node_id in node_list if node_type == "issue"
            ]
            pr_ids = [node_id for node_type, node_id in node_list if node_type == "pr"]
            commit_ids = [
                node_id for node_type, node_id in node_list if node_type == "commit"
            ]

            work_items = fetch_work_items(client, work_item_ids=issue_ids)
            work_item_map = {
                str(item.get("work_item_id")): item
                for item in work_items
                if item.get("work_item_id")
            }

            pr_repo_numbers: dict[str, list[int]] = {}
            for pr_id in pr_ids:
                repo_id, number = parse_pr_from_id(pr_id)
                if repo_id and number is not None:
                    pr_repo_numbers.setdefault(str(repo_id), []).append(int(number))
            prs = fetch_pull_requests(client, repo_numbers=pr_repo_numbers)
            pr_map: dict[str, dict[str, object]] = {}
            for pr in prs:
                repo = str(pr.get("repo_id") or "")
                number = pr.get("number")
                if repo and number is not None:
                    pr_map[f"{repo}#pr{int(number)}"] = pr

            commit_repo_hashes: dict[str, list[str]] = {}
            for commit_id in commit_ids:
                repo_id, commit_hash = parse_commit_from_id(commit_id)
                if repo_id and commit_hash:
                    commit_repo_hashes.setdefault(str(repo_id), []).append(
                        str(commit_hash)
                    )
            commits = fetch_commits(client, repo_commits=commit_repo_hashes)
            commit_map: dict[str, dict[str, object]] = {}
            for commit in commits:
                repo = str(commit.get("repo_id") or "")
                commit_hash = str(commit.get("hash") or "")
                if repo and commit_hash:
                    commit_map[f"{repo}@{commit_hash}"] = commit

            parent_ids = {
                str(item.get("parent_id") or "")
                for item in work_items
                if item.get("parent_id")
            }
            epic_ids = {
                str(item.get("epic_id") or "")
                for item in work_items
                if item.get("epic_id")
            }
            parent_titles = fetch_parent_titles(client, work_item_ids=parent_ids)
            epic_titles = fetch_parent_titles(client, work_item_ids=epic_ids)

            bundle = build_text_bundle(
                issue_ids=issue_ids,
                pr_ids=pr_ids,
                commit_ids=commit_ids,
                work_item_map=work_item_map,
                pr_map=pr_map,
                commit_map=commit_map,
                parent_titles=parent_titles,
                epic_titles=epic_titles,
                work_unit_id=f"validate:{idx}",
            )
            if bundle.text_char_count < MIN_EVIDENCE_CHARS:
                logging.error(
                    "FAIL: Evidence bundle too small for WorkUnit sample %d (chars=%d, required>=%d).",
                    idx,
                    bundle.text_char_count,
                    MIN_EVIDENCE_CHARS,
                )
                return 1

        logging.info(
            "Evidence sanity: PASS (sampled %d work units, min_chars=%d)",
            len(eligible),
            MIN_EVIDENCE_CHARS,
        )
        return 0
    except Exception as e:
        logging.error(f"FAIL: Evidence sanity check failed: {e}")
        return 1


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    import os

    fix = subparsers.add_parser("fixtures", help="Data simulation and fixtures.")
    fix_sub = fix.add_subparsers(dest="fixtures_command", required=True)

    fix_gen = fix_sub.add_parser("generate", help="Generate synthetic data.")
    fix_gen.add_argument(
        "--db",
        default=os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL"),
        help="Target DB URI.",
    )
    fix_gen.add_argument(
        "--db-type", help="Explicit DB type (postgres, clickhouse, etc)."
    )
    fix_gen.add_argument("--repo-name", default="acme/demo-app", help="Repo name.")
    fix_gen.add_argument(
        "--repo-count", type=int, default=1, help="Number of repos to generate."
    )
    fix_gen.add_argument("--days", type=int, default=30, help="Number of days of data.")
    fix_gen.add_argument(
        "--commits-per-day", type=int, default=5, help="Avg commits per day."
    )
    fix_gen.add_argument("--pr-count", type=int, default=20, help="Total PRs.")
    fix_gen.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Deterministic seed for fixtures (repeatable runs).",
    )
    fix_gen.add_argument(
        "--provider",
        default="synthetic",
        choices=["synthetic", "github", "gitlab", "jira"],
        help="Provider label to use for generated work items.",
    )
    fix_gen.add_argument(
        "--with-work-graph",
        action="store_true",
        help="Build work graph edges after fixture generation (ClickHouse only).",
    )
    fix_gen.add_argument(
        "--with-metrics", action="store_true", help="Also generate derived metrics."
    )
    fix_gen.add_argument(
        "--team-count", type=int, default=8, help="Number of synthetic teams to create."
    )
    fix_gen.set_defaults(func=run_fixtures_generation)

    fix_val = fix_sub.add_parser("validate", help="Validate fixture data quality.")
    fix_val.add_argument(
        "--db",
        required=True,
        help="Database URI.",
    )
    fix_val.set_defaults(func=run_fixtures_validation)
