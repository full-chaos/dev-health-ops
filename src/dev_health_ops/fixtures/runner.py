import argparse
import asyncio
import logging
import os
import random
from datetime import datetime, timedelta, timezone

from dev_health_ops.fixtures.generator import SyntheticDataGenerator
from dev_health_ops.work_graph.runner import materialize_fixture_investments
from dev_health_ops.metrics.job_daily import run_daily_metrics_job
from dev_health_ops.metrics.compute_work_item_state_durations import (
    compute_work_item_state_durations_daily,
)
from dev_health_ops.providers.teams import load_team_resolver
from dev_health_ops.storage import SQLAlchemyStore, resolve_db_type, run_with_store
from dev_health_ops.utils import BATCH_SIZE, MAX_WORKERS


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


def _build_repo_team_assignments(all_teams, repo_count: int, seed: int | None):
    if repo_count <= 0:
        return []
    if not all_teams:
        return [[] for _ in range(repo_count)]

    rng = random.Random(seed if seed is not None else 0)
    repo_indices = list(range(repo_count))
    rng.shuffle(repo_indices)

    max_unowned = int(repo_count * 0.1)
    max_unowned = min(max_unowned, repo_count - 1)
    unowned_count = max_unowned
    owned_repos = repo_indices[unowned_count:]
    if not owned_repos:
        owned_repos = [repo_indices[0]]

    repo_to_teams = {idx: [] for idx in range(repo_count)}
    team_to_repos = {team.id: [] for team in all_teams}

    teams_shuffled = list(all_teams)
    rng.shuffle(teams_shuffled)
    for i, repo_idx in enumerate(owned_repos):
        team = teams_shuffled[i % len(teams_shuffled)]
        repo_to_teams[repo_idx].append(team)
        team_to_repos[team.id].append(repo_idx)

    missing_teams = [team for team in teams_shuffled if not team_to_repos[team.id]]
    for i, team in enumerate(missing_teams):
        repo_idx = owned_repos[i % len(owned_repos)]
        repo_to_teams[repo_idx].append(team)
        team_to_repos[team.id].append(repo_idx)

    multi_team_count = min(len(teams_shuffled), max(3, len(teams_shuffled) // 3))
    for team in teams_shuffled[:multi_team_count]:
        target = min(rng.randint(2, 3), len(owned_repos))
        assigned = set(team_to_repos[team.id])
        if len(assigned) >= target:
            continue
        available = [idx for idx in owned_repos if idx not in assigned]
        rng.shuffle(available)
        need = min(target - len(assigned), len(available))
        for idx in available[:need]:
            repo_to_teams[idx].append(team)
            team_to_repos[team.id].append(idx)

    return [repo_to_teams[idx] for idx in range(repo_count)]


async def run_fixtures_generation(ns: argparse.Namespace) -> int:
    now = datetime.now(timezone.utc)
    db_type = resolve_db_type(ns.db, ns.db_type)
    fixture_data = {"work_items": [], "transitions": []}

    async def _handler(store):
        if isinstance(store, SQLAlchemyStore):
            await store.ensure_tables()

        repo_count = max(1, ns.repo_count)
        base_name = ns.repo_name
        team_count = getattr(ns, "team_count", 8)
        team_assignment = SyntheticDataGenerator(
            repo_name=base_name, seed=ns.seed
        ).get_team_assignment(count=team_count)

        all_teams = team_assignment.get("teams", [])
        repo_team_assignments = _build_repo_team_assignments(
            all_teams, repo_count, ns.seed
        )
        if hasattr(store, "insert_teams") and all_teams:
            await store.insert_teams(all_teams)
            logging.info("Inserted %d synthetic teams.", len(all_teams))

        allow_parallel_inserts = not isinstance(store, SQLAlchemyStore)

        sink = None
        if ns.with_metrics:
            from dev_health_ops.metrics.job_daily import (
                ClickHouseMetricsSink,
                MongoMetricsSink,
                PostgresMetricsSink,
                SQLiteMetricsSink,
                _normalize_sqlite_url,
            )

            if db_type == "clickhouse":
                sink = ClickHouseMetricsSink(ns.db)
            elif db_type == "sqlite":
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

        for i in range(repo_count):
            r_name = base_name if repo_count == 1 else f"{base_name}-{i + 1}"
            logging.info(
                f"Generating fixture data for repo {i + 1}/{repo_count}: {r_name}"
            )
            seed_value = (int(ns.seed) + i) if ns.seed is not None else None

            assigned_teams = repo_team_assignments[i]

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
                fixture_data["work_items"].extend(work_items)
            if hasattr(store, "insert_work_item_transitions"):
                await _insert_batches(
                    store.insert_work_item_transitions,
                    transitions,
                    allow_parallel=allow_parallel_inserts,
                )
                fixture_data["transitions"].extend(transitions)

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
            if ns.with_metrics and sink:
                comp_data = generator.generate_complexity_metrics(days=ns.days)
                if hasattr(sink, "write_file_complexity_snapshots"):
                    if comp_data["snapshots"]:
                        sink.write_file_complexity_snapshots(comp_data["snapshots"])
                    if comp_data["dailies"]:
                        sink.write_repo_complexity_daily(comp_data["dailies"])

    await run_with_store(ns.db, db_type, _handler)

    if ns.with_metrics:
        await run_daily_metrics_job(
            db_url=ns.db,
            day=now.date(),
            backfill_days=ns.days,
            provider="auto",
        )

        if fixture_data["work_items"] and fixture_data["transitions"]:
            from dev_health_ops.metrics.job_daily import (
                ClickHouseMetricsSink,
                MongoMetricsSink,
                PostgresMetricsSink,
                SQLiteMetricsSink,
                _normalize_sqlite_url,
            )

            if db_type == "clickhouse":
                sink = ClickHouseMetricsSink(ns.db)
            elif db_type == "sqlite":
                sink = SQLiteMetricsSink(_normalize_sqlite_url(ns.db))
            elif db_type == "mongo":
                sink = MongoMetricsSink(ns.db)
            elif db_type == "postgres":
                sink = PostgresMetricsSink(ns.db)
            else:
                sink = None

            if sink:
                team_resolver = load_team_resolver()
                computed_at = now
                end_day = now.date()
                start_day = end_day - timedelta(days=ns.days - 1)

                for day_offset in range(ns.days):
                    day = start_day + timedelta(days=day_offset)
                    state_durations = compute_work_item_state_durations_daily(
                        day=day,
                        work_items=fixture_data["work_items"],
                        transitions=fixture_data["transitions"],
                        computed_at=computed_at,
                        team_resolver=team_resolver,
                    )
                    if state_durations:
                        sink.write_work_item_state_durations(state_durations)

                sink.close()
                logging.info("Wrote work_item_state_durations for %d days", ns.days)

    if ns.with_work_graph:
        from dev_health_ops.work_graph.builder import BuildConfig, WorkGraphBuilder

        config = BuildConfig(
            dsn=ns.db,
            from_date=(now - timedelta(days=ns.days)),
            to_date=now,
        )
        builder = WorkGraphBuilder(config)
        try:
            builder.build()
            if config.from_date and config.to_date:
                await materialize_fixture_investments(
                    db_url=ns.db,
                    from_ts=config.from_date,
                    to_ts=config.to_date,
                )
        finally:
            builder.close()
    return 0


def run_fixtures_validation(ns: argparse.Namespace) -> int:
    """Validate that fixture data is sufficient for work graph and investment."""
    import clickhouse_connect

    from dev_health_ops.work_graph.ids import parse_commit_from_id, parse_pr_from_id
    from dev_health_ops.work_graph.investment.constants import MIN_EVIDENCE_CHARS
    from dev_health_ops.work_graph.investment.evidence import build_text_bundle
    from dev_health_ops.work_graph.investment.queries import (
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

    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    sink = ClickHouseMetricsSink(dsn=db_url, client=client)

    logging.info("Running fixture validation...")

    def _table_exists(name: str) -> bool:
        try:
            result = client.query(
                "SELECT count() FROM system.tables "
                "WHERE database = currentDatabase() AND name = {name:String}",
                parameters={"name": name},
            )
            return int(result.result_rows[0][0]) > 0
        except Exception:
            return False

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

    # 1b. Check team mappings in derived work item metrics
    try:
        if not _table_exists("teams"):
            logging.error(
                "FAIL: teams table missing (sync teams or fixtures generate)."
            )
            return 1
        if not _table_exists("work_item_cycle_times"):
            logging.error(
                "FAIL: work_item_cycle_times missing (run fixtures with --with-metrics)."
            )
            return 1

        team_count = int(client.query("SELECT count() FROM teams").result_rows[0][0])
        cycle_count = int(
            client.query("SELECT count() FROM work_item_cycle_times").result_rows[0][0]
        )
        assigned_count = int(
            client.query(
                "SELECT count() FROM work_item_cycle_times "
                "WHERE lower(ifNull(nullIf(team_id, ''), 'unassigned')) != 'unassigned'"
            ).result_rows[0][0]
        )
        logging.info(
            "Team mappings: teams=%d, cycle_times=%d, assigned_cycle_times=%d",
            team_count,
            cycle_count,
            assigned_count,
        )
        if team_count == 0 or cycle_count == 0 or assigned_count == 0:
            logging.error(
                "FAIL: Missing team mappings in work_item_cycle_times "
                "(teams=%d, cycle_times=%d, assigned=%d).",
                team_count,
                cycle_count,
                assigned_count,
            )
            return 1
    except Exception as e:
        logging.error(f"FAIL: Could not validate team mappings: {e}")
        return 1

    # 1c. Check Phase 2 metrics in work_item_cycle_times
    try:
        cycle_with_metrics = int(
            client.query(
                "SELECT count() FROM work_item_cycle_times "
                "WHERE cycle_time_hours IS NOT NULL"
            ).result_rows[0][0]
        )
        cycle_with_flow = int(
            client.query(
                "SELECT count() FROM work_item_cycle_times "
                "WHERE flow_efficiency IS NOT NULL AND flow_efficiency > 0"
            ).result_rows[0][0]
        )
        logging.info(
            "Cycle time metrics: total=%d, with_cycle_time=%d, with_flow_efficiency=%d",
            cycle_count,
            cycle_with_metrics,
            cycle_with_flow,
        )
        if cycle_with_metrics == 0:
            logging.error(
                "FAIL: No work_item_cycle_times records have non-null cycle_time_hours."
            )
            return 1
        # flow_efficiency may be NULL if no transitions exist, but warn if all are NULL
        if cycle_with_flow == 0:
            logging.warning(
                "WARN: No work_item_cycle_times records have non-null flow_efficiency. "
                "This may indicate missing work_item_transitions data."
            )
    except Exception as e:
        logging.error(f"FAIL: Could not validate cycle time metrics: {e}")
        return 1

    # 1d. Check Phase 2 metrics in work_item_metrics_daily
    try:
        if not _table_exists("work_item_metrics_daily"):
            logging.error(
                "FAIL: work_item_metrics_daily missing (run fixtures with --with-metrics)."
            )
            return 1

        metrics_count = int(
            client.query("SELECT count() FROM work_item_metrics_daily").result_rows[0][
                0
            ]
        )
        metrics_with_predictability = int(
            client.query(
                "SELECT count() FROM work_item_metrics_daily "
                "WHERE predictability_score > 0"
            ).result_rows[0][0]
        )
        metrics_with_congestion = int(
            client.query(
                "SELECT count() FROM work_item_metrics_daily "
                "WHERE wip_congestion_ratio > 0"
            ).result_rows[0][0]
        )
        metrics_with_new_items = int(
            client.query(
                "SELECT count() FROM work_item_metrics_daily WHERE new_items_count > 0"
            ).result_rows[0][0]
        )
        logging.info(
            "Phase 2 metrics: total=%d, with_predictability=%d, with_congestion=%d, with_new_items=%d",
            metrics_count,
            metrics_with_predictability,
            metrics_with_congestion,
            metrics_with_new_items,
        )
        if metrics_count == 0:
            logging.error("FAIL: work_item_metrics_daily is empty.")
            return 1
        if metrics_with_predictability == 0:
            logging.warning(
                "WARN: No work_item_metrics_daily records have predictability_score > 0."
            )
        if metrics_with_new_items == 0:
            logging.warning(
                "WARN: No work_item_metrics_daily records have new_items_count > 0 "
                "(defect_intro_rate will be NULL)."
            )
    except Exception as e:
        logging.error(f"FAIL: Could not validate Phase 2 metrics: {e}")
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
        edges = fetch_work_graph_edges(sink)
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
        distinct_repos = int(
            client.query("SELECT countDistinct(repo_id) FROM work_items").result_rows[
                0
            ][0]
        )
        min_components = max(2, distinct_repos)
        logging.info(
            "WorkUnits (connected components): %d (min_required=%d, repos=%d)",
            component_count,
            min_components,
            distinct_repos,
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

            work_items = fetch_work_items(sink, work_item_ids=issue_ids)
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
            prs = fetch_pull_requests(sink, repo_numbers=pr_repo_numbers)
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
            commits = fetch_commits(sink, repo_commits=commit_repo_hashes)
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
            parent_titles = fetch_parent_titles(sink, work_item_ids=parent_ids)
            epic_titles = fetch_parent_titles(sink, work_item_ids=epic_ids)

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
