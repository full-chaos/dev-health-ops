"""Work item, work item metrics, transitions, dependencies, worklogs, sprints, and reopen events."""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta, timezone
from typing import Any, cast

from dev_health_ops.fixtures.demo_identity import DEFAULT_DEMO_TEAM
from dev_health_ops.fixtures.generators.base import BaseGeneratorMixin
from dev_health_ops.metrics.schemas import (
    UserMetricsDailyRecord,
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemUserMetricsDailyRecord,
)
from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
    WorkItemDependency,
    WorkItemProvider,
    WorkItemReopenEvent,
    WorkItemStatusCategory,
    WorkItemStatusTransition,
    WorkItemType,
    Worklog,
)
from dev_health_ops.providers.teams import normalize_team_id, normalize_team_name


class WorkItemsGeneratorMixin(BaseGeneratorMixin):
    """Generates work items, work item metrics, transitions, dependencies, worklogs, sprints, and reopen events."""

    def generate_work_item_metrics(
        self, days: int = 30
    ) -> list[WorkItemMetricsDailyRecord]:
        records = []
        end_date = datetime.now(timezone.utc).date()

        teams_to_use = []
        if self.assigned_teams is None:
            teams_to_use = [DEFAULT_DEMO_TEAM]
        elif self.assigned_teams:
            teams_to_use = [(t.id, t.name) for t in self.assigned_teams]
        else:
            teams_to_use = [("unassigned", "Unassigned")]

        for i in range(days):
            day = end_date - timedelta(days=i)
            for team_id, team_name in teams_to_use:
                # --- Coherence-safe allocation (Rule 1) -------------------
                # Unassigned sub-counts must be subsets of their totals.
                # Lead time must be ≥ cycle time (lead = queue + cycle).
                # Percentile pairs must be non-decreasing (p50 ≤ p90).
                items_started = random.randint(2, 8)
                items_started_unassigned = random.randint(0, items_started)
                items_completed = random.randint(1, 6)
                items_completed_unassigned = random.randint(0, items_completed)
                wip_count = random.randint(5, 15)
                wip_unassigned = random.randint(0, wip_count)

                # Build cycle / lead times so all four constraints hold:
                #   ct_p50 ≤ ct_p90, lt_p50 ≤ lt_p90, ct_p50 ≤ lt_p50, ct_p90 ≤ lt_p90
                # Strategy: generate base cycle-p50, add deltas for p90 and
                # queue, so every derived value is monotonically larger.
                ct_p50 = float(random.randint(24, 72))
                ct_p90 = ct_p50 + float(random.randint(0, 48))
                queue_p50 = float(random.randint(12, 48))
                queue_p90 = queue_p50 + float(random.randint(0, 24))
                lt_p50 = ct_p50 + queue_p50
                lt_p90 = ct_p90 + queue_p90

                # WIP age: p50 then p90 ≥ p50
                wip_age_p50 = float(random.randint(12, 48))
                wip_age_p90 = float(random.randint(48, 168))

                records.append(
                    WorkItemMetricsDailyRecord(
                        day=day,
                        provider=self.provider,
                        work_scope_id=self.repo_name,
                        team_id=team_id,
                        team_name=team_name,
                        items_started=items_started,
                        items_completed=items_completed,
                        items_started_unassigned=items_started_unassigned,
                        items_completed_unassigned=items_completed_unassigned,
                        wip_count_end_of_day=wip_count,
                        wip_unassigned_end_of_day=wip_unassigned,
                        cycle_time_p50_hours=ct_p50,
                        cycle_time_p90_hours=ct_p90,
                        lead_time_p50_hours=lt_p50,
                        lead_time_p90_hours=lt_p90,
                        wip_age_p50_hours=wip_age_p50,
                        wip_age_p90_hours=wip_age_p90,
                        bug_completed_ratio=random.uniform(0.1, 0.4),
                        story_points_completed=float(random.randint(10, 50)),
                        # Phase 2 metrics
                        new_bugs_count=random.randint(0, 3),
                        new_items_count=random.randint(3, 10),
                        defect_intro_rate=random.uniform(0.0, 0.3),
                        wip_congestion_ratio=random.uniform(0.5, 2.0),
                        predictability_score=random.uniform(0.5, 1.0),
                        computed_at=datetime.now(timezone.utc),
                    )
                )
        return records

    def generate_work_item_cycle_times(
        self,
        work_items: list[WorkItem] | None = None,
        count: int = 50,
    ) -> list[WorkItemCycleTimeRecord]:
        """Generate cycle time records.

        When *work_items* is provided the records use the real work-item IDs
        so that team-linkage queries (which join via structural_evidence_json
        → work_item_cycle_times) resolve correctly.
        """
        records = []
        computed_at = datetime.now(timezone.utc)

        teams_to_use: list[tuple[str, str]] = []
        if self.assigned_teams is None:
            teams_to_use = [DEFAULT_DEMO_TEAM]
        elif self.assigned_teams:
            teams_to_use = [(t.id, t.name) for t in self.assigned_teams]
        else:
            teams_to_use = [("unassigned", "Unassigned")]

        # Build a member→team lookup for assignee-based resolution
        member_map = self._get_member_map()

        items_to_process: list[
            tuple[str, str, str, str | None, datetime, datetime | None, datetime | None]
        ] = []

        if work_items:
            for item in work_items:
                if item.type == "epic":
                    continue
                assignee = item.assignees[0] if item.assignees else None
                items_to_process.append(
                    (
                        item.work_item_id,
                        item.provider,
                        item.type or "task",
                        assignee,
                        item.created_at,
                        item.started_at,
                        item.completed_at,
                    )
                )
        else:
            # Fallback: generate synthetic items
            end_date = datetime.now(timezone.utc)
            for i in range(count):
                created_at = end_date - timedelta(days=random.randint(0, 60))
                started_at = created_at + timedelta(hours=random.randint(4, 48))
                completed_at = started_at + timedelta(hours=random.randint(24, 168))
                author_name, _ = random.choice(self.repo_authors)
                items_to_process.append(
                    (
                        f"synth:{self.repo_name}#{i}",
                        self.provider,
                        random.choice(["story", "bug", "task"]),
                        author_name,
                        created_at,
                        started_at,
                        completed_at,
                    )
                )

        fallback_team_plan = self._build_fallback_team_plan(
            items_to_process=items_to_process,
            member_map=member_map,
            teams_to_use=teams_to_use,
        )

        for item_index, item_data in enumerate(items_to_process):
            (
                work_item_id,
                provider,
                item_type,
                assignee,
                created_at_value,
                started_at_value,
                completed_at_value,
            ) = item_data
            if started_at_value is None or completed_at_value is None:
                continue

            created_at = created_at_value
            started_at = started_at_value
            completed_at = completed_at_value

            cycle_time = (completed_at - started_at).total_seconds() / 3600
            if cycle_time <= 0:
                continue

            # Resolve team from assignee
            team_id, team_name = None, None
            if assignee and member_map:
                entry = member_map.get(str(assignee).strip().lower())
                if entry:
                    team_id, team_name = entry
            if team_id is None:
                team_id, team_name = fallback_team_plan.get(
                    item_index,
                    teams_to_use[0],
                )

            efficiency = random.uniform(0.1, 0.6)
            active_hours = cycle_time * efficiency
            wait_hours = cycle_time * (1.0 - efficiency)

            records.append(
                WorkItemCycleTimeRecord(
                    work_item_id=work_item_id,
                    provider=provider,
                    day=completed_at.date(),
                    work_scope_id=self.repo_name,
                    team_id=team_id,
                    team_name=team_name,
                    assignee=assignee,
                    type=item_type,
                    status="done",
                    created_at=created_at,
                    started_at=started_at,
                    completed_at=completed_at,
                    cycle_time_hours=cycle_time,
                    lead_time_hours=(completed_at - created_at).total_seconds() / 3600,
                    active_time_hours=active_hours,
                    wait_time_hours=wait_hours,
                    flow_efficiency=efficiency,
                    computed_at=computed_at,
                )
            )
        return records

    def generate_user_metrics_daily(
        self,
        *,
        day: date,
        member_map: dict[str, Any] | None = None,
    ) -> list[UserMetricsDailyRecord]:
        records = []
        computed_at = datetime.now(timezone.utc)
        for author_name, author_email in self.repo_authors:
            team_id, team_name = self._resolve_team(
                member_map, author_name, author_email
            )
            commits = random.randint(0, 6)
            loc_added = random.randint(0, 400)
            loc_deleted = random.randint(0, loc_added)
            files_changed = random.randint(0, 10)
            prs = random.randint(0, 3)
            records.append(
                UserMetricsDailyRecord(
                    repo_id=self.repo_id,
                    day=day,
                    author_email=author_email,
                    identity_id=author_email,
                    commits_count=commits,
                    loc_added=loc_added,
                    loc_deleted=loc_deleted,
                    files_changed=files_changed,
                    large_commits_count=int(commits * 0.1),
                    avg_commit_size_loc=float(loc_added + loc_deleted) / commits
                    if commits
                    else 0.0,
                    prs_authored=prs,
                    prs_merged=prs,
                    avg_pr_cycle_hours=24.0,
                    median_pr_cycle_hours=24.0,
                    pr_cycle_p75_hours=24.0,
                    pr_cycle_p90_hours=24.0,
                    prs_with_first_review=prs,
                    pr_first_review_p50_hours=4.0,
                    pr_first_review_p90_hours=8.0,
                    pr_review_time_p50_hours=20.0,
                    pr_pickup_time_p50_hours=2.0,
                    reviews_given=random.randint(0, 5),
                    changes_requested_given=random.randint(0, 1),
                    reviews_received=random.randint(0, 5),
                    review_reciprocity=0.8,
                    team_id=normalize_team_id(team_id),
                    team_name=normalize_team_name(team_name),
                    active_hours=6.0,
                    weekend_days=0,
                    loc_touched=loc_added + loc_deleted,
                    prs_opened=prs,
                    work_items_completed=random.randint(0, 2),
                    work_items_active=random.randint(0, 3),
                    delivery_units=random.randint(1, 10),
                    cycle_p50_hours=48.0,
                    cycle_p90_hours=72.0,
                    computed_at=computed_at,
                )
            )
        return records

    def generate_work_item_user_metrics_daily(
        self,
        *,
        day: date,
        member_map: dict[str, Any] | None = None,
    ) -> list[WorkItemUserMetricsDailyRecord]:
        records = []
        computed_at = datetime.now(timezone.utc)
        for author_name, author_email in self.repo_authors:
            team_id, team_name = self._resolve_team(
                member_map, author_name, author_email
            )
            user_identity = author_email or "unknown"
            records.append(
                WorkItemUserMetricsDailyRecord(
                    day=day,
                    provider=self.provider,
                    work_scope_id=self.repo_name,
                    user_identity=user_identity,
                    team_id=normalize_team_id(team_id),
                    team_name=normalize_team_name(team_name),
                    items_started=random.randint(0, 1),
                    items_completed=random.randint(0, 1),
                    wip_count_end_of_day=random.randint(0, 3),
                    cycle_time_p50_hours=48.0,
                    cycle_time_p90_hours=72.0,
                    computed_at=computed_at,
                )
            )
        return records

    def generate_work_items(
        self,
        days: int = 30,
        projects: list[str] | None = None,
        investment_weights: dict[str, float] | None = None,
        provider: str | None = None,
    ) -> list[WorkItem]:
        items = []
        end_date = datetime.now(timezone.utc)
        provider_value = provider or self.provider
        description_keywords = {
            "story": ["feature", "implement"],
            "task": ["refactor", "cleanup"],
            "bug": ["bug", "fix"],
            "epic": ["feature", "introduce"],
            "incident": ["incident", "hotfix"],
            "chore": ["cleanup", "upgrade"],
            "issue": ["feature", "fix"],
        }

        # Defaults
        if not projects:
            projects = [self.repo_name]

        if not investment_weights:
            investment_weights = {
                "product": 0.5,
                "security": 0.1,
                "infra": 0.15,
                "quality": 0.1,
                "docs": 0.05,
                "data": 0.1,
            }

        sub_categories_map = {
            "product": [
                "feature",
                "ux",
                "onboarding",
                "mobile",
                "api",
                "growth",
                "monetization",
            ],
            "security": [
                "auth",
                "vulnerability",
                "compliance",
                "audit",
                "encryption",
                "access-control",
            ],
            "infra": [
                "k8s",
                "terraform",
                "ci-cd",
                "monitoring",
                "cost",
                "network",
                "database",
            ],
            "quality": [
                "testing",
                "flake",
                "coverage",
                "perf",
                "reliability",
                "automation",
            ],
            "docs": ["api-docs", "user-guide", "tutorial", "readme", "release-notes"],
            "data": [
                "pipeline",
                "schema",
                "analytics",
                "warehouse",
                "etl",
                "visualization",
            ],
        }

        # Normalize weights
        total_weight = sum(investment_weights.values())
        normalized_weights = {
            k: v / total_weight for k, v in investment_weights.items()
        }
        categories = list(normalized_weights.keys())
        weights = list(normalized_weights.values())

        # Generate Epics per project (Long running)
        project_epics: dict[str, list[WorkItem]] = {}
        for proj in projects:
            project_epics[proj] = []
            # Create 1-3 active epics per project
            for i in range(random.randint(1, 3)):
                epic_created_at = end_date - timedelta(
                    days=random.randint(days, days + 60)
                )
                epic_number = 9000 + i + 1
                project_key = proj.split("/")[-1].upper()[:3]
                if provider_value == "github":
                    epic_id = f"gh:{proj}#{epic_number}"
                elif provider_value == "gitlab":
                    epic_id = f"gitlab:{proj}#{epic_number}"
                elif provider_value == "jira":
                    epic_id = f"jira:{project_key}-{epic_number}"
                else:
                    epic_id = f"{proj}-EPIC-{i + 1}"
                category = random.choices(categories, weights=weights, k=1)[0]

                # Pick a random sub-category for the epic
                sub_cats = sub_categories_map.get(category, [])
                sub_category = random.choice(sub_cats) if sub_cats else category

                epic_keywords = description_keywords.get(
                    "epic", ["feature", "implement"]
                )
                epic_description = (
                    f"{category.title()} epic focused on {sub_category}. "
                    f"{epic_keywords[0].title()} and {epic_keywords[1]} work planned."
                )
                # Create the Epic item
                epic = WorkItem(
                    work_item_id=epic_id,
                    provider=cast(WorkItemProvider, provider_value),
                    title=f"Epic: {category.title()} - {sub_category.title()} Initiative {i + 1}",
                    type="epic",
                    status="in_progress",  # Epics often stay open
                    status_raw="In Progress",
                    description=epic_description,
                    repo_id=self.repo_id,
                    project_id=proj,
                    project_key=project_key if provider_value == "jira" else proj,
                    created_at=epic_created_at,
                    updated_at=epic_created_at,
                    started_at=epic_created_at + timedelta(days=1),
                    completed_at=None,
                    closed_at=None,
                    reporter=random.choice(self.repo_authors)[1],
                    assignees=[random.choice(self.repo_authors)[1]],
                    labels=[category, sub_category, "strategic"],
                    story_points=None,
                )
                items.append(epic)
                project_epics[proj].append(epic)

        # Generate standard work items
        # Roughly 2 items per day per project
        total_items = days * 2 * len(projects)

        for i in range(total_items):
            project = random.choice(projects)
            author_name, author_email = random.choice(self.repo_authors)

            # Random date within range
            created_at = end_date - timedelta(
                days=random.randint(0, days), hours=random.randint(0, 23)
            )

            # Determine Investment Category & Parent
            category = random.choices(categories, weights=weights, k=1)[0]

            # Pick a random sub-category
            sub_cats = sub_categories_map.get(category, [])
            sub_category = random.choice(sub_cats) if sub_cats else category

            labels = [category, sub_category]

            # Link to an Epic if available (50% chance)
            parent_epic_id = None
            if project_epics.get(project) and random.random() > 0.5:
                parent_epic = random.choice(project_epics[project])
                # Inherit category from Epic if linked, or keep random?
                # Usually child items relate to Epic. Let's align them often.
                if random.random() > 0.3:
                    # primary category is the first label
                    category = parent_epic.labels[0]
                    # Try to inherit sub-category or pick a related one
                    if len(parent_epic.labels) > 1:
                        sub_category = parent_epic.labels[1]
                    else:
                        sub_cats = sub_categories_map.get(category, [])
                        sub_category = random.choice(sub_cats) if sub_cats else category

                    labels = [category, sub_category]

                parent_epic_id = parent_epic.work_item_id

            # Determine Type
            is_bug = (
                random.random() > 0.7
                if category == "quality"
                else random.random() > 0.85
            )
            item_type: WorkItemType = (
                "bug" if is_bug else random.choice(["story", "task"])
            )

            # For bugs, add 'bug' label
            if is_bug:
                labels.append("bug")

            # Lifecycle
            is_done = random.random() > 0.3
            started_at = None
            completed_at = None
            status = "done" if is_done else "in_progress"

            if is_done or random.random() > 0.5:
                # Started 1-5 days after creation
                started_at = created_at + timedelta(hours=random.randint(1, 120))
                if started_at > end_date:
                    started_at = end_date - timedelta(hours=1)

                if is_done:
                    # Completed 1-7 days after start
                    completed_at = started_at + timedelta(hours=random.randint(4, 168))
                    if completed_at > end_date:
                        completed_at = end_date
                        status = "in_progress"  # Can't be done if date is future

            issue_number = i + 100
            project_key = project.split("/")[-1].upper()[:3]
            if provider_value == "github":
                work_item_id = f"gh:{project}#{issue_number}"
            elif provider_value == "gitlab":
                work_item_id = f"gitlab:{project}#{issue_number}"
            elif provider_value == "jira":
                work_item_id = f"jira:{project_key}-{issue_number}"
            else:
                work_item_id = f"{project}-{issue_number}"

            item_keywords = description_keywords.get(item_type, ["feature", "fix"])
            description = (
                f"{category.title()} work in {sub_category}. "
                f"{item_keywords[0].title()} focus with {item_keywords[1]} checks."
            )
            updated_at = completed_at or started_at or created_at

            items.append(
                WorkItem(
                    work_item_id=work_item_id,
                    provider=cast(WorkItemProvider, provider_value),
                    title=f"[{project}] {category.title()}/{sub_category.title()} {item_type} {i}",
                    type=item_type,
                    status=cast(WorkItemStatusCategory, status),
                    status_raw=status,
                    description=description,
                    repo_id=self.repo_id,
                    project_id=project,
                    project_key=project_key
                    if provider_value == "jira"
                    else project,  # Jira style
                    created_at=created_at,
                    updated_at=updated_at,
                    started_at=started_at,
                    completed_at=completed_at,
                    closed_at=completed_at,
                    reporter=author_email,
                    assignees=[author_email] if random.random() > 0.3 else [],
                    labels=labels,
                    epic_id=parent_epic_id,
                    parent_id=parent_epic_id,  # Simplified: parent is epic
                    story_points=random.choice([1, 2, 3, 5, 8])
                    if item_type == "story"
                    else None,
                )
            )

        # Sort by created_at for realism
        items.sort(key=lambda x: x.created_at)
        items = self._ensure_work_type_cooccurrence(items)
        return items

    def _ensure_work_type_cooccurrence(
        self,
        items: list[WorkItem],
    ) -> list[WorkItem]:
        """Guarantee >=2 distinct work_item types per (repo_id, day) bucket
        with >=2 items. Without this pass, random per-item type selection
        can produce monotype buckets on low-item days, leaving the
        flow_matrix WORK_TYPE template (which bridges on repo_id + day)
        with zero cross-type edges for that day. CHAOS-1292.

        WorkItem is frozen, so we rewrite the offending item's type via
        dataclasses.replace. Deterministic: the LAST item in each monotype
        bucket is flipped to the next type in preference order, so the same
        input always produces the same output.
        """
        if len(items) < 2:
            return items

        from collections import defaultdict
        from dataclasses import replace

        type_preference: list[WorkItemType] = ["story", "task", "bug"]

        bucket_indices: dict[date, list[int]] = defaultdict(list)
        for idx, item in enumerate(items):
            bucket_day = (
                item.completed_at or item.started_at or item.created_at
            ).date()
            bucket_indices[bucket_day].append(idx)

        rewrites: dict[int, WorkItemType] = {}
        for indices in bucket_indices.values():
            if len(indices) < 2:
                continue
            bucket_types = {items[i].type for i in indices}
            if len(bucket_types) >= 2:
                continue
            current_type = items[indices[0]].type
            alt_type = next(
                (t for t in type_preference if t != current_type),
                type_preference[0],
            )
            rewrites[indices[-1]] = alt_type

        if not rewrites:
            return items

        return [
            replace(item, type=rewrites[idx]) if idx in rewrites else item
            for idx, item in enumerate(items)
        ]

    def generate_work_item_transitions(
        self, items: list[WorkItem]
    ) -> list[WorkItemStatusTransition]:
        transitions = []
        for item in items:
            # Simple transition from todo -> in_progress -> done
            transitions.append(
                WorkItemStatusTransition(
                    work_item_id=item.work_item_id,
                    provider=item.provider,
                    occurred_at=item.created_at,
                    from_status_raw=None,
                    to_status_raw="todo",
                    from_status="backlog",
                    to_status="todo",
                )
            )
            if item.started_at:
                transitions.append(
                    WorkItemStatusTransition(
                        work_item_id=item.work_item_id,
                        provider=item.provider,
                        occurred_at=item.started_at,
                        from_status_raw="todo",
                        to_status_raw="in_progress",
                        from_status="todo",
                        to_status="in_progress",
                    )
                )

                # Randomly inject a wait state (blocked) between start and complete
                if item.completed_at and random.random() > 0.5:
                    duration = (item.completed_at - item.started_at).total_seconds()
                    if duration > 7200:  # If duration > 2 hours
                        blocked_at = item.started_at + timedelta(
                            seconds=random.randint(3600, int(duration * 0.4))
                        )
                        unblocked_at = blocked_at + timedelta(
                            seconds=random.randint(1800, int(duration * 0.4))
                        )

                        transitions.append(
                            WorkItemStatusTransition(
                                work_item_id=item.work_item_id,
                                provider=item.provider,
                                occurred_at=blocked_at,
                                from_status_raw="in_progress",
                                to_status_raw="blocked",
                                from_status="in_progress",
                                to_status="blocked",
                            )
                        )
                        transitions.append(
                            WorkItemStatusTransition(
                                work_item_id=item.work_item_id,
                                provider=item.provider,
                                occurred_at=unblocked_at,
                                from_status_raw="blocked",
                                to_status_raw="in_progress",
                                from_status="blocked",
                                to_status="in_progress",
                            )
                        )

            if item.completed_at:
                # Need to determine the 'from' status
                # Ideally we track current status, but for now assuming we return to 'in_progress' before done
                transitions.append(
                    WorkItemStatusTransition(
                        work_item_id=item.work_item_id,
                        provider=item.provider,
                        occurred_at=item.completed_at,
                        from_status_raw="in_progress",
                        to_status_raw="done",
                        from_status="in_progress",
                        to_status="done",
                    )
                )
        return transitions

    def generate_work_item_dependencies(
        self, items: list[WorkItem]
    ) -> list[WorkItemDependency]:
        dependencies = []
        synced_at = datetime.now(timezone.utc)
        parent_edge_rate = 0.2

        # 1. Parent/Child (Epic -> Story)
        # Note: In generate_work_items, we already set parent_id/epic_id on items.
        # We should reflect these as explicit dependencies.
        for item in items:
            if item.parent_id and random.random() < parent_edge_rate:
                dependencies.append(
                    WorkItemDependency(
                        source_work_item_id=item.parent_id,
                        target_work_item_id=item.work_item_id,
                        relationship_type="parent",
                        relationship_type_raw="Parent",
                        last_synced=synced_at,
                    )
                )
                dependencies.append(
                    WorkItemDependency(
                        source_work_item_id=item.work_item_id,
                        target_work_item_id=item.parent_id,
                        relationship_type="child",
                        relationship_type_raw="Child",
                        last_synced=synced_at,
                    )
                )

        candidates = [i for i in items if i.type != "epic"]
        if len(candidates) > 2:
            num_links = min(len(candidates) // 20, 10)
            for idx in range(num_links):
                source_idx = (idx * 7) % len(candidates)
                target_idx = (source_idx + 1) % len(candidates)
                source = candidates[source_idx]
                target = candidates[target_idx]

                dependencies.append(
                    WorkItemDependency(
                        source_work_item_id=source.work_item_id,
                        target_work_item_id=target.work_item_id,
                        relationship_type="blocks",
                        relationship_type_raw="Blocks",
                        last_synced=synced_at,
                    )
                )
                dependencies.append(
                    WorkItemDependency(
                        source_work_item_id=target.work_item_id,
                        target_work_item_id=source.work_item_id,
                        relationship_type="is_blocked_by",
                        relationship_type_raw="Is Blocked By",
                        last_synced=synced_at,
                    )
                )

        return dependencies

    def generate_worklogs(self, work_items: list[WorkItem]) -> list[Worklog]:
        now = datetime.now(timezone.utc)
        worklogs: list[Worklog] = []
        for work_item in work_items:
            if not work_item.started_at:
                continue
            if random.random() > 0.4:
                continue
            count = random.randint(1, 3)
            end_bound = work_item.completed_at or now
            if end_bound <= work_item.started_at:
                end_bound = work_item.started_at + timedelta(hours=1)
            for i in range(count):
                span = (end_bound - work_item.started_at).total_seconds()
                offset = random.uniform(0, max(span, 1))
                started_at = work_item.started_at + timedelta(seconds=offset)
                time_spent = random.randint(900, 28800)
                created_at = started_at + timedelta(seconds=random.randint(1, 300))
                _, author_email = random.choice(self.repo_authors)
                worklogs.append(
                    Worklog(
                        work_item_id=work_item.work_item_id,
                        provider=work_item.provider,
                        worklog_id=f"wl-{work_item.work_item_id}-{i}",
                        author=author_email,
                        started_at=started_at,
                        time_spent_seconds=time_spent,
                        created_at=created_at,
                        updated_at=created_at,
                    )
                )
        return worklogs

    def generate_work_item_reopen_events(
        self, transitions: list[WorkItemStatusTransition]
    ) -> list[WorkItemReopenEvent]:
        """Extract reopen events from transitions where from_status is 'done' and
        to_status is not 'done' or 'canceled'.

        Also synthetically generates reopen events for ~10% of completed work items
        that do not already have a reopen transition.
        """
        reopen_events = []
        last_synced = datetime.now(timezone.utc)
        reopened_item_ids: set = set()

        for t in transitions:
            if t.from_status == "done" and t.to_status not in ("done", "canceled"):
                reopen_events.append(
                    WorkItemReopenEvent(
                        work_item_id=t.work_item_id,
                        occurred_at=t.occurred_at,
                        from_status=t.from_status,
                        to_status=t.to_status,
                        from_status_raw=t.from_status_raw,
                        to_status_raw=t.to_status_raw,
                        actor=getattr(t, "actor", None),
                        last_synced=last_synced,
                    )
                )
                reopened_item_ids.add(t.work_item_id)

        # Collect completed items not already reopened, then add ~10% more
        done_transitions_by_item: dict = {}
        for t in transitions:
            if t.to_status == "done":
                done_transitions_by_item[t.work_item_id] = t

        candidates = [
            t
            for item_id, t in done_transitions_by_item.items()
            if item_id not in reopened_item_ids
        ]
        num_extra = max(0, int(len(candidates) * 0.1))
        if num_extra > 0 and candidates:
            extra = random.sample(candidates, min(num_extra, len(candidates)))
            for done_t in extra:
                # Reopen occurs 1-7 days after completion
                reopen_at = done_t.occurred_at + timedelta(
                    days=random.randint(1, 7), hours=random.randint(0, 23)
                )
                actor_name, actor_email = random.choice(self.repo_authors)
                reopen_events.append(
                    WorkItemReopenEvent(
                        work_item_id=done_t.work_item_id,
                        occurred_at=reopen_at,
                        from_status="done",
                        to_status="in_progress",
                        from_status_raw="done",
                        to_status_raw="in_progress",
                        actor=actor_email,
                        last_synced=last_synced,
                    )
                )

        return reopen_events

    def generate_sprints(self, days: int = 30) -> list[Sprint]:
        """Generate 2-week sprints covering the time window."""
        sprints = []
        last_synced = datetime.now(timezone.utc)
        now = datetime.now(timezone.utc)

        sprint_duration = timedelta(days=14)
        # Start far enough back to cover the full window
        window_start = now - timedelta(days=days)

        # Align sprint start to the earliest 2-week boundary before window_start
        sprint_start = window_start - timedelta(
            days=window_start.weekday()
        )  # align to Monday

        # Generate enough sprints to cover window + a couple future sprints
        sprint_index = 1
        current_start = sprint_start
        while current_start < now + timedelta(days=28):
            sprint_end = current_start + sprint_duration

            if sprint_end < now:
                state = "closed"
                completed_at = sprint_end
            elif current_start <= now < sprint_end:
                state = "active"
                completed_at = None
            else:
                state = "future"
                completed_at = None

            sprints.append(
                Sprint(
                    provider=cast(WorkItemProvider, self.provider),
                    sprint_id=f"sprint-{sprint_index}",
                    name=f"Sprint {sprint_index}",
                    state=state,
                    started_at=current_start,
                    ended_at=sprint_end,
                    completed_at=completed_at,
                    last_synced=last_synced,
                )
            )

            current_start = sprint_end
            sprint_index += 1

        return sprints

    def assign_sprints_to_work_items(
        self, work_items: list[WorkItem], sprints: list[Sprint]
    ) -> list[WorkItem]:
        """Assign sprint_id/sprint_name to ~60% of non-epic work items.

        For each eligible work item, picks the sprint whose time window contains
        the item's created_at, falling back to any closed/active sprint.
        """
        import dataclasses

        if not sprints:
            return work_items

        closed_or_active = [s for s in sprints if s.state in ("closed", "active")]
        if not closed_or_active:
            closed_or_active = list(sprints)

        result = []
        for item in work_items:
            if item.type == "epic" or random.random() > 0.6:
                result.append(item)
                continue

            # Find the sprint that contains the item's created_at
            chosen_sprint = None
            for s in sprints:
                if s.started_at and s.ended_at:
                    if s.started_at <= item.created_at <= s.ended_at:
                        chosen_sprint = s
                        break

            if chosen_sprint is None:
                chosen_sprint = random.choice(closed_or_active)

            result.append(
                dataclasses.replace(
                    item,
                    sprint_id=chosen_sprint.sprint_id,
                    sprint_name=chosen_sprint.name,
                )
            )

        return result
