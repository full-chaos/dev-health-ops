"""Investment classification, work-unit investment, and investment metrics generators."""

from __future__ import annotations

import hashlib
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from dev_health_ops.fixtures.demo_identity import DEFAULT_DEMO_TEAM
from dev_health_ops.fixtures.generators.base import BaseGeneratorMixin
from dev_health_ops.models.work_items import WorkItem


class InvestmentsGeneratorMixin(BaseGeneratorMixin):
    """Generates investment classification, work-unit investment, and investment metric records."""

    def generate_investment_classifications(
        self, work_items: list[WorkItem], days: int = 30
    ) -> list[Any]:
        """Generate investment classification records from work items."""
        from dev_health_ops.metrics.schemas import InvestmentClassificationRecord

        records = []
        computed_at = datetime.now(timezone.utc)

        for item in work_items:
            if item.type == "epic":
                continue
            # Use the first label as investment_area (items already have category labels)
            investment_area = item.labels[0] if item.labels else "product"
            project_stream = item.labels[1] if len(item.labels) > 1 else ""
            day = item.created_at.date()

            records.append(
                InvestmentClassificationRecord(
                    repo_id=self.repo_id,
                    day=day,
                    artifact_type="work_item",
                    artifact_id=item.work_item_id,
                    provider=item.provider,
                    investment_area=investment_area,
                    project_stream=project_stream,
                    confidence=random.uniform(0.7, 1.0),
                    rule_id="synthetic-label-match",
                    computed_at=computed_at,
                )
            )
        return records

    def generate_work_unit_investments(
        self,
        work_items: list[WorkItem],
        days: int = 30,
        *,
        org_id: str = "",
        categorization_run_id: str | None = None,
    ) -> list[Any]:
        """Generate synthetic work unit investment records from work items.

        Theme and subcategory keys are imported from investment_taxonomy to
        stay in sync with the canonical taxonomy used by the LLM categorizer
        and API query layer.
        """
        from dev_health_ops.investment_taxonomy import (
            SUBCATEGORY_TO_THEME,
            THEMES,
        )
        from dev_health_ops.metrics.schemas import WorkUnitInvestmentRecord
        from dev_health_ops.utils.normalization import evidence_quality_band

        # Build theme → [subcategory, ...] lookup from canonical taxonomy
        _theme_subcats: dict[str, list[str]] = {t: [] for t in sorted(THEMES)}
        for subcat, theme in sorted(SUBCATEGORY_TO_THEME.items()):
            _theme_subcats.setdefault(theme, []).append(subcat)

        def _normalize_distribution(
            distribution: dict[str, float],
        ) -> dict[str, float]:
            total = sum(distribution.values())
            if total <= 0:
                return distribution
            normalized = {key: value / total for key, value in distribution.items()}
            keys = list(normalized.keys())
            if keys:
                normalized[keys[-1]] += 1.0 - sum(normalized.values())
            return normalized

        def _theme_distribution_for_item(item: WorkItem) -> dict[str, float]:
            item_type = (item.type or "").lower()
            labels = {label.lower() for label in item.labels}

            if item_type == "bug" or "bug" in labels:
                return _normalize_distribution(
                    {
                        "feature_delivery": random.uniform(0.02, 0.08),
                        "operational": random.uniform(0.04, 0.12),
                        "maintenance": random.uniform(0.08, 0.18),
                        "quality": random.uniform(0.68, 0.82),
                        "risk": random.uniform(0.02, 0.08),
                    }
                )

            if item_type == "story":
                return _normalize_distribution(
                    {
                        "feature_delivery": random.uniform(0.68, 0.84),
                        "operational": random.uniform(0.02, 0.07),
                        "maintenance": random.uniform(0.06, 0.14),
                        "quality": random.uniform(0.05, 0.14),
                        "risk": random.uniform(0.01, 0.05),
                    }
                )

            if item_type == "incident":
                return _normalize_distribution(
                    {
                        "feature_delivery": random.uniform(0.01, 0.05),
                        "operational": random.uniform(0.70, 0.85),
                        "maintenance": random.uniform(0.05, 0.12),
                        "quality": random.uniform(0.03, 0.08),
                        "risk": random.uniform(0.02, 0.06),
                    }
                )

            # Default (task, etc.)
            theme_distribution = {
                "feature_delivery": random.uniform(0.20, 0.38),
                "operational": random.uniform(0.06, 0.15),
                "maintenance": random.uniform(0.24, 0.42),
                "quality": random.uniform(0.12, 0.26),
                "risk": random.uniform(0.02, 0.10),
            }
            if "security" in labels:
                theme_distribution["risk"] += 0.10
                theme_distribution["feature_delivery"] = max(
                    0.05, theme_distribution["feature_delivery"] - 0.05
                )
            return _normalize_distribution(theme_distribution)

        def _subcategory_distribution_for_item(
            item: WorkItem, theme_distribution: dict[str, float]
        ) -> dict[str, float]:
            """Split each theme's weight across its canonical subcategories."""
            item_type = (item.type or "").lower()
            labels = {label.lower() for label in item.labels}

            # Per-theme split ratios keyed by subcategory suffix.
            # Each tuple maps to the subcategories in canonical order.
            # feature_delivery: customer, roadmap, enablement
            if item_type == "story":
                fd_split = (0.50, 0.35, 0.15)
            elif item_type == "task":
                fd_split = (0.20, 0.30, 0.50)
            else:
                fd_split = (0.35, 0.40, 0.25)

            # operational: incident_response, on_call, support
            if item_type == "incident":
                op_split = (0.70, 0.20, 0.10)
            else:
                op_split = (0.40, 0.30, 0.30)

            # maintenance: refactor, upgrade, debt
            if "infra" in labels or "dependencies" in labels:
                mt_split = (0.25, 0.50, 0.25)
            else:
                mt_split = (0.50, 0.20, 0.30)

            # quality: testing, bugfix, reliability
            if item_type == "bug":
                qa_split = (0.10, 0.75, 0.15)
            else:
                qa_split = (0.40, 0.25, 0.35)

            # risk: security, compliance, vulnerability
            if "security" in labels:
                rk_split = (0.50, 0.15, 0.35)
            else:
                rk_split = (0.35, 0.30, 0.35)

            split_map: dict[str, tuple[float, ...]] = {
                "feature_delivery": fd_split,
                "operational": op_split,
                "maintenance": mt_split,
                "quality": qa_split,
                "risk": rk_split,
            }

            result: dict[str, float] = {}
            for theme, subcats in _theme_subcats.items():
                theme_value = theme_distribution.get(theme, 0.0)
                splits = split_map.get(theme, ())
                for i, subcat in enumerate(subcats):
                    weight = splits[i] if i < len(splits) else 1.0 / len(subcats)
                    result[subcat] = theme_value * weight
            return result

        records = []
        computed_at = datetime.now(timezone.utc)
        max_duration_days = max(1, min(days, 14))
        run_id = categorization_run_id or str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"fixture-work-unit-investments:{self.repo_id}:{days}:{org_id}",
            )
        )

        for item in work_items:
            if item.type == "epic":
                continue

            from_ts = item.created_at
            if item.completed_at is not None:
                to_ts = item.completed_at
            else:
                to_ts = from_ts + timedelta(days=random.randint(1, max_duration_days))

            if to_ts < from_ts:
                to_ts = from_ts

            theme_distribution = _theme_distribution_for_item(item)
            subcategory_distribution = _subcategory_distribution_for_item(
                item, theme_distribution
            )

            quality = round(random.uniform(0.5, 1.0), 3)
            input_hash = hashlib.md5(
                "|".join(
                    [
                        item.work_item_id,
                        item.type or "",
                        item.title or "",
                        item.status or "",
                        item.provider or "",
                        from_ts.isoformat(),
                        to_ts.isoformat(),
                    ]
                ).encode("utf-8"),
                usedforsecurity=False,
            ).hexdigest()

            records.append(
                WorkUnitInvestmentRecord(
                    work_unit_id=item.work_item_id,
                    work_unit_type=item.type,
                    work_unit_name=item.title,
                    from_ts=from_ts,
                    to_ts=to_ts,
                    repo_id=self.repo_id,
                    provider=item.provider,
                    effort_metric="churn_loc",
                    effort_value=float(random.randint(80, 3200)),
                    theme_distribution_json=theme_distribution,
                    subcategory_distribution_json=subcategory_distribution,
                    structural_evidence_json=json.dumps(
                        {"issues": [item.work_item_id]}
                    ),
                    evidence_quality=quality,
                    evidence_quality_band=evidence_quality_band(quality),
                    categorization_status="ok",
                    categorization_errors_json=json.dumps({}),
                    categorization_model_version="synthetic-v1",
                    categorization_input_hash=input_hash,
                    categorization_run_id=run_id,
                    computed_at=computed_at,
                    org_id=org_id,
                )
            )

        return records

    def generate_work_unit_memberships(
        self,
        work_unit_investments: list[Any],
        *,
        org_id: str = "",
    ) -> list[Any]:
        """Generate work_unit_membership rows from generated investment records.

        Derived directly from each ``WorkUnitInvestmentRecord``'s theme and
        subcategory distributions so fixtures stay consistent with investments
        and mirror the materializer's multi-membership rules exactly: one row
        per (node, category) at/above MEMBERSHIP_WEIGHT_THRESHOLD, plus the
        argmax of each kind (is_dominant=1) even below threshold, lexical
        tie-break. Each fixture work unit is a single issue node
        (work_unit_id == work_item_id), so the node is ("issue", work_unit_id).

        Populating this in fixtures means the live-e2e/demo org exercises theme
        filtering directly and the degraded path (MEMBERSHIP_NOT_MATERIALIZED)
        is only ever hit pre-materialization (CHAOS-2430).
        """
        from dev_health_ops.metrics.schemas import WorkUnitMembershipRecord
        from dev_health_ops.work_graph.investment.materialize import (
            _membership_categories,
        )

        records: list[Any] = []
        for inv in work_unit_investments:
            node_id = inv.work_unit_id
            theme_rows = _membership_categories(inv.theme_distribution_json or {})
            subcat_rows = _membership_categories(
                inv.subcategory_distribution_json or {}
            )
            for kind, cat_rows in (
                ("theme", theme_rows),
                ("subcategory", subcat_rows),
            ):
                for category, weight, is_dominant in cat_rows:
                    records.append(
                        WorkUnitMembershipRecord(
                            org_id=org_id,
                            node_type="issue",
                            node_id=node_id,
                            work_unit_id=inv.work_unit_id,
                            category_kind=kind,
                            category=category,
                            weight=weight,
                            is_dominant=is_dominant,
                            categorization_status=inv.categorization_status,
                            computed_at=inv.computed_at,
                        )
                    )
        return records

    def generate_investment_metrics(self, days: int = 30) -> list[Any]:
        """Generate investment metrics daily rollup records."""
        from dev_health_ops.investment_taxonomy import THEMES
        from dev_health_ops.metrics.schemas import InvestmentMetricsRecord

        records = []
        end_date = datetime.now(timezone.utc).date()
        computed_at = datetime.now(timezone.utc)

        investment_areas = sorted(THEMES)

        teams_to_use = []
        if self.assigned_teams is None:
            teams_to_use = [DEFAULT_DEMO_TEAM]
        elif self.assigned_teams:
            teams_to_use = [(t.id, t.name) for t in self.assigned_teams]
        else:
            teams_to_use = [("unassigned", "Unassigned")]

        for i in range(days):
            day = end_date - timedelta(days=i)
            for team_id, _ in teams_to_use:
                for area in investment_areas:
                    records.append(
                        InvestmentMetricsRecord(
                            repo_id=self.repo_id,
                            day=day,
                            team_id=team_id,
                            investment_area=area,
                            project_stream="",
                            delivery_units=random.randint(0, 5),
                            work_items_completed=random.randint(0, 3),
                            prs_merged=random.randint(0, 2),
                            churn_loc=random.randint(0, 500),
                            cycle_p50_hours=random.uniform(12.0, 72.0),
                            computed_at=computed_at,
                        )
                    )
        return records
