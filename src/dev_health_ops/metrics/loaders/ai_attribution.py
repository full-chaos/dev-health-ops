from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from dev_health_ops.metrics.loaders.base import parse_uuid
from dev_health_ops.metrics.query_builder import OrgScopedQuery


class AIAttributionClickHouseLoader:
    """Reads resolved AI attribution evidence directly from ClickHouse.

    Sourced from ``ai_attribution_resolved`` — the highest-precedence,
    non-superseded signal per subject (see docs/architecture/ai-attribution.md).
    This is the only read path backing the dedicated ``/ai/attribution`` page:
    it never aggregates away provenance, and it never infers a "human" count
    by process of elimination (that inference already lives in
    ``ai_impact_metrics_daily`` / ``aiImpactSummary`` and requires the full PR
    population, not just detected signals — mixing the two here would silently
    duplicate that job's methodology with a different, undocumented one).
    """

    def __init__(self, client: Any, org_id: str = "") -> None:
        self.client = client
        self.org_id = org_id
        self._scope = OrgScopedQuery(org_id)

    async def load_mix(
        self,
        *,
        start: datetime,
        end: datetime,
        repo_id: uuid.UUID | None = None,
        repo_ids: list[uuid.UUID] | None = None,
        kinds: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Count resolved attribution records per kind in the window.

        Every row in ``ai_attribution_resolved`` already represents the
        winning (highest-precedence, non-superseded) signal for its subject,
        so a plain ``GROUP BY kind`` cannot double-count a subject across
        sources.
        """
        from dev_health_ops.api.queries.client import query_dicts

        params: dict[str, Any] = {
            "start": start.replace(tzinfo=None),
            "end": end.replace(tzinfo=None),
        }
        repo_filter = self._repo_filter(params, repo_id, repo_ids)
        kind_filter = self._kind_filter(params, kinds)
        params = self._scope.inject(params)
        org_filter = self._scope.filter_uuid()

        query = f"""
        SELECT
            kind,
            count() AS count
        FROM ai_attribution_resolved
        WHERE observed_at >= {{start:DateTime}}
          AND observed_at < {{end:DateTime}}
          {repo_filter}
          {kind_filter}
          {org_filter}
        GROUP BY kind
        ORDER BY kind
        """
        return await query_dicts(self.client, query, params)

    async def load_evidence(
        self,
        *,
        start: datetime,
        end: datetime,
        repo_id: uuid.UUID | None = None,
        repo_ids: list[uuid.UUID] | None = None,
        kinds: list[str] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Load resolved AI attribution records with full provenance.

        Every row is a persisted, resolved signal — no aggregation, no
        fabrication. ``source``, ``confidence``, and ``evidence`` are always
        populated because they are non-nullable on the base ``ai_attribution``
        table (see ``models/ai_attribution.py::AIAttributionRecord``).
        """
        from dev_health_ops.api.queries.client import query_dicts

        params: dict[str, Any] = {
            "start": start.replace(tzinfo=None),
            "end": end.replace(tzinfo=None),
        }
        repo_filter = self._repo_filter(params, repo_id, repo_ids)
        kind_filter = self._kind_filter(params, kinds)
        limit_clause = ""
        if limit is not None and int(limit) > 0:
            params["limit"] = int(limit)
            params["offset"] = max(0, int(offset))
            limit_clause = "LIMIT {limit:UInt32} OFFSET {offset:UInt32}"
        params = self._scope.inject(params)
        org_filter = self._scope.filter_uuid()

        query = f"""
        SELECT
            subject_type,
            subject_id,
            repo_id,
            provider,
            kind,
            source,
            confidence,
            actor,
            evidence,
            observed_at
        FROM ai_attribution_resolved
        WHERE observed_at >= {{start:DateTime}}
          AND observed_at < {{end:DateTime}}
          {repo_filter}
          {kind_filter}
          {org_filter}
        ORDER BY observed_at DESC, subject_id
        {limit_clause}
        """
        raw_rows = await query_dicts(self.client, query, params)
        rows: list[dict[str, Any]] = []
        for raw in raw_rows:
            rows.append(
                {
                    "subject_type": raw.get("subject_type"),
                    "subject_id": raw.get("subject_id"),
                    "repo_id": parse_uuid(raw.get("repo_id")),
                    "provider": raw.get("provider"),
                    "kind": raw.get("kind"),
                    "source": raw.get("source"),
                    "confidence": raw.get("confidence"),
                    "actor": raw.get("actor"),
                    "evidence": raw.get("evidence"),
                    "observed_at": raw.get("observed_at"),
                }
            )
        return rows

    def _repo_filter(
        self,
        params: dict[str, Any],
        repo_id: uuid.UUID | None,
        repo_ids: list[uuid.UUID] | None,
    ) -> str:
        clause = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            clause += "\n          AND repo_id = {repo_id:UUID}"
        if repo_ids is not None:
            params["repo_ids"] = [str(r) for r in repo_ids]
            clause += "\n          AND toString(repo_id) IN {repo_ids:Array(String)}"
        return clause

    def _kind_filter(
        self,
        params: dict[str, Any],
        kinds: list[str] | None,
    ) -> str:
        """AND-filter on the resolved view's ``kind`` column (CHAOS-2744).

        Backs the GraphQL ``AIAttributionScopeInput.buckets`` filter -- the
        UI's active bucket filters must reach the SQL WHERE clause, not be
        silently dropped after the query already ran.
        """
        if not kinds:
            return ""
        params["kinds"] = list(kinds)
        return "\n          AND kind IN {kinds:Array(String)}"
