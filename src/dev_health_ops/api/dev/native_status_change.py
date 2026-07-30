"""Bounded ClickHouse reader for Ask Dev status and observed-change facts.

Only canonical links are followed.  Current schema gaps (required CI/check
designation, consistent blocker direction, and project declared state)
remain explicit unknown/unavailable inputs to the deterministic rule service.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from dev_health_ops.api.graphql.resolvers._membership_run_scope import (
    LATEST_COMPLETE_RUN_SUBQUERY,
    LEGACY_NODE_MAX_JOIN,
    RUN_SCOPE_PREDICATE,
)
from dev_health_ops.api.queries.client import query_dicts

from .contracts import ClaimKind, DevScope, DirectScope, FreshnessState
from .native_evidence import (
    SourceFreshnessPolicy,
    default_native_freshness_policies,
)
from .status_change_service import (
    MAX_STATUS_ASSESSMENT_ITEMS,
    ChangeCategory,
    ChangeWindow,
    CIFact,
    DeploymentFact,
    IncidentFact,
    ObservedChange,
    PullRequestFact,
    RawChangeSummary,
    RawStatusSnapshot,
    SourceReference,
    StatusFact,
)

NATIVE_STATUS_SOURCE_VERSION = "native-status-change.v1"
NATIVE_STATUS_QUERY_VERSION = "native-status-change-query.v1"
QUERY_TIMEOUT_SECONDS = 15

_BLOCKER_PROJECTION_RULE_VERSION = "canonical-blocks.v2"

_WORK_UNIT_MEMBERSHIP_WATERMARK_SQL = """
SELECT max(completed_at) AS last_synced
FROM work_unit_membership_runs
WHERE org_id = {org_id:String}
  AND completed_at <= {as_of:DateTime64(3, 'UTC')}
HAVING count() > 0
"""

_WORK_UNIT_MEMBERS_SQL = f"""
WITH latest_run AS (
{LATEST_COMPLETE_RUN_SUBQUERY.replace("%(org_id)s", "{org_id:String}")}
)
SELECT m.node_type, m.node_id, max(m.computed_at) AS last_synced
FROM work_unit_membership AS m FINAL
INNER JOIN latest_run ON 1 = 1
{LEGACY_NODE_MAX_JOIN.replace("%(org_id)s", "{org_id:String}")}
WHERE m.org_id = {{org_id:String}}
  AND m.work_unit_id = {{entity_id:String}}
  AND latest_run.latest_run_id != ''
  AND ({RUN_SCOPE_PREDICATE})
GROUP BY m.node_type, m.node_id
ORDER BY m.node_type, m.node_id
LIMIT {{limit:UInt32}}
"""

_WORK_ITEMS_SQL = """
SELECT toString(repo_id) AS repository_id, work_item_id, title, status,
       parent_id, project_id, project_key, updated_at, last_synced
FROM work_items FINAL
WHERE org_id = {org_id:String}
  AND toString(repo_id) IN {repository_ids:Array(String)}
  AND updated_at <= {as_of:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'issue'
      AND (work_item_id = {entity_id:String} OR parent_id = {entity_id:String}))
    OR ({scope_type:String} = 'project'
      AND (project_id = {entity_id:String} OR project_key = {entity_id:String}))
    OR ({scope_type:String} = 'work_unit'
      AND work_item_id IN {member_issue_ids:Array(String)})
  )
ORDER BY (work_item_id = {entity_id:String}) DESC, updated_at DESC, work_item_id
LIMIT {limit:UInt32}
"""

_BLOCKER_WATERMARK_SQL = """
SELECT if(
         countIf(scope_repo_id IS NULL) > 0,
         maxIf(last_completed, scope_repo_id IS NULL),
         min(last_completed)
       ) AS last_synced
FROM (
  SELECT scope_repo_id, max(completed_at) AS last_completed
  FROM work_graph_projection_runs
  WHERE org_id = {org_id:String}
    AND projection_name = 'issue_blockers'
    AND rule_version = {blocker_rule_version:String}
    AND completed_at <= {as_of:DateTime64(3, 'UTC')}
    AND (scope_repo_id IS NULL OR toString(scope_repo_id) IN {repository_ids:Array(String)})
  GROUP BY scope_repo_id
)
HAVING countIf(scope_repo_id IS NULL) > 0
    OR countDistinctIf(toString(scope_repo_id), scope_repo_id IS NOT NULL)
       = length({repository_ids:Array(String)})
"""

_BLOCKERS_SQL = """
SELECT blocker.work_item_id AS entity_id,
       blocker.title AS display_label,
       blocker.status,
       greatest(blocker.updated_at, edge.event_ts) AS observed_at,
       greatest(blocker.last_synced, edge.last_synced) AS last_synced
FROM work_graph_edges AS edge FINAL
INNER JOIN work_items AS blocker FINAL
  ON blocker.org_id = edge.org_id AND blocker.work_item_id = edge.source_id
INNER JOIN work_items AS blocked FINAL
  ON blocked.org_id = edge.org_id AND blocked.work_item_id = edge.target_id
WHERE edge.org_id = {org_id:String}
  AND edge.source_type = 'issue'
  AND edge.target_type = 'issue'
  AND edge.edge_type = 'blocks'
  AND edge.provenance = 'native'
  AND toString(blocker.repo_id) IN {repository_ids:Array(String)}
  AND toString(blocked.repo_id) IN {repository_ids:Array(String)}
  AND edge.event_ts <= {as_of:DateTime64(3, 'UTC')}
  AND blocker.updated_at <= {as_of:DateTime64(3, 'UTC')}
  AND blocked.updated_at <= {as_of:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'issue' AND edge.target_id = {entity_id:String})
    OR ({scope_type:String} = 'project'
      AND (blocked.project_id = {entity_id:String} OR blocked.project_key = {entity_id:String}))
    OR ({scope_type:String} = 'work_unit'
      AND edge.target_id IN {member_issue_ids:Array(String)})
  )
ORDER BY observed_at DESC, entity_id
LIMIT {limit:UInt32}
"""

_PULL_REQUESTS_SQL = """
WITH linked AS (
  SELECT toString(link.repo_id) AS repository_id, link.pr_number
  FROM work_graph_issue_pr AS link FINAL
  LEFT JOIN work_items AS item FINAL
    ON item.repo_id = link.repo_id AND item.work_item_id = link.work_item_id
  WHERE link.org_id = {org_id:String}
    AND item.org_id = {org_id:String}
    AND toString(link.repo_id) IN {repository_ids:Array(String)}
    AND (
      ({scope_type:String} = 'issue' AND link.work_item_id = {entity_id:String})
      OR ({scope_type:String} = 'project'
        AND (item.project_id = {entity_id:String} OR item.project_key = {entity_id:String}))
      OR ({scope_type:String} = 'work_unit'
        AND link.work_item_id IN {member_issue_ids:Array(String)})
    )
), latest_reviews AS (
  SELECT toString(repo_id) AS repository_id, number, reviewer,
         argMax(state, (submitted_at, last_synced, review_id)) AS state,
         max(submitted_at) AS submitted_at
  FROM git_pull_request_reviews FINAL
  WHERE org_id = {org_id:String}
    AND toString(repo_id) IN {repository_ids:Array(String)}
  GROUP BY repository_id, number, reviewer
), reviews AS (
  SELECT repository_id, number,
         multiIf(
           countIf(upper(state) = 'CHANGES_REQUESTED') > 0,
           'CHANGES_REQUESTED',
           countIf(upper(state) = 'APPROVED') > 0,
           'APPROVED',
           argMax(state, submitted_at)
         ) AS review_state,
         countIf(upper(state) = 'CHANGES_REQUESTED') AS changes_requested
  FROM latest_reviews
  GROUP BY repository_id, number
)
SELECT toString(pr.repo_id) AS repository_id, pr.number,
       concat(toString(pr.repo_id), '#pr', toString(pr.number)) AS entity_id,
       ifNull(pr.title, concat('Pull request #', toString(pr.number))) AS display_label,
       ifNull(pr.state, 'unknown') AS state,
       reviews.review_state AS review_state,
       ifNull(reviews.changes_requested, 0) AS changes_requested,
       isNotNull(pr.merged_at) AS merged,
       coalesce(pr.merged_at, pr.closed_at, pr.created_at) AS observed_at,
       pr.last_synced
FROM git_pull_requests AS pr FINAL
LEFT JOIN reviews
  ON reviews.repository_id = toString(pr.repo_id) AND reviews.number = pr.number
WHERE pr.org_id = {org_id:String}
  AND toString(pr.repo_id) IN {repository_ids:Array(String)}
  AND pr.created_at <= {as_of:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'pull_request' AND pr.number = {pr_number:UInt32})
    OR ({scope_type:String} IN ('issue', 'project')
      AND (toString(pr.repo_id), pr.number) IN
          (SELECT repository_id, pr_number FROM linked))
    OR ({scope_type:String} = 'work_unit'
      AND (
        (toString(pr.repo_id), pr.number) IN
          (SELECT repository_id, pr_number FROM linked)
        OR concat(toString(pr.repo_id), '#pr', toString(pr.number))
          IN {member_pr_ids:Array(String)}
      ))
    OR ({scope_type:String} = 'repository')
  )
ORDER BY observed_at DESC, entity_id
LIMIT {limit:UInt32}
"""

_CI_SQL = """
SELECT toString(repo_id) AS repository_id, run_id,
       concat(toString(repo_id), '#ci', run_id) AS entity_id,
       ifNull(pipeline_name, concat('CI run ', run_id)) AS display_label,
       ifNull(status, 'unknown') AS conclusion,
       ifNull(pr_number, 0) AS pr_number,
       coalesce(finished_at, started_at) AS observed_at, last_synced
FROM ci_pipeline_runs FINAL
WHERE org_id = {org_id:String}
  AND toString(repo_id) IN {repository_ids:Array(String)}
  AND ifNull(pr_number, 0) IN {pr_numbers:Array(UInt32)}
  AND started_at <= {as_of:DateTime64(3, 'UTC')}
ORDER BY observed_at DESC, entity_id
LIMIT {limit:UInt32}
"""

_CI_ACCEPTANCE_SQL = """
SELECT toString(repo_id) AS repository_id, run_id, check_key,
       concat(toString(repo_id), '#ci', run_id, '#check', check_key) AS entity_id,
       check_name AS display_label, requirement, result AS conclusion,
       ifNull(pr_number, 0) AS pr_number, observed_at, last_synced,
       provenance, rule_version, source_url
FROM ci_acceptance_checks FINAL
WHERE org_id = {org_id:String}
  AND toString(repo_id) IN {repository_ids:Array(String)}
  AND ifNull(pr_number, 0) IN {pr_numbers:Array(UInt32)}
  AND observed_at <= {as_of:DateTime64(3, 'UTC')}
ORDER BY observed_at DESC, entity_id
LIMIT {limit:UInt32}
"""

_DEPLOYMENTS_SQL = """
SELECT toString(repo_id) AS repository_id, deployment_id AS entity_id,
       concat('Deployment ', deployment_id) AS display_label,
       ifNull(status, 'unknown') AS status, environment,
       ifNull(pull_request_number, 0) AS pr_number,
       coalesce(deployed_at, finished_at, started_at, last_synced) AS observed_at,
       last_synced
FROM deployments FINAL
WHERE org_id = {org_id:String}
  AND toString(repo_id) IN {repository_ids:Array(String)}
  AND (
    ({scope_type:String} = 'repository')
    OR ifNull(pull_request_number, 0) IN {pr_numbers:Array(UInt32)}
  )
  AND coalesce(deployed_at, finished_at, started_at, last_synced)
      <= {as_of:DateTime64(3, 'UTC')}
ORDER BY observed_at DESC, entity_id
LIMIT {limit:UInt32}
"""

_INCIDENTS_SQL = """
SELECT incident.id AS entity_id, incident.title AS display_label,
       ifNull(incident.normalized_status, 'unknown') AS status,
       incident.resolved_at IS NULL AND incident.is_deleted = 0 AS active,
       coalesce(incident.source_event_at, incident.observed_at) AS observed_at,
       incident.last_synced
FROM operational_incidents AS incident FINAL
INNER JOIN work_graph_deployment_incident_edges AS edge FINAL
  ON edge.org_id = toUUIDOrZero(incident.org_id)
 AND edge.incident_id = incident.id
WHERE incident.org_id = {org_id:String}
  AND edge.deployment_id IN {deployment_ids:Array(String)}
  AND toString(edge.repo_id) IN {repository_ids:Array(String)}
  AND coalesce(incident.source_event_at, incident.observed_at)
      <= {as_of:DateTime64(3, 'UTC')}
ORDER BY observed_at DESC, entity_id
LIMIT {limit:UInt32}
"""

_ORGANIZATION_AUTHORIZED_REPOSITORIES_SQL = """
SELECT toString(id) AS repository_id
FROM repos FINAL
WHERE org_id = {org_id:String}
"""

_TRANSITIONS_SQL = """
SELECT transition.work_item_id AS entity_id, item.title AS display_label,
       transition.from_status, transition.to_status,
       transition.occurred_at AS observed_at, transition.last_synced
FROM work_item_transitions AS transition FINAL
INNER JOIN work_items AS item FINAL
  ON item.org_id = transition.org_id
 AND item.repo_id = transition.repo_id
 AND item.work_item_id = transition.work_item_id
WHERE transition.org_id = {org_id:String}
  AND item.org_id = {org_id:String}
  AND toString(item.repo_id) IN {repository_ids:Array(String)}
  AND transition.occurred_at >= {start:DateTime64(3, 'UTC')}
  AND transition.occurred_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'issue' AND transition.work_item_id = {entity_id:String})
    OR ({scope_type:String} = 'project'
      AND (item.project_id = {entity_id:String} OR item.project_key = {entity_id:String}))
    OR ({scope_type:String} = 'repository')
  )
ORDER BY observed_at, entity_id, from_status, to_status
LIMIT {limit:UInt32}
"""

_RELATIONSHIPS_SQL = """
SELECT edge_id AS change_id, source_type, source_id, edge_type,
       target_type, target_id, provenance, confidence,
       discovered_at AS observed_at, last_synced
FROM work_graph_edges FINAL
WHERE org_id = {org_id:String}
  AND toString(repo_id) IN {repository_ids:Array(String)}
  AND discovered_at >= {start:DateTime64(3, 'UTC')}
  AND discovered_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} IN ('issue', 'pull_request')
      AND (source_id = {entity_id:String} OR target_id = {entity_id:String}))
    OR ({scope_type:String} = 'project' AND (
      source_id IN (
        SELECT work_item_id FROM work_items FINAL
        WHERE org_id = {org_id:String}
          AND toString(repo_id) IN {repository_ids:Array(String)}
          AND (project_id = {entity_id:String} OR project_key = {entity_id:String})
      )
      OR target_id IN (
        SELECT work_item_id FROM work_items FINAL
        WHERE org_id = {org_id:String}
          AND toString(repo_id) IN {repository_ids:Array(String)}
          AND (project_id = {entity_id:String} OR project_key = {entity_id:String})
      )
    ))
    OR ({scope_type:String} = 'repository')
  )
ORDER BY observed_at, source_type, source_id, edge_type, target_type, target_id
LIMIT {limit:UInt32}
"""

_PULL_REQUEST_CHANGES_SQL = """
WITH linked AS (
  SELECT toString(link.repo_id) AS repository_id, link.pr_number
  FROM work_graph_issue_pr AS link FINAL
  INNER JOIN work_items AS item FINAL
    ON item.org_id = link.org_id
   AND item.repo_id = link.repo_id
   AND item.work_item_id = link.work_item_id
  WHERE link.org_id = {org_id:String}
    AND item.org_id = {org_id:String}
    AND toString(link.repo_id) IN {repository_ids:Array(String)}
    AND (
      ({scope_type:String} = 'issue' AND link.work_item_id = {entity_id:String})
      OR ({scope_type:String} = 'project'
        AND (item.project_id = {entity_id:String} OR item.project_key = {entity_id:String}))
    )
)
SELECT concat(toString(pr.repo_id), '#pr', toString(pr.number), '#state#',
              if(isNotNull(pr.merged_at), 'merged',
                 if(isNotNull(pr.closed_at), 'closed', ifNull(pr.state, 'open')))) AS change_id,
       concat(toString(pr.repo_id), '#pr', toString(pr.number)) AS entity_id,
       ifNull(pr.title, concat('Pull request #', toString(pr.number))) AS display_label,
       if(isNotNull(pr.merged_at) OR isNotNull(pr.closed_at),
          CAST('open', 'Nullable(String)'), CAST(NULL, 'Nullable(String)')) AS before_value,
       if(isNotNull(pr.merged_at), 'merged',
          if(isNotNull(pr.closed_at), 'closed', ifNull(pr.state, 'open'))) AS after_value,
       coalesce(pr.merged_at, pr.closed_at, pr.created_at) AS observed_at,
       pr.last_synced
FROM git_pull_requests AS pr FINAL
WHERE pr.org_id = {org_id:String}
  AND toString(pr.repo_id) IN {repository_ids:Array(String)}
  AND observed_at >= {start:DateTime64(3, 'UTC')}
  AND observed_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'pull_request' AND pr.number = {pr_number:UInt32})
    OR ({scope_type:String} IN ('issue', 'project')
      AND (toString(pr.repo_id), pr.number) IN
          (SELECT repository_id, pr_number FROM linked))
    OR ({scope_type:String} IN ('organization', 'repository'))
  )
ORDER BY observed_at, entity_id, change_id
LIMIT {limit:UInt32}
"""

_REVIEW_CHANGES_SQL = """
WITH linked AS (
  SELECT toString(link.repo_id) AS repository_id, link.pr_number
  FROM work_graph_issue_pr AS link FINAL
  INNER JOIN work_items AS item FINAL
    ON item.org_id = link.org_id
   AND item.repo_id = link.repo_id
   AND item.work_item_id = link.work_item_id
  WHERE link.org_id = {org_id:String}
    AND item.org_id = {org_id:String}
    AND toString(link.repo_id) IN {repository_ids:Array(String)}
    AND (
      ({scope_type:String} = 'issue' AND link.work_item_id = {entity_id:String})
      OR ({scope_type:String} = 'project'
        AND (item.project_id = {entity_id:String} OR item.project_key = {entity_id:String}))
    )
)
SELECT concat(toString(review.repo_id), '#pr', toString(review.number),
              '#review#', review.review_id) AS change_id,
       change_id AS entity_id,
       concat('Review by ', review.reviewer) AS display_label,
       CAST(NULL, 'Nullable(String)') AS before_value,
       review.state AS after_value,
       review.submitted_at AS observed_at,
       review.last_synced
FROM git_pull_request_reviews AS review FINAL
WHERE review.org_id = {org_id:String}
  AND toString(review.repo_id) IN {repository_ids:Array(String)}
  AND review.submitted_at >= {start:DateTime64(3, 'UTC')}
  AND review.submitted_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'pull_request' AND review.number = {pr_number:UInt32})
    OR ({scope_type:String} IN ('issue', 'project')
      AND (toString(review.repo_id), review.number) IN
          (SELECT repository_id, pr_number FROM linked))
    OR ({scope_type:String} IN ('organization', 'repository'))
  )
ORDER BY observed_at, entity_id, change_id
LIMIT {limit:UInt32}
"""

_CI_CHANGES_SQL = """
WITH linked AS (
  SELECT toString(link.repo_id) AS repository_id, link.pr_number
  FROM work_graph_issue_pr AS link FINAL
  INNER JOIN work_items AS item FINAL
    ON item.org_id = link.org_id
   AND item.repo_id = link.repo_id
   AND item.work_item_id = link.work_item_id
  WHERE link.org_id = {org_id:String}
    AND item.org_id = {org_id:String}
    AND toString(link.repo_id) IN {repository_ids:Array(String)}
    AND (
      ({scope_type:String} = 'issue' AND link.work_item_id = {entity_id:String})
      OR ({scope_type:String} = 'project'
        AND (item.project_id = {entity_id:String} OR item.project_key = {entity_id:String}))
    )
)
SELECT concat(toString(run.repo_id), '#ci#', run.run_id) AS change_id,
       change_id AS entity_id,
       ifNull(run.pipeline_name, concat('CI run ', run.run_id)) AS display_label,
       CAST(NULL, 'Nullable(String)') AS before_value,
       ifNull(run.status, 'unknown') AS after_value,
       coalesce(run.finished_at, run.started_at) AS observed_at,
       run.last_synced
FROM ci_pipeline_runs AS run FINAL
WHERE run.org_id = {org_id:String}
  AND toString(run.repo_id) IN {repository_ids:Array(String)}
  AND observed_at >= {start:DateTime64(3, 'UTC')}
  AND observed_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'pull_request' AND ifNull(run.pr_number, 0) = {pr_number:UInt32})
    OR ({scope_type:String} IN ('issue', 'project')
      AND (toString(run.repo_id), ifNull(run.pr_number, 0)) IN
          (SELECT repository_id, pr_number FROM linked))
    OR ({scope_type:String} IN ('organization', 'repository'))
  )
ORDER BY observed_at, entity_id, change_id
LIMIT {limit:UInt32}
"""

_DEPLOYMENT_CHANGES_SQL = """
WITH linked AS (
  SELECT toString(link.repo_id) AS repository_id, link.pr_number
  FROM work_graph_issue_pr AS link FINAL
  INNER JOIN work_items AS item FINAL
    ON item.org_id = link.org_id
   AND item.repo_id = link.repo_id
   AND item.work_item_id = link.work_item_id
  WHERE link.org_id = {org_id:String}
    AND item.org_id = {org_id:String}
    AND toString(link.repo_id) IN {repository_ids:Array(String)}
    AND (
      ({scope_type:String} = 'issue' AND link.work_item_id = {entity_id:String})
      OR ({scope_type:String} = 'project'
        AND (item.project_id = {entity_id:String} OR item.project_key = {entity_id:String}))
    )
)
SELECT concat(toString(deployment.repo_id), '#deployment#',
              deployment.deployment_id) AS change_id,
       change_id AS entity_id,
       concat('Deployment ', deployment.deployment_id) AS display_label,
       CAST(NULL, 'Nullable(String)') AS before_value,
       ifNull(deployment.status, 'unknown') AS after_value,
       coalesce(deployment.deployed_at, deployment.finished_at,
                deployment.started_at, deployment.last_synced) AS observed_at,
       deployment.last_synced
FROM deployments AS deployment FINAL
WHERE deployment.org_id = {org_id:String}
  AND toString(deployment.repo_id) IN {repository_ids:Array(String)}
  AND observed_at >= {start:DateTime64(3, 'UTC')}
  AND observed_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'pull_request'
      AND ifNull(deployment.pull_request_number, 0) = {pr_number:UInt32})
    OR ({scope_type:String} IN ('issue', 'project')
      AND (toString(deployment.repo_id), ifNull(deployment.pull_request_number, 0)) IN
          (SELECT repository_id, pr_number FROM linked))
    OR ({scope_type:String} IN ('organization', 'repository'))
  )
ORDER BY observed_at, entity_id, change_id
LIMIT {limit:UInt32}
"""

_INCIDENT_CHANGES_SQL = """
WITH linked AS (
  SELECT toString(link.repo_id) AS repository_id, link.pr_number
  FROM work_graph_issue_pr AS link FINAL
  INNER JOIN work_items AS item FINAL
    ON item.org_id = link.org_id
   AND item.repo_id = link.repo_id
   AND item.work_item_id = link.work_item_id
  WHERE link.org_id = {org_id:String}
    AND item.org_id = {org_id:String}
    AND toString(link.repo_id) IN {repository_ids:Array(String)}
    AND (
      ({scope_type:String} = 'issue' AND link.work_item_id = {entity_id:String})
      OR ({scope_type:String} = 'project'
        AND (item.project_id = {entity_id:String} OR item.project_key = {entity_id:String}))
    )
)
SELECT concat(incident.id, '#state#',
              ifNull(incident.normalized_status, 'unknown')) AS change_id,
       incident.id AS entity_id,
       incident.title AS display_label,
       CAST(NULL, 'Nullable(String)') AS before_value,
       ifNull(incident.normalized_status, 'unknown') AS after_value,
       edge.deployment_id AS deployment_id,
       edge.source AS relationship_source,
       edge.confidence AS relationship_confidence,
       coalesce(incident.resolved_at, incident.source_event_at,
                incident.observed_at) AS observed_at,
       incident.last_synced
FROM operational_incidents AS incident FINAL
INNER JOIN work_graph_deployment_incident_edges AS edge FINAL
  ON edge.org_id = toUUIDOrZero(incident.org_id)
 AND edge.incident_id = incident.id
INNER JOIN deployments AS deployment FINAL
  ON deployment.org_id = incident.org_id
 AND deployment.repo_id = edge.repo_id
 AND deployment.deployment_id = edge.deployment_id
WHERE incident.org_id = {org_id:String}
  AND deployment.org_id = {org_id:String}
  AND toString(edge.repo_id) IN {repository_ids:Array(String)}
  AND incident.is_deleted = 0
  AND observed_at >= {start:DateTime64(3, 'UTC')}
  AND observed_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'pull_request'
      AND ifNull(deployment.pull_request_number, 0) = {pr_number:UInt32})
    OR ({scope_type:String} IN ('issue', 'project')
      AND (toString(deployment.repo_id), ifNull(deployment.pull_request_number, 0)) IN
          (SELECT repository_id, pr_number FROM linked))
    OR ({scope_type:String} IN ('organization', 'repository'))
  )
ORDER BY observed_at, entity_id, change_id
LIMIT {limit:UInt32}
"""


class ClickHouseStatusChangeSource:
    """Read only facts with server-owned repository bounds and query timeout."""

    def __init__(
        self,
        client: Any,
        *,
        policies: Mapping[str, SourceFreshnessPolicy] | None = None,
        now: datetime | None = None,
    ) -> None:
        self._client = client
        self._policies = dict(policies or default_native_freshness_policies())
        self._now = now

    async def _authorized_repository_ids(
        self, org_id: str, scope: DevScope
    ) -> list[str]:
        """Bound status/change reads to the server-owned repository set.

        Organization scope cannot serialize its full authorized repository
        set onto the wire (``DevScope.repositories`` is capped at 20 entries
        by the ask-dev/v1 contract), so it is re-derived here directly from
        the same ``org_id`` boundary every other native query already
        enforces (CHAOS-3255). This scales to any repository count without
        truncation or widening. Every other direct scope keeps using the
        scope's own bounded repository/entity refs.
        """
        if scope.direct_scope is not DirectScope.ORGANIZATION:
            return self._repository_ids(scope)
        try:
            async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                rows = await query_dicts(
                    self._client,
                    _ORGANIZATION_AUTHORIZED_REPOSITORIES_SQL,
                    {"org_id": org_id},
                )
        except Exception:
            return []
        return sorted(
            {str(row["repository_id"]) for row in rows if row.get("repository_id")}
        )

    async def status_snapshot(
        self, *, org_id: str, scope: DevScope, as_of: datetime, limit: int
    ) -> RawStatusSnapshot:
        repositories = await self._authorized_repository_ids(org_id, scope)
        entity_id = self._entity_id(scope)
        scope_type = scope.direct_scope.value
        warnings: list[str] = []
        source_refs: list[SourceReference] = []
        if not repositories:
            return RawStatusSnapshot(
                declared=None,
                source_refs=(self._unavailable_ref("authorized_repositories", scope),),
                warnings=(
                    "Status reads require the complete authorized repository set; scope was not widened.",
                ),
            )

        common = {
            "org_id": org_id,
            "repository_ids": repositories,
            "scope_type": scope_type,
            "entity_id": entity_id,
            "pr_number": self._pr_number(entity_id),
            "as_of": as_of.astimezone(UTC),
            "limit": min(limit, MAX_STATUS_ASSESSMENT_ITEMS),
            "member_issue_ids": [],
            "member_pr_ids": [],
        }
        if scope.direct_scope is DirectScope.WORK_UNIT:
            marker_rows, membership_ref, warning = await self._read(
                "work_units",
                _WORK_UNIT_MEMBERSHIP_WATERMARK_SQL,
                common,
                scope,
            )
            source_refs.append(membership_ref)
            if warning:
                warnings.append(warning)
            if not marker_rows:
                return RawStatusSnapshot(
                    declared=None,
                    source_refs=tuple(source_refs),
                    warnings=tuple(
                        warnings
                        + ["canonical work-unit membership has no complete run"]
                    ),
                )
            try:
                async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                    member_rows = await query_dicts(
                        self._client, _WORK_UNIT_MEMBERS_SQL, common
                    )
            except Exception:
                return RawStatusSnapshot(
                    declared=None,
                    source_refs=(self._unavailable_ref("work_units", scope),),
                    warnings=("work_units source unavailable",),
                )
            common["member_issue_ids"] = sorted(
                {
                    str(row.get("node_id") or "")
                    for row in member_rows
                    if str(row.get("node_type") or "").casefold() == "issue"
                    and row.get("node_id")
                }
            )
            common["member_pr_ids"] = sorted(
                {
                    str(row.get("node_id") or "")
                    for row in member_rows
                    if str(row.get("node_type") or "").casefold()
                    in {"pr", "pull_request"}
                    and row.get("node_id")
                }
            )
            if not common["member_issue_ids"] and not common["member_pr_ids"]:
                return RawStatusSnapshot(
                    declared=None,
                    source_refs=tuple(source_refs),
                    warnings=tuple(warnings),
                )

        work_item_rows: list[dict[str, Any]] = []
        if scope.direct_scope in {DirectScope.ISSUE, DirectScope.PROJECT} or (
            scope.direct_scope is DirectScope.WORK_UNIT
            and bool(common["member_issue_ids"])
        ):
            work_item_rows, ref, warning = await self._read(
                "work_items", _WORK_ITEMS_SQL, common, scope
            )
            source_refs.append(ref)
            if warning:
                warnings.append(warning)

        blocker_rows: list[dict[str, Any]] = []
        blocker_ref: SourceReference | None = None
        if scope.direct_scope in {DirectScope.ISSUE, DirectScope.PROJECT} or (
            scope.direct_scope is DirectScope.WORK_UNIT
            and bool(common["member_issue_ids"])
        ):
            marker_rows, blocker_ref, warning = await self._read(
                "work_graph",
                _BLOCKER_WATERMARK_SQL,
                {
                    **common,
                    "blocker_rule_version": _BLOCKER_PROJECTION_RULE_VERSION,
                },
                scope,
            )
            if warning:
                warnings.append(warning)
            if marker_rows:
                try:
                    async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                        blocker_rows = await query_dicts(
                            self._client, _BLOCKERS_SQL, common
                        )
                except Exception:
                    blocker_ref = self._unavailable_ref("work_graph", scope)
                    warnings.append("work_graph blocker source unavailable")

        pr_rows, pr_ref, warning = await self._read(
            "pull_requests", _PULL_REQUESTS_SQL, common, scope
        )
        source_refs.append(pr_ref)
        if warning:
            warnings.append(warning)
        pr_pairs = {
            (str(row.get("repository_id") or ""), int(row.get("number") or 0))
            for row in pr_rows
        }
        pr_numbers = sorted({number for _, number in pr_pairs if number})

        ci_rows: list[dict[str, Any]] = []
        ci_acceptance_rows: list[dict[str, Any]] = []
        ci_acceptance_ref: SourceReference | None = None
        if pr_numbers:
            ci_rows, ci_ref, warning = await self._read(
                "ci_runs", _CI_SQL, {**common, "pr_numbers": pr_numbers}, scope
            )
            source_refs.append(ci_ref)
            if warning:
                warnings.append(warning)
            ci_rows = [
                row
                for row in ci_rows
                if (str(row.get("repository_id") or ""), int(row.get("pr_number") or 0))
                in pr_pairs
            ]
            ci_acceptance_rows, ci_acceptance_ref, warning = await self._read(
                "ci_acceptance_checks",
                _CI_ACCEPTANCE_SQL,
                {**common, "pr_numbers": pr_numbers},
                scope,
            )
            source_refs.append(ci_acceptance_ref)
            if warning:
                warnings.append(warning)
            ci_acceptance_rows = [
                row
                for row in ci_acceptance_rows
                if (str(row.get("repository_id") or ""), int(row.get("pr_number") or 0))
                in pr_pairs
            ]
            ci_rows = self._latest_ci_run_rows(ci_rows)
            latest_pipeline_runs = {
                self._ci_run_scope(row): str(row.get("run_id") or "") for row in ci_rows
            }
            ci_acceptance_rows = [
                row
                for row in self._latest_ci_run_rows(ci_acceptance_rows)
                if latest_pipeline_runs.get(
                    self._ci_run_scope(row), str(row.get("run_id") or "")
                )
                == str(row.get("run_id") or "")
            ]

        deployment_rows, deployment_ref, warning = await self._read(
            "deployments",
            _DEPLOYMENTS_SQL,
            {**common, "pr_numbers": pr_numbers},
            scope,
        )
        source_refs.append(deployment_ref)
        if warning:
            warnings.append(warning)
        deployment_ids = [str(row.get("entity_id") or "") for row in deployment_rows]

        incident_rows: list[dict[str, Any]] = []
        if deployment_ids:
            incident_rows, incident_ref, warning = await self._read(
                "incidents",
                _INCIDENTS_SQL,
                {**common, "org_id": org_id, "deployment_ids": deployment_ids},
                scope,
            )
            source_refs.append(incident_ref)
            if warning:
                warnings.append(warning)

        declared, children = self._work_item_facts(work_item_rows, scope, source_refs)
        pull_requests = tuple(
            PullRequestFact(
                entity_id=str(row.get("entity_id") or ""),
                display_label=str(row.get("display_label") or "Pull request"),
                state=str(row.get("state") or "unknown"),
                review_state=str(row["review_state"])
                if row.get("review_state")
                else None,
                changes_requested=int(row.get("changes_requested") or 0),
                merged=bool(row.get("merged")),
                observed_at=self._datetime(row.get("observed_at"), as_of),
                source_ref_id=pr_ref.ref_id,
                evidence_ref_ids=(),
                required=scope.direct_scope
                in {
                    DirectScope.ISSUE,
                    DirectScope.PROJECT,
                    DirectScope.WORK_UNIT,
                    DirectScope.PULL_REQUEST,
                },
            )
            for row in pr_rows
        )
        if scope.direct_scope is DirectScope.PULL_REQUEST and pull_requests:
            pr = pull_requests[0]
            declared = StatusFact(
                entity_type="pull_request",
                entity_id=pr.entity_id,
                display_label=pr.display_label,
                status=pr.state,
                observed_at=pr.observed_at,
                source_ref_id=pr.source_ref_id,
                evidence_ref_ids=pr.evidence_ref_ids,
            )

        gap_refs: list[SourceReference] = []
        acceptance_run_ids = {
            (str(row.get("repository_id") or ""), str(row.get("run_id") or ""))
            for row in ci_acceptance_rows
        }
        missing_classification_rows = [
            row
            for row in ci_rows
            if (str(row.get("repository_id") or ""), str(row.get("run_id") or ""))
            not in acceptance_run_ids
        ]
        if missing_classification_rows:
            warnings.append(
                "CI requirement classification is missing for one or more runs; green CI cannot prove required work ran."
            )
        ci_facts = [
            CIFact(
                entity_id=str(row.get("entity_id") or ""),
                display_label=str(row.get("display_label") or "CI check"),
                conclusion=str(row.get("conclusion") or "unknown"),
                required=(
                    True
                    if row.get("requirement") == "required"
                    else False
                    if row.get("requirement") == "optional"
                    else None
                ),
                skipped_required_work=(
                    str(row.get("conclusion") or "").casefold() == "skipped"
                    if row.get("requirement") == "required"
                    else None
                ),
                observed_at=self._datetime(row.get("observed_at"), as_of),
                source_ref_id=ci_acceptance_ref.ref_id
                if ci_acceptance_ref is not None
                else ci_ref.ref_id,
                evidence_ref_ids=(),
            )
            for row in ci_acceptance_rows
        ]
        ci_facts.extend(
            CIFact(
                entity_id=str(row.get("entity_id") or ""),
                display_label=str(row.get("display_label") or "CI run"),
                conclusion=str(row.get("conclusion") or "unknown"),
                required=None,
                skipped_required_work=None,
                observed_at=self._datetime(row.get("observed_at"), as_of),
                source_ref_id=ci_ref.ref_id,
                evidence_ref_ids=(),
            )
            for row in missing_classification_rows
        )

        return RawStatusSnapshot(
            declared=declared,
            children=children,
            blockers=tuple(
                StatusFact(
                    entity_type="issue",
                    entity_id=str(row.get("entity_id") or ""),
                    display_label=str(row.get("display_label") or "Work item"),
                    status=str(row.get("status") or "unknown"),
                    observed_at=self._datetime(row.get("observed_at"), as_of),
                    source_ref_id=blocker_ref.ref_id
                    if blocker_ref is not None
                    else "source:work-graph-unavailable",
                    evidence_ref_ids=(),
                    required=True,
                )
                for row in blocker_rows
            ),
            pull_requests=pull_requests,
            ci=tuple(ci_facts),
            deployments=tuple(
                DeploymentFact(
                    entity_id=str(row.get("entity_id") or ""),
                    display_label=str(row.get("display_label") or "Deployment"),
                    status=str(row.get("status") or "unknown"),
                    environment=str(row["environment"])
                    if row.get("environment")
                    else None,
                    required=True,
                    observed_at=self._datetime(row.get("observed_at"), as_of),
                    source_ref_id=deployment_ref.ref_id,
                    evidence_ref_ids=(),
                )
                for row in deployment_rows
            ),
            incidents=tuple(
                IncidentFact(
                    entity_id=str(row.get("entity_id") or ""),
                    display_label=str(row.get("display_label") or "Incident"),
                    status=str(row.get("status") or "unknown"),
                    active=bool(row.get("active")),
                    blocking=False,
                    observed_at=self._datetime(row.get("observed_at"), as_of),
                    source_ref_id=next(
                        ref.ref_id
                        for ref in source_refs
                        if ref.source_system == "incidents"
                    ),
                    evidence_ref_ids=(),
                )
                for row in incident_rows
            ),
            source_refs=tuple(
                source_refs
                + ([blocker_ref] if blocker_ref is not None else [])
                + gap_refs
            ),
            warnings=tuple(warnings),
        )

    async def change_summary(
        self,
        *,
        org_id: str,
        scope: DevScope,
        current: ChangeWindow,
        comparison: ChangeWindow,
        limit: int,
    ) -> RawChangeSummary:
        del comparison
        repositories = await self._authorized_repository_ids(org_id, scope)
        if not repositories:
            return RawChangeSummary(
                changes=(),
                source_refs=(self._unavailable_ref("authorized_repositories", scope),),
                warnings=("Observed-change scope was not widened.",),
            )
        entity_id = self._entity_id(scope)
        params = {
            "org_id": org_id,
            "repository_ids": repositories,
            "scope_type": scope.direct_scope.value,
            "entity_id": entity_id,
            "pr_number": self._pr_number(entity_id),
            "start": current.start.astimezone(UTC),
            "end": current.end.astimezone(UTC),
            "limit": min(limit, 100),
        }
        transitions, transition_ref, transition_warning = await self._read(
            "work_items", _TRANSITIONS_SQL, params, scope
        )
        relationships, relationship_ref, relationship_warning = await self._read(
            "work_graph", _RELATIONSHIPS_SQL, params, scope
        )
        delivery_specs = (
            (
                "pull_requests",
                _PULL_REQUEST_CHANGES_SQL,
                ChangeCategory.PULL_REQUEST,
                "pull_request",
            ),
            ("reviews", _REVIEW_CHANGES_SQL, ChangeCategory.REVIEW, "review"),
            ("ci_runs", _CI_CHANGES_SQL, ChangeCategory.CI, "ci_run"),
            (
                "deployments",
                _DEPLOYMENT_CHANGES_SQL,
                ChangeCategory.DEPLOYMENT,
                "deployment",
            ),
            (
                "incidents",
                _INCIDENT_CHANGES_SQL,
                ChangeCategory.INCIDENT,
                "incident",
            ),
        )
        delivery_results = await asyncio.gather(
            *(
                self._read(source_system, sql, params, scope)
                for source_system, sql, _, _ in delivery_specs
            )
        )
        changes = [
            ObservedChange(
                change_id=self._change_id("status", row),
                category=ChangeCategory.STATUS,
                entity_type="issue",
                entity_id=str(row.get("entity_id") or ""),
                display_label=str(row.get("display_label") or "Work item"),
                before=str(row.get("from_status") or "unknown"),
                after=str(row.get("to_status") or "unknown"),
                observed_at=self._datetime(row.get("observed_at"), current.end),
                claim_kind=ClaimKind.OBSERVED,
                relationship_chain=(),
                metric_id=None,
                metric_value=None,
                metric_comparison_value=None,
                source_ref_ids=(transition_ref.ref_id,),
                evidence_ref_ids=(),
            )
            for row in transitions
        ]
        changes.extend(
            ObservedChange(
                change_id=str(row.get("change_id") or self._change_id("edge", row)),
                category=self._relationship_change_category(
                    str(row.get("edge_type") or "")
                ),
                entity_type=str(row.get("source_type") or "entity"),
                entity_id=str(row.get("source_id") or ""),
                display_label=(
                    f"{row.get('source_type')} {row.get('source_id')} "
                    f"{row.get('edge_type')} {row.get('target_type')} {row.get('target_id')}"
                ),
                before=None,
                after="present",
                observed_at=self._datetime(row.get("observed_at"), current.end),
                claim_kind=ClaimKind.OBSERVED,
                relationship_chain=(
                    str(row.get("source_id") or ""),
                    str(row.get("edge_type") or "related_to"),
                    str(row.get("target_id") or ""),
                ),
                metric_id=None,
                metric_value=None,
                metric_comparison_value=None,
                source_ref_ids=(relationship_ref.ref_id,),
                evidence_ref_ids=(),
            )
            for row in relationships
        )
        source_refs = [transition_ref, relationship_ref]
        warnings = [
            warning
            for warning in (transition_warning, relationship_warning)
            if warning is not None
        ]
        for (
            (_, _, category, entity_type),
            (rows, source_ref, warning),
        ) in zip(delivery_specs, delivery_results, strict=True):
            source_refs.append(source_ref)
            if warning is not None:
                warnings.append(warning)
            changes.extend(
                self._delivery_changes(
                    rows,
                    category=category,
                    entity_type=entity_type,
                    source_ref_id=source_ref.ref_id,
                    fallback=current.end,
                )
            )
        changes.sort(key=self._observed_change_key)
        return RawChangeSummary(
            changes=tuple(changes[:limit]),
            source_refs=tuple(source_refs),
            warnings=tuple(warnings),
        )

    async def _read(
        self,
        source: str,
        sql: str,
        params: dict[str, Any],
        scope: DevScope,
    ) -> tuple[list[dict[str, Any]], SourceReference, str | None]:
        try:
            async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                rows = await query_dicts(self._client, sql, params)
        except Exception:
            return (
                [],
                self._unavailable_ref(source, scope),
                f"{source} source unavailable",
            )
        watermarks = [
            self._datetime(row.get("last_synced"), None)
            for row in rows
            if row.get("last_synced") is not None
        ]
        watermark = max(watermarks, default=None)
        ref = self._source_ref(source, scope, watermark)
        return rows, ref, None

    def _source_ref(
        self, source: str, scope: DevScope, watermark: datetime | None
    ) -> SourceReference:
        policy = self._policies.get(source)
        freshness = (
            policy.classify(watermark, now=self._now or datetime.now(UTC))
            if policy
            else FreshnessState.UNKNOWN
        )
        return SourceReference(
            ref_id=self._ref_id(source, scope, watermark),
            source_system=source,
            source_version=(
                f"{NATIVE_STATUS_SOURCE_VERSION}:"
                f"{policy.policy_version if policy else NATIVE_STATUS_QUERY_VERSION}"
            ),
            freshness=freshness,
            watermark=watermark,
            evidence_ref_ids=(),
        )

    def _unavailable_ref(self, source: str, scope: DevScope) -> SourceReference:
        return SourceReference(
            ref_id=self._ref_id(source, scope, None),
            source_system=source,
            source_version=NATIVE_STATUS_SOURCE_VERSION,
            freshness=FreshnessState.UNAVAILABLE,
            watermark=None,
            evidence_ref_ids=(),
        )

    @staticmethod
    def _work_item_facts(
        rows: list[dict[str, Any]],
        scope: DevScope,
        source_refs: list[SourceReference],
    ) -> tuple[StatusFact | None, tuple[StatusFact, ...]]:
        ref_id = next(
            (ref.ref_id for ref in source_refs if ref.source_system == "work_items"),
            "source:work-items-unavailable",
        )
        entity_id = ClickHouseStatusChangeSource._entity_id(scope)
        facts = tuple(
            StatusFact(
                entity_type="issue",
                entity_id=str(row.get("work_item_id") or ""),
                display_label=str(row.get("title") or "Work item"),
                status=str(row.get("status") or "unknown"),
                observed_at=ClickHouseStatusChangeSource._datetime(
                    row.get("updated_at"), scope.time_range.end
                ),
                source_ref_id=ref_id,
                evidence_ref_ids=(),
                # The provider-neutral v2 completion rule treats canonical direct
                # parent-child and project-work-item membership as required work.
                # Provider-specific labels, statuses, and issue-key prefixes never
                # change this policy.
                required=(
                    scope.direct_scope is DirectScope.PROJECT
                    or scope.direct_scope is DirectScope.WORK_UNIT
                    or str(row.get("parent_id") or "") == entity_id
                ),
            )
            for row in rows
        )
        declared = next((fact for fact in facts if fact.entity_id == entity_id), None)
        children = tuple(fact for fact in facts if fact is not declared)
        return declared, children

    @classmethod
    def _latest_ci_run_rows(cls, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Keep every row from the latest run for each repository and PR."""

        minimum = datetime.min.replace(tzinfo=UTC)
        latest: dict[tuple[str, int], tuple[datetime, datetime, str]] = {}
        for row in rows:
            order = (
                cls._datetime(row.get("observed_at"), minimum),
                cls._datetime(row.get("last_synced"), minimum),
                str(row.get("run_id") or ""),
            )
            scope = cls._ci_run_scope(row)
            if order > latest.get(scope, (minimum, minimum, "")):
                latest[scope] = order
        return [
            row
            for row in rows
            if str(row.get("run_id") or "") == latest[cls._ci_run_scope(row)][2]
        ]

    @staticmethod
    def _ci_run_scope(row: Mapping[str, Any]) -> tuple[str, int]:
        return (
            str(row.get("repository_id") or ""),
            int(row.get("pr_number") or 0),
        )

    @classmethod
    def _delivery_changes(
        cls,
        rows: list[dict[str, Any]],
        *,
        category: ChangeCategory,
        entity_type: str,
        source_ref_id: str,
        fallback: datetime,
    ) -> tuple[ObservedChange, ...]:
        return tuple(
            ObservedChange(
                change_id=str(
                    row.get("change_id")
                    or cls._change_id(
                        category.value,
                        {
                            "entity_id": row.get("entity_id"),
                            "after_value": row.get("after_value"),
                            "observed_at": row.get("observed_at"),
                        },
                    )
                ),
                category=category,
                entity_type=entity_type,
                entity_id=str(row.get("entity_id") or ""),
                display_label=str(row.get("display_label") or entity_type),
                before=(
                    str(row["before_value"])
                    if row.get("before_value") is not None
                    else None
                ),
                after=(
                    str(row["after_value"])
                    if row.get("after_value") is not None
                    else None
                ),
                observed_at=cls._datetime(row.get("observed_at"), fallback),
                claim_kind=(
                    ClaimKind.INFERRED
                    if str(row.get("relationship_source") or "").casefold()
                    == "heuristic"
                    else ClaimKind.OBSERVED
                ),
                relationship_chain=cls._delivery_relationship_chain(row, category),
                metric_id=None,
                metric_value=None,
                metric_comparison_value=None,
                source_ref_ids=(source_ref_id,),
                evidence_ref_ids=(),
                confidence=(
                    float(row["relationship_confidence"])
                    if row.get("relationship_confidence") is not None
                    else None
                ),
            )
            for row in rows
        )

    @staticmethod
    def _relationship_change_category(edge_type: str) -> ChangeCategory:
        normalized = edge_type.casefold()
        if normalized == "blocks":
            return ChangeCategory.BLOCKER
        if normalized in {"depends_on", "dependency", "is_blocked_by"}:
            return ChangeCategory.DEPENDENCY
        return ChangeCategory.RELATIONSHIP

    @staticmethod
    def _delivery_relationship_chain(
        row: Mapping[str, Any], category: ChangeCategory
    ) -> tuple[str, ...]:
        deployment_id = str(row.get("deployment_id") or "")
        incident_id = str(row.get("entity_id") or "")
        if category is ChangeCategory.INCIDENT and deployment_id and incident_id:
            return (deployment_id, "associated_with", incident_id)
        return ()

    @staticmethod
    def _observed_change_key(change: ObservedChange) -> tuple:
        return (
            change.observed_at,
            change.category.value,
            change.display_label.casefold(),
            change.change_id,
        )

    @staticmethod
    def _repository_ids(scope: DevScope) -> list[str]:
        return sorted(
            set(scope.repositories)
            | {
                ref.repository_id
                for ref in scope.entity_refs
                if ref.repository_id is not None
            }
        )

    @staticmethod
    def _entity_id(scope: DevScope) -> str:
        return scope.entity_refs[0].entity_id if scope.entity_refs else ""

    @staticmethod
    def _pr_number(entity_id: str) -> int:
        marker = "#pr"
        if marker not in entity_id:
            return 0
        try:
            return int(entity_id.rsplit(marker, 1)[1])
        except ValueError:
            return 0

    @staticmethod
    def _datetime(value: object, fallback: datetime | None) -> datetime:
        if isinstance(value, datetime):
            return (
                value.replace(tzinfo=UTC)
                if value.tzinfo is None
                else value.astimezone(UTC)
            )
        if value:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return (
                parsed.replace(tzinfo=UTC)
                if parsed.tzinfo is None
                else parsed.astimezone(UTC)
            )
        return fallback or datetime.now(UTC)

    @staticmethod
    def _ref_id(source: str, scope: DevScope, watermark: datetime | None) -> str:
        digest = hashlib.sha256(
            json.dumps(
                {
                    "source": source,
                    "scope": scope.model_dump(mode="json"),
                    "watermark": watermark.isoformat() if watermark else None,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()[:32]
        return f"status-source:{digest}"

    @staticmethod
    def _change_id(prefix: str, row: Mapping[str, object]) -> str:
        digest = hashlib.sha256(
            json.dumps(
                dict(row), sort_keys=True, default=str, separators=(",", ":")
            ).encode()
        ).hexdigest()[:32]
        return f"{prefix}:{digest}"
