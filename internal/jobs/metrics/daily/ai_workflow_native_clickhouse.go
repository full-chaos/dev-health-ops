package daily

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aiworkflow"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Loaders and writers for the ai_workflow native family (CHAOS-4280 part B /
// CHAOS-4286 part B), porting job_daily.py:301-367's PR-load + linkage-load
// half of _extract_ai_workflow_for_day -- the OTHER half
// (review/deployment/incident load) is work_graph_edges' (#2263/#2273), a
// separate family with no overlapping inputs.
//
// # THE PR PROJECTION IS DELIBERATELY IMPOVERISHED -- DO NOT WIDEN IT
//
// codex round chaos-4280 astra review, finding 2: production's PR SELECT
// (job_daily.py:301-314) has no labels/author_login/author_user_type/
// author_app_slug. Confirmed by reading _signals_from_pr
// (work_graph/extractors/ai_workflow.py:86-114): `row.get("labels")` is
// always `[]` in production, so label detection (the HIGHEST-confidence
// signal, 0.95) never fires; `author_user_type` is always "", so the
// unknown-bot 0.55-confidence branch of detect_from_author never fires
// either. Only exact-known-bot-login matches (0.90) and the weak
// branch/body signals (0.35/0.25) are reachable in production today.
//
// This loader MUST replicate that same impoverished projection byte-for-byte
// to match the live-Python oracle -- adding the missing columns would
// silently widen detection versus Python and break parity. The gap is filed
// as CHAOS-5217 (scribe), out of scope for this port.
// FINAL, not raw: job_daily.py's own wf_pr_rows query has no FINAL, but this
// is the SAME table (git_pull_requests) LoadAIImpactPullRequests already
// applies the FINAL-not-argMax fix to (CHAOS-4280 round 3), for the same
// measured nondeterminism reason (#2229 round 3, CHAOS-5086 citation).
// Consistent choice across every native reader of this table.
func LoadAIWorkflowPullRequests(
	ctx context.Context, conn repositoryRows, organizationID string,
	repoIDs []uuid.UUID, start, end time.Time,
) ([]aiworkflow.PullRequestRow, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || !start.Before(end) {
		return nil, ErrInvalidState
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}

	rows, err := conn.Query(ctx, `
SELECT repo_id, number, title, body, head_branch, author_name, author_email,
       created_at, merged_at, closed_at, last_synced
FROM git_pull_requests FINAL
WHERE org_id = ? AND repo_id IN ?
  AND ((created_at >= ? AND created_at < ?)
    OR (merged_at IS NOT NULL AND merged_at >= ? AND merged_at < ?))
ORDER BY repo_id, number`,
		organizationID, repositoryUUIDStrings(repoIDs),
		start.UTC(), end.UTC(), start.UTC(), end.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("load ai workflow pull requests: %w", err)
	}
	defer rows.Close()

	var result []aiworkflow.PullRequestRow
	for rows.Next() {
		var (
			repoID       uuid.UUID
			number       uint32
			title        *string
			body         *string
			headBranch   *string
			authorName   *string
			authorEmail  *string
			createdAt    time.Time
			mergedAt     *time.Time
			closedAt     *time.Time
			lastSynced   time.Time
		)
		if err := rows.Scan(&repoID, &number, &title, &body, &headBranch,
			&authorName, &authorEmail, &createdAt, &mergedAt, &closedAt, &lastSynced); err != nil {
			return nil, fmt.Errorf("scan ai workflow pull request row: %w", err)
		}
		_ = authorEmail // Python's row shape carries it but _signals_from_pr never reads it.
		row := aiworkflow.PullRequestRow{
			RepoID:     repoID,
			Number:     int64(number),
			CreatedAt:  createdAt.UTC(),
			MergedAt:   utcOrNil(mergedAt),
			ClosedAt:   utcOrNil(closedAt),
			LastSynced: lastSynced.UTC(),
		}
		if title != nil {
			row.Title = *title
		}
		if body != nil {
			row.Body = *body
		}
		if headBranch != nil {
			row.HeadBranch = *headBranch
		}
		if authorName != nil {
			row.AuthorName = *authorName
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai workflow pull request rows: %w", err)
	}
	return result, nil
}

// LoadAIWorkflowIssuePRLinks ports job_daily.py:340-367's work_graph_issue_pr
// linkage load, scoped to the in-window PR numbers this partition already
// loaded (mirrors production's own scoping, not a wider org-wide read).
//
// # FINAL + org_id, per team-lead's 2026-09-04 ruling (ai_governance) and its
// # own precedent template, team_repo_ownership_derivation_clickhouse.go
//
// codex round chaos-4280 astra review, finding 3: job_daily.py's linkage
// query has no FINAL, unlike the sibling `deployments FINAL` query four
// lines above it in the same function. Migration 084 changed this table's
// version column to a provenance-ranked `version_rank`; the sorting key is
// UNCHANGED, `(org_id, repo_id, work_item_id, pr_number)` -- org_id first,
// confirmed preserved by that migration's own check and originally set in
// 027_add_org_id_to_sorting_keys.py. Since work_item_id is part of the
// sorting key, a non-FINAL duplicate is the SAME link re-synced, not a
// different/wrong issue -- so the risk from skipping FINAL is duplicate
// ROWS in the result slice (this family's own callers must tolerate/dedupe
// repeats), not incorrect linkage. Go reads FINAL where Python reads raw,
// same accepted-and-documented tradeoff as ai_governance
// (ai_governance_native_clickhouse.go); Python's own raw-read gap is filed
// as its own ticket (CHAOS-5218, scribe), not fixed here.
func LoadAIWorkflowIssuePRLinks(
	ctx context.Context, conn repositoryRows, organizationID string,
	repoIDs []uuid.UUID, prNumbers []int64,
) ([]aiworkflow.IssuePRLink, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	if len(repoIDs) == 0 || len(prNumbers) == 0 {
		return nil, nil
	}

	numberStrings := make([]string, len(prNumbers))
	for i, n := range prNumbers {
		numberStrings[i] = strconv.FormatInt(n, 10)
	}

	rows, err := conn.Query(ctx, `
SELECT repo_id, pr_number, work_item_id
FROM work_graph_issue_pr FINAL
WHERE org_id = ? AND repo_id IN ? AND pr_number IN ?`,
		organizationID, repositoryUUIDStrings(repoIDs), numberStrings,
	)
	if err != nil {
		return nil, fmt.Errorf("load ai workflow issue-pr links: %w", err)
	}
	defer rows.Close()

	var result []aiworkflow.IssuePRLink
	for rows.Next() {
		var (
			repoID     uuid.UUID
			prNumber   uint32
			workItemID string
		)
		if err := rows.Scan(&repoID, &prNumber, &workItemID); err != nil {
			return nil, fmt.Errorf("scan ai workflow issue-pr link row: %w", err)
		}
		if workItemID == "" {
			continue
		}
		result = append(result, aiworkflow.IssuePRLink{
			RepoID:     repoID,
			PRNumber:   int64(prNumber),
			WorkItemID: workItemID,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai workflow issue-pr link rows: %w", err)
	}
	return result, nil
}

func utcOrNil(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	utc := t.UTC()
	return &utc
}

type aiWorkflowBatchConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// WriteAIWorkflowRuns ports write_ai_workflow_runs (sinks/clickhouse/ai_workflow.py).
// Column order is AI_WORKFLOW_RUN_COLUMNS exactly.
func WriteAIWorkflowRuns(
	ctx context.Context, conn aiWorkflowBatchConn, runs []aiworkflow.Run, computedAt time.Time,
) (int, error) {
	if len(runs) == 0 {
		return 0, nil
	}
	if conn == nil {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO ai_workflow_runs (
		run_id, org_id, provider, run_kind, status, tool, model, actor, repo_id,
		prompts_redacted, prompt_hash, prompt_length, started_at, completed_at,
		observed_at, metadata, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare ai_workflow_runs batch: %w", err)
	}
	computedAtUTC := computedAt.UTC()
	for _, run := range runs {
		var repoID *uuid.UUID
		if run.RepoID != nil {
			id := *run.RepoID
			repoID = &id
		}
		metadataJSONBytes, jsonErr := pythonparity.MarshalPythonJSONCompact(run.Metadata)
		if jsonErr != nil {
			return 0, fmt.Errorf("marshal ai_workflow_runs metadata: %w", jsonErr)
		}
		metadataJSON := string(metadataJSONBytes)
		if err := batch.Append(
			run.RunID, run.OrgID, run.Provider, run.RunKind, run.Status, run.Tool,
			(*string)(nil), run.Actor, repoID, run.PromptsRedacted,
			(*string)(nil), (*uint32)(nil), run.StartedAt, run.CompletedAt,
			run.ObservedAt.UTC(), metadataJSON, computedAtUTC,
		); err != nil {
			return 0, fmt.Errorf("append ai_workflow_runs row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		// Same ambiguous-ack reasoning as work_graph_edges/ai_impact's
		// writers: Send crosses the network, so an error here does not
		// prove the insert never landed. Report len(rows), not 0.
		return len(runs), fmt.Errorf("send ai_workflow_runs batch: %w", err)
	}
	return len(runs), nil
}

// WriteAIWorkflowArtifactEdges ports write_ai_workflow_artifact_edges.
func WriteAIWorkflowArtifactEdges(
	ctx context.Context, conn aiWorkflowBatchConn, edges []aiworkflow.ArtifactEdge, computedAt time.Time,
) (int, error) {
	if len(edges) == 0 {
		return 0, nil
	}
	if conn == nil {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO ai_workflow_artifact_edges (
		edge_id, org_id, run_id, artifact_type, artifact_id, provider, repo_id,
		confidence, source, evidence, observed_at, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare ai_workflow_artifact_edges batch: %w", err)
	}
	computedAtUTC := computedAt.UTC()
	for _, edge := range edges {
		if err := batch.Append(
			edge.EdgeID, edge.OrgID, edge.RunID, edge.ArtifactType, edge.ArtifactID,
			edge.Provider, edge.RepoID, float32(edge.Confidence), edge.Source, edge.Evidence,
			edge.ObservedAt.UTC(), computedAtUTC,
		); err != nil {
			return 0, fmt.Errorf("append ai_workflow_artifact_edges row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return len(edges), fmt.Errorf("send ai_workflow_artifact_edges batch: %w", err)
	}
	return len(edges), nil
}

// WriteAIWorkflowIssueEdges ports write_ai_workflow_issue_edges.
func WriteAIWorkflowIssueEdges(
	ctx context.Context, conn aiWorkflowBatchConn, edges []aiworkflow.IssueEdge, computedAt time.Time,
) (int, error) {
	if len(edges) == 0 {
		return 0, nil
	}
	if conn == nil {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO ai_workflow_issue_edges (
		edge_id, org_id, issue_id, run_id, provider, repo_id, confidence, source,
		evidence, observed_at, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare ai_workflow_issue_edges batch: %w", err)
	}
	computedAtUTC := computedAt.UTC()
	for _, edge := range edges {
		if err := batch.Append(
			edge.EdgeID, edge.OrgID, edge.IssueID, edge.RunID, edge.Provider,
			edge.RepoID, float32(edge.Confidence), edge.Source, edge.Evidence,
			edge.ObservedAt.UTC(), computedAtUTC,
		); err != nil {
			return 0, fmt.Errorf("append ai_workflow_issue_edges row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return len(edges), fmt.Errorf("send ai_workflow_issue_edges batch: %w", err)
	}
	return len(edges), nil
}
