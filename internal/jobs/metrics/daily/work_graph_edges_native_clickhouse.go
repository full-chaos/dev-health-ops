package daily

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workgraphedges"
)

// Loaders and writers for the work_graph_edges native family (CHAOS-4286),
// porting the reads in job_daily.py:258 _extract_ai_workflow_for_day that feed
// extract_review_deployment_incident_edges.
//
// # DEDUP: argMax OVER A TUPLE, GROUPED BY THE CURRENT SORTING KEY
//
// Same contract as every other native family here, and the same two traps.
//
// The GROUP BY must be the sorting key as migration 027 rekeyed it, NOT the
// key written in the original CREATE TABLE. 014_work_graph.sql still reads
// `ORDER BY (repo_id, pr_number, commit_hash)`; 027 prepended org_id to that
// and to git_pull_request_reviews and deployments. Reading the CREATE TABLE
// and citing it as current is exactly how #2229's round-1 P1 happened.
//
// The projection must be ONE argMax over a tuple, never one argMax per
// column: two aggregates over the same GROUP BY resolve ties independently and
// can splice values from different rows into one result (CHAOS-2787). The
// tuple also preserves NULLs -- argMax(x, v) SKIPS rows where x is NULL, so an
// older non-NULL value would outlive a genuinely NULL latest one, while
// tuple(x) is never itself NULL. Both reasons are live here: reviews.state is
// Nullable(String) and deployments.pull_request_number is Nullable(UInt32).
// Dropping the tuple because only one of the two reasons applies is what
// #2229's round-2 P1 was.
//
// # PYTHON DOES NOT DEDUP THESE (except deployments)
//
// job_daily.py reads git_pull_request_reviews with no FINAL and no argMax, so
// Python's own edge output is merge-timing dependent (CHAOS-5086). It DOES
// dedup deployments, with `FROM deployments FINAL`. Deduplicating all of them
// is a deliberate, documented divergence -- Go deterministic where Python is
// not -- matching the call already approved on #2229.

// LoadWorkGraphEdgeReviews ports the wf_review_rows query (job_daily.py:368).
//
// The window is HALF-OPEN (`>= start AND < end`), unlike ai_governance's
// inclusive time.max bound. Do not copy PR1's bound handling here.
func LoadWorkGraphEdgeReviews(
	ctx context.Context, conn repositoryRows, organizationID string,
	repoIDs []uuid.UUID, dayStart, dayEnd time.Time,
) ([]workgraphedges.ReviewRow, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || !dayStart.Before(dayEnd) {
		return nil, ErrInvalidState
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}

	rows, err := conn.Query(ctx, `
SELECT
    repo_id,
    number,
    review_id,
    tupleElement(latest, 1) AS state,
    tupleElement(latest, 2) AS submitted_at,
    tupleElement(latest, 3) AS last_synced
FROM (
    SELECT
        repo_id,
        number,
        review_id,
        argMax(tuple(state, submitted_at, last_synced), last_synced) AS latest
    FROM git_pull_request_reviews
    WHERE org_id = ? AND repo_id IN ?
    GROUP BY org_id, repo_id, number, review_id
)
WHERE submitted_at >= ? AND submitted_at < ?
ORDER BY repo_id, number, review_id`,
		organizationID, repositoryUUIDStrings(repoIDs), dayStart.UTC(), dayEnd.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("load work_graph_edges reviews: %w", err)
	}
	defer rows.Close()

	var reviews []workgraphedges.ReviewRow
	for rows.Next() {
		var (
			repoID      uuid.UUID
			number      uint32
			reviewID    string
			state       *string
			submittedAt *time.Time
			lastSynced  *time.Time
		)
		if err := rows.Scan(&repoID, &number, &reviewID, &state, &submittedAt, &lastSynced); err != nil {
			return nil, fmt.Errorf("scan work_graph_edges review row: %w", err)
		}
		reviews = append(reviews, workgraphedges.ReviewRow{
			RepoID:      repoID,
			Number:      number,
			ReviewID:    reviewID,
			State:       state,
			SubmittedAt: submittedAt,
			LastSynced:  lastSynced,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_graph_edges review rows: %w", err)
	}
	return reviews, nil
}

// LoadWorkGraphEdgeDeployments ports the wf_deployment_rows query
// (job_daily.py:408), replacing its `FROM deployments FINAL` with the argMax
// dedup this codebase uses everywhere else.
//
// The window predicate is on coalesce(deployed_at, finished_at, started_at,
// last_synced) -- the SAME coalesce chain the extractor later uses to pick
// observed_at. It is applied AFTER the dedup, because Python applies it to the
// FINAL-collapsed row, not to pre-merge versions.
func LoadWorkGraphEdgeDeployments(
	ctx context.Context, conn repositoryRows, organizationID string,
	repoIDs []uuid.UUID, dayStart, dayEnd time.Time,
) ([]workgraphedges.DeploymentRow, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || !dayStart.Before(dayEnd) {
		return nil, ErrInvalidState
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}

	rows, err := conn.Query(ctx, `
SELECT
    repo_id,
    deployment_id,
    tupleElement(latest, 1) AS pull_request_number,
    tupleElement(latest, 2) AS started_at,
    tupleElement(latest, 3) AS finished_at,
    tupleElement(latest, 4) AS deployed_at,
    tupleElement(latest, 5) AS last_synced
FROM (
    SELECT
        repo_id,
        deployment_id,
        argMax(
            tuple(pull_request_number, started_at, finished_at, deployed_at, last_synced),
            last_synced
        ) AS latest
    FROM deployments
    WHERE org_id = ? AND repo_id IN ?
    GROUP BY org_id, repo_id, deployment_id
)
WHERE coalesce(deployed_at, finished_at, started_at, last_synced) >= ?
  AND coalesce(deployed_at, finished_at, started_at, last_synced) < ?
ORDER BY repo_id, deployment_id`,
		organizationID, repositoryUUIDStrings(repoIDs), dayStart.UTC(), dayEnd.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("load work_graph_edges deployments: %w", err)
	}
	defer rows.Close()

	var deployments []workgraphedges.DeploymentRow
	for rows.Next() {
		var (
			repoID       uuid.UUID
			deploymentID string
			prNumber     *uint32
			startedAt    *time.Time
			finishedAt   *time.Time
			deployedAt   *time.Time
			lastSynced   *time.Time
		)
		if err := rows.Scan(
			&repoID, &deploymentID, &prNumber,
			&startedAt, &finishedAt, &deployedAt, &lastSynced,
		); err != nil {
			return nil, fmt.Errorf("scan work_graph_edges deployment row: %w", err)
		}
		deployments = append(deployments, workgraphedges.DeploymentRow{
			RepoID:            repoID,
			DeploymentID:      deploymentID,
			PullRequestNumber: prNumber,
			StartedAt:         startedAt,
			FinishedAt:        finishedAt,
			DeployedAt:        deployedAt,
			LastSynced:        lastSynced,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_graph_edges deployment rows: %w", err)
	}
	return deployments, nil
}

// workGraphEdgeIncidents adapts the ALREADY-PORTED LoadIncidentsStarted to
// this family's row shape, rather than restating that join a second time.
//
// Python's ai_workflow path issues active_incidents_query(window=STARTED) --
// the exact query LoadIncidentsStarted ports, including the
// `LIMIT 1 BY mapping.repo_id, incident.id` dedup and the Python-side
// deduplicate_active_incidents pass. Reproducing it here would give this
// family a second copy that could drift from the incident family's.
//
// # DeploymentID IS ALWAYS EMPTY, AND THAT IS FAITHFUL -- CHAOS-5110
//
// active_incidents_query selects repo_id, incident_id, status, started_at,
// resolved_at and last_synced. It does NOT select deployment_id, and nothing
// between it and the extractor adds one. So Python's
// `_str(row, "deployment_id")` is always "", every incident takes the
// HEURISTIC branch, and every work_graph_deployment_incident_edges row is
// source="heuristic", confidence=0.3. The native 1.0 edge cannot occur from
// the daily job.
//
// This is a pre-existing Python defect (CHAOS-5110), reproduced rather than
// fixed. The kernel keeps both branches so the port stays correct if the
// loader is ever taught to select deployment_id -- but note that `source` is
// IN the sorting key, so such a fix would ADD a native row beside the
// heuristic one rather than replacing it.
//
// StartedAt is non-nil by construction: LoadIncidentsStarted's own WHERE
// requires started_at within [dayStart, dayEnd), so the _dt() fallback chain
// never reaches last_synced and the row type's LastSynced stays nil.
func workGraphEdgeIncidents(started []IncidentRow) []workgraphedges.IncidentRow {
	incidents := make([]workgraphedges.IncidentRow, 0, len(started))
	for _, row := range started {
		startedAt := row.StartedAt
		incidents = append(incidents, workgraphedges.IncidentRow{
			RepoID:       row.RepoID,
			IncidentID:   row.IncidentID,
			DeploymentID: "",
			StartedAt:    &startedAt,
			LastSynced:   nil,
		})
	}
	return incidents
}

// LoadWorkGraphEdgeRepoProviders ports repo_provider_by_id (job_daily.py:1200)
// together with the `source` rule inside discover_repos (:194).
//
// # THIS IS NOT repos.provider
//
//	source = r[3] if len(r) > 3 and r[3] != "unknown" else provider
//	repo_provider_by_id = {str(r.repo_id): (r.source or "unknown")}
//
// so the resolution order is:
//
//  1. repos.provider, when it is non-empty AND not the literal "unknown";
//  2. otherwise the JOB's provider argument -- commonly the literal "auto";
//  3. otherwise "unknown".
//
// A repo whose provider column literally reads "unknown" therefore does NOT
// get "unknown"; it gets the job provider. "auto" is a real and expected value
// in all three edge tables' provider columns.
//
// Getting this wrong is invisible to every structural check: `provider` is in
// none of the three sorting keys, so a wrong value splits no rows, changes no
// counts, and trips no dedup assertion. Only the live-Python oracle sees it.
func LoadWorkGraphEdgeRepoProviders(
	ctx context.Context, conn repositoryRows, organizationID, jobProvider string,
) (map[string]string, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	rows, err := conn.Query(ctx, `
SELECT id, tupleElement(latest, 3) AS provider
FROM (
    SELECT id, argMax(tuple(repo, settings, provider), last_synced) AS latest
    FROM repos
    WHERE org_id = ?
    GROUP BY org_id, id
)
ORDER BY id`, organizationID)
	if err != nil {
		return nil, fmt.Errorf("load work_graph_edges repo providers: %w", err)
	}
	defer rows.Close()

	providers := make(map[string]string)
	for rows.Next() {
		var (
			repoID   uuid.UUID
			provider string
		)
		if err := rows.Scan(&repoID, &provider); err != nil {
			return nil, fmt.Errorf("scan work_graph_edges repo provider row: %w", err)
		}
		providers[repoID.String()] = resolveRepoProvider(provider, jobProvider)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_graph_edges repo provider rows: %w", err)
	}
	return providers, nil
}

// resolveRepoProvider is the three-step rule from this file's
// LoadWorkGraphEdgeRepoProviders comment, kept separate so it can be tested
// without a ClickHouse connection.
func resolveRepoProvider(repoProvider, jobProvider string) string {
	resolved := repoProvider
	if resolved == "" || resolved == "unknown" {
		resolved = jobProvider
	}
	if resolved == "" {
		return "unknown"
	}
	return resolved
}

// workGraphEdgesProviderFor mirrors _by_provider's lookup (job_daily.py:444):
// a repo absent from the map falls back to the literal "unknown", NOT to the
// job provider -- the job-provider fallback happens inside the map's
// construction, and a repo that discover_repos never returned was never
// subject to it.
func workGraphEdgesProviderFor(providers map[string]string, repoID uuid.UUID) string {
	if provider, ok := providers[repoID.String()]; ok && provider != "" {
		return provider
	}
	return "unknown"
}

type workGraphEdgesBatchConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// WriteWorkGraphPRReviewOutcomeEdges ports
// metrics/sinks/clickhouse/ai_workflow.py write_work_graph_pr_review_outcome_edges.
// Column order is PR_REVIEW_OUTCOME_EDGE_COLUMNS (:83) exactly.
func WriteWorkGraphPRReviewOutcomeEdges(
	ctx context.Context, conn workGraphEdgesBatchConn,
	rows []workgraphedges.PRReviewOutcomeEdge, computedAt time.Time,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_graph_pr_review_outcome_edges (
		edge_id, org_id, pr_id, review_outcome_id, outcome, provider, repo_id,
		confidence, source, evidence, observed_at, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_graph_pr_review_outcome_edges batch: %w", err)
	}
	computedAtUTC := computedAt.UTC()
	for _, row := range rows {
		if err := batch.Append(
			row.EdgeID, row.OrgID, row.PRID, row.ReviewOutcomeID, row.Outcome,
			row.Provider, row.RepoID, float32(row.Confidence), row.Source,
			row.Evidence, row.ObservedAt.UTC(), computedAtUTC,
		); err != nil {
			return 0, fmt.Errorf("append work_graph_pr_review_outcome_edges row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send work_graph_pr_review_outcome_edges batch: %w", err)
	}
	return len(rows), nil
}

// WriteWorkGraphPRDeploymentEdges ports write_work_graph_pr_deployment_edges;
// column order is PR_DEPLOYMENT_EDGE_COLUMNS (:98).
func WriteWorkGraphPRDeploymentEdges(
	ctx context.Context, conn workGraphEdgesBatchConn,
	rows []workgraphedges.PRDeploymentEdge, computedAt time.Time,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_graph_pr_deployment_edges (
		edge_id, org_id, pr_id, deployment_id, provider, repo_id,
		confidence, source, evidence, observed_at, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_graph_pr_deployment_edges batch: %w", err)
	}
	computedAtUTC := computedAt.UTC()
	for _, row := range rows {
		if err := batch.Append(
			row.EdgeID, row.OrgID, row.PRID, row.DeploymentID, row.Provider,
			row.RepoID, float32(row.Confidence), row.Source, row.Evidence,
			row.ObservedAt.UTC(), computedAtUTC,
		); err != nil {
			return 0, fmt.Errorf("append work_graph_pr_deployment_edges row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send work_graph_pr_deployment_edges batch: %w", err)
	}
	return len(rows), nil
}

// WriteWorkGraphDeploymentIncidentEdges ports
// write_work_graph_deployment_incident_edges; column order is
// DEPLOYMENT_INCIDENT_EDGE_COLUMNS (:112).
func WriteWorkGraphDeploymentIncidentEdges(
	ctx context.Context, conn workGraphEdgesBatchConn,
	rows []workgraphedges.DeploymentIncidentEdge, computedAt time.Time,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_graph_deployment_incident_edges (
		edge_id, org_id, deployment_id, incident_id, provider, repo_id,
		confidence, source, evidence, observed_at, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_graph_deployment_incident_edges batch: %w", err)
	}
	computedAtUTC := computedAt.UTC()
	for _, row := range rows {
		if err := batch.Append(
			row.EdgeID, row.OrgID, row.DeploymentID, row.IncidentID, row.Provider,
			row.RepoID, float32(row.Confidence), row.Source, row.Evidence,
			row.ObservedAt.UTC(), computedAtUTC,
		); err != nil {
			return 0, fmt.Errorf("append work_graph_deployment_incident_edges row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send work_graph_deployment_incident_edges batch: %w", err)
	}
	return len(rows), nil
}
