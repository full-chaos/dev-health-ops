package daily

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workgraphedges"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Loaders and writers for the work_graph_edges native family (CHAOS-4286),
// porting the reads in job_daily.py:258 _extract_ai_workflow_for_day that feed
// extract_review_deployment_incident_edges.
//
// # DEDUP: FINAL, NOT argMax
//
// Both reads use `FINAL`. An earlier version used
// `argMax(tuple(...), last_synced) GROUP BY <the 027 sorting key>` and that was
// wrong for a measured reason, not a stylistic one.
//
// On ClickHouse 26.7.6.57, with 400 keys each holding two rows at an IDENTICAL
// last_synced across 40 unmerged parts and merges stopped, the SAME query
// returned 60, 300, 180, 120 and 80 disagreeing keys on five consecutive runs:
// argMax returned a mix of both values while FINAL returned the last-inserted
// value every time. They converge only after a merge. (#2229 codex round 3 P1,
// confirmed by execution.)
//
// So argMax is nondeterministic when the version ties, and `last_synced` ties
// constantly -- a batch sync writes many rows of these tables in the same
// millisecond. Pre-merge is the normal state during an active sync, which is
// when this job runs.
//
// Per site (team-lead ruling, 2026-09-04):
//
//	deployments               Python reads `FROM deployments FINAL`
//	                          (job_daily.py:415) -> FINAL. Mandatory: this is
//	                          the "Python reads FINAL" case exactly.
//	git_pull_request_reviews  Python reads it RAW, with no FINAL and no dedup.
//	                          Ours is an ADDED dedup on a ReplacingMergeTree,
//	                          so FINAL: it is what Python's raw read converges
//	                          to once merges settle, and it is deterministic
//	                          before that. argMax was neither.
//	repos (providers loader)  Python's discover_repos uses argMax, not FINAL
//	                          -> argMax, matching Python.
//
// A deterministic tie-break on the argMax version was tried first and
// rejected: stable, but still disagreeing with Python, because FINAL's tie rule
// is "last inserted wins" and insertion order is not a column.
//
// The tuple-vs-per-column rule (CHAOS-2787) and the NULL-skip rule still apply
// wherever argMax IS used -- see the repos loader below.
//
// One thing the old comment got right and is worth keeping: the GROUP BY of any
// argMax must be the sorting key as migration 027 rekeyed it, never the key in
// the original CREATE TABLE. 014_work_graph.sql still reads
// `ORDER BY (repo_id, pr_number, commit_hash)`. Reading a stale CREATE TABLE
// and citing it as current is how #2229's round-1 P1 happened, and citing a
// stale migration the same way is how its round-3 finding on git_commit_stats
// happened.
//
// Python's own merge-timing dependence on the raw reads stays recorded as a
// pre-existing defect (CHAOS-5086), not silently corrected here.

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
SELECT repo_id, number, review_id, state, submitted_at, last_synced
FROM git_pull_request_reviews FINAL
WHERE org_id = ? AND repo_id IN ?
  AND submitted_at >= ? AND submitted_at < ?
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
		// DecodeClickHouseStringValue on every String that reaches a hash or a
		// Python comparison (chstring.go:46-52). review_id feeds edge_id's
		// sha256; state feeds the evidence JSON, which the oracle compares
		// byte-for-byte. clickhouse-go scans a String without validating it, so
		// invalid UTF-8 arrives here as raw bytes while Python's driver has
		// already hex-encoded it -- same row, two different strings, two
		// different edge_ids.
		if state != nil {
			decoded := pythonparity.DecodeClickHouseStringValue(*state)
			state = &decoded
		}
		reviews = append(reviews, workgraphedges.ReviewRow{
			RepoID:      repoID,
			Number:      number,
			ReviewID:    pythonparity.DecodeClickHouseStringValue(reviewID),
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
// (job_daily.py:408) and KEEPS its `FROM deployments FINAL` verbatim.
//
// An earlier revision of this comment claimed the FINAL was "replaced with the
// argMax dedup this codebase uses everywhere else". That was false in two ways
// at once: the code below never stopped saying FINAL, and argMax is not
// equivalent to it -- argMax is unspecified when several rows share the max
// version, and measurably varies run to run. See the DEDUP note at the top of
// this file. Mirroring Python's FINAL is mandatory here, not stylistic.
//
// The window predicate is on coalesce(deployed_at, finished_at, started_at,
// last_synced) -- the SAME coalesce chain the extractor later uses to pick
// observed_at. FINAL is a table-level modifier, so the predicate sees the
// collapsed row, which is where Python applies it too.
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
SELECT repo_id, deployment_id, pull_request_number,
       started_at, finished_at, deployed_at, last_synced
FROM deployments FINAL
WHERE org_id = ? AND repo_id IN ?
  AND coalesce(deployed_at, finished_at, started_at, last_synced) >= ?
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
			RepoID: repoID,
			// Feeds edge_id's sha256 -- see the review loop's note.
			DeploymentID:      pythonparity.DecodeClickHouseStringValue(deploymentID),
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
			RepoID: row.RepoID,
			// Feeds edge_id's sha256 -- see the review loop's note.
			IncidentID:   pythonparity.DecodeClickHouseStringValue(row.IncidentID),
			DeploymentID: "",
			StartedAt:    &startedAt,
			LastSynced:   nil,
		})
	}
	return incidents
}

// LoadWorkGraphEdgeRepoProviders ports repo_provider_by_id (job_daily.py:1208).
//
// # THE DAILY WORKER NEVER READS repos.provider
//
// An earlier version of this function -- and of this comment -- documented
// the table-reading rule at job_daily.py:197:
//
//	source = r[3] if len(r) > 3 and r[3] != "unknown" else provider
//
// That branch is UNREACHABLE from the daily worker. discover_repos returns
// early when repo_id is set (job_daily.py:129-136):
//
//	if repo_id:
//	    return [DiscoveredRepo(repo_id=repo_id, ..., source=provider, ...)]
//
// and the worker always supplies one (worker_metrics.py:1729, CHAOS-4264 --
// one repo_id per run_daily_metrics_job call), while job_daily.py:1198 passes
// no provider= at all, so the parameter default provider="auto" applies. So on
// the production path repo_provider_by_id is {repo: "auto"} for every repo,
// every run. The repos table is never consulted.
//
// This is NOT a cosmetic column. `provider` is the SPLIT KEY for
// extractPerProvider: Python, seeing one provider, makes ONE pass with a
// single deployments_by_repo index, while reading repos.provider could yield
// {github, gitlab, ...} and split the same repos across several passes. The
// heuristic incident fallback walks that index, so a deployment and an
// incident Python links can land in different passes and fail to link. An
// edge-set difference, not a presentation one.
//
// The live oracle cannot see it either: it drives the kernel directly with a
// providers map, so it never exercises this adapter. Found by codex r1 (P1) on
// #2240 and confirmed by reading the Python, not by the oracle.
//
// The query below is kept for the repo_id-is-None path, which enumerates the
// org's repos -- that path exists in Python and the daily worker does not take
// it. What CHANGED is the VALUE: every discovered repo maps to the job's
// provider argument, exactly as the early return does.
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
		// PARITY: the job provider, NOT the column. `provider` is scanned and
		// deliberately discarded -- keeping the scan documents that the column
		// exists and is not what Python uses here.
		_ = provider
		providers[repoID.String()] = jobProvider
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_graph_edges repo provider rows: %w", err)
	}
	return providers, nil
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
