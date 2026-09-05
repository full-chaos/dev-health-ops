package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aiimpact"
)

// AIImpactExecutor is the NATIVE implementation of the ai_impact
// metrics.daily family (CHAOS-4280). It is a thin ClickHouse adapter over the
// pure internal/jobs/metrics/aiimpact kernel; the compute's fidelity notes
// live on that package and the query notes on
// ai_impact_native_clickhouse.go.
//
// Unlike ai_governance (org-scoped), this family IS repo-scoped: Python's
// compute takes pull_request_rows/review_rows already scoped to the
// partition's repos, and groups its output by (team_id, repo_id, work_type).
// So the usual `len(repoIDs) == 0 -> return 0, nil` guard applies here and
// the org-scope exception does NOT.
type AIImpactExecutor struct {
	conn   driver.Conn
	nowUTC func() time.Time
}

var errAIImpactUnavailable = fmt.Errorf("ai_impact native executor unavailable")

// NewAIImpactExecutor fails closed on a nil connection, matching every other
// native family's construction-time policy.
func NewAIImpactExecutor(conn driver.Conn) (*AIImpactExecutor, error) {
	if conn == nil {
		return nil, errAIImpactUnavailable
	}
	return &AIImpactExecutor{
		conn:   conn,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *AIImpactExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil {
		return 0, errAIImpactUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}
	if len(repoIDs) == 0 {
		return 0, nil
	}

	targetDay := run.TargetDay.UTC()
	dayStart := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.AddDate(0, 0, 1)
	computedAt := executor.nowUTC()

	pullRequests, err := LoadAIImpactPullRequests(ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}
	if len(pullRequests) == 0 {
		// No PRs in the window means no facts, and compute would produce no
		// rows. Returning early also keeps the linkage query from running with
		// an empty pr_numbers list.
		return 0, nil
	}

	reviews, err := LoadAIImpactReviews(ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}
	attributions, err := LoadAIImpactAttributions(ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}

	// REUSES the incident family's loader rather than re-implementing the
	// mapping join -- the brief's "two implementations of one concept that
	// disagree" rule. Python feeds ai_impact from the SAME load_incidents call
	// that feeds compute_incident_metrics_daily, so sharing the reader here is
	// the faithful shape, not a shortcut.
	//
	// INHERITED DIVERGENCE, stated so it is not rediscovered as a parity bug:
	// LoadIncidentsStarted carries CHAOS-4269's port-with-fix guard
	// (`valid_from IS NULL OR valid_from <= as_of`), which Python lacks. For an
	// org with repository-derived incident mappings, Python silently sees ZERO
	// incidents and this executor sees the real ones -- so incidents_count and
	// incident_drag_rate will differ, in the fixed direction. That is the same
	// accepted divergence the incident family already ships.
	incidentRows, err := LoadIncidentsStarted(
		ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd, computedAt, nil,
	)
	if err != nil {
		return 0, err
	}
	incidents := make([]aiimpact.IncidentRow, 0, len(incidentRows))
	for _, row := range incidentRows {
		incidents = append(incidents, aiimpact.IncidentRow{RepoID: row.RepoID, StartedAt: row.StartedAt})
	}

	prNumbers := make([]uint32, 0, len(pullRequests))
	for _, pullRequest := range pullRequests {
		prNumbers = append(prNumbers, uint32(pullRequest.Number))
	}

	// nil vs empty is LOAD-BEARING (CHAOS-2183). Python wraps its linkage build
	// in try/except and leaves pr_commit_stats as None on ANY failure, which
	// makes has_test_change unknown for every PR and test_gap_rate null rather
	// than 100%. A hard error here would instead fail the whole partition, so
	// the failure is swallowed to unavailable exactly as Python does -- the one
	// place in this executor where an error is not propagated.
	//
	// codex round chaos-4280-r1, finding 5: the swallow itself is correct
	// parity (see above), but it used to be INDISTINGUISHABLE from a healthy
	// day with no linkage rows -- the outer handler's outcome=computed label
	// is the same either way, and this executor logs nothing. Python at least
	// logs a warning here (job_daily.py's pr_commit_stats build). Rather than
	// bolt ad-hoc logging onto a package that has none, this increments a
	// dedicated counter (aiimpact.RecordLinkageUnavailable) so the condition
	// is observable in the SAME place every other native-family signal lives,
	// without changing what gets computed or written.
	linkage, linkageErr := LoadAIImpactPRCommitLinkage(
		ctx, executor.conn, run.OrganizationID, repoIDs, prNumbers,
	)
	hasLinkage := linkageErr == nil
	if !hasLinkage {
		linkage = nil
		aiimpact.RecordLinkageUnavailable()
	}

	// codex round chaos-4280-r1, finding 4 (REFUTED as a port defect, team-lead
	// ruling): this loads teams.repo_patterns only, no team_repo_ownership.
	// The architecture doc says native ownership should count, but the actual
	// PRODUCTION call site (job_daily.py:1809-1820) passes
	// `team_resolver=lambda ...: repo_team_resolver.resolve(repo_name)` --
	// patterns only, no ownership map, for ai_impact specifically. Porting
	// ownership here would be a BEHAVIOR CHANGE relative to what Python
	// actually runs today, not a parity fix. Tracked as a Python-side gap,
	// CHAOS-5117; out of scope for this port.
	teams, err := LoadAIImpactTeams(ctx, executor.conn, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	repoNames, err := LoadAIImpactRepoNames(ctx, executor.conn, run.OrganizationID, repoIDs)
	if err != nil {
		return 0, err
	}
	resolver := aiimpact.BuildRepoPatternResolver(teams)

	records := aiimpact.Compute(aiimpact.Params{
		Day: dayStart, OrgID: run.OrganizationID,
		PullRequests: pullRequests, Reviews: reviews,
		Attributions: attributions, Incidents: incidents,
		PRCommitStats: linkage, HasCommitStats: hasLinkage,
		TeamResolver: resolver.TeamResolverFunc(), RepoNamesByID: repoNames,
	})
	if len(records) == 0 {
		return 0, nil
	}
	return WriteAIImpactMetrics(ctx, executor.conn, records, computedAt)
}

var _ NativeFamilyExecutor = (*AIImpactExecutor)(nil)
