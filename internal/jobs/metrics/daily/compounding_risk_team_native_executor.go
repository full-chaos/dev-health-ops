package daily

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/compoundingrisk"
	"github.com/full-chaos/dev-health-ops/internal/teamresolve"
)

// CompoundingRiskTeamExecutor is the NATIVE, FINALIZE-SCOPE implementation of
// compounding_risk's TEAM scope (CHAOS-5084), the sibling CHAOS-4287's own
// docstring said was waiting on a finalize-family hook -- that hook
// (CHAOS-4290, #2241) now exists, so this is its first non-test consumer.
//
// Like TeamCognitiveLoadExecutor, this runs once per RUN via
// daily.FinalizeHandler's native-finalize-family mechanism. Python's
// equivalent (_write_compounding_risk_team_rows_for_day,
// job_daily.py:613-675 as of this executor's introduction) is DELETED
// (CHAOS-5084/no-straddle, #2275 v2) -- this executor is the sole writer
// for this scope now, with no Python compute or fallback anywhere in the
// codebase (see finalize_family_gate_agreement_test.go's
// finalizeFamiliesWithPythonComputeDeleted exemption). The deleted Python
// function read back THIS RUN's already-computed repo_metrics_daily rows
// from ClickHouse rather than sharing in-process state with the
// daily_partition jobs that wrote them, because a Go finalize job shares no
// memory with them either -- this executor does the same readback
// (compoundingrisk.LoadRepoMetricsForOrgDay, org-wide, already
// argMax-deduped).
//
// Team resolution goes through internal/teamresolve.ResolveOwnershipThenPatterns,
// the SAME shared entry point team_cognitive_load (#2255) and team_complexity
// (#2256) use for the identical Python function
// (_repo_to_team_map_for_compounding_risk, job_daily.py:497) -- this executor
// never calls teamownership.OwnedRepoIDs directly, by design: a later fix to
// OwnedRepoIDs' ranking or error-propagation behaviour (CHAOS-5141-class)
// reaches this family automatically the next time teamresolve is
// merge-forwarded in, with no change needed here.
type CompoundingRiskTeamExecutor struct {
	conn   driver.Conn
	loader *compoundingrisk.ClickHouseLoader
	writer *compoundingrisk.Writer
	nowUTC func() time.Time
}

// CompoundingRiskTeamFamilyName re-exports the single source of truth for
// this family's skip_families key, mirroring TeamCognitiveLoadFamilyName's
// shape: a registration site indexes by this constant rather than restating
// the literal. The same literal is asserted against
// pythonRecognisedFinalizeFamilies (daily.go) and job_daily.py's gate line
// (finalize_family_gate_agreement_test.go) -- three places, one value.
const CompoundingRiskTeamFamilyName = "compounding_risk_team"

var errCompoundingRiskTeamUnavailable = fmt.Errorf("compounding_risk_team native executor unavailable")

// NewCompoundingRiskTeamExecutor fails closed on a nil connection, matching
// every other native family's construction-time policy.
func NewCompoundingRiskTeamExecutor(conn driver.Conn) (*CompoundingRiskTeamExecutor, error) {
	if conn == nil {
		return nil, errCompoundingRiskTeamUnavailable
	}
	loader, err := compoundingrisk.NewClickHouseLoader(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCompoundingRiskTeamUnavailable, err)
	}
	writer, err := compoundingrisk.NewWriter(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCompoundingRiskTeamUnavailable, err)
	}
	return &CompoundingRiskTeamExecutor{
		conn:   conn,
		loader: loader,
		writer: writer,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFinalizeFamily implements daily.NativeFinalizeFamilyExecutor.
//
// NO FAIL-OPEN (CHAOS-4290's finalize-scope policy, same as
// TeamCognitiveLoadExecutor): any error here must propagate so the finalize
// retries rather than let the Python bridge recompute and double-write.
// Idempotent by construction: compounding_risk_daily is append-only and every
// read is argMax(*, computed_at)-deduped (compoundingrisk's package doc
// comment), so a redrive never accumulates -- it just adds a later
// generation readers already know to prefer.
func (executor *CompoundingRiskTeamExecutor) ComputeFinalizeFamily(
	ctx context.Context, run Run,
) (int, error) {
	if executor == nil || executor.conn == nil || executor.loader == nil || executor.writer == nil {
		return 0, errCompoundingRiskTeamUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: run has no organization or target day", ErrInvalidState)
	}
	targetDay := run.TargetDay.UTC()
	day := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 0, 0, 0, 0, time.UTC)

	// Python: `org_repo_metrics = _fetch_repo_metrics_for_day(...); if not
	// org_repo_metrics: return 0`.
	orgRepoMetrics, err := executor.loader.LoadRepoMetricsForOrgDay(ctx, run.OrganizationID, day)
	if err != nil {
		return 0, err
	}
	if len(orgRepoMetrics) == 0 {
		return 0, nil
	}

	teams, err := LoadWellbeingTeams(ctx, executor.conn, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	patternResolver := NewRepoPatternResolver(teams)

	repoIDs := make([]uuid.UUID, 0, len(orgRepoMetrics))
	for _, row := range orgRepoMetrics {
		repoID, parseErr := uuid.Parse(row.RepoID)
		if parseErr != nil {
			return 0, fmt.Errorf("%w: repo_metrics_daily repo_id %q: %v", ErrInvalidState, row.RepoID, parseErr)
		}
		repoIDs = append(repoIDs, repoID)
	}
	repoNamesByID, err := LoadRepoNames(ctx, executor.conn, run.OrganizationID, repoIDs)
	if err != nil {
		return 0, err
	}

	// Python passes `as_of=datetime.combine(day, datetime.min.time(),
	// tzinfo=utc)` -- day-start midnight UTC, no sub-second component. `day`
	// above is already exactly that.
	//
	// #2298 r1 (P1): ResolveOwnershipThenPatterns no longer takes an explicit
	// teamIDs list -- it resolves ownership via a single org-wide
	// teamownership.AuthoritativeOwnerByRepo query (correctly ranked by
	// is_primary/specificity/updated_at/team_id), not a per-team loop, so
	// there is nothing left here to build a teamIDs slice from.
	repoToTeam := teamresolve.ResolveOwnershipThenPatterns(
		ctx, executor.conn, run.OrganizationID, day, repoIDs, repoNamesByID, patternResolver,
	)
	// Python: `if not repo_to_team_map: return 0`.
	//
	// TELEMETRY (codex round chaos-5084-2275-r1, P2): ResolveOwnershipThenPatterns
	// degrades to pattern-only resolution on an ownership-query failure
	// (logging it itself, see that function's own doc comment) rather than
	// propagating, so an empty repoToTeam here is genuinely ambiguous: it
	// could mean "no repo in this org resolves to any team today" (a
	// legitimate, retry-pointless zero), or it could mean "the ownership
	// query failed AND the pattern-only fallback also resolved nothing" (a
	// transient ClickHouse problem CHAOS-4290's finalize-scope NO-FAIL-OPEN
	// policy says should have propagated and retried, not silently
	// succeeded with zero rows). Distinguishing the two would need
	// ResolveOwnershipThenPatterns to report its own failure, a teamresolve
	// API change out of scope for this PR (#2298 owns that package and is
	// under separate review). This log line is the same
	// operator-visible-signal-only tradeoff computePostBridgeNativeFamilies'
	// own doc comment already documents and CHAOS-5183 ticketed for the
	// analogous post_bridge case -- not a full fix, but Python's own prior
	// path "at least logged the resolver/zero-row condition"
	// (job_daily.py:650, job_daily.py:2331), and this restores that
	// visibility on the Go side.
	if len(repoToTeam) == 0 {
		slog.Default().Warn("compounding_risk_team finalize resolved zero repo-to-team mappings",
			"run_id", run.ID, "organization_id", run.OrganizationID,
			"target_day", day.Format("2006-01-02"),
			"org_repo_count", len(orgRepoMetrics), "teams_in_org", len(teams),
			"cause", "either no repo resolves to any team today, or the "+
				"ownership query failed -- ResolveOwnershipThenPatterns cannot "+
				"distinguish the two (see comment above)",
		)
		return 0, nil
	}

	// Python's build_compounding_risk_rows_for_day loops EVERY org_repo_metrics
	// row (team-resolved or not) to fetch that repo's complexity delta and
	// build its Inputs, then discards the repo-scope Records it also computes
	// along the way and keeps only the rows _build_team_rows produces from
	// repo_to_team-matched entries (job_daily.py:663-666's `team_rows = [r for
	// r in all_rows if r.scope == "team"]`). A repo team resolution never
	// matched cannot contribute to ANY team row -- compute_compounding_risk is
	// documented pure (compounding_risk.py's own module doc comment), so
	// skipping its repo-scope Record for such a repo changes no observable
	// output. Restricting the complexity-delta fetch (one ClickHouse query per
	// repo) and the Inputs build to repos repo_to_team actually resolved is
	// therefore a scale optimisation, not a parity risk: for an org where most
	// repos are unowned by any team, this is the difference between N queries
	// and the (typically much smaller) |repo_to_team| queries.
	repoInputs := make([]compoundingrisk.RepoInputs, 0, len(repoToTeam))
	for _, row := range orgRepoMetrics {
		if _, resolved := repoToTeam[row.RepoID]; !resolved {
			continue
		}
		repoUUID, parseErr := uuid.Parse(row.RepoID)
		if parseErr != nil {
			return 0, fmt.Errorf("%w: repo_metrics_daily repo_id %q: %v", ErrInvalidState, row.RepoID, parseErr)
		}
		delta, deltaErr := executor.loader.LoadComplexityDelta(
			ctx, run.OrganizationID, repoUUID, day, compoundingrisk.ComplexityWindowDays,
		)
		if deltaErr != nil {
			return 0, deltaErr
		}
		repoInputs = append(repoInputs, compoundingrisk.RepoInputs{
			RepoID: row.RepoID,
			Inputs: compoundingrisk.Inputs{
				ReworkChurn:       row.ReworkChurnRatio30D,
				ComplexityDelta:   delta,
				ReviewLatencyP90H: row.PRFirstReviewP90Hours,
				SingleOwnerRatio:  row.SingleOwnerFileRatio30D,
				OwnershipGini:     row.CodeOwnershipGini,
				BusFactor:         row.BusFactor,
			},
		})
	}

	computedAt := executor.nowUTC()
	records := compoundingrisk.BuildTeamRows(
		day, run.OrganizationID, repoInputs, repoToTeam, computedAt,
		compoundingrisk.DefaultWeights, compoundingrisk.DefaultThresholds, compoundingrisk.DefaultReferences,
	)
	if len(records) == 0 {
		return 0, nil
	}

	rowsWritten, err := executor.writer.WriteRecords(ctx, records, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	return rowsWritten, nil
}

var _ NativeFinalizeFamilyExecutor = (*CompoundingRiskTeamExecutor)(nil)
