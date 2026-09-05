package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// TeamComplexityFamilyName re-exports the single source of truth for this
// family's families.json/skip_families key, mirroring
// TeamCognitiveLoadFamilyName's shape (CHAOS-5141) and icfinalize.FamilyName's
// (#2243).
const TeamComplexityFamilyName = "team_complexity"

// TeamComplexityExecutor is the NATIVE, FINALIZE-SCOPE implementation of the
// team_complexity family (CHAOS-5051, CHAOS-4365 item 3).
//
// UNLIKE team_cognitive_load, this family's only input --
// repo_complexity_daily -- is written by a COMPLETELY SEPARATE job
// (run_complexity_db_job / its eventual native successor, PR1/PR1b of
// CHAOS-4291), never by another FINALIZE family in the SAME run. There is
// therefore no same-attempt producer-ordering hazard to guard against here:
// repo_complexity_daily either already has today's rows (from that job's own,
// independently-scheduled run) or it does not, and "does not" is
// indistinguishable from -- and handled identically to -- a genuinely quiet
// day (zero rows, 0/nil, no error). This is why this executor carries NO
// construction-time co-registration assertion the way team_cognitive_load's
// does for ic_finalize.
type TeamComplexityExecutor struct {
	conn   driver.Conn
	writer *teamComplexityWriter
	nowUTC func() time.Time
}

var errTeamComplexityUnavailable = fmt.Errorf("team_complexity native executor unavailable")

// NewTeamComplexityExecutor fails closed on a nil connection, matching every
// other native family's construction-time policy.
func NewTeamComplexityExecutor(conn driver.Conn) (*TeamComplexityExecutor, error) {
	if conn == nil {
		return nil, errTeamComplexityUnavailable
	}
	return &TeamComplexityExecutor{
		conn:   conn,
		writer: &teamComplexityWriter{conn: conn},
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFinalizeFamily implements daily.NativeFinalizeFamilyExecutor.
//
// NO FAIL-OPEN (CHAOS-4290's finalize-scope policy): any error propagates so
// the finalize retries rather than let the Python bridge recompute and
// double-write. Idempotent by construction: a redrive writes new rows with a
// later computed_at, and every reader dedups via
// argMax(tuple(...), computed_at) -- migration 087 (CHAOS-4291) additionally
// makes the table itself a ReplacingMergeTree(computed_at), so a redrive
// never accumulates duplicates even before a background merge runs.
func (executor *TeamComplexityExecutor) ComputeFinalizeFamily(
	ctx context.Context, run Run,
) (int, error) {
	if executor == nil || executor.conn == nil || executor.writer == nil {
		return 0, errTeamComplexityUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: run has no organization or target day", ErrInvalidState)
	}
	day := time.Date(
		run.TargetDay.UTC().Year(), run.TargetDay.UTC().Month(), run.TargetDay.UTC().Day(),
		0, 0, 0, 0, time.UTC,
	)

	repoRows, err := loadRepoComplexityInputsForDay(ctx, executor.conn, run.OrganizationID, day)
	if err != nil {
		return 0, err
	}
	if len(repoRows) == 0 {
		// Python: `if not repo_complexity_rows: return 0` (implicit -- the
		// aggregator's own loop simply produces no buckets). Matches
		// team_cognitive_load's identical early-empty contract.
		return 0, nil
	}

	teams, err := LoadWellbeingTeams(ctx, executor.conn, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	patternResolver := NewRepoPatternResolver(teams)

	repoIDs := make([]uuid.UUID, 0, len(repoRows))
	for _, row := range repoRows {
		repoIDs = append(repoIDs, row.RepoID)
	}
	repoNamesByID, err := LoadRepoNames(ctx, executor.conn, run.OrganizationID, repoIDs)
	if err != nil {
		return 0, err
	}

	repoToTeam := resolveDailyFinalizeRepoToTeam(ctx, executor.conn, run.OrganizationID, day, teams, repoIDs, repoNamesByID, patternResolver)

	computedAt := executor.nowUTC()
	rows := buildTeamComplexityRows(run.OrganizationID, day, repoRows, repoToTeam, computedAt)
	if len(rows) == 0 {
		return 0, nil
	}
	if err := executor.writer.write(ctx, rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}

var _ NativeFinalizeFamilyExecutor = (*TeamComplexityExecutor)(nil)
