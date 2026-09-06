package daily

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// repoComplexityInput is one deduped repo_complexity_daily row this day.
// Mirrors what build_team_complexity_rows_for_day reads via
// getattr(row, "repo_id"/"loc_total"/"cyclomatic_total"/
// "high_complexity_functions"/"very_high_complexity_functions", None).
type repoComplexityInput struct {
	RepoID                      uuid.UUID
	LOCTotal                    int
	CyclomaticTotal             int
	HighComplexityFunctions     int
	VeryHighComplexityFunctions int
}

// loadRepoComplexityInputsForDay ports _fetch_repo_complexity_for_day
// (job_daily.py:784) exactly, including its CHAOS-4365 codex R1 fix: a
// SINGLE argMax(tuple(...), computed_at) per repo_id, not four independent
// per-column argMax calls, so a tie on computed_at (repo_complexity_daily's
// own computed_at is a second-precision DateTime, per migration 087's own
// comment) can never assemble a "Frankenstein" row from two different
// physical rows.
func loadRepoComplexityInputsForDay(
	ctx context.Context, conn driver.Conn, organizationID string, day time.Time,
) ([]repoComplexityInput, error) {
	rows, err := conn.Query(ctx, `
SELECT
	repo_id,
	tupleElement(latest, 1) AS loc_total,
	tupleElement(latest, 2) AS cyclomatic_total,
	tupleElement(latest, 3) AS high_complexity_functions,
	tupleElement(latest, 4) AS very_high_complexity_functions
FROM (
	SELECT
		repo_id,
		argMax(
			tuple(loc_total, cyclomatic_total, high_complexity_functions, very_high_complexity_functions),
			computed_at
		) AS latest
	FROM repo_complexity_daily
	WHERE org_id = ? AND day = ?
	GROUP BY repo_id
)`,
		organizationID, day,
	)
	if err != nil {
		return nil, fmt.Errorf("load repo_complexity_daily for team complexity: %w", err)
	}
	defer rows.Close()

	var result []repoComplexityInput
	for rows.Next() {
		var row repoComplexityInput
		var locTotal, cyclomaticTotal, high, veryHigh uint64
		if err := rows.Scan(&row.RepoID, &locTotal, &cyclomaticTotal, &high, &veryHigh); err != nil {
			return nil, fmt.Errorf("scan repo_complexity_daily row: %w", err)
		}
		row.LOCTotal = int(locTotal)
		row.CyclomaticTotal = int(cyclomaticTotal)
		row.HighComplexityFunctions = int(high)
		row.VeryHighComplexityFunctions = int(veryHigh)
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate repo_complexity_daily rows: %w", err)
	}
	return result, nil
}

// teamComplexityRow is one team_complexity_daily row this executor writes --
// the Go mirror of TeamComplexityDailyRecord (schemas.py).
type teamComplexityRow struct {
	OrganizationID              string
	TeamID                      string
	Day                         time.Time
	LOCTotal                    int
	CyclomaticTotal             int
	CyclomaticPerKLOC           float64
	HighComplexityFunctions     int
	VeryHighComplexityFunctions int
	ContributingRepoCount       int
	ComputedAt                  time.Time
}

type teamComplexityBucket struct {
	locTotal                    int
	cyclomaticTotal             int
	highComplexityFunctions     int
	veryHighComplexityFunctions int
	repoIDs                     map[string]struct{}
}

func newTeamComplexityBucket() *teamComplexityBucket {
	return &teamComplexityBucket{repoIDs: map[string]struct{}{}}
}

// buildTeamComplexityRows ports build_team_complexity_rows_for_day
// (team_complexity.py) exactly: bucket every repo_complexity_daily input row
// by its repo's resolved team, sum the absolute counters, and recompute
// cyclomatic_per_kloc from the SUMMED totals (loc-weighted) -- never averaged
// directly across owned repos' own per-repo ratios, which would let a small
// repo's ratio dominate a large one's.
func buildTeamComplexityRows(
	organizationID string, day time.Time,
	repoRows []repoComplexityInput, repoToTeam map[string]string, computedAt time.Time,
) []teamComplexityRow {
	buckets := map[string]*teamComplexityBucket{}
	bucket := func(teamID string) *teamComplexityBucket {
		existing, ok := buckets[teamID]
		if !ok {
			existing = newTeamComplexityBucket()
			buckets[teamID] = existing
		}
		return existing
	}

	for _, row := range repoRows {
		repoIDStr := row.RepoID.String()
		teamID, ok := repoToTeam[repoIDStr]
		if !ok || teamID == "" {
			// Ownership genuinely does not cover this repo -- never guessed,
			// matching the Python aggregator's own `if not team_id: continue`.
			continue
		}
		b := bucket(teamID)
		b.locTotal += row.LOCTotal
		b.cyclomaticTotal += row.CyclomaticTotal
		b.highComplexityFunctions += row.HighComplexityFunctions
		b.veryHighComplexityFunctions += row.VeryHighComplexityFunctions
		b.repoIDs[repoIDStr] = struct{}{}
	}

	teamIDs := make([]string, 0, len(buckets))
	for teamID := range buckets {
		teamIDs = append(teamIDs, teamID)
	}
	sort.Strings(teamIDs)

	records := make([]teamComplexityRow, 0, len(teamIDs))
	for _, teamID := range teamIDs {
		b := buckets[teamID]
		var cyclomaticPerKLOC float64
		if b.locTotal > 0 {
			cyclomaticPerKLOC = float64(b.cyclomaticTotal) / (float64(b.locTotal) / 1000.0)
		}
		records = append(records, teamComplexityRow{
			OrganizationID:              organizationID,
			TeamID:                      teamID,
			Day:                         day,
			LOCTotal:                    b.locTotal,
			CyclomaticTotal:             b.cyclomaticTotal,
			CyclomaticPerKLOC:           cyclomaticPerKLOC,
			HighComplexityFunctions:     b.highComplexityFunctions,
			VeryHighComplexityFunctions: b.veryHighComplexityFunctions,
			ContributingRepoCount:       len(b.repoIDs),
			ComputedAt:                  computedAt,
		})
	}
	return records
}

// teamComplexityWriter persists teamComplexityRow batches to
// team_complexity_daily. Append-only, matching TeamComplexityMixin exactly: a
// redrive writes NEW rows with a later computed_at, never an UPDATE.
// migration 087 (CHAOS-4291) additionally makes the table a
// ReplacingMergeTree(computed_at), so a redrive never accumulates duplicates
// even before a background merge runs.
type teamComplexityWriter struct {
	conn driver.Conn
}

func (writer *teamComplexityWriter) write(ctx context.Context, rows []teamComplexityRow) error {
	if writer == nil || writer.conn == nil {
		return errTeamComplexityUnavailable
	}
	if len(rows) == 0 {
		return nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO team_complexity_daily (
		org_id, team_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc,
		high_complexity_functions, very_high_complexity_functions,
		contributing_repo_count, computed_at
	)`)
	if err != nil {
		return fmt.Errorf("prepare team_complexity_daily batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.OrganizationID, row.TeamID, row.Day, uint64(row.LOCTotal), uint64(row.CyclomaticTotal),
			row.CyclomaticPerKLOC, uint64(row.HighComplexityFunctions), uint64(row.VeryHighComplexityFunctions),
			uint32(row.ContributingRepoCount), row.ComputedAt,
		); err != nil {
			return fmt.Errorf("append team_complexity_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send team_complexity_daily batch: %w", err)
	}
	return nil
}
