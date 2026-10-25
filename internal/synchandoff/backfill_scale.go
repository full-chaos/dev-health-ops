package synchandoff

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ScaleRefusal is a window and scope the scheduler would refuse over its unit
// cap. Routes answer it 422 with Error() as the detail.
type ScaleRefusal struct{ floor, unitCap int64 }

func (r *ScaleRefusal) Error() string {
	return fmt.Sprintf("Backfill is too large: at least %d work units for this window and scope, over the limit of %d. Narrow the window or the scope.", r.floor, r.unitCap)
}

// CheckBackfillScale refuses a window and scope the scheduler would refuse over
// SYNC_RUN_MAX_UNITS (schedsync.UnitCapFromEnv; materializer.go: a plan over the
// cap is quarantined). It is a floor, never an estimate above the truth: every
// source plans at least one unit per window of the widest planner chunk (14
// days), so only a run over that floor is refused. No cap (0) or no known
// sources refuses nothing. The text names only the caller's own numbers.
func CheckBackfillScale(days, sources, unitCap int64) error {
	if unitCap <= 0 || sources <= 0 {
		return nil
	}
	const widestChunkDays = 14
	chunks := (days + widestChunkDays - 1) / widestChunkDays
	if chunks < 1 {
		chunks = 1
	}
	if floor := sources * chunks; floor > unitCap {
		return &ScaleRefusal{floor: floor, unitCap: unitCap}
	}
	return nil
}

// rowQuerier is the one read CountEnabledSources needs (a pgx.Tx or pool).
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// canonicalSourceIDs is the parseable ids in their canonical lower-case form,
// each once, in first-seen order. An id that is not a uuid plans no source.
func canonicalSourceIDs(ids []string) []string {
	seen := make(map[uuid.UUID]bool, len(ids))
	out := make([]string, 0, len(ids))
	for _, raw := range ids {
		parsed, err := uuid.Parse(raw)
		if err != nil || seen[parsed] {
			continue
		}
		seen[parsed] = true
		out = append(out, parsed.String())
	}
	return out
}

// CountEnabledSources is how many distinct explicit source ids are enabled
// sources of the org's integration: the sources the scheduler plans for them
// (loadPlanSources filters on the same org, integration and is_enabled). A
// duplicate, an unmatched id or another org's id adds nothing. It is one indexed
// count, read in the caller's transaction.
func CountEnabledSources(ctx context.Context, q rowQuerier, orgID, integrationID string, ids []string) (int64, error) {
	canonical := canonicalSourceIDs(ids)
	if len(canonical) == 0 {
		return 0, nil
	}
	var count int64
	err := q.QueryRow(ctx, `
SELECT count(*) FROM public.integration_sources
WHERE org_id = $1 AND integration_id = $2::uuid AND is_enabled AND id = ANY($3::uuid[])`,
		orgID, integrationID, canonical).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count the enabled explicit sources: %w", err)
	}
	return count, nil
}
