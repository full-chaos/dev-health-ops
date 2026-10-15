package remaining

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Manual run kinds: the generation prefixes StartManualBackfillRun
// ("manual-backfill:<family>:<org>:<from>..<to>") and StartManualRecomputeRun
// (ManualRecomputeGeneration) start their runs under. An automatic run never
// carries either prefix.
const (
	manualRunKindBackfill  = "manual_backfill"
	manualRunKindRecompute = "manual_recompute"
)

func manualRunKind(generation string) string {
	switch {
	case strings.HasPrefix(generation, "manual-backfill:"):
		return manualRunKindBackfill
	case strings.HasPrefix(generation, "manual-recompute:"):
		return manualRunKindRecompute
	}
	return ""
}

// ManualOverlapObserver reports one automatic dora start whose window overlaps
// a pending or running manual backfill or manual recompute run of the same
// organization (CHAOS-9692). The automatic start does not wait for the manual
// run and does not skip a day: both runs compute the overlapping days. The
// values are identical and readers take the newest computed_at, so this costs
// a duplicate computation and never a wrong number; this counter is how that
// duplicate is seen.
type ManualOverlapObserver interface {
	ObserveRemainingMetricsManualOverlap(family string) error
}

// SetManualOverlapObserver wires the optional CHAOS-9692 signal. A nil observer
// (the default) means telemetry never gates a start -- observeManualOverlap is
// always a safe no-op.
func (store *PostgresStore) SetManualOverlapObserver(observer ManualOverlapObserver) {
	store.manualOverlapObserver = observer
}

func (store *PostgresStore) observeManualOverlap(family string) {
	if store.manualOverlapObserver != nil {
		_ = store.manualOverlapObserver.ObserveRemainingMetricsManualOverlap(family)
	}
}

// manualOverlap is one pending or running manual run whose window shares days
// with the window an automatic start is about to compute.
type manualOverlap struct {
	runID string
	kind  string
	from  string
	to    string
	days  int
}

// windowIntersection returns the days shared by the window of requestDays days
// ending on requestAnchor and the window of runDays days ending on runAnchor.
func windowIntersection(requestAnchor time.Time, requestDays int, runAnchor time.Time, runDays int) (from, to time.Time, days int) {
	requestStart := requestAnchor.AddDate(0, 0, -(requestDays - 1))
	runStart := runAnchor.AddDate(0, 0, -(runDays - 1))
	from, to = requestStart, requestAnchor
	if runStart.After(from) {
		from = runStart
	}
	if runAnchor.Before(to) {
		to = runAnchor
	}
	if to.Before(from) {
		return time.Time{}, time.Time{}, 0
	}
	return from, to, int(to.Sub(from).Hours()/24) + 1
}

// findManualWindowOverlaps lists the pending or running manual backfill and
// manual recompute runs of (org, family) whose window shares a day with the
// window of backfillDays days ending on anchorDay. It reads the candidate
// partitions with the same query and the same window containment rule
// findManualBackfillBlocker uses.
func (store *PostgresStore) findManualWindowOverlaps(
	ctx context.Context, tx pgx.Tx, organizationID, family, anchorDay string, backfillDays int,
) ([]manualOverlap, error) {
	anchor, err := time.Parse("2006-01-02", anchorDay)
	if err != nil {
		return nil, ErrInvalidState
	}
	start := anchor.AddDate(0, 0, -(backfillDays - 1))
	// A manual run anchored after the request can still reach back into it,
	// by at most the widest window one partition can hold.
	upperBound := anchor.AddDate(0, 0, maxDayScopedBackfillDays-1)
	rows, err := store.queryWindowRunRows(ctx, tx, organizationID, family,
		start.Format("2006-01-02"), upperBound.Format("2006-01-02"), false, nil)
	if err != nil {
		return nil, err
	}
	var overlaps []manualOverlap
	for _, row := range rows {
		kind := manualRunKind(row.generation)
		if kind == "" || (row.runStatus != "pending" && row.runStatus != "running") {
			continue
		}
		runAnchor, parseErr := time.Parse("2006-01-02", row.anchorDay)
		if parseErr != nil {
			continue
		}
		from, to, days := windowIntersection(anchor, backfillDays, runAnchor, row.backfillDays)
		if days == 0 {
			continue
		}
		overlaps = append(overlaps, manualOverlap{
			runID: row.runID, kind: kind, from: from.Format("2006-01-02"), to: to.Format("2006-01-02"), days: days,
		})
	}
	return overlaps, nil
}

// reportManualWindowOverlaps logs and counts every manual run the automatic
// run runID, just inserted for (org, family, anchorDay, backfillDays), computes
// alongside. It is telemetry: it never changes the start and never fails it. The
// lookup runs inside a savepoint so a failing read cannot abort the caller's
// transaction.
func (store *PostgresStore) reportManualWindowOverlaps(
	ctx context.Context, tx pgx.Tx, request StartRunRequest, runID, anchorDay string, backfillDays int,
) {
	logger := store.logger
	if logger == nil {
		return
	}
	savepoint, err := tx.Begin(ctx)
	if err != nil {
		logger.WarnContext(ctx, "remaining metrics manual overlap check skipped",
			"family", request.Family, "organization_id", request.OrganizationID, "operation", "begin savepoint", "error", err.Error())
		return
	}
	overlaps, err := store.findManualWindowOverlaps(ctx, savepoint, request.OrganizationID, request.Family, anchorDay, backfillDays)
	if err != nil {
		_ = savepoint.Rollback(ctx)
		logger.WarnContext(ctx, "remaining metrics manual overlap check failed",
			"family", request.Family, "organization_id", request.OrganizationID, "anchor_day", anchorDay, "error", err.Error())
		return
	}
	if err := savepoint.Commit(ctx); err != nil {
		logger.WarnContext(ctx, "remaining metrics manual overlap check failed",
			"family", request.Family, "organization_id", request.OrganizationID, "operation", "release savepoint", "error", err.Error())
		return
	}
	for _, overlap := range overlaps {
		logger.InfoContext(ctx, "remaining metrics automatic start overlaps a manual run",
			"family", request.Family,
			"organization_id", request.OrganizationID,
			"run_id", runID,
			"generation", request.Generation,
			"anchor_day", anchorDay,
			"backfill_days", backfillDays,
			"manual_run_id", overlap.runID,
			"manual_kind", overlap.kind,
			"overlap_from", overlap.from,
			"overlap_to", overlap.to,
			"overlap_days", overlap.days,
		)
		store.observeManualOverlap(request.Family)
	}
}
