package syncdispatchruntime

import (
	"context"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
)

// bumpSyncRunRollup is THE seam (CHAOS-4586): every terminal-status write to
// public.sync_run_units in this package must, in the same transaction,
// recompute the parent sync_runs row's completed_units/failed_units through
// this function -- never a hand-rolled UPDATE. A thin wrapper over the
// shared internal/syncrunrollup.Bump: after chris's "Not again" review
// generalized CHAOS-4586, providersync, this package, and syncreconciler's
// unreclaimable sweep all call that ONE function, never a local copy of its
// SQL. Kept as a package-local name (rather than every call site importing
// syncrunrollup directly) so the call sites below read no differently than
// they did before the generalization. See syncrunrollup.Bump's doc comment
// for the lock-first ordering that makes concurrent callers from any of
// these packages safe to interleave.
func bumpSyncRunRollup(
	ctx context.Context, tx pgx.Tx, syncRunID string,
) (completedUnits, failedUnits, totalUnits int, err error) {
	return syncrunrollup.Bump(ctx, tx, syncRunID)
}

// Rollup PATH labels for RecordSyncRunRollupBumped (CHAOS-4586, chris:
// "same family name so it is one counter with a path label"). The counter
// already exists (CHAOS-4559, dev_health_sync_run_rollup_bumped_total) with
// an OUTCOME label naming which terminal STAMP triggered a bump -- every
// mechanism in this package only ever writes a FAILED stamp, so outcome is
// always "failed" for all five of these (see flushRollupBumpTally). PATH is
// the dimension that actually distinguishes them, joining
// providerfoundation.Metrics' own closed path vocabulary as five new
// members alongside providersync's "provider_unit".
const (
	rollupPathDenied                   = "denied"
	rollupPathUnroutable               = "unroutable"
	rollupPathInvalidClaim             = "invalid_claim"
	rollupPathBudgetExhausted          = "budget_exhausted"
	rollupPathReferenceDiscoveryFailed = "reference_discovery_failed"
	// rollupPathFeatureDisabled (codex round 10, CHAOS-4586): added once
	// terminalizeFeatureDisabledRun's OWN full-COUNT recompute was found to
	// need the same lock-first protection every other path here already
	// has (syncrunrollup.LockRun) -- it was previously exempt from this
	// vocabulary on the mistaken premise that it needed no fix at all.
	rollupPathFeatureDisabled = "feature_disabled"
)

// rollupBumpTally accumulates this package's RecordSyncRunRollupBumped path
// bumps for ONE caller-owned transaction, so telemetry can be emitted only
// AFTER that transaction actually commits -- never before (CHAOS-4559 codex
// round 1 P2: recording before commit overcounts a bump whose transaction
// later rolls back, and that exact defect class is why providersync's own
// recordSyncRunRollupBump is called post-commit only). The budget chokepoint
// call chain (enforceRun -> resolveCooldownBlockedUnit -> terminalizeUnit,
// reconfirmCooldowns -> terminalizeUnit, ...) is several calls deep with no
// shared owner besides Dispatch() itself, so the tally is threaded via
// context instead of a parameter added to every intermediate signature --
// context.Context is already the one thing every one of those calls
// carries all the way down.
type rollupBumpTally struct {
	mu     sync.Mutex
	counts map[string]int
}

type rollupBumpTallyKey struct{}

// withRollupBumpTally returns a context carrying a fresh, empty tally. Call
// this once at the top of any function that owns a transaction and commits
// it itself (Dispatch, denyRun, handleFailure) -- never inside a helper that
// merely participates in someone else's transaction.
func withRollupBumpTally(ctx context.Context) context.Context {
	return context.WithValue(ctx, rollupBumpTallyKey{}, &rollupBumpTally{counts: map[string]int{}})
}

// recordRollupBump notes that bumpSyncRunRollup ran for the given path on
// ctx's transaction. A no-op if ctx was never wrapped with
// withRollupBumpTally (e.g. a unit test calling a mechanism function
// directly) -- callers still get the durable rollup write either way, only
// the telemetry is skipped.
func recordRollupBump(ctx context.Context, path string) {
	tally, ok := ctx.Value(rollupBumpTallyKey{}).(*rollupBumpTally)
	if !ok || tally == nil {
		return
	}
	tally.mu.Lock()
	defer tally.mu.Unlock()
	tally.counts[path]++
}

// flushRollupBumpTally emits RecordSyncRunRollupBumped once per accumulated
// path (outcome is always "failed" -- every mechanism in this package only
// ever writes a failed stamp) and logs a debug line per path, matching
// providersync's recordSyncRunRollupBump convention. Call this ONLY
// immediately after the owning transaction's Commit has returned nil.
// Nil-safe: a nil metrics or an untallied ctx is a no-op, and each path is
// cleared after flushing so a second, mistaken flush of the same ctx never
// double-counts.
func flushRollupBumpTally(ctx context.Context, metrics *providerfoundation.Metrics) {
	tally, ok := ctx.Value(rollupBumpTallyKey{}).(*rollupBumpTally)
	if !ok || tally == nil {
		return
	}
	tally.mu.Lock()
	defer tally.mu.Unlock()
	for path, n := range tally.counts {
		for i := 0; i < n; i++ {
			metrics.RecordSyncRunRollupBumped("failed", path)
		}
		slog.Debug("sync_run.rollup_bumped", slog.String("outcome", "failed"), slog.String("path", path), slog.Int("count", n))
		delete(tally.counts, path)
	}
}

// rollupBumpPathVocabulary lists every path value this package ever passes
// to RecordSyncRunRollupBumped -- test-facing only (see
// rollup_seam_registry_test.go), so a new path added here without updating
// this list is caught at test time rather than silently folding into
// providerfoundation.Metrics' "other" bucket in production.
var rollupBumpPathVocabulary = map[string]bool{
	rollupPathDenied:                   true,
	rollupPathUnroutable:               true,
	rollupPathInvalidClaim:             true,
	rollupPathBudgetExhausted:          true,
	rollupPathReferenceDiscoveryFailed: true,
	rollupPathFeatureDisabled:          true,
}
