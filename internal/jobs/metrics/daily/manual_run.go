package daily

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"
)

// ManualDailyRunOutcome reports what StartManualDailyRun did for one day.
type ManualDailyRunOutcome struct {
	Day        string
	RunID      string
	Generation string
}

// ManualDailyRunGeneration derives a deterministic generation for a manual
// `metrics daily-start` dispatch (CHAOS-5055: `dev-hops metrics
// daily`/`rebuild` used to call run_daily_metrics_job directly, recomputing
// -- and rewriting -- native families (file_hotspots, cicd, deploy, ...) the
// worker's own bridge call had already computed for the SAME (org, day,
// repo) scope, because the bare CLI path never passed skip_families the way
// worker_metrics.py's HTTP bridge does. Enqueuing through this same
// StartRunTx coordinator transaction the post-sync/fixed-schedule fanout
// paths use closes that gap structurally: the worker decides the
// native/bridge split either way, so there is no longer a second, unguarded
// write path).
//
// Deterministic per LOGICAL request (org + day + repository scope), not a
// wall-clock timestamp, so a retried CLI invocation lands on StartRunTx's own
// ON CONFLICT DO NOTHING idempotency instead of inserting a second run.
// Hashed rather than the raw values: normalizeStartRunRequest caps
// Generation at 64 bytes and a raw org+day+repo-id-list easily exceeds that
// once more than a couple of repositories are named.
func ManualDailyRunGeneration(organizationID, day string, repositoryIDs []RepositoryID) string {
	sorted := make([]string, len(repositoryIDs))
	for i, id := range repositoryIDs {
		sorted[i] = string(id)
	}
	sort.Strings(sorted)
	seed := organizationID + "|" + day + "|" + strings.Join(sorted, ",")
	sum := sha256.Sum256([]byte(seed))
	return "manual-daily:" + hex.EncodeToString(sum[:])[:16]
}

// StartManualDailyRun starts an operator-triggered daily-metrics run for one
// (organization, day), dispatched through the SAME StartRunTx coordinator
// transaction the post-sync and fixed-schedule fanout paths use -- never a
// direct Python compute call. repositoryIDs empty means deferred discovery
// (every org repository, resolved later by the worker from live ClickHouse
// identity, exactly like the fixed-schedule fanout); non-empty
// repository-scopes this run the same way rebuild's --repo-id flags do
// today. generation MUST be deterministic per logical request (see
// ManualDailyRunGeneration) so a retried CLI invocation is idempotent rather
// than dispatching a second run for the same day.
func (store *PostgresStore) StartManualDailyRun(
	ctx context.Context,
	organizationID, day, generation string,
	repositoryIDs []RepositoryID,
	publisher RunPublisher,
) (ManualDailyRunOutcome, error) {
	if !store.valid() {
		return ManualDailyRunOutcome{}, ErrUnavailable
	}
	if !validUUID(organizationID) {
		return ManualDailyRunOutcome{}, ErrInvalidState
	}
	targetDay, err := time.Parse("2006-01-02", day)
	if err != nil {
		return ManualDailyRunOutcome{}, ErrInvalidState
	}

	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return ManualDailyRunOutcome{}, ErrUnavailable
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	run, err := store.StartRunTx(ctx, tx, StartRunRequest{
		OrganizationID: organizationID,
		TargetDay:      targetDay,
		Generation:     generation,
		RepositoryIDs:  repositoryIDs,
	}, publisher)
	if err != nil {
		return ManualDailyRunOutcome{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ManualDailyRunOutcome{}, ErrUnavailable
	}
	committed = true
	return ManualDailyRunOutcome{Day: day, RunID: run.ID, Generation: generation}, nil
}
