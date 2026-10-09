package daily

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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
	return manualDailyGeneration(organizationID, day, repositoryIDs, "")
}

// ManualDailyRerunGeneration derives the generation of a re-run: the logical
// request of ManualDailyRunGeneration plus the operator's re-run token.
//
// The deterministic generation of a manual request makes a second identical
// request start nothing, which is right for a retried invocation and wrong
// when the operator must compute a stored day again (the teams changed after
// the day was computed). The token names one such re-run. The same token
// derives the same generation, so a retried re-run still starts nothing; a new
// token derives a new generation and so a new run. It carries the manual
// prefix, so the worker handles the run as any manual run.
func ManualDailyRerunGeneration(organizationID, day string, repositoryIDs []RepositoryID, rerunToken string) string {
	return manualDailyGeneration(organizationID, day, repositoryIDs, rerunToken)
}

// ValidManualDailyRerunToken reports whether a re-run token is 1 to 64
// characters of letters, digits, '.', '_' and '-'. The token is hashed into
// the generation and is printed in no error.
func ValidManualDailyRerunToken(token string) bool {
	if len(token) == 0 || len(token) > 64 {
		return false
	}
	for _, character := range token {
		switch {
		case character >= 'a' && character <= 'z', character >= 'A' && character <= 'Z',
			character >= '0' && character <= '9', character == '.', character == '_', character == '-':
		default:
			return false
		}
	}
	return true
}

func manualDailyGeneration(organizationID, day string, repositoryIDs []RepositoryID, rerunToken string) string {
	sorted := make([]string, len(repositoryIDs))
	for i, id := range repositoryIDs {
		sorted[i] = string(id)
	}
	sort.Strings(sorted)
	seed := organizationID + "|" + day + "|" + strings.Join(sorted, ",")
	if rerunToken != "" {
		// A repository id is a uuid and holds no '|', so a token cannot make
		// the seed of another request.
		seed += "|rerun:" + rerunToken
	}
	sum := sha256.Sum256([]byte(seed))
	return ManualDailyGenerationPrefix + hex.EncodeToString(sum[:])[:16]
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
//
// Deferred-discovery requests (repositoryIDs empty) are refused with
// ErrDayAlreadyCovered when the same (org, day) already has a succeeded run
// under ANY OTHER generation -- a prior scheduled fan-out, post-sync
// re-drive, or earlier manual trigger (CHAOS-5055, codex adversarial review
// round 2, P1): StartRunTx's (org_id, target_day, generation) uniqueness
// only makes THIS generation idempotent against ITS OWN replays, so without
// this check a manual trigger for a day the schedule already computed would
// insert a second, independent all-repository run and duplicate-write every
// native daily family. A transaction-scoped advisory lock keyed on (org,
// day) serializes this check against a CONCURRENT manual trigger for the
// same day (the same shape remaining/manual_backfill.go's
// startManualTriggerRun uses); it does NOT serialize against a concurrent
// scheduled-fanout or post-sync transaction, since neither of those take
// this lock -- closing that direction would mean changing the nightly
// schedule's own behavior, which is deliberately out of this fix's scope
// (see ErrDayAlreadyCovered's doc comment). Repository-scoped requests
// (repositoryIDs non-empty) skip this check entirely: a narrower,
// deliberately-scoped manual recompute of specific repositories is not the
// scenario this guards against.
func (store *PostgresStore) StartManualDailyRun(
	ctx context.Context,
	organizationID, day, generation string,
	repositoryIDs []RepositoryID,
	publisher RunPublisher,
) (ManualDailyRunOutcome, error) {
	return store.startManualDailyRun(ctx, organizationID, day, generation, repositoryIDs, publisher, false)
}

// StartManualDailyRerun starts a run that computes a stored day again. It is
// StartManualDailyRun with two differences:
//
//   - the day may already be covered: ErrDayAlreadyCovered is never returned,
//     because computing the covered day again is the request;
//   - the request is refused with ErrManualRunInFlight while another manual
//     run of the same (organization, day) is pending or running. With that, a
//     caller that sends a new token on each attempt has at most one manual run
//     of a day in flight, and a loop cannot stack runs of one day. The check
//     and the insert are one transaction under the advisory lock of the day,
//     so two concurrent re-runs cannot both pass it.
//
// generation MUST come from ManualDailyRerunGeneration. A request whose
// generation already has a run is the retry of that re-run: it starts nothing
// and is not refused, whatever is in flight.
func (store *PostgresStore) StartManualDailyRerun(
	ctx context.Context,
	organizationID, day, generation string,
	repositoryIDs []RepositoryID,
	publisher RunPublisher,
) (ManualDailyRunOutcome, error) {
	return store.startManualDailyRun(ctx, organizationID, day, generation, repositoryIDs, publisher, true)
}

func (store *PostgresStore) startManualDailyRun(
	ctx context.Context,
	organizationID, day, generation string,
	repositoryIDs []RepositoryID,
	publisher RunPublisher,
	rerun bool,
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

	if len(repositoryIDs) == 0 || rerun {
		if _, err := tx.Exec(ctx,
			"SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))",
			"daily_metrics_manual_day", organizationID+":"+day,
		); err != nil {
			return ManualDailyRunOutcome{}, ErrUnavailable
		}
	}
	switch {
	case rerun:
		exists, err := store.RunExistsTx(ctx, tx, organizationID, targetDay, generation)
		if err != nil {
			return ManualDailyRunOutcome{}, err
		}
		if !exists {
			inFlight, err := store.hasManualRunInFlightForDay(ctx, tx, organizationID, day)
			if err != nil {
				return ManualDailyRunOutcome{}, err
			}
			if inFlight {
				return ManualDailyRunOutcome{}, ErrManualRunInFlight
			}
		}
	case len(repositoryIDs) == 0:
		covered, err := store.HasSucceededRunForDay(ctx, tx, organizationID, day, generation)
		if err != nil {
			return ManualDailyRunOutcome{}, err
		}
		if covered {
			return ManualDailyRunOutcome{}, ErrDayAlreadyCovered
		}
	}

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

// hasManualRunInFlightForDay reports whether a manual run of the
// (organization, day) is pending or running, under any manual generation.
func (store *PostgresStore) hasManualRunInFlightForDay(
	ctx context.Context, tx pgx.Tx, organizationID, day string,
) (bool, error) {
	if !store.valid() || tx == nil || !validUUID(organizationID) || day == "" {
		return false, ErrUnavailable
	}
	var exists bool
	err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM public.daily_metrics_runs
    WHERE org_id = $1::uuid AND target_day = $2::date
      AND generation LIKE $3
      AND status IN ('pending', 'running')
)`, organizationID, day, escapeLikePrefix(ManualDailyGenerationPrefix)+"%").Scan(&exists)
	if err != nil {
		return false, ErrUnavailable
	}
	return exists, nil
}
