package daily

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ManualDailyRunOutcome reports what StartManualDailyRun did for one day.
type ManualDailyRunOutcome struct {
	Day        string
	RunID      string
	Generation string
	// AlreadyStarted is true when the run row of this (org, day, generation)
	// existed before the call, so the call started nothing new (the
	// idempotent replay of an earlier identical request). It is not part of
	// the command's output: a caller that wants it reads it from here.
	AlreadyStarted bool `json:"-"`
	// CoveredDayOverriddenBy is the run id of the scheduled-fanout or post-sync
	// run that already covered the day when a call with a rerun tag was
	// admitted (StartManualDailyRerun). Empty when the day was not covered or
	// the call carried no tag. Like AlreadyStarted it is not part of the plain
	// command output.
	CoveredDayOverriddenBy string `json:"-"`
}

// MaxRerunTagLength bounds a rerun tag. The tag is hashed into the
// generation, never embedded, so the bound only keeps argv, audit and log
// lines small.
const MaxRerunTagLength = 32

// ValidRerunTag reports whether tag is a well-formed rerun tag: 1 to
// MaxRerunTagLength characters of [A-Za-z0-9._-]. The set holds no "|" and no
// "," so a tag can never shift the field boundaries of the generation seed.
func ValidRerunTag(tag string) bool {
	if tag == "" || len(tag) > MaxRerunTagLength {
		return false
	}
	for _, r := range tag {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '.', r == '_', r == '-':
		default:
			return false
		}
	}
	return true
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
	return ManualDailyRerunGeneration(organizationID, day, repositoryIDs, "")
}

// ManualDailyRerunGeneration is ManualDailyRunGeneration with an optional
// caller-chosen rerun tag. An empty tag gives byte-for-byte the generation of
// ManualDailyRunGeneration. A non-empty tag is hashed into the seed after a
// "|rerun:" field, so (org, day, repository set, tag) names one logical
// request: the same tag twice lands on StartRunTx's ON CONFLICT DO NOTHING
// and starts nothing, and a new tag names a new run for a day that already has
// one. The tag must pass ValidRerunTag; "|" is not in its alphabet and a
// repository id is a uuid, so no tag can make two different requests hash from
// the same seed.
func ManualDailyRerunGeneration(organizationID, day string, repositoryIDs []RepositoryID, rerunTag string) string {
	sorted := make([]string, len(repositoryIDs))
	for i, id := range repositoryIDs {
		sorted[i] = string(id)
	}
	sort.Strings(sorted)
	seed := organizationID + "|" + day + "|" + strings.Join(sorted, ",")
	if rerunTag != "" {
		seed += "|rerun:" + rerunTag
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

// StartManualDailyRerun is StartManualDailyRun for a call that carries a
// rerun tag (generation = ManualDailyRerunGeneration with that tag). The tag
// is the operator's statement "compute this day again": an all-repository call
// is admitted on a day that a scheduled-fanout or post-sync run already
// covers, and the outcome names the run it overrode
// (ManualDailyRunOutcome.CoveredDayOverriddenBy) so the caller can say so
// loudly. Nothing else changes: the tag stays in the generation, so the same
// tag twice for one (org, day, repository set) is still one run, and a call
// without a tag keeps the refusal. The Python daily job has no such check at
// all (it recomputes any day it is asked for); the refusal is a Go-side guard
// against an accidental duplicate, and this is its one narrow exception.
func (store *PostgresStore) StartManualDailyRerun(
	ctx context.Context,
	organizationID, day, generation string,
	repositoryIDs []RepositoryID,
	publisher RunPublisher,
	rerunTag string,
) (ManualDailyRunOutcome, error) {
	if !ValidRerunTag(rerunTag) {
		return ManualDailyRunOutcome{}, ErrInvalidState
	}
	return store.startManualDailyRun(ctx, organizationID, day, generation, repositoryIDs, publisher, true)
}

func (store *PostgresStore) startManualDailyRun(
	ctx context.Context,
	organizationID, day, generation string,
	repositoryIDs []RepositoryID,
	publisher RunPublisher,
	admitCoveredDay bool,
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
	overriddenBy := ""
	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	if len(repositoryIDs) == 0 {
		if _, err := tx.Exec(ctx,
			"SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))",
			"daily_metrics_manual_day", organizationID+":"+day,
		); err != nil {
			return ManualDailyRunOutcome{}, ErrUnavailable
		}
		coveringRunID, err := store.coveringRunForDay(ctx, tx, organizationID, day, generation)
		if err != nil {
			return ManualDailyRunOutcome{}, err
		}
		if coveringRunID != "" {
			if !admitCoveredDay {
				return ManualDailyRunOutcome{}, ErrDayAlreadyCovered
			}
			overriddenBy = coveringRunID
		}
	}

	var alreadyStarted bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM public.daily_metrics_runs WHERE id = $1::uuid)`,
		newRun(uuid.MustParse(organizationID).String(), targetDay, generation).ID,
	).Scan(&alreadyStarted); err != nil {
		return ManualDailyRunOutcome{}, ErrUnavailable
	}

	if alreadyStarted {
		// A replay of the same tag starts nothing: it overrides nothing.
		overriddenBy = ""
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
	return ManualDailyRunOutcome{
		Day: day, RunID: run.ID, Generation: generation, AlreadyStarted: alreadyStarted,
		CoveredDayOverriddenBy: overriddenBy,
	}, nil
}
