package syncreconciler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// # CHAOS-5453: a provider unit whose delivery is TERMINAL and whose River row
// is completed or gone
//
// Sync run 1410329c sat at 62/63 units for eight days. Its last unit,
// e784daf9 (github/cicd), was `dispatching` with attempts=2 -- a handler had
// run, twice, and asked for a continuation. Its worker_job_outbox row
// 74e5f374 was `delivered`, attempt_count 1 of 5, naming river_job 103174,
// which River's cleaner had already removed after the job COMPLETED.
//
// Nothing in the system could reach it:
//
//   - The dispatcher republishes the unit every countdown. joboutbox's
//     producer finds the existing row by dedupe key, agrees on
//     kind/version/payload_hash, and -- since CHAOS-5428 -- returns
//     ErrDeliveryAlreadyTerminal, which the dispatcher logs and moves past.
//     The relay never claims a 'delivered' row again, so the republish puts no
//     delivery in front of the unit. There is no lease to expire and the
//     outbox attempt budget never moves, so no timeout ever fires.
//   - joboutbox.StrandRepair's provider-unit shape INNER JOINs river_job (no
//     row, no candidate) and additionally requires unit.attempts = 0 and a
//     'discarded'/'cancelled' job.
//   - joboutbox.TerminalDeliveryRepair requires 'discarded' plus a
//     unique-state bitmask that sync.provider_unit's own UniqueOptsByStateDefault
//     can never satisfy (CHAOS-5453 addendum).
//   - syncreconciler.UnreclaimableSweep -- the destroy path -- also INNER JOINs
//     river_job and also requires unit.attempts = 0.
//
// # Why this is a NEW outbox row and not a re-arm of the terminal one
//
// Every sibling repair returns a terminal row to 'pending'. This one must not,
// for two independent reasons, and they matter in opposite directions:
//
//  1. CORRECTNESS. joboutbox's relay inserts provider units with
//     UniqueOpts{ByArgs: true, ByState: UniqueOptsByStateDefault()}
//     (inserter.go's uniqueStatesForKind). That state set INCLUDES 'completed'.
//     A row re-armed in place carries the same idempotency key, so the same
//     River unique key; when the completed job's row is still present, River
//     dedupes the replacement into it and the relay's own verifyInsertResult
//     reports a delivery that never happened. The sibling repairs are safe from
//     this only because they accept 'discarded'/'cancelled' -- the two states
//     that set deliberately EXCLUDES. A repair whose whole population is
//     completed-or-reaped deliveries cannot borrow their mechanism.
//  2. TRUTHFULNESS. The unit asked for a CONTINUATION. Delivery 1 really did
//     happen and really did complete; re-arming its row would overwrite the
//     only durable record of that, and would present a fresh delivery as a
//     retry of the finished one. The replacement is a new logical delivery and
//     is recorded as one.
//
// The unique constraint on worker_job_outbox.dedupe_key makes those two
// decisions the same decision: a second live row for this unit REQUIRES a
// second key. Provenance is therefore carried in the key itself --
// "sync.provider_unit:<unit id>/reclaim/<n>" -- and the terminal row is left
// byte for byte as it was.
//
// # Disjointness, decided by River state alone
//
// This repair admits exactly two River verdicts, and neither is reachable by
// any other path:
//
//	orphan_delivery_missing    job row absent      -- every sibling INNER JOINs river_job
//	orphan_delivery_completed  state='completed'   -- no sibling admits 'completed' for a provider unit
//
// 'discarded' and 'cancelled' are refused outright and counted as
// skipped_other_repair. They are StrandRepair's and the sweep's population,
// separated between themselves by outbox.attempt_count; adding a third claimant
// on top of that split is how the codebase got its documented overlap problem
// in the first place. Where such a row is unreachable because unit.attempts > 0,
// that is CHAOS-5453 remedy 2 -- widening the sweep's attempts=0 guard -- and it
// is a different change to a different query.
//
// # A residual this repair creates, stated rather than hidden
//
// Both sibling provider-unit queries bind the key with EQUALITY
// (`outbox.dedupe_key = 'sync.provider_unit:' || unit.id::text`), so neither can
// see a row under a reclaim key. If a replacement delivery is later DISCARDED or
// CANCELLED by River, this repair refuses it (skip_other_repair) and no sibling
// can reach it either: that generation stalls until a human looks.
//
// Two things bound how bad that is, and one makes it visible:
//
//   - It cannot cost DATA. The sweep -- the only destroy path -- reads the base
//     key alone and requires a 'cancelled' or budget-spent 'discarded' River job
//     there. For every unit in this repair's population the base delivery is
//     'completed' or reaped, which the sweep drops fail-closed, so it can never
//     terminalize a unit that has a live replacement.
//     TestLive1410329cShapeIsUnreachableByEveryExistingRepairPath measures this.
//   - The refusal is COUNTED, not filtered. skipped_other_repair climbing while
//     re_armed stays flat is exactly this state, and it is on every pass line.
//
// The fix is to widen the sibling bindings from '=' to a key PREFIX, which
// belongs with CHAOS-5453 remedy 2 (widening the sweep's attempts=0 guard) --
// the same family of query, changed once, with its own guard matrix. Doing it
// here would mean editing two merged queries this ticket has no measurement
// for, which is how the overlap above was introduced the first time.
//
// # Two pools, and which write may run where
//
// The pool split is not a preference; it is what the grants permit
// (internal/storage/river/migrate.go):
//
//	queue role   SELECT, UPDATE, DELETE on worker_job_outbox; SELECT on
//	             sync_run_units/sync_runs; USAGE on the river schema.
//	domain role  SELECT, INSERT on worker_job_outbox; SELECT/INSERT/UPDATE on
//	             sync_run_units and sync_runs; NO access to the river schema.
//
// So the River-liveness read can ONLY run on the queue pool and the INSERT can
// ONLY run on the domain pool. Survey on one, mutate on the other, exactly the
// shape docs/contribute/architecture/go-worker-runtime.md prescribes for a
// component whose statements span two jurisdictions. Widening either role to
// collapse them is the thing that rule exists to stop (CHAOS-4035).
//
// # The residual window between the two pools
//
// The survey's River verdict is not re-provable inside the domain transaction.
// It does not need to be: River never resurrects a completed or deleted job, so
// the verdict cannot become false. What CAN change is the domain side, and all
// of it IS re-proved under a row lock on the unit -- the same row the
// dispatcher's own claimUnits locks -- against the surveyed
// (outbox id, dedupe key, river_job_id) TRIPLE rather than the outbox id alone.
// Matching the id alone would be the ABA hole strand_repair's phase 3 documents:
// the triple is the delivery generation, and requiring it makes the insert a
// true compare-and-set.
// # Guard matrix, CHAOS-5453
//
// Every predicate below was mutated one clause at a time against real
// PostgreSQL 18 with River's own migrated schema, in an isolated tree copy
// (this worktree is never mutated; each arm is restored by sha256 before the
// next). 24 of 30 turn a NAMED subtest red. The six that do not are recorded
// here rather than left as an unexplained gap, because "no test went red" and
// "the clause cannot be reached" look identical in a matrix that only counts:
//
//   - job.finalized_at IS NOT NULL on the completed branch -- River's own
//     river_job CHECK forbids a completed row with a NULL finalized_at, so the
//     excluded shape cannot be seeded at all. The guard-matrix arm that would
//     pin it skips itself when River refuses the seed, and starts pinning it
//     the day a River migration drops that constraint.
//   - the three compare-and-set relaxations (source.dedupe_key and
//     source.river_job_id in readOrphanedSourceRowSQL, and the same pair
//     restated in insertReclaimRowSQL) -- exercising them needs another actor
//     to change the outbox row BETWEEN the queue-pool survey and the
//     domain-pool transaction. That is a genuine race guard, and pinning it
//     deterministically would mean injecting the race. What is pinned instead
//     is the rule they enforce, at the layer above: the surveyed triple is
//     matched, not the id alone, and the replacement-key NOT EXISTS in the same
//     statement IS pinned (the_replacement_key_already_exists).
//   - RowsAffected() != 1 and the untargeted ON CONFLICT DO NOTHING -- the
//     INSERT re-states the same compare-and-set the SELECT above it just
//     matched, inside the same transaction, so no reachable state makes it
//     affect a number of rows other than one. Both stay as the cheap detectors
//     for a future predicate change that makes zero reachable.
//
// The full matrix, with the mutation and the killing subtest for each of the
// 24, is in the lane's evidence. Three of those arms (M27-M29) pin the identity
// guards added after round 1 reproduced them as real: an envelope that
// disagrees with itself, a non-positive transport id, and a River job whose
// relay metadata names a different outbox row.
//
// A fourth round-1 finding -- that a replacement already 'delivered' with a
// LIVE River job is not excluded by the live-row guard, so a second delivery
// could be minted on top of it -- was REPRODUCED AND REFUTED: the
// replacement-key NOT EXISTS in readOrphanedSourceRowSQL refuses it, measured
// by the a_replacement_delivery_is_already_out_and_live arm. No code changed;
// the arm stays, because it pins WHICH clause carries that invariant, and the
// answer is not the one the design's prose would lead a reader to expect.
type OrphanedUnitRepair struct {
	queryQueue  func(context.Context, string, ...any) (pgx.Rows, error)
	beginDomain func(context.Context) (pgx.Tx, error)
	surveyQuery string
	staleAge    time.Duration
}

// OrphanedUnitRepairResult is the per-pass tally. EVERY field is reported on
// EVERY pass, including the pass that found nothing -- CHAOS-5456's whole cost
// was a backstop whose silent pass was indistinguishable in the log from one
// that was never reached, and this repair's population is rarer still, so the
// zero is the reading an operator gets almost every tick and has to be able to
// trust.
type OrphanedUnitRepairResult struct {
	// Found counts deliveries this pass surveyed, whatever it then did with
	// them -- the denominator every skip below is a share of.
	Found int
	// ReArmed counts replacement rows actually inserted.
	ReArmed int
	// DeliveryMissing / DeliveryCompleted split ReArmed by the River verdict
	// that licensed it. They are reported separately because they mean
	// different things operationally: a reaped row is River's cleaner doing its
	// job after a completion nobody acted on, while a still-present completed
	// row means the job finished recently enough that the evidence is still
	// there to read.
	DeliveryMissing   int
	DeliveryCompleted int

	SkippedJobLive int
	// SkippedJobIdentity counts a delivery whose River job is not, or is not
	// provably, THIS row's own: a non-positive transport id, a job of another
	// kind, or a job whose relay metadata names a different outbox row.
	SkippedJobIdentity int
	// SkippedEnvelopeIdentity counts a row whose own envelope disagrees with
	// itself -- payload.unit_id is not domain.id. jobcontract does not enforce
	// that equality (ProviderUnitPayload.validate only checks the UUID shape),
	// but internal/jobs/providerunit's handler DOES and answers DomainMismatch,
	// so a replacement minted from such a row is refused on arrival. Counted
	// rather than filtered: a row of this shape means a PRODUCER is writing
	// disagreeing envelopes, which is worth seeing.
	SkippedEnvelopeIdentity int
	SkippedOtherRepair      int
	SkippedReclaimBudget    int
	SkippedNotOrphaned      int
	SkippedPrerequisite     int
	SkippedEnvelope         int
	SkippedRaceLost         int

	RemainingBudget int
}

const (
	// providerUnitReclaimInfix separates the base idempotency key from the
	// reclaim generation. '/' is deliberate: jobcontract's safeIDPattern
	// (^[A-Za-z0-9][A-Za-z0-9._:/-]*$) admits it, so a reclaim key is a legal
	// envelope idempotency key and the replacement row round-trips through
	// jobcontract.Decode and the relay's prepareRow exactly as an original
	// does. A '#' -- the first separator this reached for -- does not.
	providerUnitReclaimInfix = "/reclaim/"

	// maxProviderUnitReclaims bounds this repair the way outbox.attempt_count
	// bounds its siblings. It cannot use attempt_count itself: a NEW row starts
	// at 0, so the counter that separates StrandRepair from the sweep resets
	// here by construction and would license an unbounded rearm loop -- the
	// "requeue into a void" failure repairStrandedWorkGraphSQL's comment names.
	// The generation IS the durable counter, it lives in the key, and it
	// survives every restart and every replica.
	//
	// Three is chosen against the failure it bounds: a transport that kills
	// deliveries deterministically costs at most three replacement rows per
	// unit before this repair stops and the strand becomes visible as a strand
	// again, which is the outcome to want -- a bounded, legible stall rather
	// than a self-renewing one nobody can see the end of.
	maxProviderUnitReclaims = 3

	dispositionOrphanMissing    = "orphan_delivery_missing"
	dispositionOrphanCompleted  = "orphan_delivery_completed"
	dispositionSkipJobIdentity  = "skip_job_identity"
	dispositionSkipEnvelope     = "skip_envelope_identity"
	dispositionSkipOtherRepair  = "skip_other_repair"
	dispositionSkipOrphanedLive = "skip_job_live"
)

// ErrReclaimBudgetSpent is returned by nextProviderUnitReclaimKey when a unit
// has already been handed maxProviderUnitReclaims replacement deliveries. It is
// a named error rather than a bare bool so the refusal reaches the telemetry
// line as its own counter instead of merging into a generic skip.
var ErrReclaimBudgetSpent = errors.New("syncreconciler: provider unit reclaim budget spent")

func NewOrphanedUnitRepair(
	domainPool *pgxpool.Pool,
	queueControlPool *pgxpool.Pool,
	riverSchema string,
) (*OrphanedUnitRepair, error) {
	if domainPool == nil || queueControlPool == nil || !riverSchemaPattern.MatchString(riverSchema) {
		return nil, ErrInvalidConfiguration
	}
	jobTable := pgx.Identifier{riverSchema, "river_job"}.Sanitize()
	return &OrphanedUnitRepair{
		queryQueue:  queueControlPool.Query,
		beginDomain: domainPool.Begin,
		surveyQuery: fmt.Sprintf(selectOrphanedUnitDeliverySQL, jobTable),
		// The same clock the dispatcher's own stale-reclaim uses, read through
		// syncdispatchcontract so an operator's SYNC_UNIT_DISPATCH_STALE_SECONDS
		// moves both together instead of leaving this backstop on a private
		// horizon that quietly disagrees with the loop it backstops.
		staleAge: syncdispatchcontract.DispatchStaleAge(),
	}, nil
}

// orphanedCandidate is one surveyed delivery. The triple
// (outboxID, dedupeKey, riverJobID) is the delivery GENERATION, and phase 2
// re-proves all three; see the type doc's residual-window note.
type orphanedCandidate struct {
	outboxID    string
	dedupeKey   string
	unitID      string
	runID       string
	riverJobID  int64
	disposition string
}

// Step runs one bounded pass: survey on the queue pool, mutate on the domain
// pool.
func (repair *OrphanedUnitRepair) Step(
	ctx context.Context,
	now time.Time,
	limit int,
) (OrphanedUnitRepairResult, error) {
	if repair == nil || repair.queryQueue == nil || repair.beginDomain == nil ||
		ctx == nil || now.IsZero() ||
		limit < minimumStepLimit || limit > maximumStepLimit {
		return OrphanedUnitRepairResult{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return OrphanedUnitRepairResult{}, err
	}
	now = now.UTC()
	result := OrphanedUnitRepairResult{RemainingBudget: limit}

	candidates, err := repair.survey(ctx, now, limit)
	if err != nil {
		repair.logOutcome(ctx, now, result)
		return result, err
	}
	result.Found = len(candidates)

	approved := make([]orphanedCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		switch candidate.disposition {
		case dispositionOrphanMissing, dispositionOrphanCompleted:
			approved = append(approved, candidate)
		case dispositionSkipOrphanedLive:
			result.SkippedJobLive++
		case dispositionSkipJobIdentity:
			result.SkippedJobIdentity++
		case dispositionSkipEnvelope:
			result.SkippedEnvelopeIdentity++
		case dispositionSkipOtherRepair:
			result.SkippedOtherRepair++
		default:
			// The disposition is chosen by this repair's own CASE, so an
			// unknown one means a row was surveyed under a branch this code
			// does not know it has. Fail the pass rather than silently drop it
			// or, worse, act on it.
			repair.logOutcome(ctx, now, result)
			return result, fmt.Errorf("orphaned-unit disposition %q: %w", candidate.disposition, ErrUnavailable)
		}
	}
	if len(approved) == 0 {
		repair.logOutcome(ctx, now, result)
		return result, nil
	}

	// The whole mutating half is ONE transaction, so its counters are merged
	// only after the commit succeeds. This is the OPPOSITE of what
	// joboutbox.StrandRepair does, and deliberately so: that repair commits per
	// shape, so a later failure must never erase an earlier shape's already
	// durable work. Here a failure means nothing was written at all, and
	// reporting re_armed > 0 for a rolled-back transaction would put a number
	// an operator alerts on behind work that did not happen.
	committed, err := repair.rearm(ctx, now, approved)
	if err != nil {
		repair.logOutcome(ctx, now, result)
		return result, err
	}
	result.merge(committed)
	result.RemainingBudget = limit - result.ReArmed
	repair.logOutcome(ctx, now, result)
	for _, record := range committed.rearms {
		// Emitted AFTER the commit, for the same reason the counters are merged
		// after it: a line claiming a re-arm that rolled back is worse than no
		// line, because it is the line an operator would trust.
		slog.InfoContext(ctx, "syncreconciler.orphaned_unit_rearmed",
			"unit_id", record.unitID,
			"sync_run_id", record.runID,
			"source_outbox_id", record.sourceOutboxID,
			"source_dedupe_key", record.sourceDedupeKey,
			"source_river_job_id", record.sourceRiverJobID,
			"dedupe_key", record.dedupeKey,
			"evidence", record.evidence,
		)
	}
	return result, nil
}

// committedOutcome is the mutating half's tally, kept out of the pass result
// until the transaction that produced it commits. rearms carries the per-row
// log lines for the same reason.
type committedOutcome struct {
	reArmed              int
	deliveryMissing      int
	deliveryCompleted    int
	skippedReclaimBudget int
	skippedNotOrphaned   int
	skippedPrerequisite  int
	skippedEnvelope      int
	skippedRaceLost      int
	rearms               []rearmRecord
}

// rearmRecord is one committed re-arm, named by both ends of the lineage: the
// terminal delivery it replaces and the reclaim key it replaces it with.
type rearmRecord struct {
	unitID           string
	runID            string
	sourceOutboxID   string
	sourceDedupeKey  string
	sourceRiverJobID int64
	dedupeKey        string
	evidence         string
}

func (result *OrphanedUnitRepairResult) merge(committed committedOutcome) {
	result.ReArmed += committed.reArmed
	result.DeliveryMissing += committed.deliveryMissing
	result.DeliveryCompleted += committed.deliveryCompleted
	result.SkippedReclaimBudget += committed.skippedReclaimBudget
	result.SkippedNotOrphaned += committed.skippedNotOrphaned
	result.SkippedPrerequisite += committed.skippedPrerequisite
	result.SkippedEnvelope += committed.skippedEnvelope
	result.SkippedRaceLost += committed.skippedRaceLost
}

func (repair *OrphanedUnitRepair) survey(
	ctx context.Context, now time.Time, limit int,
) ([]orphanedCandidate, error) {
	rows, err := repair.queryQueue(ctx, repair.surveyQuery, now, now.Add(-repair.staleAge), limit)
	if err != nil {
		return nil, repair.failed(ctx, "survey", err)
	}
	defer rows.Close()
	candidates := make([]orphanedCandidate, 0, limit)
	for rows.Next() {
		var candidate orphanedCandidate
		if err := rows.Scan(
			&candidate.outboxID, &candidate.dedupeKey, &candidate.unitID,
			&candidate.runID, &candidate.riverJobID, &candidate.disposition,
		); err != nil {
			return nil, repair.failed(ctx, "survey_scan", err)
		}
		if !uuidPattern.MatchString(candidate.outboxID) || !uuidPattern.MatchString(candidate.unitID) {
			return nil, repair.failed(ctx, "survey_identity", ErrUnavailable)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, repair.failed(ctx, "survey_rows", err)
	}
	if len(candidates) > limit {
		return nil, repair.failed(ctx, "survey_bound", ErrUnavailable)
	}
	return candidates, nil
}

// rearm opens ONE domain transaction for the pass. Every per-candidate refusal
// is decided before any statement runs for that candidate, so a refusal never
// aborts the transaction and never costs an already-committed sibling its
// insert; only a genuine database failure ends the pass.
func (repair *OrphanedUnitRepair) rearm(
	ctx context.Context,
	now time.Time,
	approved []orphanedCandidate,
) (committedOutcome, error) {
	outcome := committedOutcome{}
	tx, err := repair.beginDomain(ctx)
	if err != nil || tx == nil {
		return committedOutcome{}, repair.failed(ctx, "domain_begin", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, candidate := range approved {
		newKey, err := nextProviderUnitReclaimKey(candidate.dedupeKey, candidate.unitID)
		if errors.Is(err, ErrReclaimBudgetSpent) {
			outcome.skippedReclaimBudget++
			continue
		}
		if err != nil {
			// A key this repair cannot parse is a row it does not understand.
			// Counted with the envelope refusals rather than acted on.
			outcome.skippedEnvelope++
			continue
		}

		// Lock the unit and re-prove every DOMAIN fact the survey asserted.
		// This is the serialization point against the dispatcher, which locks
		// the same row in claimUnits.
		var locked bool
		lockErr := tx.QueryRow(ctx, lockOrphanedUnitSQL,
			now, now.Add(-repair.staleAge), candidate.unitID).Scan(&locked)
		if errors.Is(lockErr, pgx.ErrNoRows) {
			outcome.skippedNotOrphaned++
			continue
		}
		if lockErr != nil {
			return committedOutcome{}, repair.failed(ctx, "unit_lock", lockErr)
		}

		var args string
		var prerequisite string
		sourceErr := tx.QueryRow(ctx, readOrphanedSourceRowSQL,
			candidate.outboxID, candidate.dedupeKey, candidate.riverJobID,
			providerUnitKeyPrefix(candidate.unitID), newKey,
		).Scan(&args, &prerequisite)
		if errors.Is(sourceErr, pgx.ErrNoRows) {
			// The triple no longer matches, a live row appeared for this unit,
			// or the replacement key already exists. All three mean another
			// actor got there first.
			outcome.skippedRaceLost++
			continue
		}
		if sourceErr != nil {
			return committedOutcome{}, repair.failed(ctx, "source_read", sourceErr)
		}
		if prerequisite != "" {
			// PublishAfter's durable fence. No provider-unit publisher sets one
			// today; refusing rather than silently dropping or blindly copying
			// it keeps a shape this repair has never seen out of the write path
			// AND visible in the counters.
			outcome.skippedPrerequisite++
			continue
		}

		replacement, hash, err := reclaimEnvelope(args, newKey)
		if err != nil {
			outcome.skippedEnvelope++
			continue
		}

		tag, err := tx.Exec(ctx, insertReclaimRowSQL,
			joboutbox.OutboxRowID(newKey), newKey, replacement, hash, now,
			candidate.outboxID, candidate.dedupeKey, candidate.riverJobID,
		)
		if err != nil {
			return committedOutcome{}, repair.failed(ctx, "insert_reclaim", err)
		}
		if tag.RowsAffected() != 1 {
			// ON CONFLICT DO NOTHING, or the SELECT-side CAS stopped matching
			// between the read above and this statement.
			outcome.skippedRaceLost++
			continue
		}
		outcome.reArmed++
		if candidate.disposition == dispositionOrphanMissing {
			outcome.deliveryMissing++
		} else {
			outcome.deliveryCompleted++
		}
		// The unit id and the key it was re-armed under are the only handles an
		// operator gets: River's own record of why the delivery died is either
		// gone or a completion, so there is nothing to correlate against unless
		// this says so. HELD until the commit -- see Step.
		outcome.rearms = append(outcome.rearms, rearmRecord{
			unitID:           candidate.unitID,
			runID:            candidate.runID,
			sourceOutboxID:   candidate.outboxID,
			sourceDedupeKey:  candidate.dedupeKey,
			sourceRiverJobID: candidate.riverJobID,
			dedupeKey:        newKey,
			evidence:         candidate.disposition,
		})
	}

	if err := tx.Commit(ctx); err != nil {
		return committedOutcome{}, repair.failed(ctx, "domain_commit", err)
	}
	return outcome, nil
}

// failed logs the failing step WITH the driver's SQLSTATE before collapsing to
// ErrUnavailable. The collapse is deliberate -- the pipeline classifies this
// stage as continue-safe -- but a bare step name tells an operator nothing: a
// missing grant (42501), a missing relation (42P01) and a serialization failure
// (40001) all read identically once collapsed, and this repair's INSERT is the
// FIRST domain-role write to worker_job_outbox anywhere in the reconciler, so a
// grant fault here is a live deployment risk rather than a theoretical one.
func (repair *OrphanedUnitRepair) failed(ctx context.Context, step string, cause error) error {
	sqlstate := ""
	detail := ""
	if cause != nil {
		detail = cause.Error()
		var pgErr *pgconn.PgError
		if errors.As(cause, &pgErr) {
			sqlstate = pgErr.Code
		}
	}
	slog.ErrorContext(ctx, "syncreconciler.orphaned_unit_repair_failed",
		"step", step,
		"sqlstate", sqlstate,
		"error", detail,
	)
	if cause == nil {
		return fmt.Errorf("orphaned-unit %s: %w", step, ErrUnavailable)
	}
	// BOTH wrapped, deliberately. The pipeline's stageSQLState (pipeline.go)
	// recovers a stage's SQLSTATE with errors.As over *pgconn.PgError -- so
	// collapsing to ErrUnavailable alone kept the SQLSTATE in THIS file's log
	// line but dropped it from the stage-level failure telemetry an operator
	// actually alerts on. Wrapping both keeps errors.Is(ErrUnavailable) true
	// for the caller's continue-safe classification AND leaves the driver error
	// reachable for the stage counter (r1 finding F5).
	return fmt.Errorf("orphaned-unit %s: %w: %w", step, ErrUnavailable, cause)
}

func (repair *OrphanedUnitRepair) logOutcome(
	ctx context.Context, now time.Time, result OrphanedUnitRepairResult,
) {
	staleSeconds := int64(0)
	if repair != nil {
		staleSeconds = int64(repair.staleAge.Seconds())
	}
	slog.InfoContext(ctx, "syncreconciler.orphaned_unit_pass",
		"orphaned_units_found", result.Found,
		"re_armed", result.ReArmed,
		"delivery_missing", result.DeliveryMissing,
		"delivery_completed", result.DeliveryCompleted,
		"skipped_job_live", result.SkippedJobLive,
		"skipped_job_identity", result.SkippedJobIdentity,
		"skipped_envelope_identity", result.SkippedEnvelopeIdentity,
		"skipped_other_repair", result.SkippedOtherRepair,
		"skipped_reclaim_budget", result.SkippedReclaimBudget,
		"skipped_not_orphaned", result.SkippedNotOrphaned,
		"skipped_prerequisite", result.SkippedPrerequisite,
		"skipped_envelope", result.SkippedEnvelope,
		"skipped_race_lost", result.SkippedRaceLost,
		"remaining_budget", result.RemainingBudget,
		"stale_age_seconds", staleSeconds,
		"max_reclaims", maxProviderUnitReclaims,
		"now", now.UTC().Format(time.RFC3339Nano),
	)
}

// providerUnitKeyPrefix is the LIKE pattern covering a unit's base key and
// every reclaim generation of it. unit is a UUID rendered by Postgres, so it
// carries no LIKE metacharacter and needs no escape.
func providerUnitKeyPrefix(unitID string) string {
	return jobcontract.KindSyncProviderUnit + ":" + unitID + "%"
}

// nextProviderUnitReclaimKey derives generation n+1 from the surveyed key.
//
// The base key ("sync.provider_unit:<unit id>", written by
// NativeDispatchSyncRunService) is generation 0. Anything else must be this
// repair's own "<base>/reclaim/<n>", and the unit id embedded in it must match
// the unit the survey joined -- a key that names a DIFFERENT unit is not a
// generation of this one, and deriving n+1 from it would mint a delivery
// against the wrong domain row.
func nextProviderUnitReclaimKey(dedupeKey, unitID string) (string, error) {
	base := jobcontract.KindSyncProviderUnit + ":" + unitID
	if dedupeKey == base {
		return base + providerUnitReclaimInfix + "1", nil
	}
	suffix, found := strings.CutPrefix(dedupeKey, base+providerUnitReclaimInfix)
	if !found {
		return "", fmt.Errorf("unrecognized provider-unit dedupe key: %w", ErrInvalidConfiguration)
	}
	generation, err := strconv.Atoi(suffix)
	if err != nil || generation < 1 {
		return "", fmt.Errorf("unrecognized provider-unit reclaim generation: %w", ErrInvalidConfiguration)
	}
	if generation >= maxProviderUnitReclaims {
		return "", ErrReclaimBudgetSpent
	}
	return base + providerUnitReclaimInfix + strconv.Itoa(generation+1), nil
}

// reclaimEnvelope rebuilds the source envelope under a new idempotency key and
// returns it with its payload hash.
//
// It goes through jobcontract.Decode and MarshalCanonical rather than editing
// the stored JSON, because those two functions are exactly what the relay's
// prepareRow re-runs before it will insert anything: prepareRow rejects a row
// unless envelope.IdempotencyKey == row.DedupeKey AND
// canonicalHash(MarshalCanonical(Decode(args))) == row.PayloadHash. A hand-patched
// args string that happened to disagree with either would produce a row the relay
// silently refuses forever -- a strand replacing a strand.
//
// Everything else is carried over untouched, TraceParent included: the
// replacement delivery belongs to the same investigation as the one it
// replaces.
func reclaimEnvelope(args string, idempotencyKey string) (string, string, error) {
	envelope, err := jobcontract.Decode(jobcontract.KindSyncProviderUnit, []byte(args))
	if err != nil {
		return "", "", err
	}
	envelope.IdempotencyKey = idempotencyKey
	encoded, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		return "", "", err
	}
	// Round-tripped for the same reason joboutbox's producer does it: a key
	// that does not survive Decode would be accepted here and rejected by the
	// relay, which is the failure this whole file exists to stop happening
	// twice.
	decoded, err := jobcontract.Decode(jobcontract.KindSyncProviderUnit, encoded)
	if err != nil || decoded.IdempotencyKey != idempotencyKey {
		return "", "", fmt.Errorf("reclaim key not round-trippable: %w", ErrInvalidConfiguration)
	}
	digest := sha256.Sum256(encoded)
	return string(encoded), "sha256:" + hex.EncodeToString(digest[:]), nil
}

// providerUnitDomainIdentity binds the outbox row to its unit four ways -- the
// envelope's domain type, the domain id behind a UUID format guard, the org,
// and the derived key prefix -- mirroring the sibling provider-unit repairs
// rather than inventing a second convention for the same table. The domain id
// is cast to uuid rather than unit.id being cast to text: CHAOS-4092 turned
// exactly that inversion into a 9.5-hour crash loop, because a text cast of the
// primary key is not sargable against it.
const providerUnitDomainIdentity = `
	JOIN public.sync_run_units AS unit
		ON unit.id = CASE
			WHEN (outbox.args #>> '{domain,id}') ~
				'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
			THEN (outbox.args #>> '{domain,id}')::uuid
			ELSE NULL
		END
		AND unit.org_id = outbox.args ->> 'organization_id'
	JOIN public.sync_runs AS run
		ON run.id = unit.sync_run_id
		AND run.org_id = unit.org_id`

// orphanedUnitIdlePredicate is the ONE definition of "no genuine progress on
// this unit for a stale window", shared by the queue-side survey and the
// domain-side lock.
//
// It hangs off last_heartbeat_at, falling back to created_at -- deliberately
// NOT updated_at, which is the column the sibling sweep gates on. The
// dispatcher's stale reclaim re-stamps updated_at on EVERY redispatch pass, and
// for exactly this population every one of those passes is a no-op that put no
// delivery in front of the unit (production reached 1,344 of them on run
// 1410329c). An idle gate on updated_at would therefore be held open forever by
// the very loop this repair exists to break, and would never once fire.
// CHAOS-5453 remedy 3 names a durable last_progress_at column as the real fix;
// this is the interim it also names, and it needs no migration.
const orphanedUnitIdlePredicate = `COALESCE(unit.last_heartbeat_at, unit.created_at) <= $2`

// orphanedUnitDomainPredicate is the ONE definition of the domain half of "this
// unit is stranded behind a settled delivery". Shared verbatim between the
// survey and the FOR UPDATE re-read so the two cannot drift into disagreeing
// about what is eligible -- the same reason 5456's ready-finalizer backstop
// collapsed its two look-alike predicate pairs into shared constants after
// discovering each copy masked the other's mutations.
//
// A RUNNING unit is never selected, and neither is one holding any lease: both
// mean a handler is alive and the outbox row's terminal status is simply the
// finished delivery of work still in flight.
const orphanedUnitDomainPredicate = `unit.status = 'dispatching'
	AND unit.lease_owner IS NULL
	AND unit.lease_expires_at IS NULL
	AND (unit.available_at IS NULL OR unit.available_at <= $1)
	AND ` + orphanedUnitIdlePredicate + `
	AND ` + nonterminalSyncRunStatusPredicate

// noLiveDeliveryForUnit refuses a unit that already has an executable delivery
// under ANY generation of its key. Without it a pass could mint generation 2
// while generation 1 sits 'pending' in the relay's own queue, which is a
// duplicate delivery of the same work -- the one outcome a repair must never
// produce.
//
// It is a FUNCTION over the key expression rather than two hand-kept copies:
// the survey reaches the unit through a join and phrases the pattern as
// `'sync.provider_unit:' || unit.id::text || '%'`, while the compare-and-set
// has only a bind parameter, and those are the only two things that differ.
// 5456's ready-finalizer backstop found the hard way that two look-alike
// predicates mask each other's mutations -- neither copy can be pinned,
// because the other still refuses the row.
//
// 'pending' and 'claimed' are named POSITIVELY rather than as NOT IN
// ('delivered','dead'), so a status added to ck_worker_job_outbox_status later
// is treated as non-executable by default: that direction leaves a strand
// visible, the other direction double-delivers.
func noLiveDeliveryForUnit(keyPattern string) string {
	return `NOT EXISTS (
		SELECT 1 FROM public.worker_job_outbox AS live
		WHERE live.dedupe_key LIKE ` + keyPattern + `
			AND live.status IN ('pending', 'claimed')
	)`
}

// selectOrphanedUnitDeliverySQL is the queue-pool survey. It never filters on
// River state; it REPORTS it, so a refusal is counted instead of vanishing --
// this package's standing rule, and the reason UnreclaimableSweep's own
// deferred-to-repair count exists.
//
// The join to river_job is a LEFT JOIN on the bigint primary key. That is the
// single most important line in this file: every sibling repair INNER JOINs it,
// which is precisely why a delivery whose row River's cleaner has removed is
// invisible to all of them. river_job_id IS NOT NULL is guaranteed by
// ck_worker_job_outbox_delivery_state whenever status = 'delivered', so a NULL
// job.id here means one thing only -- the recorded, well-formed job identity no
// longer has a row.
var selectOrphanedUnitDeliverySQL = `
	SELECT outbox.id::text, outbox.dedupe_key, unit.id::text, unit.sync_run_id::text,
		outbox.river_job_id,
		CASE
			WHEN outbox.args #>> '{payload,unit_id}'
				IS DISTINCT FROM outbox.args #>> '{domain,id}'
				THEN '` + dispositionSkipEnvelope + `'
			WHEN outbox.river_job_id <= 0 THEN '` + dispositionSkipJobIdentity + `'
			WHEN job.id IS NULL THEN '` + dispositionOrphanMissing + `'
			WHEN job.kind <> outbox.job_kind THEN '` + dispositionSkipJobIdentity + `'
			WHEN job.metadata ->> 'worker_outbox_id'
				IS DISTINCT FROM outbox.id::text
				THEN '` + dispositionSkipJobIdentity + `'
			WHEN job.state::text = 'completed' AND job.finalized_at IS NOT NULL
				THEN '` + dispositionOrphanCompleted + `'
			WHEN job.state::text IN ('discarded', 'cancelled')
				THEN '` + dispositionSkipOtherRepair + `'
			ELSE '` + dispositionSkipOrphanedLive + `'
		END AS disposition
	FROM public.worker_job_outbox AS outbox` + providerUnitDomainIdentity + `
	LEFT JOIN %s AS job
		ON job.id = outbox.river_job_id
	WHERE outbox.job_kind = 'sync.provider_unit'
		AND outbox.status = 'delivered'
		AND outbox.args #>> '{domain,type}' = 'sync_run_unit'
		AND outbox.dedupe_key LIKE 'sync.provider_unit:' || unit.id::text || '%%'
		AND ` + orphanedUnitDomainPredicate + `
		AND ` + noLiveDeliveryForUnit(`'sync.provider_unit:' || unit.id::text || '%%'`) + `
	ORDER BY outbox.delivered_at, outbox.id
	LIMIT $3`

// lockOrphanedUnitSQL re-proves the domain half under a row lock on the unit --
// the same row internal/syncdispatchruntime's claimUnits locks, which is what
// serializes this repair against a concurrent redispatch pass.
//
// Only the unit is locked. sync_runs is read in the same statement but not
// locked: the run's terminal statuses are monotonic (a run that is not terminal
// now cannot have been terminal a moment ago), so a shared read is sufficient
// and locking a row every dispatch pass also touches is not worth the
// contention.
const lockOrphanedUnitSQL = `
	SELECT TRUE
	FROM public.sync_run_units AS unit
	JOIN public.sync_runs AS run
		ON run.id = unit.sync_run_id
		AND run.org_id = unit.org_id
	WHERE unit.id = $3::uuid
		AND ` + orphanedUnitDomainPredicate + `
	FOR UPDATE OF unit`

// readOrphanedSourceRowSQL is the compare-and-set. It matches the surveyed
// (id, dedupe_key, river_job_id) TRIPLE, re-checks that the row is still
// terminal, re-checks that no live delivery appeared for the unit, and refuses
// if the replacement key already exists. Returning no row is the race being
// lost, which the caller counts.
var readOrphanedSourceRowSQL = `
	SELECT source.args::text, COALESCE(source.prerequisite_completion_key, '')
	FROM public.worker_job_outbox AS source
	WHERE source.id = $1::uuid
		AND source.dedupe_key = $2
		AND source.river_job_id = $3
		AND source.status = 'delivered'
		AND ` + noLiveDeliveryForUnit(`$4`) + `
		AND NOT EXISTS (
			SELECT 1 FROM public.worker_job_outbox AS existing
			WHERE existing.dedupe_key = $5
		)`

// insertReclaimRowSQL mints the replacement. Every immutable field except the
// identity is COPIED from the source row inside the same statement rather than
// re-derived from the checked-in registry: the source row was validated against
// that registry when it was published, and the relay's prepareRow validates
// queue/priority/max_attempts against it again before delivery, so re-deriving
// here would add a third opinion that can disagree with both.
//
// prerequisite_completion_key is copied rather than written NULL even though
// the caller refuses any source row that carries one. The two are not
// redundant: the refusal is the DECISION -- a durable fence this repair has
// never seen must be neither silently dropped nor silently honored, so it is
// counted and skipped -- while the copy is what keeps this statement correct on
// the day that decision changes. A hardcoded NULL would quietly strip the fence
// the moment the caller started admitting one.
//
// The SELECT re-states the compare-and-set so the read and the write cannot be
// separated by a concurrent change even within this transaction. ON CONFLICT DO
// NOTHING is untargeted deliberately: the derived id and the dedupe key are
// both unique and both functions of the same string, so which index reports the
// collision is not this statement's business.
const insertReclaimRowSQL = `
	INSERT INTO public.worker_job_outbox (
		id, dedupe_key, job_kind, contract_version, args, payload_hash,
		queue, priority, max_attempts, scheduled_at, status, attempt_count,
		next_attempt_at, prerequisite_completion_key, created_at, updated_at
	)
	SELECT $1::uuid, $2, source.job_kind, source.contract_version, $3::json, $4,
		source.queue, source.priority, source.max_attempts, $5, 'pending', 0,
		$5, source.prerequisite_completion_key, $5, $5
	FROM public.worker_job_outbox AS source
	WHERE source.id = $6::uuid
		AND source.dedupe_key = $7
		AND source.river_job_id = $8
		AND source.status = 'delivered'
	ON CONFLICT DO NOTHING`
