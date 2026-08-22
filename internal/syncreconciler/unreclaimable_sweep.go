package syncreconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The unreclaimable-dispatching sweep (CHAOS-4005, porting CHAOS-3990).
//
// Lease repair can only reach a RUNNING unit whose lease expired. A unit that
// was claimed to 'dispatching' and never published holds no lease at all, so
// there is nothing to expire and no mechanism in the system can reach it: it
// keeps a per-bucket concurrency slot forever and its run can never finalize.
// That is the strand CHAOS-3990 diagnosed in production.
//
// CHAOS-3990 shipped this sweep in Python, but the 60s reconcile cadence is
// owned by THIS package (internal/scheduler/fixed/inventory.go), with no
// bridge, so the Python implementation never executed in production. This is
// the port. The Python version is the specification; every guard below exists
// because an adversarial review found the naive form broken, and each is
// annotated with what it prevents.
// DefaultUnreclaimableAge and DefaultUnreclaimableIdle mirror Python's
// SYNC_UNIT_UNRECLAIMABLE_SECONDS (3600) and SYNC_UNIT_DISPATCH_STALE_SECONDS
// (900) defaults.
const (
	DefaultUnreclaimableAge   = time.Hour
	DefaultUnreclaimableIdle  = 15 * time.Minute
	unreclaimableDefaultAge   = time.Hour
	unreclaimableMinimumLimit = 1
	unreclaimableMaximumLimit = 100
	// Bounded work per pass, expressed as rows SCANNED rather than returned.
	// The filters run after the SQL window, so a degraded state -- a large
	// backlog of old dispatching rows that all belong to River -- must not walk
	// the whole table every 60s.
	//
	// Paging is KEYSET, not OFFSET, which is what makes this bound safe. An
	// offset-based cap restarts at zero every pass, so a persistent prefix of
	// ineligible rows would hide a strand behind it forever (review finding).
	// A keyset cursor resumes past that prefix within the pass, and because it
	// is ordered by the same (created_at, id) the query is, it also cannot skip
	// or double-count rows when the result set shifts between snapshots.
	unreclaimableMaximumScan    = 1000
	unreclaimableErrorCategory  = "feature_disabled"
	unreclaimableProviderUnitID = "sync.provider_unit"
	// unreclaimableTerminalDeliveryCategory is a SEPARATE durable category
	// from unreclaimableErrorCategory, and deliberately so (CHAOS-4097).
	//
	// The two branches of this sweep terminalize for opposite reasons and an
	// operator has to be able to tell them apart from the row alone:
	//
	//   - feature_disabled means the capability matrix declines the pair, so
	//     nothing was ever going to run it. That string is load-bearing
	//     elsewhere -- sync/canonical_incident_gate.py's
	//     FEATURE_DISABLED_ERROR_CATEGORY and sync/dispatch_outbox.py's
	//     _TERMINAL_DENIAL_ERROR are the same token -- and reusing it for a
	//     dead delivery would file an infrastructure failure as a deliberate
	//     feature denial, hiding it from exactly the reporting that should
	//     surface it.
	//   - terminal_river_delivery means the pair was routable, the unit WAS
	//     published, and its River job then died terminally. That is an
	//     incident, not a denial.
	//
	// The exact River state is carried in the reason string and in the result
	// payload rather than being encoded into more category tokens, so a state
	// River adds later widens the evidence without minting a new vocabulary
	// entry every consumer has to learn.
	unreclaimableTerminalDeliveryCategory = "terminal_river_delivery"
)

// PER-STEP ERROR IDENTITY (CHAOS-4035 AC4, the CHAOS-4036 masking class).
//
// This file used to return the bare package-wide ErrUnavailable from fourteen
// distinct failure paths (fifteen now, with the closing route fence). When the sweep's route read started returning 42501
// once a second in production, the only log line an operator had said "sync
// dispatch observer database unavailable" -- naming a component that was
// healthy and uninvolved, and saying nothing about permissions, about
// worker_job_routes, or about which statement failed. Root-causing it needed
// the postgres server log and a hand-run has_table_privilege matrix.
//
// So each path now names its own step, and carries the SQLSTATE when the
// driver supplied one. errors.Is(err, ErrUnavailable) still holds, because
// the pipeline and the lifecycle both branch on that classification and a
// narrowed error that silently stopped matching would be a worse defect than
// the one this fixes.
//
// Only the five-character SQLSTATE is copied out of the driver error, never
// its Message, Detail or Hint: those can carry row values, and the
// "no connection material in an error string" property this package already
// had must survive being made more informative. internal/storage/postgres/
// posture_diagnostics.go is the precedent.
const (
	sweepStepBegin              = "begin domain transaction"
	sweepStepRouteQuery         = "durable route read of public.worker_job_routes on the coordinator pool"
	sweepStepRouteFence         = "durable route re-read fencing the terminalize commit"
	sweepStepRouteFenceBegin    = "begin coordinator transaction for the route fence"
	sweepStepRouteFenceTimeout  = "arm the route fence lock timeout"
	sweepStepRouteScan          = "durable route read scan"
	sweepStepRouteRows          = "durable route read iteration"
	sweepStepCandidateQuery     = "candidate page read of public.sync_run_units"
	sweepStepCandidateScan      = "candidate page scan"
	sweepStepCandidateRows      = "candidate page iteration"
	sweepStepOutboxQuery        = "published-outbox filter on public.worker_job_outbox"
	sweepStepOutboxScan         = "published-outbox scan"
	sweepStepOutboxRows         = "published-outbox iteration"
	sweepStepJobStateQuery      = "delivery liveness read of river_job on the queue-control pool"
	sweepStepJobStateScan       = "delivery liveness scan"
	sweepStepJobStateRows       = "delivery liveness iteration"
	sweepStepTerminalizePayload = "terminalize payload encode"
	sweepStepTerminalizeExec    = "terminalize write to public.sync_run_units"
	sweepStepTerminalizeRows    = "terminalize affected-row count"
	sweepStepCommit             = "commit domain transaction"
)

// unreclaimableStepError names the sweep step that failed while keeping the
// package-stable ErrUnavailable classification callers branch on.
type unreclaimableStepError struct {
	step     string
	sqlState string
}

func (stepErr unreclaimableStepError) Error() string {
	if stepErr.sqlState != "" {
		return "unreclaimable sweep " + stepErr.step + " failed (sqlstate " +
			stepErr.sqlState + "): " + ErrUnavailable.Error()
	}
	return "unreclaimable sweep " + stepErr.step + " failed: " + ErrUnavailable.Error()
}

// Unwrap is what keeps errors.Is(err, ErrUnavailable) true for every step.
func (stepErr unreclaimableStepError) Unwrap() error { return ErrUnavailable }

// sweepUnavailable builds the step-identified error. cause is used ONLY to
// recover a SQLSTATE; it is never wrapped, so no driver string can reach a log
// line through this path.
func sweepUnavailable(step string, cause error) error {
	stepErr := unreclaimableStepError{step: step}
	var pgErr *pgconn.PgError
	if errors.As(cause, &pgErr) {
		stepErr.sqlState = pgErr.Code
	}
	return stepErr
}

// SweepStepIdentity returns the step name carried by a sweep failure, or "" if
// the error did not come from the sweep. Exported so a caller can key a metric
// or an alert on the step rather than on a substring of a message.
func SweepStepIdentity(err error) string {
	var stepErr unreclaimableStepError
	if errors.As(err, &stepErr) {
		return stepErr.step
	}
	return ""
}

// SweepMode decides whether the sweep may write.
//
// It defaults to shadow: this is the safety net that was silently absent from
// production, so its selection is proven against real rows before it is
// allowed to terminalize anything.
type SweepMode string

const (
	// SweepModeOff disables the sweep outright.
	SweepModeOff SweepMode = "off"
	// SweepModeShadow selects and reports without writing. The default, so
	// every deployment gets would-terminalize observability at zero write risk
	// and with no activation step.
	SweepModeShadow SweepMode = "shadow"
	// SweepModeActive permits terminalization.
	//
	// Setting it IS the operator's declaration that no Celery consumer serves
	// provider units for this deployment. That assertion used to be a separate
	// environment variable; collapsing it into the mode keeps one knob instead
	// of two saying the same thing (CHAOS-4020).
	SweepModeActive SweepMode = "active"
)

// ParseSweepMode resolves the operator's choice. An unrecognised value is
// rejected rather than silently defaulting: "active" is an assertion about the
// deployment, so a typo must not quietly become one -- nor quietly disable the
// safety net.
func ParseSweepMode(raw string) (SweepMode, error) {
	switch SweepMode(strings.ToLower(strings.TrimSpace(raw))) {
	case "":
		return SweepModeShadow, nil
	case SweepModeOff:
		return SweepModeOff, nil
	case SweepModeShadow:
		return SweepModeShadow, nil
	case SweepModeActive:
		return SweepModeActive, nil
	default:
		return "", ErrInvalidConfiguration
	}
}

// UnreclaimableSweepConfig is the policy half of the sweep.
type UnreclaimableSweepConfig struct {
	// Age bounds how long a unit may exist without a single attempt before it
	// is presumed dead. Deliberately far longer than the stale-dispatch
	// window: a Celery-published unit legitimately waits in 'dispatching'
	// until a consumer claims it.
	Age time.Duration
	// Idle is the ordinary stale-dispatch window. A unit the dispatcher only
	// just published is never swept.
	Idle time.Duration
	// Mode gates writing. Shadow selects and reports without mutating.
	Mode SweepMode
}

func (config UnreclaimableSweepConfig) valid() bool {
	return config.Age > 0 && config.Idle > 0 &&
		(config.Mode == SweepModeOff || config.Mode == SweepModeShadow ||
			config.Mode == SweepModeActive)
}

// UnreclaimableSweepResult reports what one pass saw and did. Candidates is
// what selection matched; Terminalized is what was actually written, and is
// always zero in shadow mode.
type UnreclaimableSweepResult struct {
	Mode         SweepMode
	Candidates   int
	Terminalized int
	RunIDs       []string
	Pairs        []string
	UnitIDs      []string
	// DeclinedRouteChange reports that the durable route moved underneath this
	// pass and the write was abandoned. It is distinct from a zero
	// Terminalized, which is also what a pass with nothing to do returns.
	DeclinedRouteChange bool
}

// routeFence is the durable route's identity at one instant: which transport
// owns provider units, and the generation stamp every route mutation bumps
// (internal/jobroute/control.go ApplyCheckedIn and Rollback both do
// `generation = generation + 1` under an optimistic CAS on the old value).
//
// The generation is what makes an ownership flip DETECTABLE across two pools.
// A transport comparison alone would miss celery -> river -> celery inside one
// pass; the counter cannot be walked back.
type routeFence struct {
	transport  string
	paused     bool
	generation int64
}

// riverOwns is the whole authorization to destroy work, so it is fail-closed
// on both axes.
//
// PAUSED is load-bearing and was missing (review finding). The comment above
// riverOwnsProviderUnits has always said a "missing, paused, duplicated or
// unreadable row is NOT read as River owns it", but the statement selected
// only `transport`, so the paused half was never implemented. Every other
// reader of this table honours it -- jobroute.Controller.Resolve returns
// ErrPaused -- which means an operator pausing the route during an incident
// stopped producers and relays while leaving the one component that DELETES
// work still running. Pause is the control-plane stop; it must stop this too.
//
// Both `river` and `river_canary` count as River ownership. A canary that has
// been promoted to full River ownership owns provider units MORE completely,
// not less, and refusing to sweep then would disable the safety net exactly
// when it is most correct. Whether the row matches the checked-in policy
// artifact is a different question, and it belongs to the route fence in
// internal/syncroute, not here.
func (fence routeFence) riverOwns() bool {
	if fence.paused {
		return false
	}
	return fence.transport == "river" || fence.transport == "river_canary"
}

// unreclaimableRouteReader is the coordinator-side seam. It is deliberately
// the smallest thing that can run one read: *pgxpool.Pool satisfies it, and so
// does pgx.Tx, which is what lets the unit tests drive it with the same fake
// they already use for the domain transaction.
type unreclaimableRouteReader interface {
	// Query runs the unlocked opening read.
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	// Begin opens the short coordinator transaction that holds the closing
	// fence's row lock across the domain commit.
	Begin(ctx context.Context) (pgx.Tx, error)
}

// UnreclaimableSweep terminalizes units nothing is working.
//
// It spans TWO roles, and therefore two pools (CHAOS-4035):
//
//   - routes reads public.worker_job_routes, which the role manifest
//     attributes exclusively to the COORDINATOR role
//     (internal/storage/postgres/domain_authorization.go coordinatorPosture);
//   - begin opens the transaction that selects candidates and terminalizes
//     them, all on DOMAIN-granted tables, and coordinatorPosture declares
//     sync_run_units SELECT-only, so the write cannot move to the coordinator
//     role either.
//
// Neither pool alone can run the whole sweep. This is the same two-pool split
// the Materializer already uses; cmd/dev-health-reconciler/dependencies.go
// documents the composition.
// unreclaimableJobReader is the QUEUE-CONTROL seam, and it is a third role
// (CHAOS-4097). internal/storage/river/migrate.go grants USAGE on the River
// schema to the queue role and to nobody else, so neither the domain
// transaction below nor the coordinator route reader can see river_job at all:
// this is not a stylistic split, it is the only role that can run the read.
//
// Only Query is needed. The sweep never writes, locks or deletes a River row;
// proving a delivery is dead is a read, and the recovery it authorizes happens
// entirely on the domain side.
type unreclaimableJobReader interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type UnreclaimableSweep struct {
	begin    leaseRepairBeginFunc
	routes   unreclaimableRouteReader
	jobs     unreclaimableJobReader
	jobState string
	config   UnreclaimableSweepConfig
}

// NewUnreclaimableSweep takes both pools because the sweep genuinely needs
// both. The coordinator pool is a separate positional parameter rather than an
// optional field for the reason CHAOS-4035 exists: the first version took one
// pool, the caller passed the domain pool, and the mistake was invisible until
// production answered 42501 once a second.
func NewUnreclaimableSweep(
	coordinatorPool *pgxpool.Pool,
	domainPool *pgxpool.Pool,
	queueControlPool *pgxpool.Pool,
	riverSchema string,
	config UnreclaimableSweepConfig,
) (*UnreclaimableSweep, error) {
	if coordinatorPool == nil || domainPool == nil || queueControlPool == nil ||
		!riverSchemaPattern.MatchString(riverSchema) || !config.valid() {
		return nil, ErrInvalidConfiguration
	}
	return newUnreclaimableSweep(
		coordinatorPool, domainPool.Begin, queueControlPool, riverSchema, config,
	)
}

func newUnreclaimableSweep(
	routes unreclaimableRouteReader,
	begin leaseRepairBeginFunc,
	jobs unreclaimableJobReader,
	riverSchema string,
	config UnreclaimableSweepConfig,
) (*UnreclaimableSweep, error) {
	if routes == nil || begin == nil || jobs == nil ||
		!riverSchemaPattern.MatchString(riverSchema) || !config.valid() {
		return nil, ErrInvalidConfiguration
	}
	return &UnreclaimableSweep{
		begin:  begin,
		routes: routes,
		jobs:   jobs,
		// The schema is interpolated ONCE, here, through pgx.Identifier's
		// quoting, and riverSchemaPattern has already refused anything that is
		// not a bare lower-case identifier. The statement itself is then a
		// constant for the sweep's whole life, so no caller can reach the
		// interpolation.
		jobState: fmt.Sprintf(
			selectTerminalDeliveryStatesSQL,
			pgx.Identifier{riverSchema, "river_job"}.Sanitize(),
		),
		config: config,
	}, nil
}

type unreclaimableCandidate struct {
	id         string
	syncRunID  string
	orgID      string
	provider   string
	datasetKey string
	costClass  string
	createdAt  time.Time
	updatedAt  time.Time
	// delivery is set ONLY on the CHAOS-4097 branch: the unit holds a
	// 'delivered' outbox row whose River job was proven terminal and
	// non-success. Its zero value means "never published", which is the
	// original CHAOS-4005 branch and the one unroutable() gates.
	delivery terminalDelivery
}

// terminalDelivery is the liveness PROOF, carried from the queue-control read
// to the domain write so the write can re-assert it under the same CAS.
//
// jobID is what makes the proof re-checkable. Between the read and the commit
// another component can rearm the outbox row and mint a REPLACEMENT delivery
// (internal/joboutbox.TerminalDeliveryRepair does exactly that), and a unit
// whose replacement is live must not be terminalized. Pinning the pair the
// proof was taken on turns that into a CAS instead of a race -- the same ABA
// lesson internal/joboutbox.StrandRepair's phase-3 lock records.
type terminalDelivery struct {
	dedupeKey string
	jobID     int64
	state     string
}

func (delivery terminalDelivery) proven() bool {
	return delivery.jobID > 0 && delivery.state != "" && delivery.dedupeKey != ""
}

func (candidate unreclaimableCandidate) pair() string {
	return candidate.provider + "/" + candidate.datasetKey
}

// Step runs one bounded pass.
func (sweep *UnreclaimableSweep) Step(
	ctx context.Context,
	now time.Time,
	limit int,
) (UnreclaimableSweepResult, error) {
	if sweep == nil || sweep.begin == nil || sweep.routes == nil || ctx == nil ||
		now.IsZero() ||
		limit < unreclaimableMinimumLimit || limit > unreclaimableMaximumLimit {
		return UnreclaimableSweepResult{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return UnreclaimableSweepResult{}, err
	}
	result := UnreclaimableSweepResult{Mode: sweep.config.Mode}

	if sweep.config.Mode == SweepModeOff {
		return result, nil
	}

	now = now.UTC()

	// Durable route first, exactly as Python orders it. If River does not own
	// provider units, Celery does, and nothing here may terminalize its work.
	//
	// TWO POOLS, and the read runs OUTSIDE the transaction below (CHAOS-4035).
	// worker_job_routes belongs exclusively to the coordinator role, while the
	// candidate selection and the terminalize write belong exclusively to the
	// domain role, so no single transaction can hold both: the first cut ran
	// this read on the domain pool inside the domain transaction and returned
	// 42501 on every tick from its first production deploy.
	//
	// THE TRADE-OFF, stated here rather than left to be rediscovered: this
	// gives up reading the route row and the candidate rows in one snapshot.
	// A route flip that lands between these two statements is therefore acted
	// on one pass late. That is acceptable and deliberate -- Python resolves
	// worker_job_routes in its own separate statement and has exactly the same
	// property, so this is parity with the specification rather than a
	// regression against it -- and the window is one 1s tick against a route
	// flip that is an operator action. Do NOT "fix" this by moving the read
	// back inside the transaction; that is the defect, not a tidier form of it.
	opening, usable, err := readProviderUnitRoute(ctx, sweep.routes, sweepStepRouteQuery)
	if err != nil {
		return UnreclaimableSweepResult{}, err
	}
	if !usable || !opening.riverOwns() {
		return result, nil
	}

	tx, err := sweep.begin(ctx)
	if err != nil || tx == nil {
		return UnreclaimableSweepResult{}, sweepUnavailable(sweepStepBegin, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	candidates, err := sweep.selectUnreclaimable(ctx, tx, now, limit)
	if err != nil {
		return UnreclaimableSweepResult{}, err
	}
	result.Candidates = len(candidates)
	seenRuns := make(map[string]struct{}, len(candidates))
	seenPairs := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		result.UnitIDs = append(result.UnitIDs, candidate.id)
		if _, ok := seenRuns[candidate.syncRunID]; !ok {
			seenRuns[candidate.syncRunID] = struct{}{}
			result.RunIDs = append(result.RunIDs, candidate.syncRunID)
		}
		if _, ok := seenPairs[candidate.pair()]; !ok {
			seenPairs[candidate.pair()] = struct{}{}
			result.Pairs = append(result.Pairs, candidate.pair())
		}
	}

	// Shadow mode reports selection and writes nothing. The transaction is
	// rolled back by the deferred call.
	if sweep.config.Mode != SweepModeActive {
		return result, nil
	}

	for _, candidate := range candidates {
		affected, err := sweep.terminalize(ctx, tx, candidate, now)
		if err != nil {
			return UnreclaimableSweepResult{}, err
		}
		if affected < 0 || affected > 1 {
			return UnreclaimableSweepResult{}, sweepUnavailable(sweepStepTerminalizeRows, nil)
		}
		result.Terminalized += int(affected)
	}

	// ROUTE FENCE (adversarial review finding). The writes above are staged
	// and NOT yet committed.
	//
	// The failure this closes: the sweep reads river_canary, an operator rolls
	// provider units back to Celery, and the sweep then commits 'failed' onto
	// dispatching units Celery is about to claim. Neither existing guard can
	// see it -- the updated_at CAS only catches a runtime that already touched
	// the row, and the outbox filter only catches a unit already in the River
	// relay. Nothing had touched these rows, which is the entire definition of
	// the strand this sweep selects.
	//
	// A comparison alone is not enough, and shipping one while calling the
	// race closed would be worse than the race. The route decision and the
	// write live on different pools, so they cannot share a transaction -- but
	// they CAN be serialized: holdRouteFence takes FOR SHARE on the route row
	// in a short coordinator transaction and HOLDS it across the domain commit
	// below, so a concurrent route UPDATE blocks until this pass has finished
	// deciding. The generation is compared under that lock as well, which
	// catches a flip that committed earlier in the pass, including a
	// celery -> river -> celery round trip the transport alone would miss.
	//
	// The comparison is over the WHOLE fence -- transport, paused and
	// generation -- so a mid-pass pause is caught even if a future pause
	// statement forgets to bump the generation.
	//
	// It is taken here rather than at the opening read on purpose: holding it
	// for the whole pass would block operator route mutations across all the
	// candidate paging, and this is a safety net, not a control plane.
	//
	// Shadow mode never reaches this: it writes nothing, so there is nothing
	// to fence, and it must not block a rollback either.
	held, err := sweep.holdRouteFence(ctx, opening)
	if err != nil {
		return UnreclaimableSweepResult{}, err
	}
	if !held.ok {
		result.Terminalized = 0
		result.DeclinedRouteChange = true
		return result, nil
	}
	// The lock is still held here, so no route mutation can interleave with
	// this commit.
	commitErr := tx.Commit(ctx)
	held.release()
	if commitErr != nil {
		return UnreclaimableSweepResult{}, sweepUnavailable(sweepStepCommit, commitErr)
	}
	return result, nil
}

// selectUnreclaimable pages until it holds `limit` genuinely sweepable units.
//
// Both the outbox filter and the routability filter are applied INSIDE the
// loop. Applying either after the limit would let a page of ineligible rows
// mask a genuine strand behind them on every pass -- the same deterministic
// loser that CHAOS-3990 fixed one layer up.
//
// TWO SWEEPABLE POPULATIONS, and they need OPPOSITE proofs (CHAOS-4097).
//
//   - NEVER PUBLISHED: no outbox row at all. Nothing owns it, but nothing has
//     tried to run it either, so destroying it is only safe when the
//     capability matrix declines the pair -- unroutable() is that proof, and
//     it stays mandatory on this branch.
//   - PUBLISHED AND DEAD: a 'delivered' outbox row whose River job is in a
//     terminal NON-SUCCESS state. Here routability is the wrong question and
//     asking it is what left CHAOS-4093 stranded: every one of those 650 units
//     was routable, which is precisely why they were published in the first
//     place. The proof that matters is that the thing which took ownership has
//     finished and did not do the work. A cancelled or discarded job is never
//     run again by anyone, and a job cancelled before the idempotency claim
//     (internal/jobruntime/adapter.go's validate/decode/drift block) wrote
//     nothing back to the domain at all.
//
// The old code had only the first branch and treated the mere PRESENCE of an
// outbox row as ownership -- "a unit holding an outbox row DID enter the River
// relay. River owns its fate" -- with no liveness behind the claim. That is
// true for available, running, retryable and completed jobs. It is false for
// the two terminal failure states, and this is where that gets proven rather
// than assumed.
func (sweep *UnreclaimableSweep) selectUnreclaimable(
	ctx context.Context,
	tx pgx.Tx,
	now time.Time,
	limit int,
) ([]unreclaimableCandidate, error) {
	ageCutoff := now.Add(-sweep.config.Age)
	idleCutoff := now.Add(-sweep.config.Idle)
	selected := make([]unreclaimableCandidate, 0, limit)
	// The zero cursor: every real row sorts after it.
	cursorCreatedAt := time.Time{}
	cursorID := "00000000-0000-0000-0000-000000000000"
	scanned := 0
	for len(selected) < limit && scanned < unreclaimableMaximumScan {
		page, err := scanUnreclaimablePage(
			ctx, tx, ageCutoff, idleCutoff, cursorCreatedAt, cursorID, limit,
		)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		scanned += len(page)
		last := page[len(page)-1]
		cursorCreatedAt, cursorID = last.createdAt, last.id
		unpublished, delivered, err := partitionPublishedUnits(ctx, tx, page)
		if err != nil {
			return nil, err
		}
		dead, err := sweep.deadDeliveries(ctx, delivered)
		if err != nil {
			return nil, err
		}
		for _, candidate := range unpublished {
			if !sweep.unroutable(candidate) {
				continue
			}
			selected = append(selected, candidate)
			if len(selected) == limit {
				break
			}
		}
		for _, candidate := range dead {
			if len(selected) == limit {
				break
			}
			selected = append(selected, candidate)
		}
		if len(page) < limit {
			break
		}
	}
	return selected, nil
}

// riverOwnsProviderUnits reads the DURABLE transport route.
//
// Capability configuration is not route ownership (review finding). Python
// resolves `worker_job_routes` FIRST and only then applies switches and
// presence: when the durable route is still Celery -- the rollback and
// coexistence state -- Celery owns every provider unit regardless of what the
// Go capability flags say. Reading the row here keeps that ordering, and keeps
// the answer durable rather than an env-var inference.
//
// A missing, paused, duplicated or unreadable row is NOT read as "River owns
// it": the sweep declines to act, matching Python's refusal to fall back to a
// transport during a control-plane fault. The paused half of that sentence
// was an unimplemented claim until CHAOS-4035 -- the statement selected only
// `transport` -- which is why routeFence.riverOwns now carries the check and
// says so.
const selectProviderUnitRouteSQL = `
SELECT transport, paused, generation
FROM public.worker_job_routes
WHERE job_kind = $1
`

// heldRouteFence is a lock held open. release MUST be called once the domain
// commit has been attempted, and never before.
type heldRouteFence struct {
	ok      bool
	release func()
}

func (sweep *UnreclaimableSweep) holdRouteFence(
	ctx context.Context, opening routeFence,
) (heldRouteFence, error) {
	tx, err := sweep.routes.Begin(ctx)
	if err != nil || tx == nil {
		return heldRouteFence{}, sweepUnavailable(sweepStepRouteFenceBegin, err)
	}
	release := func() { _ = tx.Rollback(ctx) }
	// SET LOCAL, so the bound dies with this transaction rather than leaking
	// onto a pooled connection every other coordinator statement then inherits.
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '"+unreclaimableFenceLockTimeout+"'"); err != nil {
		release()
		return heldRouteFence{}, sweepUnavailable(sweepStepRouteFenceTimeout, err)
	}
	closing, usable, err := readProviderUnitRoute(ctx, tx, sweepStepRouteFence)
	if err != nil {
		release()
		// A contended row means a route mutation is in flight, so the answer
		// is already "decline". Only that specific SQLSTATE is swallowed; a
		// permission or connection fault still surfaces as a step error.
		if isLockNotAvailable(err) {
			return heldRouteFence{}, nil
		}
		return heldRouteFence{}, err
	}
	if !usable || closing != opening {
		release()
		return heldRouteFence{}, nil
	}
	return heldRouteFence{ok: true, release: release}, nil
}

// isLockNotAvailable reads the SQLSTATE off the STEP error, not off a wrapped
// driver error.
//
// This is the one place the deliberate non-wrapping in sweepUnavailable bites:
// the driver error is intentionally dropped so no connection material can ever
// reach a message, which means errors.As can never find a *pgconn.PgError in
// this chain. An earlier cut of this fence checked for one anyway, so the
// contention path it exists to serve was dead code and a contended route row
// surfaced as a generic failure instead of the decline it is (review finding).
// The SQLSTATE the step error already carries is the right source, and it
// works whether pgx reported the timeout from Query or deferred it to
// rows.Err() -- the two land on different steps but the same code.
func isLockNotAvailable(err error) bool {
	var stepErr unreclaimableStepError
	return errors.As(err, &stepErr) && stepErr.sqlState == lockNotAvailableSQLState
}

// lockNotAvailableSQLState is PostgreSQL's lock_not_available. Reaching it here
// means a route mutation holds the row, so the sweep's answer is already
// "decline" and waiting longer would only delay it.
const lockNotAvailableSQLState = "55P03"

// The closing fence takes a ROW LOCK, and FOR SHARE specifically.
//
// FOR SHARE conflicts with FOR NO KEY UPDATE, which is what a plain UPDATE
// takes, so it genuinely blocks internal/jobroute/control.go's Rollback and
// ApplyCheckedIn rather than merely racing them. FOR KEY SHARE would NOT: it
// only conflicts with key updates, and the route mutation changes transport
// and generation, neither of which is the key.
//
// PostgreSQL requires the UPDATE privilege to take this lock even though no
// row is written. coordinatorPosture already declares worker_job_routes with
// AllowUpdate (internal/storage/postgres/domain_authorization.go), for the
// route mutation itself, so this needs no widening -- and the role-split
// integration suite executes this exact statement as the real coordinator
// login rather than assuming it.
const fenceProviderUnitRouteSQL = `
SELECT transport, paused, generation
FROM public.worker_job_routes
WHERE job_kind = $1
FOR SHARE
`

// The fence must never become the thing that stalls the 1s reconcile pass. If
// a route mutation is in flight and holds the row, waiting it out is pointless
// -- the answer is already going to be "declined" -- so the lock wait is
// bounded and a timeout IS a decline rather than an error.
const unreclaimableFenceLockTimeout = "250ms"

// routeQuerier is the subset both the pool (unlocked opening read) and the
// fence transaction (locked closing read) satisfy.
type routeQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func readProviderUnitRoute(
	ctx context.Context, routes routeQuerier, step string,
) (routeFence, bool, error) {
	statement := selectProviderUnitRouteSQL
	if step == sweepStepRouteFence {
		statement = fenceProviderUnitRouteSQL
	}
	rows, err := routes.Query(ctx, statement, unreclaimableProviderUnitID)
	if err != nil {
		return routeFence{}, false, sweepUnavailable(step, err)
	}
	defer rows.Close()
	fences := make([]routeFence, 0, 1)
	for rows.Next() {
		var fence routeFence
		if err := rows.Scan(&fence.transport, &fence.paused, &fence.generation); err != nil {
			return routeFence{}, false, sweepUnavailable(sweepStepRouteScan, err)
		}
		fence.transport = strings.TrimSpace(strings.ToLower(fence.transport))
		fences = append(fences, fence)
	}
	if err := rows.Err(); err != nil {
		return routeFence{}, false, sweepUnavailable(sweepStepRouteRows, err)
	}
	// A missing, duplicated or unreadable row is a control-plane fault. The
	// second return value is "usable", never "River owns it".
	if len(fences) != 1 {
		return routeFence{}, false, nil
	}
	return fences[0], true, nil
}

// unroutable mirrors Python's routes_to_river (workers/provider_unit_route.py):
// a pair is sweepable only when the capability matrix declines it. Before
// CHAOS-4054 step 4 the Python twin was resolve_unit_transport, which had to
// additionally prove the Celery fallthrough had no consumer; that plane is
// deleted, so "the matrix declines it" is now the whole predicate on both
// sides.
func (sweep *UnreclaimableSweep) unroutable(candidate unreclaimableCandidate) bool {
	descriptor, known := providersync.Descriptor(
		candidate.provider, candidate.datasetKey,
	)
	// An unknown pair is not proof of anything; refuse to destroy on a guess.
	if !known {
		return false
	}
	// River declines the pair. The caller has already confirmed from the
	// DURABLE route that River owns provider units at all, so nothing else is
	// going to execute this one.
	return !(descriptor.RouteReady && descriptor.Plannable)
}

// TWO CLOCKS, deliberately.
//
// created_at bounds how long the unit has existed without a single attempt;
// updated_at gates the ordinary stale window. The age bound MUST hang off
// created_at, because the dispatcher's stale reclaim re-stamps updated_at on
// every pass -- an age bound on updated_at resets forever and the sweep
// silently never fires, which is the exact failure this sweep exists to catch.
//
// The heartbeat predicate is NULL *or stale*, not strict NULL: a budget
// deferral stamps last_heartbeat_at even though no worker ran, and the later
// claim never clears it, so strict NULL would permanently exempt every unit
// that was ever budget-deferred.
//
// status = 'dispatching' only. A RUNNING unit is never selected.
const selectUnreclaimableCandidatesSQL = `
SELECT unit.id::text, unit.sync_run_id::text, unit.org_id,
	unit.provider, unit.dataset_key, unit.cost_class,
	unit.created_at, unit.updated_at
FROM public.sync_run_units AS unit
JOIN public.sync_runs AS run ON run.id = unit.sync_run_id
WHERE unit.status = 'dispatching'
	AND unit.lease_owner IS NULL
	AND unit.lease_expires_at IS NULL
	AND (unit.last_heartbeat_at IS NULL OR unit.last_heartbeat_at <= $2)
	AND unit.attempts = 0
	AND unit.created_at <= $1
	AND unit.updated_at <= $2
	AND run.status NOT IN ('success', 'partial_failed', 'failed')
	AND run.org_id = unit.org_id
	AND (unit.created_at, unit.id) > ($3, $4)
ORDER BY unit.created_at, unit.id
LIMIT $5
`

func scanUnreclaimablePage(
	ctx context.Context,
	tx pgx.Tx,
	ageCutoff time.Time,
	idleCutoff time.Time,
	cursorCreatedAt time.Time,
	cursorID string,
	limit int,
) ([]unreclaimableCandidate, error) {
	rows, err := tx.Query(
		ctx, selectUnreclaimableCandidatesSQL,
		ageCutoff, idleCutoff, cursorCreatedAt, cursorID, limit,
	)
	if err != nil {
		return nil, sweepUnavailable(sweepStepCandidateQuery, err)
	}
	defer rows.Close()
	page := make([]unreclaimableCandidate, 0, limit)
	for rows.Next() {
		var candidate unreclaimableCandidate
		if err := rows.Scan(
			&candidate.id, &candidate.syncRunID, &candidate.orgID,
			&candidate.provider, &candidate.datasetKey, &candidate.costClass,
			&candidate.createdAt, &candidate.updatedAt,
		); err != nil {
			return nil, sweepUnavailable(sweepStepCandidateScan, err)
		}
		page = append(page, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, sweepUnavailable(sweepStepCandidateRows, err)
	}
	return page, nil
}

// A unit holding an outbox row DID enter the River relay -- but "entered the
// relay" is not "is owned by something that will run it", and the difference is
// CHAOS-4097. river_job_id is selected alongside the key so the caller can ask
// the queue-control role what actually became of that delivery.
//
// river_job_id is projected only for a 'delivered' row. The check constraint
// ck_worker_job_outbox_delivery_state already makes it NOT NULL exactly then,
// so the CASE is not defending against a shape this schema produces; it makes
// the intent explicit and makes any other status fall through to "published,
// no proof", which is dropped. A row rearmed back to 'pending' by
// internal/joboutbox.TerminalDeliveryRepair is precisely that case: a
// replacement delivery is on its way and the unit is not ours to destroy.
const selectPublishedDedupeKeysSQL = `
SELECT dedupe_key,
	CASE WHEN status = 'delivered' THEN river_job_id END AS delivered_job_id
FROM public.worker_job_outbox
WHERE dedupe_key = ANY($1)
`

// partitionPublishedUnits splits a page into the units nothing ever published
// and the units whose delivery can still be asked about. It returns them as
// two disjoint slices rather than filtering to one, because the two need
// different proofs before either may be terminalized.
//
// A published row with no usable river_job_id belongs to NEITHER slice: it is
// dropped outright. That is the fail-closed default the original filter had,
// and it is kept -- absence of proof is not proof of death.
func partitionPublishedUnits(
	ctx context.Context,
	tx pgx.Tx,
	page []unreclaimableCandidate,
) (unpublished []unreclaimableCandidate, delivered []unreclaimableCandidate, err error) {
	keys := make([]string, 0, len(page))
	for _, candidate := range page {
		keys = append(keys, unreclaimableDedupeKey(candidate.id))
	}
	rows, err := tx.Query(ctx, selectPublishedDedupeKeysSQL, keys)
	if err != nil {
		return nil, nil, sweepUnavailable(sweepStepOutboxQuery, err)
	}
	defer rows.Close()
	type publication struct {
		jobID int64
		known bool
	}
	published := make(map[string]publication, len(keys))
	for rows.Next() {
		var key string
		var jobID *int64
		if err := rows.Scan(&key, &jobID); err != nil {
			return nil, nil, sweepUnavailable(sweepStepOutboxScan, err)
		}
		record := publication{}
		if jobID != nil && *jobID > 0 {
			record = publication{jobID: *jobID, known: true}
		}
		published[key] = record
	}
	if err := rows.Err(); err != nil {
		return nil, nil, sweepUnavailable(sweepStepOutboxRows, err)
	}
	unpublished = make([]unreclaimableCandidate, 0, len(page))
	delivered = make([]unreclaimableCandidate, 0, len(page))
	for _, candidate := range page {
		key := unreclaimableDedupeKey(candidate.id)
		record, isPublished := published[key]
		if !isPublished {
			unpublished = append(unpublished, candidate)
			continue
		}
		if !record.known {
			continue
		}
		candidate.delivery = terminalDelivery{dedupeKey: key, jobID: record.jobID}
		delivered = append(delivered, candidate)
	}
	return unpublished, delivered, nil
}

// The liveness read. state is compared as text so this file never has to
// import River's enum, and the accepted states are CLOSED: a state River adds
// later is refused by default rather than silently treated as dead.
// 'completed' is terminal too and is deliberately absent -- a completed job
// did the work.
//
// # "Terminal in River" is not the same as "nobody will revive it"
//
// This is the guard that keeps the sweep from destroying recoverable work, and
// it is the reason the two states carry DIFFERENT conditions.
//
// internal/joboutbox/terminal_delivery_repair.go rearms a provider-unit outbox
// row -- deleting the dead River job and minting a replacement delivery --
// when its job was DISCARDED by River's unhandled-kind rescue with attempts
// still on the clock (repairProviderUnitTerminalDeliverySQL, the
// `candidate_job.attempt < candidate_job.max_attempts` predicate). Its recovery
// requires the unit to still be 'dispatching'. So if this sweep terminalized
// such a unit first, that repair could never run again and a transport failure
// River was about to retry would become permanent domain failure instead.
//
// The two live in different reconcile loops, so ordering them is not something
// this file can rely on -- and "the sweep's one-hour age gate makes the repair
// win in practice" is a timing argument, which is exactly the kind of argument
// that stops being true during an incident.
//
// The predicates are therefore made DISJOINT BY CONSTRUCTION instead:
//
//   - discarded requires `attempt >= max_attempts`, the exact complement of the
//     repair's `attempt < max_attempts`. A spent budget means River has no
//     retry left and the repair will not claim the row either.
//   - cancelled needs no attempt condition, because the repair matches only
//     'discarded' and can never claim a cancelled job whatever its budget says.
//
// A discarded job with attempts remaining but no rescue sentinel is neither
// swept nor repaired. That is a knowingly narrower cut than "everything
// terminal", and it is the correct one: it is not a regression (nothing
// reached that row before this change either), and widening it would mean
// racing the repair for rows the repair may legitimately want.
//
// The join is on river_job.id, a bigint, against a bigint array. CHAOS-4092 is
// the reason that is stated: casting River's primary key to text is not
// sargable against river_job_pkey and turned a sibling repair into a 9.5h
// crash loop. worker_job_outbox.river_job_id is already a real bigint, so no
// cast is needed on either side and none is written.
const selectTerminalDeliveryStatesSQL = `
SELECT job.id, job.state::text
FROM %s AS job
WHERE job.id = ANY($1::bigint[])
	AND job.finalized_at IS NOT NULL
	AND (
		job.state::text = 'cancelled'
		OR (
			job.state::text = 'discarded'
			AND job.attempt >= job.max_attempts
		)
	)
`

// deadDeliveries keeps only the candidates whose delivery is provably dead.
//
// THE POOL SPLIT, stated here rather than left to be rediscovered: this read
// runs on the queue-control pool and therefore OUTSIDE the domain transaction,
// for the same reason the route read does (CHAOS-4035) -- the River schema is
// granted to the queue role alone, so no single transaction can hold both
// this and the terminalize write. Do NOT "fix" that by moving it inside the
// domain transaction; the domain role cannot even see the schema, and the
// attempt is what produced a 42501-per-tick production life once already.
//
// Unlike the route read, this one needs no fence. cancelled and discarded are
// TERMINAL: River never walks a job back out of them, so the answer cannot go
// stale in the direction that matters. What CAN change underneath is the
// outbox row -- a repair may rearm it and mint a replacement delivery -- and
// that is re-checked as a CAS at write time, not here.
func (sweep *UnreclaimableSweep) deadDeliveries(
	ctx context.Context,
	delivered []unreclaimableCandidate,
) ([]unreclaimableCandidate, error) {
	if len(delivered) == 0 {
		return nil, nil
	}
	ids := make([]int64, 0, len(delivered))
	for _, candidate := range delivered {
		ids = append(ids, candidate.delivery.jobID)
	}
	rows, err := sweep.jobs.Query(ctx, sweep.jobState, ids)
	if err != nil {
		return nil, sweepUnavailable(sweepStepJobStateQuery, err)
	}
	defer rows.Close()
	states := make(map[int64]string, len(ids))
	for rows.Next() {
		var id int64
		var state string
		if err := rows.Scan(&id, &state); err != nil {
			return nil, sweepUnavailable(sweepStepJobStateScan, err)
		}
		states[id] = state
	}
	if err := rows.Err(); err != nil {
		return nil, sweepUnavailable(sweepStepJobStateRows, err)
	}
	dead := make([]unreclaimableCandidate, 0, len(delivered))
	for _, candidate := range delivered {
		state, ok := states[candidate.delivery.jobID]
		if !ok || state == "" {
			continue
		}
		candidate.delivery.state = state
		dead = append(dead, candidate)
	}
	return dead, nil
}

func unreclaimableDedupeKey(unitID string) string {
	return unreclaimableProviderUnitID + ":" + unitID
}

// OPTIMISTIC CONCURRENCY on updated_at.
//
// The outbox check happens during selection, so a dispatcher that reclaims and
// publishes this unit afterwards would insert its outbox row while the
// never-leased shape still matched. The dispatcher's claim re-stamps
// updated_at in that same transaction, so pinning the exact value read makes
// any concurrent touch invalidate this write instead of racing it.
const terminalizeUnreclaimableSQL = `
UPDATE public.sync_run_units
SET status = 'failed',
	available_at = NULL,
	error = $2,
	last_retry_reason = $3,
	result = $4::jsonb,
	lease_owner = NULL,
	lease_expires_at = NULL,
	updated_at = $5
WHERE id = $1::uuid
	AND status = 'dispatching'
	AND lease_owner IS NULL
	AND attempts = 0
	AND updated_at = $6
`

// The published-and-dead write carries ONE extra predicate, and it is the
// whole safety of the CHAOS-4097 branch.
//
// The liveness proof was taken on the queue-control pool, outside this
// transaction. Between that read and this commit, internal/joboutbox's
// TerminalDeliveryRepair can rearm the outbox row -- river_job_id back to
// NULL, status back to 'pending' -- and the relay can mint a REPLACEMENT
// delivery for the same unit. Terminalizing then would destroy work that is
// live again.
//
// So the row is required to STILL hold the exact delivery the proof was taken
// on: same dedupe key, still 'delivered', still that river_job_id. Matching
// the key alone would be an ABA hole, because a replacement delivery reuses
// the key by construction; the PAIR is the delivery generation. This is the
// same shape internal/joboutbox.StrandRepair's phase-3 lock uses, and it is
// here for the same reason.
//
// The updated_at pin is kept alongside it, unchanged: the two guard different
// races (a runtime touching the unit, versus the delivery being replaced) and
// neither implies the other.
const terminalizeTerminalDeliverySQL = `
UPDATE public.sync_run_units
SET status = 'failed',
	available_at = NULL,
	error = $2,
	last_retry_reason = $3,
	result = $4::jsonb,
	lease_owner = NULL,
	lease_expires_at = NULL,
	updated_at = $5
WHERE id = $1::uuid
	AND status = 'dispatching'
	AND lease_owner IS NULL
	AND attempts = 0
	AND updated_at = $6
	AND EXISTS (
		SELECT 1
		FROM public.worker_job_outbox AS outbox
		WHERE outbox.dedupe_key = $7
			AND outbox.status = 'delivered'
			AND outbox.river_job_id = $8
	)
`

func (sweep *UnreclaimableSweep) terminalize(
	ctx context.Context,
	tx pgx.Tx,
	candidate unreclaimableCandidate,
	now time.Time,
) (int64, error) {
	reason := unreclaimableReason(candidate)
	category := unreclaimableErrorCategory
	if candidate.delivery.proven() {
		category = unreclaimableTerminalDeliveryCategory
	}
	fields := map[string]string{
		"error_category": category,
		"reason":         reason,
		"provider":       candidate.provider,
		"dataset_key":    candidate.datasetKey,
	}
	if candidate.delivery.proven() {
		// The exact River state is durable evidence, not decoration: it is
		// what lets an operator reading this row tell a job the runtime
		// rejected from one someone cancelled, without needing access to the
		// queue schema they may not hold.
		fields["river_job_state"] = candidate.delivery.state
		fields["river_job_id"] = strconv.FormatInt(candidate.delivery.jobID, 10)
	}
	payload, err := json.Marshal(fields)
	if err != nil {
		return 0, sweepUnavailable(sweepStepTerminalizePayload, err)
	}
	statement := terminalizeUnreclaimableSQL
	args := []any{
		candidate.id, category, reason, string(payload), now, candidate.updatedAt,
	}
	if candidate.delivery.proven() {
		statement = terminalizeTerminalDeliverySQL
		args = append(args, candidate.delivery.dedupeKey, candidate.delivery.jobID)
	}
	command, err := tx.Exec(ctx, statement, args...)
	if err != nil {
		return 0, sweepUnavailable(sweepStepTerminalizeExec, err)
	}
	return command.RowsAffected(), nil
}

// unreclaimableReason is the durable record an operator reads off the row.
// The bare category alone is what made a retry loop with thousands of attempts
// unexplainable in production (CHAOS-3990).
//
// The two branches say different things because they ARE different findings,
// and a shared sentence would make a dead delivery indistinguishable from a
// declined capability in the one place an operator looks first. The dead-
// delivery form names the River state explicitly, because "which terminal
// state" is the question a reader asks next.
func unreclaimableReason(candidate unreclaimableCandidate) string {
	if candidate.delivery.proven() {
		return fmt.Sprintf(
			"unreclaimable dispatch for %s: its River delivery (job %d) is "+
				"terminal in state %q, so nothing will execute it again and "+
				"the cancellation wrote nothing back to this unit",
			candidate.pair(), candidate.delivery.jobID, candidate.delivery.state,
		)
	}
	return fmt.Sprintf(
		"unreclaimable dispatch for %s: held 'dispatching' with no lease, no "+
			"heartbeat, no attempt and no worker_job_outbox row, so no runtime "+
			"was ever going to execute it",
		candidate.pair(),
	)
}
