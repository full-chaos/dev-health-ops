package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-4583: sync_dispatch_outbox rows never reach a terminal status.
//
// materializeDispatchSQL / materializeFinalizeSQL / materializeDiscoverySQL
// (materializer.go) only ever ARM a wakeup from non-terminal domain state, and
// every native completion path (native_dispatch_sync_run.go,
// native_finalize_sync_run.go, native_post_sync.go,
// native_reference_discovery.go's stampSuccess) arms its SUCCESSOR kind's
// outbox row but never closes the row it was itself dispatched for. The
// result, confirmed on both local and prod (CHAOS-4583): every
// sync_dispatch_outbox row that ever reaches 'dispatched' stays 'dispatched'
// forever, even after its owning domain state (the sync_run, or for
// reference_discovery, the sync_run_reference_discoveries ledger) is itself
// long since terminal. ~99.7% of rows per kind belong to an already-terminal
// owner; nothing ever shrinks the table, and it is scanned by every
// materializer/kernel/observer pass indefinitely.
//
// TerminalOutboxClose is the bounded reconciler step that closes those rows,
// sibling to UnreclaimableSweep and TerminalDeliveryRepair: it owns its own
// stage budget (StageTerminalOutboxClose) and, like Materializer/
// TerminalDeliveryRepair, a failure here is absorbed rather than fatal to the
// tick (see pipeline.go's stage classification comment) -- it is a read-
// adjacent safety net over largely disjoint state from lease repair and the
// kernel's claim-and-deliver path.
//
// One CTE-bounded UPDATE per kind, matching materializer.go's existing
// per-kind shape exactly:
//
//   - dispatch_sync_run / finalize_sync_run / post_sync close once the owning
//     public.sync_runs row itself reached a terminal status (success,
//     partial_failed, failed). All three are dispatched only once the run's
//     own lifecycle already licenses that dispatch (materializeFinalizeSQL's
//     candidates require every unit and the discovery ledger to already be
//     terminal before finalize_sync_run is even armed; native_finalize_sync_run
//     writes the post_sync ledger only once the run itself is terminal), so a
//     'dispatched' row of any of these three kinds whose run has since gone
//     terminal is safe to close -- the run cannot un-terminalize.
//   - reference_discovery closes once ITS OWN ledger
//     (public.sync_run_reference_discoveries) reached success or failed --
//     the exact CHAOS-4583 root-cause row: stampSuccess marks the ledger
//     'success' and arms dispatch_sync_run's wakeup but never closes this row.
//     Discovery's ledger can (and does) go terminal well before the owning
//     sync_run itself does, so this kind is gated on the ledger, not on
//     run.status.
//
// Every statement additionally requires NO LIVE CLAIM outstanding
// (claim_expires_at IS NULL OR already expired) at the SAME MVCC snapshot
// that proves the owner terminal -- the CHAOS-4583 ticket's own risk note: a
// row a concurrent kernel claim just re-armed for redelivery must never be
// closed out from under it. FOR UPDATE OF outbox SKIP LOCKED makes this safe
// against a concurrent claimant without blocking on it.
type TerminalOutboxCloseResult struct {
	Dispatch  int64
	Finalize  int64
	Discovery int64
	PostSync  int64
	// ClosedByOutcome is [kind][outcome] -> count for THIS PASS: outcome is
	// the owning domain state's terminal value that licensed the close (a
	// sync_runs.status for dispatch_sync_run/finalize_sync_run/post_sync, a
	// sync_run_reference_discoveries.status for reference_discovery). The
	// pipeline sums this cumulatively into
	// dev_health_sync_dispatch_outbox_closed_total{kind,outcome}.
	// Dispatch/Finalize/Discovery/PostSync above are the per-kind pass totals
	// regardless of outcome, mirroring MaterializerResult's own per-kind
	// counters.
	ClosedByOutcome map[string]map[string]int64
}

// terminalOutboxCloseStatus is the terminal status this step stamps. It is
// deliberately distinct from every value a writer elsewhere in this package
// sets ('pending', 'dispatched'): sync_dispatch_outbox.status carries no
// database CHECK constraint enumerating its values (only
// ck_sync_dispatch_outbox_dispatched_route_coherence, which is keyed on
// status = 'dispatched' vs status <> 'dispatched' and is satisfied by ANY
// non-'dispatched' value), so introducing this one needs no migration beyond
// the new (kind, status) index the CHAOS-4583 Alembic revision adds (this
// step's own candidate scan is independent of sync_run_id, so the existing
// uq_sync_dispatch_outbox_run_kind/ix_sync_dispatch_outbox_due indexes do not
// serve it). A closed row can never
// become a materializer candidate again: every materializer candidate CTE
// selects on the OWNING domain state (unit/run/discovery status), never on
// the outbox row's own status, and this step only ever closes a row whose
// owner has already gone terminal -- which this codebase treats as
// irreversible (see e.g. terminal_delivery_repair.go's route JOIN, which
// excludes a terminal run from repair for the same reason).
const terminalOutboxCloseStatus = "closed"

const (
	terminalOutboxCloseStepBegin      = "begin terminal-outbox-close transaction"
	terminalOutboxCloseStepDisableJIT = "disable JIT for the terminal-outbox-close transaction"
	terminalOutboxCloseStepDispatch   = "dispatch_sync_run terminal-close of public.sync_dispatch_outbox"
	terminalOutboxCloseStepFinalize   = "finalize_sync_run terminal-close of public.sync_dispatch_outbox"
	terminalOutboxCloseStepDiscovery  = "reference_discovery terminal-close of public.sync_dispatch_outbox"
	terminalOutboxCloseStepPostSync   = "post_sync terminal-close of public.sync_dispatch_outbox"
	terminalOutboxCloseStepRowCount   = "terminal-outbox-close affected-row count"
	terminalOutboxCloseStepCommit     = "commit terminal-outbox-close transaction"
)

// terminalOutboxCloseStepError names the failing statement while keeping the
// package-stable ErrUnavailable classification callers already branch on --
// the same shape as materializerStepError and unreclaimableStepError.
type terminalOutboxCloseStepError struct {
	step     string
	sqlState string
}

func (stepErr terminalOutboxCloseStepError) Error() string {
	if stepErr.sqlState != "" {
		return "terminal outbox close " + stepErr.step + " failed (sqlstate " +
			stepErr.sqlState + "): " + ErrUnavailable.Error()
	}
	return "terminal outbox close " + stepErr.step + " failed: " + ErrUnavailable.Error()
}

func (stepErr terminalOutboxCloseStepError) Unwrap() error { return ErrUnavailable }

func terminalOutboxCloseUnavailable(step string, cause error) error {
	stepErr := terminalOutboxCloseStepError{step: step}
	var pgErr *pgconn.PgError
	switch {
	case errors.As(cause, &pgErr):
		stepErr.sqlState = pgErr.Code
	case errors.Is(cause, context.DeadlineExceeded):
		stepErr.sqlState = stageContextDeadlineLabel
	}
	return stepErr
}

// TerminalOutboxCloseStepIdentity returns the Step statement name carried by
// a terminal-outbox-close failure, or "" if err did not come from Step.
func TerminalOutboxCloseStepIdentity(err error) string {
	var stepErr terminalOutboxCloseStepError
	if errors.As(err, &stepErr) {
		return stepErr.step
	}
	return ""
}

// TerminalOutboxCloseStepSQLState returns the SQLSTATE carried by a
// terminal-outbox-close failure, or "" if none was recovered.
func TerminalOutboxCloseStepSQLState(err error) string {
	var stepErr terminalOutboxCloseStepError
	if errors.As(err, &stepErr) {
		return stepErr.sqlState
	}
	return ""
}

// TerminalOutboxCloseStepper is the bounded terminal-close seam used by the
// command-owned mutation pipeline. Unlike UnreclaimableSweepStepper, this is
// a REQUIRED constructor argument to NewMutationPipeline -- like
// repair/terminal/materializer/kernel/observer, never optional. There is no
// staged-rollout or shadow-mode concern the way there was for the sweep's
// terminalization risk: closing a row whose owner is already terminal cannot
// lose in-flight work, so every deployment gets it from day one.
type TerminalOutboxCloseStepper interface {
	Step(context.Context, time.Time, int) (TerminalOutboxCloseResult, error)
}

// TerminalOutboxClose implements TerminalOutboxCloseStepper against the
// coordinator pool -- the same trust boundary Materializer runs on, since
// both read/write public.sync_dispatch_outbox and join public.sync_runs /
// public.sync_run_reference_discoveries.
type TerminalOutboxClose struct {
	begin          beginFunc
	closeDispatch  string
	closeFinalize  string
	closePostSync  string
	closeDiscovery string
}

func NewTerminalOutboxClose(pool *pgxpool.Pool) (*TerminalOutboxClose, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	return newTerminalOutboxClose(pool.Begin)
}

func newTerminalOutboxClose(begin beginFunc) (*TerminalOutboxClose, error) {
	if begin == nil {
		return nil, ErrInvalidConfiguration
	}
	// terminalOutboxCloseStatus is interpolated ONCE, here, into a %-driven
	// Sprintf over a package constant (never operator input), so the four
	// statements never repeat the literal 'closed' by hand -- see
	// terminalOutboxCloseStatus's doc comment.
	return &TerminalOutboxClose{
		begin:          begin,
		closeDispatch:  fmt.Sprintf(closeDispatchSyncRunSQLTemplate, terminalOutboxCloseStatus),
		closeFinalize:  fmt.Sprintf(closeFinalizeSyncRunSQLTemplate, terminalOutboxCloseStatus),
		closePostSync:  fmt.Sprintf(closePostSyncSQLTemplate, terminalOutboxCloseStatus),
		closeDiscovery: fmt.Sprintf(closeReferenceDiscoverySQLTemplate, terminalOutboxCloseStatus),
	}, nil
}

// Step closes at most limit rows PER KIND (four kinds, so up to 4*limit total
// rows in one pass) in one transaction, mirroring Materializer.Step's own
// per-kind-bounded-not-globally-bounded shape.
func (closer *TerminalOutboxClose) Step(
	ctx context.Context, now time.Time, limit int,
) (TerminalOutboxCloseResult, error) {
	if closer == nil || closer.begin == nil || ctx == nil || now.IsZero() ||
		limit < minimumStepLimit || limit > maximumStepLimit {
		return TerminalOutboxCloseResult{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return TerminalOutboxCloseResult{}, err
	}
	now = now.UTC()

	tx, err := closer.begin(ctx)
	if err != nil || tx == nil {
		return TerminalOutboxCloseResult{}, terminalOutboxCloseUnavailable(terminalOutboxCloseStepBegin, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Same CHAOS-4262 defense materializer.go's Step takes: the candidate CTEs
	// below join sync_runs from the outbox side (index-driven by the new
	// (kind, status) index), but the planner's row estimate for that join can
	// still be wrong enough to cross this stage's own budget if it reaches
	// for a JIT-compiled plan for what is actually a handful of rows.
	if _, err := tx.Exec(ctx, "SET LOCAL jit = off"); err != nil {
		return TerminalOutboxCloseResult{}, terminalOutboxCloseUnavailable(terminalOutboxCloseStepDisableJIT, err)
	}

	result := TerminalOutboxCloseResult{ClosedByOutcome: make(map[string]map[string]int64, 4)}
	steps := []struct {
		kind  string
		sql   string
		count *int64
		step  string
	}{
		{"dispatch_sync_run", closer.closeDispatch, &result.Dispatch, terminalOutboxCloseStepDispatch},
		{"finalize_sync_run", closer.closeFinalize, &result.Finalize, terminalOutboxCloseStepFinalize},
		{"reference_discovery", closer.closeDiscovery, &result.Discovery, terminalOutboxCloseStepDiscovery},
		{"post_sync", closer.closePostSync, &result.PostSync, terminalOutboxCloseStepPostSync},
	}
	for _, step := range steps {
		rows, queryErr := tx.Query(ctx, step.sql, now, limit)
		if queryErr != nil {
			return TerminalOutboxCloseResult{}, terminalOutboxCloseUnavailable(step.step, queryErr)
		}
		var total int64
		byOutcome := make(map[string]int64, 4)
		for rows.Next() {
			var outcome string
			if scanErr := rows.Scan(&outcome); scanErr != nil {
				rows.Close()
				return TerminalOutboxCloseResult{}, terminalOutboxCloseUnavailable(step.step, scanErr)
			}
			total++
			byOutcome[outcome]++
		}
		rowsErr := rows.Err()
		rows.Close()
		if rowsErr != nil {
			return TerminalOutboxCloseResult{}, terminalOutboxCloseUnavailable(step.step, rowsErr)
		}
		if total < 0 || total > int64(limit) {
			return TerminalOutboxCloseResult{}, terminalOutboxCloseUnavailable(terminalOutboxCloseStepRowCount, nil)
		}
		*step.count = total
		if total > 0 {
			result.ClosedByOutcome[step.kind] = byOutcome
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return TerminalOutboxCloseResult{}, terminalOutboxCloseUnavailable(terminalOutboxCloseStepCommit, err)
	}
	return result, nil
}

// Every statement below shares the same write shape: it nulls the four
// dispatched_*/transport_job_id columns (required the instant status moves
// away from 'dispatched' by ck_sync_dispatch_outbox_dispatched_route_coherence)
// and the four claim_* columns (matching terminal_delivery_repair.go's own
// reset write; a closed row holds no claim). dispatched_at is deliberately
// LEFT AS-IS -- unlike a reset back to 'pending', closing to a terminal
// status keeps the last-dispatched timestamp as the operationally useful
// "when did this row's final delivery happen" audit trail; nothing reads it
// as a liveness signal once status <> 'dispatched'.
const closeDispatchSyncRunSQLTemplate = `
WITH candidates AS (
	SELECT outbox.id, run.status AS outcome
	FROM public.sync_dispatch_outbox AS outbox
	JOIN public.sync_runs AS run ON run.id = outbox.sync_run_id
	WHERE outbox.kind = 'dispatch_sync_run'
		AND outbox.status = 'dispatched'
		AND run.status IN ('success', 'partial_failed', 'failed')
		AND (outbox.claim_expires_at IS NULL OR outbox.claim_expires_at <= $1)
	ORDER BY outbox.id
	FOR UPDATE OF outbox SKIP LOCKED
	LIMIT $2
)
UPDATE public.sync_dispatch_outbox AS outbox
SET status = '%s',
	dispatched_transport = NULL,
	dispatched_route_generation = NULL,
	transport_job_id = NULL,
	claim_token = NULL,
	claim_expires_at = NULL,
	claim_transport = NULL,
	claim_route_generation = NULL,
	updated_at = $1
FROM candidates
WHERE outbox.id = candidates.id
RETURNING candidates.outcome
`

const closeFinalizeSyncRunSQLTemplate = `
WITH candidates AS (
	SELECT outbox.id, run.status AS outcome
	FROM public.sync_dispatch_outbox AS outbox
	JOIN public.sync_runs AS run ON run.id = outbox.sync_run_id
	WHERE outbox.kind = 'finalize_sync_run'
		AND outbox.status = 'dispatched'
		AND run.status IN ('success', 'partial_failed', 'failed')
		AND (outbox.claim_expires_at IS NULL OR outbox.claim_expires_at <= $1)
	ORDER BY outbox.id
	FOR UPDATE OF outbox SKIP LOCKED
	LIMIT $2
)
UPDATE public.sync_dispatch_outbox AS outbox
SET status = '%s',
	dispatched_transport = NULL,
	dispatched_route_generation = NULL,
	transport_job_id = NULL,
	claim_token = NULL,
	claim_expires_at = NULL,
	claim_transport = NULL,
	claim_route_generation = NULL,
	updated_at = $1
FROM candidates
WHERE outbox.id = candidates.id
RETURNING candidates.outcome
`

const closePostSyncSQLTemplate = `
WITH candidates AS (
	SELECT outbox.id, run.status AS outcome
	FROM public.sync_dispatch_outbox AS outbox
	JOIN public.sync_runs AS run ON run.id = outbox.sync_run_id
	WHERE outbox.kind = 'post_sync'
		AND outbox.status = 'dispatched'
		AND run.status IN ('success', 'partial_failed', 'failed')
		AND (outbox.claim_expires_at IS NULL OR outbox.claim_expires_at <= $1)
	ORDER BY outbox.id
	FOR UPDATE OF outbox SKIP LOCKED
	LIMIT $2
)
UPDATE public.sync_dispatch_outbox AS outbox
SET status = '%s',
	dispatched_transport = NULL,
	dispatched_route_generation = NULL,
	transport_job_id = NULL,
	claim_token = NULL,
	claim_expires_at = NULL,
	claim_transport = NULL,
	claim_route_generation = NULL,
	updated_at = $1
FROM candidates
WHERE outbox.id = candidates.id
RETURNING candidates.outcome
`

// reference_discovery is the exact CHAOS-4583 root-cause row: gated on ITS
// OWN ledger (public.sync_run_reference_discoveries), never on the owning
// sync_run's status -- see the package doc comment above for why the two
// diverge for this one kind. sync_run_reference_discoveries.sync_run_id is
// UNIQUE (one ledger row per run), so this JOIN can never fan out.
const closeReferenceDiscoverySQLTemplate = `
WITH candidates AS (
	SELECT outbox.id, discovery.status AS outcome
	FROM public.sync_dispatch_outbox AS outbox
	JOIN public.sync_run_reference_discoveries AS discovery
		ON discovery.sync_run_id = outbox.sync_run_id
	WHERE outbox.kind = 'reference_discovery'
		AND outbox.status = 'dispatched'
		AND discovery.status IN ('success', 'failed')
		AND (outbox.claim_expires_at IS NULL OR outbox.claim_expires_at <= $1)
	ORDER BY outbox.id
	FOR UPDATE OF outbox SKIP LOCKED
	LIMIT $2
)
UPDATE public.sync_dispatch_outbox AS outbox
SET status = '%s',
	dispatched_transport = NULL,
	dispatched_route_generation = NULL,
	transport_job_id = NULL,
	claim_token = NULL,
	claim_expires_at = NULL,
	claim_transport = NULL,
	claim_route_generation = NULL,
	updated_at = $1
FROM candidates
WHERE outbox.id = candidates.id
RETURNING candidates.outcome
`
