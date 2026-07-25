package sync

import "errors"

var (
	// ErrSchedulerMutationDisabled means the active scheduler owner has not
	// explicitly transferred schedule-marker mutation to Go. Callers must not
	// treat this as a retryable handoff failure.
	ErrSchedulerMutationDisabled = errors.New("scheduler marker mutation is disabled by ownership policy")
	// ErrInvalidOwnershipPolicy identifies an unsupported owner/mode pair.
	// Policies are deliberately constructed in code rather than read from the
	// environment so an accidental deployment setting cannot activate mutation.
	ErrInvalidOwnershipPolicy = errors.New("invalid scheduler ownership policy")
)

// schedulerOwner identifies the runtime that owns production schedule-marker
// mutation. It is package-private so ownership transfer requires a source
// change inside this package.
type schedulerOwner string

const (
	schedulerOwnerCelery schedulerOwner = "celery"
	schedulerOwnerGo     schedulerOwner = "go"
)

// schedulerMode identifies the bounded behavior permitted for the owner.
// CoexistenceDisabled is the checked-in default. Shadow is read-only; Mutation
// is reserved for a future audited owner transfer and is never command-wired.
type schedulerMode string

const (
	schedulerModeCoexistenceDisabled schedulerMode = "coexistence_disabled"
	schedulerModeShadow              schedulerMode = "shadow"
	schedulerModeMutation            schedulerMode = "mutation"
)

// OwnershipPolicy makes the marker-mutation owner and runtime mode explicit.
// Its fields are opaque outside this package: external callers can validate the
// checked-in default but cannot construct mutation authority.
type OwnershipPolicy struct {
	owner schedulerOwner
	mode  schedulerMode
}

// DefaultOwnershipPolicy preserves the checked-in coexistence contract: Celery
// is the only production schedule owner and Go cannot mutate a marker.
func DefaultOwnershipPolicy() OwnershipPolicy {
	return OwnershipPolicy{
		owner: schedulerOwnerCelery,
		mode:  schedulerModeCoexistenceDisabled,
	}
}

// reviewedGoMutationOwnershipPolicy is intentionally package-private so that
// TransferScheduleMarkerOwnershipToGo is the one and only exported spelling
// of it. Tests reach the value directly because they live in this package;
// everything outside it goes through the exported wrapper below.
func reviewedGoMutationOwnershipPolicy() OwnershipPolicy {
	return OwnershipPolicy{owner: schedulerOwnerGo, mode: schedulerModeMutation}
}

// TransferScheduleMarkerOwnershipToGo performs the reviewed, in-source
// transfer of schedule-marker mutation authority from Celery to Go.
//
// This is the ownership transfer, not a step toward it. There is no
// environment variable, deployment profile, or runtime flag that can produce
// this OwnershipPolicy value: OwnershipPolicy's fields stay unexported, so
// this function and DefaultOwnershipPolicy are the complete, closed set of
// policies any caller outside this package can ever obtain. Calling this
// function and shipping the resulting binary IS the act of transfer; nothing
// else can activate it, and nothing else is required to have already
// activated it (see the composition-root gates below, which are a separate,
// additional precondition on top of this one).
//
// Obtaining this policy is necessary but not sufficient for mutation to
// actually happen in production. allowsMutation() still requires
// {owner: go, mode: mutation} on the specific Repository that runs the
// write, and cmd/dev-health-scheduler's composition root additionally gates
// on its own checkedInSchedulerActivation.goOwnsMarkers flag before it will
// even open a database pool. That gate exists precisely so that this
// function's existence does not, by itself, put a marker mutation on the
// wire.
//
// Why a second, concurrently running Celery Beat cannot also mutate the same
// marker once this policy is in effect: HandoffDueResult (transaction.go)
// deliberately reproduces the Python scheduler's exact row-lock order and
// clause — `FOR UPDATE OF config, job SKIP LOCKED` against the same
// public.sync_configurations/public.scheduled_jobs rows Beat locks with
// SQLAlchemy's `.with_for_update(skip_locked=True)` in the same
// config -> job -> occurrence order — and advances next_run_at inside the
// same transaction that holds the lock. PostgreSQL enforces that lock
// regardless of which process or language acquired it, so at most one of
// {a Beat transaction, a Go transaction} can ever hold it at a time; the
// other's SKIP LOCKED clause makes it skip the row this pass rather than
// block or race past a stale read. That is a database-level guarantee, not
// an application-level convention either side could accidentally violate by
// itself — see TestHandoffDuePostgresRespectsExternalRowLock, which proves
// it against a raw connection standing in for Beat.
//
// What this policy does NOT prove: that Go's cron evaluation always agrees
// with Python's for every schedule (see NextOccurrence's golden-vector tests
// for that), and that Go can complete a materialization once it wins a race
// (see the occurrence reconciler's Materializer — a stub until CUT-09/CUT-10
// lands). Both are separate preconditions on the composition root's
// goOwnsMarkers gate, and this function does not claim to satisfy either.
func TransferScheduleMarkerOwnershipToGo() OwnershipPolicy {
	return reviewedGoMutationOwnershipPolicy()
}

// Validate rejects every owner/mode pair except the bounded current and future
// states. In particular, a Go shadow process never acquires mutation authority.
func (policy OwnershipPolicy) Validate() error {
	switch policy {
	case OwnershipPolicy{owner: schedulerOwnerCelery, mode: schedulerModeCoexistenceDisabled},
		OwnershipPolicy{owner: schedulerOwnerCelery, mode: schedulerModeShadow},
		OwnershipPolicy{owner: schedulerOwnerGo, mode: schedulerModeMutation}:
		return nil
	default:
		return ErrInvalidOwnershipPolicy
	}
}

func (policy OwnershipPolicy) allowsMutation() bool {
	return policy.owner == schedulerOwnerGo && policy.mode == schedulerModeMutation
}
