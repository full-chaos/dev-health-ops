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

// reviewedGoMutationOwnershipPolicy is intentionally package-private. Tests
// and a future audited command composition can exercise the Go-owned kernel,
// but neither environment nor an external package can manufacture marker
// mutation authority.
func reviewedGoMutationOwnershipPolicy() OwnershipPolicy {
	return OwnershipPolicy{owner: schedulerOwnerGo, mode: schedulerModeMutation}
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
