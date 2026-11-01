package workload

import "errors"

var (
	ErrNotFound        = errors.New("workload: workload not found")
	ErrNameInvalid     = errors.New("workload: the name is outside the workload name pattern")
	ErrNameTaken       = errors.New("workload: a platform workload has that name")
	ErrAudienceInvalid = errors.New("workload: an audience is outside the audience shape or repeated")
	ErrActionInvalid   = errors.New("workload: an allowed action is not a catalog action or is repeated")
	ErrBindingInvalid  = errors.New("workload: a binding field is empty, too long or malformed")
	ErrBindingTaken    = errors.New("workload: the federation tuple is bound to a workload already")
	ErrBindingNotFound = errors.New("workload: binding not found")
	ErrStore           = errors.New("workload: store unavailable")

	// The assertion key verbs' refusals. None carries an input value.
	ErrAssertionKeyInvalid      = errors.New("workload: an assertion key field is malformed")
	ErrAssertionKeyActiveExists = errors.New("workload: the workload has an active assertion key; name -retire-previous-after to rotate")
	ErrAssertionKeyTerminal     = errors.New("workload: a retired assertion key does not change")
	ErrAssertionKeyTransition   = errors.New("workload: the assertion key change is not allowed from its current state")
	ErrAssertionKeyOverlap      = errors.New("workload: retire_after is more than 24 h after the active key's rotated_at")
	ErrAssertionKeyNoActive     = errors.New("workload: the workload has no active assertion key to overlap")
	// ErrAssertionKeyWorkloadDisabled: the verb refuses a workload whose row is disabled.
	ErrAssertionKeyWorkloadDisabled = errors.New("workload: the workload is disabled; enable it before changing its assertion keys")

	// The bootstrap lifecycle verbs' refusals. None carries an input value.
	ErrBootstrapValue    = errors.New("workload: the bootstrap value is not a canonical minted value")
	ErrBootstrapExpiry   = errors.New("workload: the bootstrap expiry is outside (0, 90 d]")
	ErrBootstrapOverlap  = errors.New("workload: the rotation overlap is outside [0, 1 h]")
	ErrBootstrapReason   = errors.New("workload: the revoke reason is not compromised or owner_request")
	ErrBootstrapLive     = errors.New("workload: the workload has a live bootstrap credential")
	ErrBootstrapRotating = errors.New("workload: a bootstrap rotation is still in its overlap")
	ErrBootstrapNone     = errors.New("workload: the workload has no live bootstrap credential")
	ErrBootstrapNotFound = errors.New("workload: bootstrap credential not found")
	// ErrBootstrapWorkloadDisabled and ErrBootstrapPrincipalDisabled: create
	// and rotate refuse a workload whose row is disabled (the first) or whose
	// principal row alone is disabled (the second); revoke and disable stay
	// allowed.
	ErrBootstrapWorkloadDisabled  = errors.New("workload: the workload is disabled")
	ErrBootstrapPrincipalDisabled = errors.New("workload: the workload's principal is disabled")

	// The enable verb's refusals. A disabled principal without
	// EnablePrincipal is ErrBootstrapPrincipalDisabled.
	ErrOperatorNotFound     = errors.New("workload: the operator principal does not exist")
	ErrOperatorDisabled     = errors.New("workload: the operator principal is disabled")
	ErrOperatorIsSubject    = errors.New("workload: the operator is the workload's own principal")
	ErrPrincipalNotWorkload = errors.New("workload: the workload's principal is not of kind workload")
	ErrEnableStateChanged   = errors.New("workload: the workload changed between the read and the locks")
	// ErrEnableLiveBootstrap: enable would revive a bootstrap value that was
	// live before the disable; revoke it first (D6505).
	ErrEnableLiveBootstrap = errors.New("workload: the workload holds a live bootstrap credential")

	// ErrRefused is the only refusal Resolve and Narrow return. The reason
	// (unknown tuple, disabled, uid mismatch, no audience) stays inside this
	// package, so a caller cannot tell the exchange's client which one fired.
	ErrRefused = errors.New("workload: refused")
)

// Reason is why Resolve refused. It is for logs and counters only: a caller
// must answer every Reason the same way.
type Reason string

const (
	ReasonUnknown     Reason = "unknown"
	ReasonDisabled    Reason = "disabled"
	ReasonUIDMismatch Reason = "uid_mismatch"
	ReasonNoAudience  Reason = "no_audience"
	ReasonStoreError  Reason = "store_error"
	// ReasonRevoked and ReasonExpired are ResolveBootstrap's: the bootstrap
	// credential is revoked, or now is at or past its expiry.
	ReasonRevoked Reason = "revoked"
	ReasonExpired Reason = "expired"
)

// RefusalError is what Resolve returns. errors.Is(err, ErrRefused) is always
// true; the text is the fixed reason word and never holds an input value.
type RefusalError struct{ Reason Reason }

func (e *RefusalError) Error() string        { return "workload: refused: " + string(e.Reason) }
func (e *RefusalError) Is(target error) bool { return target == ErrRefused }

// RefusalReason returns the reason word of a Resolve or ResolveBootstrap
// refusal (unknown, disabled, uid_mismatch, no_audience, revoked, expired,
// store_error), or "" for a nil or
// non-refusal error. It is for logs and counters only.
func RefusalReason(err error) string {
	var r *RefusalError
	if errors.As(err, &r) {
		return string(r.Reason)
	}
	return ""
}

func publicReason(r reason) Reason {
	switch r {
	case reasonWorkloadDisabled, reasonPrincipalOff, reasonBindingDisabled:
		return ReasonDisabled
	case reasonUIDMismatch:
		return ReasonUIDMismatch
	case reasonNoAudience:
		return ReasonNoAudience
	case reasonRevoked:
		return ReasonRevoked
	case reasonExpired:
		return ReasonExpired
	}
	return ReasonUnknown
}
