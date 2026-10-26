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
)

// RefusalError is what Resolve returns. errors.Is(err, ErrRefused) is always
// true; the text is the fixed reason word and never holds an input value.
type RefusalError struct{ Reason Reason }

func (e *RefusalError) Error() string        { return "workload: refused: " + string(e.Reason) }
func (e *RefusalError) Is(target error) bool { return target == ErrRefused }

// RefusalReason returns the reason word of a Resolve refusal (unknown,
// disabled, uid_mismatch, no_audience, store_error), or "" for a nil or
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
	}
	return ReasonUnknown
}
