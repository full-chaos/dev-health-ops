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
