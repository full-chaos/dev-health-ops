package syncdispatchruntime

import (
	"errors"
	"fmt"
)

// The dispatch path's error classes. Their names date from the HTTP bridge
// into the Python api, which the in-process estimator (CHAOS-6243) and the
// native reference discovery replaced; every errors.Is classification in
// this package and its callers still keys on them.
var (
	// ErrInvalidBridge: a dependency is missing (a nil estimator or
	// dispatcher) or the request is empty.
	ErrInvalidBridge = errors.New("invalid sync dispatch bridge")
	// ErrBridgeRequest: the far side is unavailable -- the estimate
	// loader's database read failed, or a discovery call failed. The caller
	// fails the affected units open, as Python's per-unit try/except did.
	ErrBridgeRequest = errors.New("sync dispatch bridge request failed")
	// ErrBridgeContractRejected wraps ErrBridgeRequest (so every
	// errors.Is(err, ErrBridgeRequest) classification still matches) and
	// marks a refused REFERENCE: a stale run, a unit outside the run, or an
	// id that is not a UUID. That is a programming error on this side, so
	// enforceRun fails its pass instead of admitting units with no budget
	// checked.
	ErrBridgeContractRejected = fmt.Errorf("%w: bridge rejected the request as malformed", ErrBridgeRequest)
)
