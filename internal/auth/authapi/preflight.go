package authapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// textAuthUnavailable is go-api's 503 detail when it cannot issue a session
// (sessionissue.UnavailableText), so the go-api routes here answer its bytes.
const textAuthUnavailable = "Authentication service unavailable"

// errSignerUnavailable marks a signer that cannot sign: a failed preflight
// inside a shared session path, or a failed EdDSA Sign at mint. Its callers
// answer 503 rather than a generic failure.
var errSignerUnavailable = errors.New("authapi: the access signer is unavailable")

// signerCheckTTL is how long one signer check answers for an EdDSA Issuer.
// A check reads the key and signs a probe, so it must not run once per
// request: within the window every request takes the last answer, success or
// failure, and a refused request retries after Retry-After (1 s).
const signerCheckTTL = time.Second

// signerCheck memoizes one Issuer's signer check. It is per Issuer: no state
// is shared between issuers and nothing is carried on a context.
type signerCheck struct {
	// slot is a one-place semaphore: its holder runs the check, and a waiter
	// gives up when its own context ends (a key read cannot be interrupted).
	init sync.Once
	slot chan struct{}
	// clock is time.Now outside tests: its monotonic reading makes the
	// window immune to wall-clock steps.
	clock   func() time.Time
	checked bool
	at      time.Time
	err     error
}

// run returns the memoized answer while it is younger than signerCheckTTL and
// otherwise runs check. One caller at a time holds the slot, so a burst runs
// one check; a caller whose context ends while it waits returns that error.
// A failure caused by the caller's own context is not memoized: it says
// nothing about the signer.
func (c *signerCheck) run(ctx context.Context, check func(context.Context) error) error {
	c.init.Do(func() { c.slot = make(chan struct{}, 1) })
	select {
	case c.slot <- struct{}{}:
	default:
		select {
		case c.slot <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defer func() { <-c.slot }()
	clock := c.clock
	if clock == nil {
		clock = time.Now
	}
	if c.checked {
		if age := clock().Sub(c.at); age >= 0 && age < signerCheckTTL {
			return c.err
		}
	}
	err := check(ctx)
	if err != nil && ctx.Err() != nil {
		return err
	}
	c.checked, c.at, c.err = true, clock(), err
	return err
}

// Preflight reports whether the Issuer's class can mint now. A handler calls
// it before its first write, so a mint that cannot sign refuses with nothing
// written. Under hs256_compat it does nothing: the token is signed with the
// shared secret and there is no key to read. Under eddsa it runs the signer's
// check (memoized, see signerCheckTTL). A key that turns bad between the
// check and the mint still fails the mint, after the write.
func (i *Issuer) Preflight(ctx context.Context) error {
	switch {
	case i.signer != nil && i.eddsa != nil:
		return fmt.Errorf("%w: both access token classes are configured", errIssue)
	case i.eddsa != nil:
		if err := i.eddsa.check.run(ctx, i.eddsa.signer.Check); err != nil {
			return fmt.Errorf("%w: %w", errIssue, err)
		}
		return nil
	case i.signer == nil:
		return fmt.Errorf("%w: no access token class is configured", errIssue)
	}
	return nil
}

// unavailableCompat answers a failed preflight on a go-api route with
// go-api's own 503: {"detail": {"message": ...}} and no Retry-After.
func (h compatHandlers) unavailableCompat(w http.ResponseWriter, r *http.Request, operation string, err error) {
	h.rec.record(r.Context(), operation, "error", reasonDependency, err)
	policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(textAuthUnavailable), nil)
}

// failMint answers a failed mint on a go-api route: go-api's 503 when the
// signer could not sign, its generic 500 for any other failure.
func (h compatHandlers) failMint(w http.ResponseWriter, r *http.Request, operation string, err error) {
	if errors.Is(err, errSignerUnavailable) {
		h.unavailableCompat(w, r, operation, err)
		return
	}
	h.fail(w, r, operation, err)
}
