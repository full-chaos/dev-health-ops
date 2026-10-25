package authapi

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// signerCheckTTL is how long one signer check answers for an EdDSA Issuer.
// A check reads the key and signs a probe, so it must not run once per
// request: within the window every request takes the last answer, success or
// failure, and a refused request retries after Retry-After (1 s).
const signerCheckTTL = time.Second

// signerCheck memoizes one Issuer's signer check. It is per Issuer: no state
// is shared between issuers and nothing is carried on a context.
type signerCheck struct {
	mu sync.Mutex
	// clock is time.Now outside tests: its monotonic reading makes the
	// window immune to wall-clock steps.
	clock   func() time.Time
	checked bool
	at      time.Time
	err     error
}

// run returns the memoized answer while it is younger than signerCheckTTL and
// otherwise runs check. Callers wait on the lock, so a burst runs one check.
// A failure caused by the caller's own cancelled context is not memoized: it
// says nothing about the signer.
func (c *signerCheck) run(ctx context.Context, check func(context.Context) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
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
