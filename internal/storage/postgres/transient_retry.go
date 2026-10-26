package postgres

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"time"
)

const (
	dialRetryInitialBackoff = 2 * time.Second
	dialRetryMaxBackoff     = 30 * time.Second
)

// DialRetry retries one startup step while it fails with a transient dial
// error (IsTransientDialError), inside Budget. A zero Budget makes no retry.
// Any other error, and a canceled context, ends the loop at once.
type DialRetry struct {
	Budget time.Duration
	Now    func() time.Time
	Sleep  func(context.Context, time.Duration)
	Jitter func(time.Duration) time.Duration
	// OnRetry runs before each wait. It receives no error: the error text can
	// name the dial address.
	OnRetry func(attempt int, elapsed, wait time.Duration)
}

// Do runs attempt until it succeeds, fails with a final error, or the budget
// is spent. It returns the last error unchanged.
func (retry DialRetry) Do(ctx context.Context, attempt func(context.Context) error) error {
	now, sleep := retry.Now, retry.Sleep
	if now == nil {
		now = time.Now
	}
	if sleep == nil {
		sleep = dialRetrySleep
	}
	start := now()
	backoff := dialRetryInitialBackoff
	for attemptNum := 1; ; attemptNum++ {
		err := attempt(ctx)
		if err == nil || !IsTransientDialError(err) || ctx.Err() != nil {
			return err
		}
		elapsed := now().Sub(start)
		if elapsed >= retry.Budget {
			return err
		}
		wait := backoff
		if retry.Jitter != nil {
			wait = retry.Jitter(wait)
		}
		if remaining := retry.Budget - elapsed; wait > remaining {
			wait = remaining
		}
		if retry.OnRetry != nil {
			retry.OnRetry(attemptNum, elapsed, wait)
		}
		sleep(ctx, wait)
		backoff = min(backoff*2, dialRetryMaxBackoff)
	}
}

func dialRetrySleep(ctx context.Context, wait time.Duration) {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// DialRetryJitter spreads a wait by up to +/-20% so replicas that started
// together do not redial in lockstep.
func DialRetryJitter(wait time.Duration) time.Duration {
	spread := int64(wait) / 5
	if spread <= 0 {
		return wait
	}
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return wait
	}
	offset := int64(binary.BigEndian.Uint64(raw[:])%uint64(2*spread+1)) - spread
	return wait + time.Duration(offset)
}

// TransientWindow tracks consecutive transient failures of a running loop. A
// success resets it. It is not safe for concurrent use.
type TransientWindow struct {
	since time.Time
}

// Exceeded reports whether err must end the loop: any non-transient error, or
// transient errors that have lasted budget without a success in between.
func (window *TransientWindow) Exceeded(now time.Time, err error, budget time.Duration) bool {
	if err == nil {
		window.since = time.Time{}
		return false
	}
	if !IsTransientDialError(err) {
		return true
	}
	if window.since.IsZero() {
		window.since = now
	}
	return now.Sub(window.since) >= budget
}

// UnreachableError is a transient dial failure returned from a startup path.
// Its text names only the bounded cause: the underlying error can carry the
// dial address, and the lifecycle runtime logs a start error verbatim. Unwrap
// keeps the cause for errors.Is and errors.As.
type UnreachableError struct{ cause error }

func (UnreachableError) Error() string   { return "dependency unreachable: cause=unreachable" }
func (e UnreachableError) Unwrap() error { return e.cause }

// MaskUnreachable returns err as an UnreachableError when it is a transient
// dial failure, and err unchanged otherwise.
func MaskUnreachable(err error) error {
	var masked UnreachableError
	if err == nil || errors.As(err, &masked) || !IsTransientDialError(err) {
		return err
	}
	return UnreachableError{cause: err}
}
