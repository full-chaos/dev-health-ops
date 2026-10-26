package workerservice

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// transientDialError marks a startup dependency failure that a later attempt
// can outlast: the endpoint refused, reset or dropped the connection, or the
// pooler had no free slot. It satisfies health.TransientError.
type transientDialError struct{ cause error }

func (e transientDialError) Error() string { return e.cause.Error() }
func (e transientDialError) Unwrap() error { return e.cause }
func (transientDialError) Transient() bool { return true }

// isTransientDialError classifies err by type and code only, never by text.
// Auth (28P01, 28000), permission (42501), other server answers, TLS and
// configuration failures are final.
func isTransientDialError(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "57P03", "57P01", "53300", "08P01":
			return true
		}
		return len(pgErr.Code) >= 2 && pgErr.Code[:2] == "08"
	}
	var dnsErr *net.DNSError
	switch {
	case errors.As(err, &dnsErr),
		errors.Is(err, syscall.ECONNREFUSED),
		errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.EPIPE),
		errors.Is(err, syscall.EHOSTUNREACH),
		errors.Is(err, syscall.ENETUNREACH),
		errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, os.ErrDeadlineExceeded):
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// startupRetryJitter spreads a retry wait by up to +/-20% so replicas that
// started together do not redial in lockstep. Production wiring only; a nil
// jitter leaves the wait unchanged.
func startupRetryJitter(wait time.Duration) time.Duration {
	if wait <= 0 {
		return wait
	}
	spread := int64(wait) / 5
	if spread == 0 {
		return wait
	}
	return wait + time.Duration(rand.Int64N(2*spread+1)-spread)
}

func retryPresenceStart(
	ctx context.Context, component workerProcessComponent, start func(context.Context) error,
) error {
	return startupRetryBudget(
		ctx, component.startBudget, component.now, component.sleep, component.jitter,
		start,
		isTransientDialError,
		func(int, time.Duration, time.Duration, error) {},
		func(int, time.Duration, error, bool) {},
	)
}
