package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

const (
	// connectWait bounds how long the run waits for its database to become
	// reachable. A hook Job can start while PostgreSQL is Ready but its
	// Service does not route yet; the wait covers that, and a database that
	// stays unreachable still fails the run loudly.
	connectWait = 60 * time.Second
	// connectRetryInterval separates two connect attempts.
	connectRetryInterval = time.Second
)

// databaseWait opens the migration pool, retrying on transport errors and on
// the server's "starting up" answer until the bound. Any other server answer
// (authentication, permission), a TLS failure or a configuration error ends the
// wait at once.
type databaseWait struct {
	open     func(context.Context) (*pgxpool.Pool, error)
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error
	bound    time.Duration
	interval time.Duration
	// attemptTimeout bounds one connect attempt (the pool's ConnectTimeout).
	attemptTimeout time.Duration
	stderr         io.Writer
}

// configError marks a failure to build the pool from its configuration, before
// any connection is attempted.
type configError struct{ error }

func (e configError) Unwrap() error { return e.error }

func newDatabaseWait(config postgresstore.Config, stderr io.Writer) databaseWait {
	return databaseWait{
		open: func(ctx context.Context) (*pgxpool.Pool, error) {
			pool, err := postgresstore.New(ctx, config)
			if err != nil {
				return nil, err
			}
			if err := pool.Ping(ctx); err != nil {
				pool.Close()
				return nil, err
			}
			return pool, nil
		},
		now:            time.Now,
		sleep:          sleepContext,
		bound:          connectWait,
		interval:       connectRetryInterval,
		attemptTimeout: config.ConnectTimeout,
		stderr:         stderr,
	}
}

// run returns the open pool, or postgresstore.ErrUnavailable. The error text of
// a failed attempt is never written: it can carry the host and the login.
func (w databaseWait) run(ctx context.Context) (*pgxpool.Pool, error) {
	start := w.now()
	for attempt := 1; ; attempt++ {
		pool, err := w.open(ctx)
		if err == nil {
			return pool, nil
		}
		category, retry := transportCategory(err)
		elapsed := w.now().Sub(start)
		if !retry || ctx.Err() != nil {
			return nil, postgresstore.ErrUnavailable
		}
		fmt.Fprintf(w.stderr, "migration waiting: PostgreSQL migration endpoint not reachable (attempt %d, %s elapsed, %s)\n",
			attempt, elapsed.Round(100*time.Millisecond), category)
		if elapsed+w.interval >= w.bound {
			return nil, postgresstore.ErrUnavailable
		}
		if err := w.sleep(ctx, w.interval); err != nil {
			return nil, postgresstore.ErrUnavailable
		}
	}
}

// transportCategory names a connect failure that a later attempt can outlast
// (the endpoint is not reachable yet) and reports whether it is one.
func transportCategory(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// cannot_connect_now: the server is starting up, shutting down or in
		// recovery, an availability answer like a refused dial. Every other
		// server answer (authentication, permission) is final.
		if pgErr.Code == "57P03" {
			return "server_starting", true
		}
		return "server", false
	}
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	var record tls.RecordHeaderError
	var alert tls.AlertError
	if errors.As(err, &unknownAuthority) || errors.As(err, &hostname) || errors.As(err, &invalid) ||
		errors.As(err, &record) || errors.As(err, &alert) {
		return "tls", false
	}
	var dnsErr *net.DNSError
	switch {
	case errors.As(err, &dnsErr):
		return "dns", true
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused", true
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return "no_route", true
	case errors.Is(err, os.ErrDeadlineExceeded), errors.Is(err, context.DeadlineExceeded):
		return "timeout", true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout", true
	}
	return "other", false
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
