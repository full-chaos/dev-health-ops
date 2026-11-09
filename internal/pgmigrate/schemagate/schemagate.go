// Package schemagate refuses to start a runtime process whose database schema is behind
// the build: the migration head the database records must not lack a revision the
// binary needs. A database ahead of the build (a rollback) starts with a WARN, and a
// read that cannot happen (unreachable, no grant, no table) is skipped with a WARN, so
// the gate adds no failure mode beyond the head compare itself. A read that fails with a
// transient class is retried within retryBudget first, so a start is held at most that long
// beyond the first read.
package schemagate

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

const readTimeout = 10 * time.Second

// retryDelays are the waits before each retry of a read that failed with a transient class; retryBudget bounds
// the time from the first failure to the last retry's end, so the delays never all run when the reads are slow.
var (
	retryDelays = []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 3 * time.Second}
	retryBudget = 8 * time.Second
	now         = time.Now
	wait        = func(ctx context.Context, d time.Duration) error {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}
)

// transientClasses are the read classes a retry can clear: a first dial of a new process that the network or the
// name service refuses for a moment. A grant, a missing table, a TLS failure or a database error is not retried, and
// neither is a refused dial to a loopback address or a unix socket (dial_refused_local): no network policy or name
// service sits on that path.
var transientClasses = map[string]bool{"dial_refused": true, "dns": true, "conn_reset": true, "timeout": true, "unreachable": true}

// Recorder is the optional capability of a process database that can read the revisions
// public.alembic_version records.
type Recorder interface {
	RecordedRevisions(ctx context.Context) ([]string, error)
}

// BehindError is the refusal for a database behind the build. Its text holds revision ids only.
type BehindError struct {
	Recorded   []string
	BuildHeads []string
}

func (e *BehindError) Error() string {
	return fmt.Sprintf("the database schema is behind this build: alembic_version holds %v, the build needs %v", e.Recorded, e.BuildHeads)
}

// ReadError is a failed read of alembic_version, reduced to a class: it never carries the driver text.
type ReadError struct{ Class string }

func (e *ReadError) Error() string { return "read alembic_version: " + e.Class }

// PoolRecorded reads the recorded revisions through pool.
func PoolRecorded(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	if pool == nil {
		return nil, &ReadError{Class: "no_pool"}
	}
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	rows, err := pool.Query(ctx, "SELECT version_num FROM public.alembic_version")
	if err != nil {
		return nil, &ReadError{Class: readClass(err)}
	}
	revisions, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, &ReadError{Class: readClass(err)}
	}
	sort.Strings(revisions)
	return revisions, nil
}

func readClass(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42501":
			return "permission_denied"
		case "42P01":
			return "table_missing"
		}
		return "database_error"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns"
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		if localDial(err) {
			return "dial_refused_local"
		}
		return "dial_refused"
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "conn_reset"
	}
	var certErr *tls.CertificateVerificationError
	var recordErr tls.RecordHeaderError
	var authorityErr x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	var invalidErr x509.CertificateInvalidError
	if errors.As(err, &certErr) || errors.As(err, &recordErr) || errors.As(err, &authorityErr) || errors.As(err, &hostnameErr) || errors.As(err, &invalidErr) {
		return "tls"
	}
	return "unreachable"
}

// localDial reports whether err is a dial to a unix socket or a loopback address.
func localDial(err error) bool {
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		return false
	}
	if opErr.Net == "unix" {
		return true
	}
	tcp, ok := opErr.Addr.(*net.TCPAddr)
	return ok && tcp.IP.IsLoopback()
}

func errorClass(err error) string {
	var read *ReadError
	if errors.As(err, &read) {
		return read.Class
	}
	return "unreachable"
}

// readWithRetry reads the recorded revisions, retrying a transient failure after each of retryDelays while the
// retry budget, counted from the first failure, has room for the wait. It returns the attempts made.
func readWithRetry(ctx context.Context, recorder Recorder) ([]string, int, error) {
	recorded, err := recorder.RecordedRevisions(ctx)
	attempts := 1
	if err == nil || !transientClasses[errorClass(err)] {
		return recorded, attempts, err
	}
	deadline := now().Add(retryBudget)
	retryCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for _, delay := range retryDelays {
		if !now().Add(delay).Before(deadline) || wait(retryCtx, delay) != nil {
			break
		}
		recorded, err = recorder.RecordedRevisions(retryCtx)
		attempts++
		if err == nil || !transientClasses[errorClass(err)] {
			break
		}
	}
	return recorded, attempts, err
}

// Enforce applies the gate to a process database. A database that cannot read its
// revisions (a test fake, no Recorder) is not gated. It returns a *BehindError when the
// database is behind this build, and nil in every other case.
func Enforce(ctx context.Context, logger *slog.Logger, process string, database any) error {
	recorder, ok := database.(Recorder)
	if !ok {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	build, err := pgmigrate.LoadBuildHeads()
	if err != nil {
		logger.WarnContext(ctx, "schema gate skipped", "process", process, "class", "build_heads_unavailable")
		return nil
	}
	return Check(ctx, logger, process, recorder, build)
}

// Check compares the recorded revisions with build.
func Check(ctx context.Context, logger *slog.Logger, process string, recorder Recorder, build pgmigrate.BuildHeads) error {
	recorded, attempts, err := readWithRetry(ctx, recorder)
	if err != nil {
		logger.WarnContext(ctx, "schema gate skipped", "process", process, "class", errorClass(err), "attempts", attempts)
		return nil
	}
	if attempts > 1 {
		logger.InfoContext(ctx, "schema gate read the revisions after a retry", "process", process, "attempts", attempts)
	}
	if len(recorded) == 0 {
		logger.WarnContext(ctx, "schema gate skipped", "process", process, "class", "no_revisions_recorded")
		return nil
	}
	switch build.CompareHeads(recorded) {
	case pgmigrate.HeadsBehind:
		refusal := &BehindError{Recorded: recorded, BuildHeads: build.Heads()}
		logger.ErrorContext(ctx, "schema gate: refusing to start, the database schema is behind this build; run the migration",
			"process", process, "database_heads", refusal.Recorded, "build_heads", refusal.BuildHeads)
		return refusal
	case pgmigrate.HeadsAhead:
		logger.WarnContext(ctx, "schema gate: the database schema is ahead of this build (rollback?); starting",
			"process", process, "database_heads", recorded, "build_heads", build.Heads())
	}
	return nil
}
