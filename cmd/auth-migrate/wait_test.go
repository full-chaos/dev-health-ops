package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// fakeWait drives databaseWait with a clock that advances only when the wait
// sleeps or an attempt takes time, so no test depends on wall time.
type fakeWait struct {
	clock    time.Time
	attempts int
	results  []error
	cost     time.Duration
}

func (f *fakeWait) wait(stderr *bytes.Buffer) databaseWait {
	return databaseWait{
		open: func(context.Context) (*pgxpool.Pool, error) {
			f.attempts++
			f.clock = f.clock.Add(f.cost)
			if f.attempts <= len(f.results) {
				if err := f.results[f.attempts-1]; err != nil {
					return nil, err
				}
			} else if len(f.results) > 0 && f.results[len(f.results)-1] != nil {
				return nil, f.results[len(f.results)-1]
			}
			return &pgxpool.Pool{}, nil
		},
		now:      func() time.Time { return f.clock },
		sleep:    func(_ context.Context, d time.Duration) error { f.clock = f.clock.Add(d); return nil },
		bound:    connectWait,
		interval: connectRetryInterval,
		stderr:   stderr,
	}
}

func dialErr(errno syscall.Errno) error {
	return fmt.Errorf("failed to connect: %w", &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", errno)})
}

func TestDatabaseWaitSucceedsAfterTransportFailures(t *testing.T) {
	var stderr bytes.Buffer
	f := &fakeWait{results: []error{
		dialErr(syscall.ECONNREFUSED),
		fmt.Errorf("failed to connect: %w", &net.DNSError{Err: "no such host", Name: "db", IsNotFound: true}),
		fmt.Errorf("failed to connect: %w", context.DeadlineExceeded),
		dialErr(syscall.EHOSTUNREACH),
		fmt.Errorf("failed to connect: %w", &pgconn.PgError{Code: "57P03"}),
		nil,
	}}
	pool, err := f.wait(&stderr).run(context.Background())
	if err != nil || pool == nil {
		t.Fatalf("run = %v, %v; want the pool after five transient failures", pool, err)
	}
	if f.attempts != 6 {
		t.Errorf("attempts = %d, want 6", f.attempts)
	}
	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	want := []string{"refused", "dns", "timeout", "no_route", "server_starting"}
	if len(lines) != len(want) {
		t.Fatalf("attempt lines = %q, want %d", lines, len(want))
	}
	for i, line := range lines {
		prefix := fmt.Sprintf("migration waiting: PostgreSQL migration endpoint not reachable (attempt %d, ", i+1)
		if !strings.HasPrefix(line, prefix) || !strings.HasSuffix(line, want[i]+")") {
			t.Errorf("line %d = %q, want attempt %d and category %s", i+1, line, i+1, want[i])
		}
	}
}

func TestDatabaseWaitGivesUpAtTheBound(t *testing.T) {
	var stderr bytes.Buffer
	f := &fakeWait{results: []error{dialErr(syscall.ECONNREFUSED)}, cost: 500 * time.Millisecond}
	start := f.clock
	pool, err := f.wait(&stderr).run(context.Background())
	if pool != nil || !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("run = %v, %v; want ErrUnavailable", pool, err)
	}
	if spent := f.clock.Sub(start); spent > connectWait || spent < connectWait-2*connectRetryInterval {
		t.Errorf("gave up after %s, want just under the %s bound", spent, connectWait)
	}
	if f.attempts < 2 {
		t.Errorf("attempts = %d; the wait did not retry", f.attempts)
	}
	if strings.Count(stderr.String(), "migration waiting:") != f.attempts {
		t.Errorf("%d attempts but %d attempt lines", f.attempts, strings.Count(stderr.String(), "migration waiting:"))
	}
}

func TestDatabaseWaitStopsAtOnceOnANonTransportError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"authentication", fmt.Errorf("failed to connect: %w", &pgconn.PgError{Code: "28P01"})},
		{"permission", fmt.Errorf("failed to connect: %w", &pgconn.PgError{Code: "42501"})},
		{"tls", fmt.Errorf("failed to connect: %w", fmt.Errorf("tls: %w", tlsAlert()))},
		{"configuration", errors.New("cannot parse the connection string")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stderr bytes.Buffer
			f := &fakeWait{results: []error{tc.err, nil}}
			pool, err := f.wait(&stderr).run(context.Background())
			if pool != nil || !errors.Is(err, postgresstore.ErrUnavailable) {
				t.Fatalf("run = %v, %v; want ErrUnavailable", pool, err)
			}
			if f.attempts != 1 {
				t.Errorf("attempts = %d, want 1: a server or TLS answer is not retried", f.attempts)
			}
			if stderr.Len() != 0 {
				t.Errorf("stderr = %q, want no attempt line", stderr.String())
			}
		})
	}
}

func TestDatabaseWaitLinesCarryNoHostOrLogin(t *testing.T) {
	var stderr bytes.Buffer
	secretHost, login := "db-7f3a.internal.example", "migrator_login"
	err := fmt.Errorf("failed to connect to `user=%s database=postgres`: %s:5432: %w", login, secretHost,
		&net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)})
	f := &fakeWait{results: []error{err, nil}}
	if _, runErr := f.wait(&stderr).run(context.Background()); runErr != nil {
		t.Fatal(runErr)
	}
	if out := stderr.String(); strings.Contains(out, secretHost) || strings.Contains(out, login) || strings.Contains(out, "5432") {
		t.Errorf("attempt line carries connection details: %q", out)
	}
}

func TestDatabaseWaitEndsWhenTheRunIsCancelled(t *testing.T) {
	var stderr bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeWait{results: []error{dialErr(syscall.ECONNREFUSED)}}
	w := f.wait(&stderr)
	w.sleep = func(context.Context, time.Duration) error { cancel(); return context.Canceled }
	if _, err := w.run(ctx); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("run = %v, want ErrUnavailable", err)
	}
	if f.attempts != 1 {
		t.Errorf("attempts = %d, want 1", f.attempts)
	}
}

func tlsAlert() error { return tls.AlertError(42) }
