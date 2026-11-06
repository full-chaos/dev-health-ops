package schemagate

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate/schemagatetest"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// fakeClock replaces now and wait: a wait advances the clock, and each read advances it by readCost.
type fakeClock struct {
	at     time.Time
	waited []time.Duration
}

func useFakeClock(t *testing.T) *fakeClock {
	t.Helper()
	clock := &fakeClock{at: time.Now()}
	oldNow, oldWait := now, wait
	now = func() time.Time { return clock.at }
	wait = func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		clock.waited = append(clock.waited, d)
		clock.at = clock.at.Add(d)
		return nil
	}
	t.Cleanup(func() { now, wait = oldNow, oldWait })
	return clock
}

// flaky fails with err for its first failures reads, then answers as answer.
type flaky struct {
	clock    *fakeClock
	readCost time.Duration
	failures int
	err      error
	answer   schemagatetest.Database
	reads    int
}

func (f *flaky) RecordedRevisions(ctx context.Context) ([]string, error) {
	f.reads++
	f.clock.at = f.clock.at.Add(f.readCost)
	if f.reads <= f.failures {
		return nil, f.err
	}
	return f.answer.RecordedRevisions(ctx)
}

func checkWith(t *testing.T, recorder Recorder) (string, error) {
	t.Helper()
	build, err := pgmigrate.LoadBuildHeads()
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	gateErr := Check(context.Background(), slog.New(slog.NewTextHandler(&logs, nil)), "test", recorder, build)
	return logs.String(), gateErr
}

func TestCheckPassesAfterATransientFirstRead(t *testing.T) {
	clock := useFakeClock(t)
	recorder := &flaky{clock: clock, failures: 2, err: &ReadError{Class: "dial_refused"}, answer: schemagatetest.AtHead(t)}
	logs, err := checkWith(t, recorder)
	if err != nil || strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "after a retry") || !strings.Contains(logs, "attempts=3") {
		t.Fatalf("error = %v, log = %q; want a clean start after 3 reads", err, logs)
	}
}

func TestCheckRefusesABehindDatabaseFoundOnARetry(t *testing.T) {
	clock := useFakeClock(t)
	recorder := &flaky{clock: clock, failures: 1, err: &ReadError{Class: "dns"}, answer: schemagatetest.OneBehind(t)}
	_, err := checkWith(t, recorder)
	var behind *BehindError
	if !errors.As(err, &behind) {
		t.Fatalf("error = %v, want *BehindError", err)
	}
}

func TestCheckSkipsAfterTheRetriesWithTheLastClass(t *testing.T) {
	clock := useFakeClock(t)
	start := clock.at
	recorder := &flaky{clock: clock, failures: 100, err: &ReadError{Class: "dial_refused"}}
	logs, err := checkWith(t, recorder)
	if err != nil || !strings.Contains(logs, "schema gate skipped") || !strings.Contains(logs, "class=dial_refused") {
		t.Fatalf("error = %v, log = %q; want a skip with the class", err, logs)
	}
	if want := len(retryDelays) + 1; recorder.reads != want || !strings.Contains(logs, fmt.Sprintf("attempts=%d", want)) {
		t.Fatalf("reads = %d, log = %q; want %d", recorder.reads, logs, want)
	}
	if held := clock.at.Sub(start); held > retryBudget {
		t.Fatalf("the gate held the start %v, over the budget %v", held, retryBudget)
	}
}

func TestCheckRetriesStayInsideTheBudgetWhenReadsAreSlow(t *testing.T) {
	clock := useFakeClock(t)
	recorder := &flaky{clock: clock, readCost: 3 * time.Second, failures: 100, err: &ReadError{Class: "timeout"}}
	start := clock.at
	logs, err := checkWith(t, recorder)
	if err != nil || !strings.Contains(logs, "class=timeout") {
		t.Fatalf("error = %v, log = %q; want a skip", err, logs)
	}
	firstRead := 3 * time.Second
	if held := clock.at.Sub(start) - firstRead; held > retryBudget+recorder.readCost {
		t.Fatalf("after the first read the gate held the start %v (%d reads, waits %v), over the budget", held, recorder.reads, clock.waited)
	}
	for i, d := range clock.waited {
		if d != retryDelays[i] {
			t.Fatalf("wait %d = %v, want %v", i, d, retryDelays[i])
		}
	}
	if recorder.reads >= len(retryDelays)+1 {
		t.Fatalf("%d reads: slow reads must cut the retries short", recorder.reads)
	}
}

func TestCheckDoesNotRetryAPermanentClass(t *testing.T) {
	for _, class := range []string{"permission_denied", "table_missing", "database_error", "tls", "no_pool"} {
		clock := useFakeClock(t)
		recorder := &flaky{clock: clock, failures: 100, err: &ReadError{Class: class}}
		logs, err := checkWith(t, recorder)
		if err != nil || recorder.reads != 1 || len(clock.waited) != 0 || !strings.Contains(logs, "class="+class) || !strings.Contains(logs, "attempts=1") {
			t.Errorf("%s: error = %v, reads = %d, waits = %v, log = %q; want one read and a skip", class, err, recorder.reads, clock.waited, logs)
		}
	}
}

func TestCheckStopsRetryingWhenTheStartIsCancelled(t *testing.T) {
	useFakeClock(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	recorder := &flaky{clock: &fakeClock{at: time.Now()}, failures: 100, err: &ReadError{Class: "unreachable"}}
	build, err := pgmigrate.LoadBuildHeads()
	if err != nil {
		t.Fatal(err)
	}
	if err := Check(ctx, slog.New(slog.DiscardHandler), "test", recorder, build); err != nil || recorder.reads != 1 {
		t.Fatalf("error = %v, reads = %d; want one read and a skip", err, recorder.reads)
	}
}

func TestReadClassNamesTheFailureWithoutDriverText(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}
	reset := &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}
	for want, err := range map[string]error{
		"permission_denied": &pgconn.PgError{Code: "42501"},
		"table_missing":     fmt.Errorf("query: %w", &pgconn.PgError{Code: "42P01"}),
		"database_error":    &pgconn.PgError{Code: "57P03"},
		"timeout":           fmt.Errorf("connect: %w", context.DeadlineExceeded),
		"dns":               fmt.Errorf("connect: %w", &net.DNSError{Err: "no such host", Name: "db.internal", IsNotFound: true}),
		"dial_refused":      fmt.Errorf("connect: %w", refused),
		"conn_reset":        fmt.Errorf("query: %w", reset),
		"tls":               fmt.Errorf("tls: %w", x509.UnknownAuthorityError{}),
		"unreachable":       errors.New("dial tcp 10.0.0.1:5432: something else"),
	} {
		if got := readClass(err); got != want {
			t.Errorf("readClass(%v) = %q, want %q", err, got, want)
		}
	}
	for _, err := range []error{io.EOF, io.ErrUnexpectedEOF} {
		if got := readClass(fmt.Errorf("read: %w", err)); got != "conn_reset" {
			t.Errorf("readClass(%v) = %q, want conn_reset", err, got)
		}
	}
	if got := readClass(&tls.CertificateVerificationError{Err: errors.New("x")}); got != "tls" {
		t.Errorf("certificate verification = %q, want tls", got)
	}
}

func TestPoolRecordedClassesARefusedDialThroughTheDriver(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := postgres.DefaultConfig("postgres://gate:secret-word@" + address + "/db?sslmode=disable")
	cfg.ConnectTimeout = 2 * time.Second
	pool, err := postgres.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, err = PoolRecorded(context.Background(), pool)
	var read *ReadError
	if !errors.As(err, &read) || read.Class != "dial_refused" || strings.Contains(err.Error(), "secret-word") {
		t.Fatalf("error = %v, want a dial_refused ReadError with no driver text", err)
	}
}

func TestRetryBoundHoldsAStartAtMostEightSeconds(t *testing.T) {
	var sum time.Duration
	for _, d := range retryDelays {
		sum += d
	}
	if retryBudget > 8*time.Second || sum >= retryBudget || len(retryDelays) == 0 {
		t.Fatalf("budget %v, delays %v (sum %v): the retries must fit inside at most 8 s", retryBudget, retryDelays, sum)
	}
}
