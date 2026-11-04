package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func refused() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
}

// fakeClockWait is a FirstConnect on a clock that only the sleeps move.
func fakeClockWait(budget time.Duration, out *bytes.Buffer) (FirstConnect, *time.Time) {
	clock := time.Unix(0, 0)
	return FirstConnect{
		Budget:         budget,
		AttemptTimeout: FirstConnectAttemptTimeout,
		Report:         TextReport(out, "test verb", "PostgreSQL"),
		Now:            func() time.Time { return clock },
		Sleep:          func(_ context.Context, d time.Duration) { clock = clock.Add(d) },
	}, &clock
}

func TestDefaultFirstConnectBounds(t *testing.T) {
	got := DefaultFirstConnect()
	if got.Budget != 60*time.Second || got.AttemptTimeout != 10*time.Second || got.Report != nil {
		t.Fatalf("DefaultFirstConnect() = %+v, want budget 60s, attempt timeout 10s, no report", got)
	}
}

func TestFirstConnectSucceedsAfterRefusals(t *testing.T) {
	var out bytes.Buffer
	wait, _ := fakeClockWait(FirstConnectBudget, &out)
	calls := 0
	err := wait.Do(context.Background(), func(context.Context) error {
		calls++
		if calls <= 3 {
			return refused()
		}
		return nil
	})
	if err != nil || calls != 4 {
		t.Fatalf("Do() = %v after %d attempts, want nil after 4", err, calls)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("want one line per retry (3), got %d:\n%s", len(lines), out.String())
	}
	for i, line := range lines {
		want := "test verb waiting: PostgreSQL endpoint not reachable (attempt " + strconv.Itoa(i+1) + ", "
		if !strings.HasPrefix(line, want) || !strings.Contains(line, ", refused, next in ") {
			t.Errorf("line %d = %q, want prefix %q and class refused", i, line, want)
		}
	}
}

func TestFirstConnectFailsAtBoundWithErrorClass(t *testing.T) {
	var out bytes.Buffer
	wait, clock := fakeClockWait(FirstConnectBudget, &out)
	start := *clock
	calls := 0
	err := wait.Do(context.Background(), func(context.Context) error {
		calls++
		return refused()
	})
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("Do() = %v, want the last attempt's refused error", err)
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("Do() = %T, want the attempt's own *net.OpError", err)
	}
	// Pauses 2, 4, 8, 16, 30 s: attempts at 0, 2, 6, 14 and 30 s; the last pause
	// ends at the 60 s bound and no attempt starts there.
	if calls != 5 || clock.Sub(start) != FirstConnectBudget {
		t.Fatalf("%d attempts over %v, want 5 over %v", calls, clock.Sub(start), FirstConnectBudget)
	}
	if lines := strings.Count(out.String(), "\n"); lines != calls {
		t.Fatalf("%d lines for %d attempts, want one per pause:\n%s", lines, calls, out.String())
	}
}

func TestFirstConnectFinalErrorStopsAtOnce(t *testing.T) {
	for name, final := range map[string]error{
		"wrong password": &pgconn.PgError{Code: "28P01"},
		"permission":     &pgconn.PgError{Code: "42501"},
		"plain":          errors.New("bad configuration"),
	} {
		var out bytes.Buffer
		wait, _ := fakeClockWait(FirstConnectBudget, &out)
		calls := 0
		err := wait.Do(context.Background(), func(context.Context) error { calls++; return final })
		if !errors.Is(err, final) || calls != 1 || out.Len() != 0 {
			t.Errorf("%s: Do() = %v after %d attempts with output %q, want the error after 1 attempt and no line", name, err, calls, out.String())
		}
	}
}

func TestFirstConnectRetriesAnAttemptThatRanOutItsTime(t *testing.T) {
	var out bytes.Buffer
	wait, _ := fakeClockWait(FirstConnectBudget, &out)
	calls := 0
	err := wait.Do(context.Background(), func(ctx context.Context) error {
		calls++
		if calls == 1 {
			// pgconn's form of a connect that hit its deadline.
			return fmt.Errorf("timeout: %w", context.DeadlineExceeded)
		}
		return nil
	})
	if err != nil || calls != 2 || !strings.Contains(out.String(), ", timeout, ") {
		t.Fatalf("Do() = %v after %d attempts, output %q; want a retried timeout", err, calls, out.String())
	}
}

func TestFirstConnectStopsWhenTheRunEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wait := DefaultFirstConnect()
	calls := 0
	err := wait.Do(ctx, func(context.Context) error {
		calls++
		cancel()
		return fmt.Errorf("dial: %w", context.Canceled)
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("Do() = %v after %d attempts, want the canceled error after 1", err, calls)
	}
}

func TestFirstConnectEachAttemptRunsUnderAttemptTimeout(t *testing.T) {
	const attemptTimeout = time.Hour
	wait := FirstConnect{Budget: time.Second, AttemptTimeout: attemptTimeout}
	var before, after, deadline time.Time
	before = time.Now()
	_ = wait.Do(context.Background(), func(ctx context.Context) error {
		after = time.Now()
		var ok bool
		if deadline, ok = ctx.Deadline(); !ok {
			t.Fatal("the attempt has no deadline")
		}
		return nil
	})
	// The deadline is set between the two reads: before+T <= deadline <= after+T.
	if deadline.Before(before.Add(attemptTimeout)) || deadline.After(after.Add(attemptTimeout)) {
		t.Fatalf("attempt deadline %v, want AttemptTimeout after the attempt start (%v..%v)", deadline, before, after)
	}
}

// freePort returns a loopback port nothing listens on.
func freePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func shortRealWait(budget time.Duration, out *bytes.Buffer) FirstConnect {
	return FirstConnect{
		Budget:         budget,
		AttemptTimeout: time.Second,
		Report:         TextReport(out, "test verb", "PostgreSQL"),
		Sleep:          func(context.Context, time.Duration) { time.Sleep(20 * time.Millisecond) },
	}
}

// A real socket: refused while nothing listens, connected once a listener opens.
func TestFirstConnectRealSocketRefusedThenListening(t *testing.T) {
	address := freePort(t)
	var out bytes.Buffer
	var listener net.Listener
	t.Cleanup(func() {
		if listener != nil {
			_ = listener.Close()
		}
	})
	calls := 0
	err := shortRealWait(30*time.Second, &out).Do(context.Background(), func(ctx context.Context) error {
		calls++
		if calls == 4 {
			var err error
			if listener, err = net.Listen("tcp", address); err != nil {
				t.Fatalf("listen again on %s: %v", address, err)
			}
		}
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
		if err != nil {
			return err
		}
		return conn.Close()
	})
	if err != nil || calls != 4 {
		t.Fatalf("Do() = %v after %d attempts, want nil after 4", err, calls)
	}
	if got := strings.Count(out.String(), ", refused, "); got != 3 {
		t.Fatalf("%d refused lines, want 3:\n%s", got, out.String())
	}
}

// pgx against a closed port: the wait ends at the bound with pgx's refused
// error, and the lines name neither the password nor the host.
func TestConnectWaitingRefusedPortFailsAtTheBound(t *testing.T) {
	address := freePort(t)
	dsn := "postgres://login:sekret-pw@" + address + "/db?sslmode=disable"
	var out bytes.Buffer
	wait, _ := fakeClockWait(10*time.Second, &out)
	conn, err := ConnectWaiting(context.Background(), dsn, wait)
	if conn != nil || !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("ConnectWaiting() = %v, %v; want no connection and a refused error", conn, err)
	}
	text := out.String()
	// A 10 s budget on a clock only the pauses (2, 4, 4 s) move: 3 attempts, 3 lines.
	if strings.Count(text, ", refused, ") != 3 {
		t.Fatalf("want 3 refused lines:\n%s", text)
	}
	if strings.Contains(text, "sekret-pw") || strings.Contains(text, "login") || strings.Contains(text, address) {
		t.Fatalf("a wait line names the password, the login or the address:\n%s", text)
	}
}

func TestOpenWaitingRefusedPortReturnsOpensError(t *testing.T) {
	address := freePort(t)
	config := DefaultConfig("postgres://login:sekret-pw@" + address + "/db?sslmode=disable")
	var out bytes.Buffer
	pool, err := OpenWaiting(context.Background(), config, shortRealWait(300*time.Millisecond, &out))
	if pool != nil || !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "sekret-pw") {
		t.Fatalf("OpenWaiting() = %v, %v; want Open's redacted ErrUnavailable", pool, err)
	}
	if !strings.Contains(out.String(), ", refused, ") {
		t.Fatalf("no refused line:\n%s", out.String())
	}
}

func TestOpenRuntimePoolsWaitingRefusedPortReturnsErrUnavailable(t *testing.T) {
	address := freePort(t)
	uri := func(role string) string {
		return "postgres://" + role + ":sekret-pw@" + address + "/db?sslmode=disable"
	}
	config := DefaultRuntimeConfig(uri("domain_role"), uri("queue_role"), "domain_role", "queue_role").WithCoordinator()
	config.CoordinatorURI = uri("coordinator_role")
	config.CoordinatorRole = "coordinator_role"
	config.CoordinatorMaxConns = 2
	var out bytes.Buffer
	pools, err := OpenRuntimePoolsWaiting(context.Background(), config, shortRealWait(300*time.Millisecond, &out))
	if pools != nil || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("OpenRuntimePoolsWaiting() = %v, %v; want ErrUnavailable", pools, err)
	}
	if strings.Count(out.String(), ", refused, ") < 2 {
		t.Fatalf("want at least 2 refused lines:\n%s", out.String())
	}
}
