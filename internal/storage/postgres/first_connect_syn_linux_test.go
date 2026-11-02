//go:build linux

package postgres

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/platform/timeoutclass"
)

// silentListener is a loopback listener with backlog 0 that never accepts.
// Once its accept queue holds one connection, Linux drops every further SYN
// (no SYN-ACK, no RST): a dial to it hangs, as a dial does while a new pod's
// NetworkPolicy drops its packets.
func silentListener(t *testing.T) string {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Close(fd) })
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Listen(fd, 0); err != nil {
		t.Fatal(err)
	}
	bound, err := syscall.Getsockname(fd)
	if err != nil {
		t.Fatal(err)
	}
	address := "127.0.0.1:" + strconv.Itoa(bound.(*syscall.SockaddrInet4).Port)
	for range 16 {
		conn, err := net.DialTimeout("tcp", address, 300*time.Millisecond)
		if err != nil {
			if timeoutclass.IsTimeout(err) {
				return address
			}
			t.Fatalf("filling the accept queue: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
	}
	t.Fatal("the accept queue never filled: a dial to the listener still completes")
	return ""
}

// A dropped SYN holds an attempt only until AttemptTimeout ends it; the next
// attempt then starts. pgx gets no connect_timeout, so only the attempt's own
// deadline can end it: each attempt returns with that deadline passed while
// the run is still live.
func TestFirstConnectAttemptTimeoutBoundsADroppedSYN(t *testing.T) {
	address := silentListener(t)
	dsn := "postgres://login:sekret-pw@" + address + "/db?sslmode=disable"
	const attemptTimeout = 400 * time.Millisecond
	var out bytes.Buffer
	wait := FirstConnect{
		Budget:         1500 * time.Millisecond,
		AttemptTimeout: attemptTimeout,
		Report:         TextReport(&out, "test verb", "PostgreSQL"),
		Sleep:          func(context.Context, time.Duration) { time.Sleep(20 * time.Millisecond) },
	}
	// A hang ceiling only: the run ends red if an attempt outlives its deadline by minutes.
	run, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var starts, ends []time.Time
	var endedByOwnDeadline []bool
	err := wait.Do(run, func(ctx context.Context) error {
		deadline, _ := ctx.Deadline()
		starts = append(starts, time.Now())
		conn, err := pgx.Connect(ctx, dsn)
		end := time.Now()
		ends = append(ends, end)
		// The socket's deadline (the attempt's) can fire a beat before ctx's own timer.
		endedByOwnDeadline = append(endedByOwnDeadline, !end.Before(deadline) && run.Err() == nil)
		if err == nil {
			_ = conn.Close(context.Background())
		}
		return err
	})
	if err == nil || !timeoutclass.IsTimeout(err) {
		t.Fatalf("Do() = %v, want the last attempt's timeout", err)
	}
	if len(starts) < 2 {
		t.Fatalf("%d attempts, want the dropped SYN retried", len(starts))
	}
	if run.Err() != nil {
		t.Fatal("the run hit its hang ceiling: an attempt was not ended by its own deadline")
	}
	for i := range starts {
		if !endedByOwnDeadline[i] {
			t.Errorf("attempt %d returned before its own deadline passed: the SYN was answered, not dropped", i+1)
		}
		if i > 0 && starts[i].Before(ends[i-1]) {
			t.Errorf("attempt %d started before attempt %d ended", i+1, i)
		}
	}
	if got := strings.Count(out.String(), ", timeout, "); got != len(starts)-1 {
		t.Fatalf("%d timeout lines for %d attempts, want one per retry:\n%s", got, len(starts), out.String())
	}
}
