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

// A dropped SYN holds an attempt only for AttemptTimeout; the next attempt
// then starts. pgx gets no connect_timeout, so only the attempt bounds it.
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
	var starts, ends []time.Time
	err := wait.Do(context.Background(), func(ctx context.Context) error {
		starts = append(starts, time.Now())
		conn, err := pgx.Connect(ctx, dsn)
		ends = append(ends, time.Now())
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
	for i := range starts {
		if took := ends[i].Sub(starts[i]); took < attemptTimeout-50*time.Millisecond || took > attemptTimeout+time.Second {
			t.Errorf("attempt %d took %v, want about the %v attempt timeout", i+1, took, attemptTimeout)
		}
		if i > 0 && starts[i].Before(ends[i-1]) {
			t.Errorf("attempt %d started before attempt %d ended", i+1, i)
		}
	}
	if got := strings.Count(out.String(), ", timeout, "); got != len(starts)-1 {
		t.Fatalf("%d timeout lines for %d attempts, want one per retry:\n%s", got, len(starts), out.String())
	}
}
