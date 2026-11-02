package usersbackfill

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// The auth backfill Job's verbs share connect: it waits for the first connect,
// writes one line per retry, and fails closed at the bound.
func TestConnectWaitsForTheFirstConnectThenFailsClosed(t *testing.T) {
	saved := firstConnect
	firstConnect = pgstorage.FirstConnect{Budget: time.Second, AttemptTimeout: time.Second}
	t.Cleanup(func() { firstConnect = saved })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	start := time.Now()
	pool, _, code := connect(context.Background(), cli.Env{Stderr: &stderr, Lookup: func(key string) (string, bool) {
		if key == dsnEnv {
			return "postgresql://auth_runtime:sekret-pw@" + address + "/auth?sslmode=disable", true
		}
		return "", false
	}}, "users-backfill")
	out := stderr.String()
	if pool != nil || code != cli.ExitFailure || !strings.Contains(out, "users-backfill: could not connect to the database: ") {
		t.Fatalf("connect() = %v, %d; want no pool and exit %d:\n%s", pool, code, cli.ExitFailure, out)
	}
	if !strings.Contains(out, "users-backfill waiting: PostgreSQL endpoint not reachable (attempt 1, ") ||
		!strings.Contains(out, ", refused, next in ") {
		t.Fatalf("no wait line before the failure:\n%s", out)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("failed after %v, want at the 1 s bound", elapsed)
	}
	if strings.Contains(out, "sekret-pw") {
		t.Fatalf("the output names the password:\n%s", out)
	}
}
