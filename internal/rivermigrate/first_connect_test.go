package rivermigrate

import (
	"bytes"
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// The package's tests that reach an unreachable database wait 1 s, not the
// 60 s production budget.
func TestMain(m *testing.M) {
	firstConnect = postgresstore.FirstConnect{Budget: time.Second, AttemptTimeout: time.Second}
	os.Exit(m.Run())
}

func refusedAddress(t *testing.T) string {
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

// fakeClockFirstConnect is a 10 s budget on a clock only the pauses move:
// pauses of 2, 4 and 4 s, so 4 attempts and 3 retry lines.
func fakeClockFirstConnect() postgresstore.FirstConnect {
	clock := time.Unix(0, 0)
	return postgresstore.FirstConnect{
		Budget: 10 * time.Second, AttemptTimeout: time.Second,
		Now:   func() time.Time { return clock },
		Sleep: func(_ context.Context, d time.Duration) { clock = clock.Add(d) },
	}
}

// `migrate river` (river-migrate hook) and `migrate roles` (provision-roles
// hook) wait for their first connect, write one line per retry, and fail
// closed at the bound with their own unavailable message.
func TestRiverAndRolesWaitForTheFirstConnectThenFailClosed(t *testing.T) {
	saved := firstConnect
	t.Cleanup(func() { firstConnect = saved })
	uri := "postgresql://migrator:sekret-pw@" + refusedAddress(t) + "/devhealth?sslmode=disable"
	for name, run := range map[string]func(stdout, stderr *bytes.Buffer) int{
		"migrate river": func(stdout, stderr *bytes.Buffer) int {
			return execute(context.Background(), []string{"--apply-and-check"}, env(map[string]string{
				"MIGRATION_DATABASE_URI":     uri,
				"RIVER_DOMAIN_DATABASE_ROLE": "devhealth_domain",
				"RIVER_QUEUE_DATABASE_ROLE":  "devhealth_queue",
			}), stdout, stderr)
		},
		"migrate roles": func(stdout, stderr *bytes.Buffer) int {
			return ExecuteRoles(context.Background(), "dho", nil, envLookup(map[string]string{
				"MIGRATION_DATABASE_URI":              uri,
				"RIVER_DOMAIN_DATABASE_ROLE":          "devhealth_domain",
				"RIVER_DOMAIN_DATABASE_PASSWORD":      "pw-domain",
				"RIVER_QUEUE_DATABASE_ROLE":           "devhealth_queue",
				"RIVER_QUEUE_DATABASE_PASSWORD":       "pw-queue",
				"RIVER_COORDINATOR_DATABASE_PASSWORD": "pw-coordinator",
			}), stdout, stderr)
		},
	} {
		firstConnect = fakeClockFirstConnect()
		var stdout, stderr bytes.Buffer
		code := run(&stdout, &stderr)
		out := stderr.String()
		if code != 1 || !strings.Contains(out, "migration error: PostgreSQL migration endpoint unavailable") {
			t.Fatalf("%s: exit %d, want 1 with the unavailable message:\n%s", name, code, out)
		}
		if !strings.Contains(out, name+" waiting: PostgreSQL migration endpoint not reachable (attempt 1, ") ||
			!strings.Contains(out, ", refused, next in ") {
			t.Fatalf("%s: no wait line before the failure:\n%s", name, out)
		}
		if got := strings.Count(out, name+" waiting: "); got != 3 {
			t.Fatalf("%s: %d wait lines, want 3 (4 attempts in the 10 s budget):\n%s", name, got, out)
		}
		if strings.Contains(out, "sekret-pw") || strings.Contains(out, "pw-domain") {
			t.Fatalf("%s: the output names a password:\n%s", name, out)
		}
	}
}
