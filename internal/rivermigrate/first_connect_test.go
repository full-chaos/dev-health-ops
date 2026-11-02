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

// `migrate river` (river-migrate hook) and `migrate roles` (provision-roles
// hook) wait for their first connect, write one line per retry, and fail
// closed at the bound with their own unavailable message.
func TestRiverAndRolesWaitForTheFirstConnectThenFailClosed(t *testing.T) {
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
		var stdout, stderr bytes.Buffer
		start := time.Now()
		code := run(&stdout, &stderr)
		out := stderr.String()
		if code != 1 || !strings.Contains(out, "migration error: PostgreSQL migration endpoint unavailable") {
			t.Fatalf("%s: exit %d, want 1 with the unavailable message:\n%s", name, code, out)
		}
		if !strings.Contains(out, name+" waiting: PostgreSQL migration endpoint not reachable (attempt 1, ") ||
			!strings.Contains(out, ", refused, next in ") {
			t.Fatalf("%s: no wait line before the failure:\n%s", name, out)
		}
		if elapsed := time.Since(start); elapsed < 900*time.Millisecond || elapsed > 5*time.Second {
			t.Fatalf("%s: failed after %v, want at the 1 s bound", name, elapsed)
		}
		if strings.Contains(out, "sekret-pw") || strings.Contains(out, "pw-domain") {
			t.Fatalf("%s: the output names a password:\n%s", name, out)
		}
	}
}
