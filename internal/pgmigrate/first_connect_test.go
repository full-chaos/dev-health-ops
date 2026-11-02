package pgmigrate

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// `migrate postgres upgrade` (the migrate hook's first step) and `migrate
// postgres build-index` (the streak-index and open-units-index hooks) wait for
// their first connect, write one line per retry, and fail closed at the bound
// with postgres_unavailable.
func TestUpgradeAndBuildIndexWaitForTheFirstConnectThenFailClosed(t *testing.T) {
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
	dsn := "postgresql://migrator:sekret-pw@" + address + "/devhealth?sslmode=disable"
	resolve := ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		return secrets.NewValue(dsn), "test", true
	})
	lookup := func(key string) (string, bool) {
		if key == CutoverEnv {
			return "1", true
		}
		return "", false
	}
	verbs := map[string]cli.Command{}
	for _, child := range Command(resolve).Children {
		verbs[child.Name] = child
	}
	for verb, args := range map[string][]string{
		"upgrade":     nil,
		"build-index": {"--deadline=10s", "open-units"},
	} {
		var stdout, stderr bytes.Buffer
		start := time.Now()
		code := verbs[verb].Run(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		out := stderr.String()
		if code != cli.ExitFailure || !strings.Contains(out, `"postgres_unavailable"`) {
			t.Fatalf("%s: exit %d, want %d with postgres_unavailable:\n%s", verb, code, cli.ExitFailure, out)
		}
		if !strings.Contains(out, "migrate postgres "+verb+" waiting: PostgreSQL endpoint not reachable (attempt 1, ") ||
			!strings.Contains(out, ", refused, next in ") {
			t.Fatalf("%s: no wait line before the failure:\n%s", verb, out)
		}
		if elapsed := time.Since(start); elapsed < 900*time.Millisecond || elapsed > 5*time.Second {
			t.Fatalf("%s: failed after %v, want at the 1 s bound", verb, elapsed)
		}
		if strings.Contains(out, "sekret-pw") {
			t.Fatalf("%s: the output names the password:\n%s", verb, out)
		}
	}
}
