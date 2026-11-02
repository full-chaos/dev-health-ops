package chmigrate

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

// `migrate clickhouse upgrade` (the migrate hook's ClickHouse step) waits for
// its first connect over the native and the HTTP protocol, writes one line
// per retry, and fails closed at the bound with clickhouse_unavailable.
func TestUpgradeWaitsForTheFirstClickHouseConnectThenFailsClosed(t *testing.T) {
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
	for _, dsn := range []string{
		"clickhouse://migrator:sekret-pw@" + address + "/devhealth",
		"http://migrator:sekret-pw@" + address + "/devhealth",
	} {
		var stdout, stderr bytes.Buffer
		start := time.Now()
		code := run(context.Background(), "upgrade", cli.Env{Stdout: &stdout, Stderr: &stderr, Lookup: func(key string) (string, bool) {
			switch key {
			case OrderingContractEnv:
				return "2", true
			case ClickHouseURIKey:
				return dsn, true
			}
			return "", false
		}})
		out := stderr.String()
		scheme := dsn[:strings.Index(dsn, ":")]
		if code != cli.ExitFailure || !strings.Contains(out, `"clickhouse_unavailable"`) {
			t.Fatalf("%s: exit %d, want %d with clickhouse_unavailable:\n%s", scheme, code, cli.ExitFailure, out)
		}
		if !strings.Contains(out, "migrate clickhouse upgrade waiting: ClickHouse endpoint not reachable (attempt 1, ") ||
			!strings.Contains(out, ", refused, next in ") {
			t.Fatalf("%s: no refused wait line before the failure:\n%s", scheme, out)
		}
		if elapsed := time.Since(start); elapsed < 900*time.Millisecond || elapsed > 5*time.Second {
			t.Fatalf("%s: failed after %v, want at the 1 s bound", scheme, elapsed)
		}
		if strings.Contains(out, "sekret-pw") {
			t.Fatalf("%s: the output names the password:\n%s", scheme, out)
		}
	}
}
