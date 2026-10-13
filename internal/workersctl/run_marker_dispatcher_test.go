package workersctl

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// The finalize-redrive VERB, not only its marker helper, opens ClickHouse only
// for a real reset of a succeeded run. With no CLICKHOUSE_URI in the process:
// a real pass with --include-succeeded fails on the ClickHouse setting before
// it touches the domain store, and a dry run or a pass without
// --include-succeeded never reads that setting. A `false &&` in front of the
// guard fails the first case; dropping the guard fails the others.
func TestFinalizeRedriveDispatcherOpensClickHouseOnlyForARealResetOfASucceededRun(t *testing.T) {
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	// A lazy pool: the verb builds its store on it, and a query fails at once.
	pool, err := pgxpool.New(context.Background(), "postgres://dho@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	base := []string{"--org", "00000000-0000-4000-8000-000000009475", "--from", "2026-10-12", "--to", "2026-10-12"}
	write := append([]string{"--review-evidence", "CHAOS-9475: dispatcher test", "--reason", "deploy_drain", "--correlation-id", "redrive-9475"}, base...)
	for _, test := range []struct {
		name           string
		args           []string
		wantClickHouse bool
	}{
		{"real pass of a succeeded run", write, true},
		{"real pass without include-succeeded", append([]string{"--include-succeeded=false"}, write...), false},
		{"dry run", append([]string{"--dry-run"}, base...), false},
		{"dry run with include-succeeded", append([]string{"--dry-run", "--include-succeeded=true"}, base...), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := commandRuntime(t, commandAuthorizer{})
			runtime.registry = registry
			runtime.pools = &postgresstore.RuntimePools{Domain: pool}
			runtime.lookup = func(string) (string, bool) { return "", false }
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var stdout, stderr bytes.Buffer
			code := dispatchMetricsFinalizeRedrive(ctx, runtime, test.args, &stdout, &stderr)
			if code == 0 {
				t.Fatalf("exit code 0 with no reachable stores; stdout %s", stdout.String())
			}
			named := strings.Contains(stderr.String(), "CLICKHOUSE_URI")
			if named != test.wantClickHouse {
				t.Fatalf("stderr names CLICKHOUSE_URI = %v, want %v; stderr %s", named, test.wantClickHouse, stderr.String())
			}
		})
	}
}
