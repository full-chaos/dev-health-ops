//go:build integration

package chschema

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// startMigratedInstance starts a container and applies the chain through Apply.
func startMigratedInstance(ctx context.Context, t *testing.T) *clickHouseHTTP {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close clickhouse: %v", err)
		}
	})
	Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("http dsn: %v", err)
	}
	client, err := newClickHouseHTTP(dsn)
	if err != nil {
		t.Fatalf("http client: %v", err)
	}
	return client
}

// fingerprints reads everything a schema guard or a table reader can observe
// about the migrated database: each object's engine and full create statement
// with its keys, each column's type/default/codec, and every seed row.
func fingerprints(ctx context.Context, t *testing.T, c *clickHouseHTTP) map[string]string {
	t.Helper()
	db := quoteString(c.database)
	queries := map[string]string{
		"objects": "SELECT name, engine, engine_full, create_table_query, sorting_key, partition_key, " +
			"primary_key, sampling_key FROM system.tables WHERE database = " + db + " ORDER BY name",
		"columns": "SELECT table, name, type, default_kind, default_expression, compression_codec, " +
			"is_in_sorting_key, is_in_partition_key FROM system.columns WHERE database = " + db +
			" ORDER BY table, position",
		"seed rows": "SELECT * FROM " + quoteIdentifier(c.database) + ".schema_migrations ORDER BY version",
	}
	out := map[string]string{}
	for name, query := range queries {
		data, err := c.do(ctx, query+" FORMAT JSONEachRow", nil)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		out[name] = string(data)
	}
	return out
}

// TestReplayedSchemaEqualsMigratedSchema pins the property the whole fast path
// rests on: a container that received the captured end state is
// indistinguishable, object by object, column by column and row by row, from
// the container the real Python chain ran against.
func TestReplayedSchemaEqualsMigratedSchema(t *testing.T) {
	ctx := context.Background()

	// Start from a cold cache so the first Apply below is the real chain, not
	// a replay left behind by another test in this process.
	snapshotMu.Lock()
	cachedSnapshots = map[string]*schemaSnapshot{}
	snapshotMu.Unlock()
	before := replayCount.Load()

	migrated := startMigratedInstance(ctx, t)
	if got := replayCount.Load(); got != before {
		t.Fatalf("the first Apply replayed (count %d -> %d); the real chain never ran", before, got)
	}
	replayed := startMigratedInstance(ctx, t)
	if got := replayCount.Load(); got != before+1 {
		t.Fatalf("the second Apply did not replay (count %d -> %d); it re-ran the chain", before, got)
	}

	want := fingerprints(ctx, t, migrated)
	got := fingerprints(ctx, t, replayed)
	for name, wantValue := range want {
		// Control: an empty fingerprint would make the equality below vacuous.
		if lines := strings.Count(wantValue, "\n"); lines < 100 && name != "seed rows" {
			t.Fatalf("%s fingerprint has only %d rows; the comparison would prove nothing", name, lines)
		}
		if strings.Count(wantValue, "\n") < 1 {
			t.Fatalf("%s fingerprint is empty", name)
		}
		if got[name] != wantValue {
			t.Errorf("%s differ between the migrated and the replayed database:\n--- migrated\n%s\n--- replayed\n%s",
				name, wantValue, got[name])
		}
	}
}

// TestReplayIsKeyedByTheChainEnvironment pins that the cache never serves a
// schema built under a different environment. OPERATIONAL_ORDERING_CONTRACT
// selects the shape of operational_incidents; a cache keyed on nothing hands a
// contract-1 test the contract-2 schema (and the reverse), which is the defect
// this test exists to catch.
func TestReplayIsKeyedByTheChainEnvironment(t *testing.T) {
	ctx := context.Background()
	const contractEnv = "OPERATIONAL_ORDERING_CONTRACT"

	snapshotMu.Lock()
	cachedSnapshots = map[string]*schemaSnapshot{}
	snapshotMu.Unlock()
	before := replayCount.Load()

	t.Setenv(contractEnv, "1")
	legacy := startMigratedInstance(ctx, t)
	t.Setenv(contractEnv, "2")
	revision := startMigratedInstance(ctx, t)
	if got := replayCount.Load(); got != before {
		t.Fatalf("a different environment replayed (count %d -> %d) instead of running the chain", before, got)
	}
	legacyPrint := fingerprints(ctx, t, legacy)
	revisionPrint := fingerprints(ctx, t, revision)
	// Control: the two environments must build different schemas, or the
	// equality checks below could not tell a wrong cache hit from a right one.
	if legacyPrint["objects"] == revisionPrint["objects"] {
		t.Fatalf("contract 1 and contract 2 built identical schemas; the control is vacuous")
	}

	t.Setenv(contractEnv, "1")
	legacyAgain := startMigratedInstance(ctx, t)
	if got := replayCount.Load(); got != before+1 {
		t.Fatalf("the repeated contract-1 environment did not replay (count %d -> %d)", before, got)
	}
	if got := fingerprints(ctx, t, legacyAgain); got["objects"] != legacyPrint["objects"] {
		t.Errorf("a replay under contract 1 differs from the contract-1 chain run:\n%s\nvs\n%s",
			got["objects"], legacyPrint["objects"])
	}
}

// TestApplyOnAMigratedDatabaseRunsTheChain pins that the fast path is only for
// an empty database. Moving an existing schema forward in place is a real use
// of Apply (a table migrated between two calls on one container), and a
// replay of CREATE statements into it cannot do that.
func TestApplyOnAMigratedDatabaseRunsTheChain(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close clickhouse: %v", err)
		}
	})

	Apply(ctx, t, instance)
	before := replayCount.Load()
	Apply(ctx, t, instance)
	if got := replayCount.Load(); got != before {
		t.Fatalf("Apply on an already-migrated database replayed (count %d -> %d)", before, got)
	}
}

// TestConcurrentApplyRunsTheChainOnce pins the lock discipline: callers that
// arrive together on a cold cache produce ONE chain run and replays for the
// rest, never several chain runs racing to capture, and never a replay of a
// half-captured snapshot.
func TestConcurrentApplyRunsTheChainOnce(t *testing.T) {
	ctx := context.Background()
	snapshotMu.Lock()
	cachedSnapshots = map[string]*schemaSnapshot{}
	snapshotMu.Unlock()
	before := replayCount.Load()

	const callers = 3
	t.Run("group", func(t *testing.T) {
		for i := 0; i < callers; i++ {
			t.Run(fmt.Sprintf("caller-%d", i), func(t *testing.T) {
				t.Parallel()
				startMigratedInstance(ctx, t)
			})
		}
	})
	if got := replayCount.Load() - before; got != callers-1 {
		t.Fatalf("%d concurrent callers produced %d replays, want %d (one chain run)", callers, got, callers-1)
	}
}
