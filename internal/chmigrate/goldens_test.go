package chmigrate_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// pythonGoldenBuild is the 40-hex commit of the last build whose Python
// ClickHouse chain produced the files below. Frozen: nothing after it can be
// recorded, because the producer is deleted with the Python CLI.
const pythonGoldenBuild = "acd02fb0a61f2d8648d3dee4e22534c426b00cc4"

// freezePoint is the last migration a Python producer ran: the golden state
// and the recorded splitter output cover every migration at or below it. A
// later migration has no Python truth and is checked by Go-only invariants.
const freezePoint = "099_team_project_ownership_last_synced.sql"

// pythonGoldens pins the sha256 of every golden recorded from the Python
// chain. A missing or edited file fails; it is never skipped.
//
// Recipe (needs a checkout of pythonGoldenBuild and its Python environment):
// run the chain with ClickHouseMetricsSink(dsn).ensure_schema(force=True) on a
// fresh database with OPERATIONAL_ORDERING_CONTRACT=2, capture every object
// (name, engine, CREATE without the database name), the seeded rows and
// schema_migrations versions as chmigrate.Baseline JSON into
// python_chain_contract2.json; run split_sql_statements over every
// src/dev_health_ops/migrations/clickhouse/*.sql into python_split.json
// ({"<file>": {"sql": text, "statements": [...]}}); re-pin the digests here.
var pythonGoldens = map[string]string{
	"python_chain_contract2.json": "d86cd8faa0ee39867eea96264de7e536af1ed4bafb7e03b68e7a6992dbb01aa4",
	"python_split.json":           "59d9e0c4f038bfa31c7b7d2492180feb74a648484c47c236158f0b3c4b343a48",
}

// readPythonGolden reads a recorded golden after checking its pinned digest.
func readPythonGolden(t *testing.T, name string) []byte {
	t.Helper()
	want, ok := pythonGoldens[name]
	if !ok {
		t.Fatalf("%s has no pinned digest", name)
	}
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("the recorded Python golden %s is missing: %v", name, err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("%s sha256 %s, pinned %s: the golden was edited or replaced; re-record it from build %s (see the recipe on pythonGoldens) and re-pin it",
			name, got, want, pythonGoldenBuild)
	}
	return raw
}

// TestPythonGoldensMatchTheirDigest fails when a recorded golden is missing or
// differs from its pin, without needing a ClickHouse.
func TestPythonGoldensMatchTheirDigest(t *testing.T) {
	if len(pythonGoldenBuild) != 40 || strings.Trim(pythonGoldenBuild, "0123456789abcdef") != "" {
		t.Fatalf("pythonGoldenBuild %q is not a 40-hex commit", pythonGoldenBuild)
	}
	for name := range pythonGoldens {
		readPythonGolden(t, name)
	}
}
