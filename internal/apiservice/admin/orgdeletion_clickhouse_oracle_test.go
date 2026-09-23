//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"os/exec"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// TestClickHouseOrgTableDiscoveryMatchesThePythonMigrationRegex is CHAOS-6306
// approval condition 1: the Go org-deletion route discovers org_id-bearing
// ClickHouse tables LIVE from system.columns (admin.DiscoverClickHouseOrgTables
// -- the exact function orgDeletionPurgeClickHouse itself calls, never a
// second hand-authored copy), while org_deletion.py discovers the same set by
// statically regex-parsing the migration files
// (_clickhouse_tables_from_migrations). Team-lead's ruling: this is an
// accepted named divergence ONLY as long as an oracle proves the two
// mechanisms agree today -- a difference here is a hard FAIL, never a
// warning, because org-deletion is destructive and its scope must never
// silently widen relative to the Python route it replaces.
//
// This test needs a real, migrated ClickHouse (chschema.Apply runs the
// actual migration chain) and a python3 interpreter; it does not need the
// live-Python FastAPI app, so it is not gated on
// DEV_HEALTH_LIVE_PYTHON_ORACLES -- only -tags=integration, like every other
// container-backed test in this tree.
func TestClickHouseOrgTableDiscoveryMatchesThePythonMigrationRegex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

	conn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer conn.Close()

	goTables, err := admin.DiscoverClickHouseOrgTables(ctx, conn)
	if err != nil {
		t.Fatalf("DiscoverClickHouseOrgTables: %v", err)
	}
	goNames := make([]string, len(goTables))
	for i, table := range goTables {
		goNames[i] = table.Name
	}
	sort.Strings(goNames)

	root := repoRoot(t)
	python := pyoracle.Resolve(t, root)
	script := `
import json
import sys
sys.path.insert(0, "src")
from dev_health_ops.api.services import org_deletion

print(json.dumps(sorted(org_deletion._clickhouse_tables_from_migrations())))
`
	cmd := exec.Command(python, "-c", script)
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python: %v\n%s", pyoracle.RunError(python, err, output), output)
	}
	var pythonNames []string
	if err := json.Unmarshal(output, &pythonNames); err != nil {
		t.Fatalf("decode python output: %v\n%s", err, output)
	}
	sort.Strings(pythonNames)

	if len(goNames) == 0 || len(pythonNames) == 0 {
		t.Fatalf("empty discovered table set (go=%d python=%d) -- the migration chain likely did not apply", len(goNames), len(pythonNames))
	}
	if len(goNames) != len(pythonNames) {
		t.Fatalf("table set SIZE diverged: go=%d python=%d\ngo=%v\npython=%v", len(goNames), len(pythonNames), goNames, pythonNames)
	}
	for i := range goNames {
		if goNames[i] != pythonNames[i] {
			t.Fatalf("table set diverged at index %d: go=%q python=%q\ngo=%v\npython=%v", i, goNames[i], pythonNames[i], goNames, pythonNames)
		}
	}
}
