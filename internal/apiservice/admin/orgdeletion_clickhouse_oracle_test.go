//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"fmt"
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

// clickHouseOrgTableKnownPythonOnlyStale is org_deletion.py's own
// migration-history staleness in _clickhouse_tables_from_migrations():
// every entry here is a table name Python's static regex still reports,
// with the reason it is stale. Team-lead's ruling (R356: no Python edit
// for this): the Go route's live system.columns discovery is the ground
// truth; this map is an ACCEPTANCE GATE naming today's exact, understood
// divergence, not a general allowance -- TestClickHouseOrgTableDiscoveryMatchesThePythonMigrationRegex
// below fails loud if the real divergence set ever stops matching this
// map exactly, in either direction.
var clickHouseOrgTableKnownPythonOnlyStale = map[string]string{
	"ci_daily_rollup":         "migration 093 DROPs this table; the regex only sees migration 026's CREATE, never 093's DROP",
	"commit_daily_rollup":     "migration 093 DROPs this table; the regex only sees migration 026's CREATE, never 093's DROP",
	"deployment_daily_rollup": "migration 093 DROPs this table; the regex only sees migration 026's/045's CREATE, never 093's DROP",
	"ai_attribution_new":      "migration 044's own shadow-swap temp table (EXCHANGE TABLES then DROP), gone by the end of that same migration",
	"statement":               "a false positive: the regex matched literal prose inside migration 027's own module docstring, not real SQL",
}

// TestClickHouseOrgTableDiscoveryMatchesThePythonMigrationRegex is CHAOS-6306
// approval condition 1: the Go org-deletion route discovers org_id-bearing
// ClickHouse tables LIVE from system.columns (admin.DiscoverClickHouseOrgTables
// -- the exact function orgDeletionPurgeClickHouse itself calls, never a
// second hand-authored copy), while org_deletion.py discovers the same set by
// statically regex-parsing the migration files
// (_clickhouse_tables_from_migrations). Team-lead's ruling (26): this is an
// accepted named divergence, proven by asserting the EXACT known-stale set
// above -- never a bare "they differ" warning, and never silently widening
// if the real set drifts from what is named here.
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
	goSet := map[string]bool{}
	for _, table := range goTables {
		goSet[table.Name] = true
	}

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
	pythonSet := map[string]bool{}
	for _, name := range pythonNames {
		pythonSet[name] = true
	}

	if len(goSet) == 0 || len(pythonSet) == 0 {
		t.Fatalf("empty discovered table set (go=%d python=%d) -- the migration chain likely did not apply", len(goSet), len(pythonSet))
	}

	// Every table Python has and Go does not must be named, with a reason,
	// in clickHouseOrgTableKnownPythonOnlyStale -- and every name in that
	// map must actually be reproduced, or the map itself has rotted.
	var unexplained []string
	seen := map[string]bool{}
	for name := range pythonSet {
		if goSet[name] {
			continue
		}
		seen[name] = true
		if _, known := clickHouseOrgTableKnownPythonOnlyStale[name]; !known {
			unexplained = append(unexplained, fmt.Sprintf("python has %q, go does not, and it is NOT in clickHouseOrgTableKnownPythonOnlyStale -- either a new staleness bug in org_deletion.py, or Go's live discovery just started missing a real table", name))
		}
	}
	for name := range clickHouseOrgTableKnownPythonOnlyStale {
		if !seen[name] {
			unexplained = append(unexplained, fmt.Sprintf("clickHouseOrgTableKnownPythonOnlyStale names %q, but python no longer reports it and go doesn't either -- the entry is stale, remove it", name))
		}
	}

	// Every table Go has and Python does not is a hard FAIL: Go's scope
	// must never silently widen beyond what this test has named and a
	// human has reviewed.
	var goOnly []string
	for name := range goSet {
		if !pythonSet[name] {
			goOnly = append(goOnly, name)
		}
	}
	sort.Strings(goOnly)

	if len(unexplained) > 0 || len(goOnly) > 0 {
		sort.Strings(unexplained)
		t.Fatalf("ClickHouse org-table discovery diverged from the accepted set:\nunexplained: %v\ngo-only (never accepted): %v", unexplained, goOnly)
	}
}
