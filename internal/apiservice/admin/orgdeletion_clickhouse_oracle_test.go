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
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
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
// This test needs a real, migrated ClickHouse (chmigrate, the migration
// chain `dho migrate clickhouse upgrade` applies) and, only while recording,
// a python3 interpreter; it does not need the live-Python FastAPI app, so it
// is not gated on DEV_HEALTH_LIVE_PYTHON_ORACLES -- only -tags=integration,
// like every other container-backed test in this tree.
func TestClickHouseOrgTableDiscoveryMatchesThePythonMigrationRegex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	venueoracle.MigrateClickHouseGo(t, ctx, instance.URI)

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

	golden := venueoracle.OpenGolden(t, adminGolden("clickhouse_org_tables", t.Name(), "a5d37df41d25222a696a4d21db8c7862068ef1ccdb951afc340cacdf519d1059"))
	root := golden.PythonRoot(t, repoRoot(t))
	script := `
import json
import sys
sys.path.insert(0, "src")
from dev_health_ops.api.services import org_deletion

print(json.dumps(sorted(org_deletion._clickhouse_tables_from_migrations())))
`
	// org_deletion.py's table list, parsed from the pinned build's migration
	// files while recording, frozen otherwise.
	answers := golden.Produce(t, root, []venueoracle.Request{venueoracle.ProgramRequest("clickhouse org tables", script, nil, nil)},
		func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
			// The harness starts the child in the closed environment: nothing
			// ambient shapes its answer.
			cmd, err := producer.Command(ctx, nil, nil, "-c", script)
			if err != nil {
				t.Fatal(err)
			}
			cmd.Dir = producer.Root
			output, err := cmd.Output()
			if err != nil {
				var stderr []byte
				if exitErr, ok := err.(*exec.ExitError); ok {
					stderr = exitErr.Stderr
				}
				t.Fatalf("python: %v", pyoracle.RunError(cmd.Path, err, stderr))
			}
			return []venueoracle.Response{{Status: 0, Body: string(output)}}
		})
	golden.Consumed(t, answers...)
	output := []byte(answers[0].Body)
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
	// Python's list is frozen at the pinned build: a table a later migration
	// added is Go-only by construction and must be named, with its migration,
	// in clickHouseOrgTablesAfterThePythonFreeze; an entry nothing reproduces
	// has rotted.
	var goOnly []string
	for name := range goSet {
		if !pythonSet[name] {
			if _, named := clickHouseOrgTablesAfterThePythonFreeze[name]; !named {
				goOnly = append(goOnly, name)
			}
		}
	}
	for name := range clickHouseOrgTablesAfterThePythonFreeze {
		if !goSet[name] || pythonSet[name] {
			unexplained = append(unexplained, fmt.Sprintf("clickHouseOrgTablesAfterThePythonFreeze names %q, but it is not a Go-only table (go has it: %v, the frozen python list has it: %v) -- remove the entry", name, goSet[name], pythonSet[name]))
		}
	}
	sort.Strings(goOnly)

	if len(unexplained) > 0 || len(goOnly) > 0 {
		sort.Strings(unexplained)
		t.Fatalf("ClickHouse org-table discovery diverged from the accepted set:\nunexplained: %v\ngo-only (never accepted): %v", unexplained, goOnly)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

// clickHouseOrgTablesAfterThePythonFreeze names each org-scoped ClickHouse
// table a migration added after adminPythonBuild, where org_deletion.py's
// list was frozen: table -> the migration that added it.
var clickHouseOrgTablesAfterThePythonFreeze = map[string]string{}
