//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Migration 131 keys work_graph_deployment_incident_edges by (org_id, ifNull(repo_id, zero uuid), deployment_id, incident_id).
// Rehearsed through the production migrator: the key and the column type, one row for each repository when two
// repositories share a deployment id, the higher link tier winning a pair whatever the time, the operator rollback
// (data kept, old key, version row gone) and the upgrade after it, which keeps a legacy row without a repository.
func TestDeploymentIncidentEdgesRepoKeyMigration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	text := func(query string) string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx, query).Scan(&out); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return out
	}
	count := func(query string) uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, query).Scan(&n); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return n
	}
	const org = "5b7e2c40-91d3-4f6a-8e25-3c0d9a7f1b61"
	const repoA, repoB = "5b7e2c40-91d3-4f6a-8e25-3c0d9a7f1b62", "5b7e2c40-91d3-4f6a-8e25-3c0d9a7f1b63"
	recorded := func() uint64 {
		return count("SELECT count() FROM schema_migrations WHERE version = '131_deployment_incident_edges_repo_key.sql'")
	}
	leftover := func() uint64 {
		return count("SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE 'work_graph_deployment_incident_edges_%'")
	}
	insert := func(deployment, incident, repo, source string, computedAt string) {
		t.Helper()
		exec(`INSERT INTO work_graph_deployment_incident_edges (edge_id, org_id, deployment_id, incident_id, provider, repo_id, confidence, source, evidence, observed_at, computed_at)
VALUES ('e-` + deployment + `', '` + org + `', '` + deployment + `', '` + incident + `', 'github', ` + repo + `, 0.5, '` + source + `', '', '2026-10-01 00:00:00', '` + computedAt + `')`)
	}

	chschema.Apply(ctx, t, instance)
	if recorded() == 0 || leftover() != 0 {
		t.Fatalf("after the upgrade: version rows %d, leftover tables %d; want the version and no leftover", recorded(), leftover())
	}
	if got := text(`SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'work_graph_deployment_incident_edges'`); !strings.HasPrefix(got, "org_id, ifNull(repo_id, ") || !strings.HasSuffix(got, "), deployment_id, incident_id") {
		t.Fatalf("sorting key = %q", got)
	}
	if got := text(`SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'work_graph_deployment_incident_edges' AND name = 'repo_id'`); got != "Nullable(UUID)" {
		t.Fatalf("repo_id is %q, want Nullable(UUID)", got)
	}

	// Two repositories, one deployment id, one incident: two rows. One pair seen as heuristic after native: the
	// native row stays although the heuristic row is newer.
	insert("d-shared", "i-1", "'"+repoA+"'", "heuristic", "2026-10-01 01:00:00")
	insert("d-shared", "i-1", "'"+repoB+"'", "heuristic", "2026-10-01 02:00:00")
	insert("d-pair", "i-2", "'"+repoA+"'", "native", "2026-10-01 01:00:00")
	insert("d-pair", "i-2", "'"+repoA+"'", "heuristic", "2026-10-01 09:00:00")
	exec(`OPTIMIZE TABLE work_graph_deployment_incident_edges FINAL`)
	if got := count(`SELECT count() FROM work_graph_deployment_incident_edges FINAL WHERE deployment_id = 'd-shared'`); got != 2 {
		t.Errorf("two repositories sharing a deployment id hold %d rows, want 2", got)
	}
	if got := text(`SELECT groupArray(source) FROM (SELECT source FROM work_graph_deployment_incident_edges FINAL WHERE deployment_id = 'd-pair')`); got != "['native']" {
		t.Errorf("the pair holds %s, want only the native row", got)
	}

	body, err := os.ReadFile("rollback/131_deployment_incident_edges_repo_key.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		exec(statement)
	}
	if recorded() != 0 || leftover() != 0 {
		t.Fatalf("after the rollback: version rows %d, leftover tables %d; want neither", recorded(), leftover())
	}
	if got := text(`SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'work_graph_deployment_incident_edges'`); got != "org_id, deployment_id, incident_id, source" {
		t.Fatalf("sorting key after the rollback = %q", got)
	}
	if got := count(`SELECT count() FROM work_graph_deployment_incident_edges FINAL`); got != 3 {
		t.Fatalf("the rollback kept %d rows, want 3", got)
	}

	// Legacy rows with no repository keep their row, one for each pair.
	insert("d-one", "i-3", "NULL", "heuristic", "2026-10-01 03:00:00")
	insert("d-one", "i-3", "'"+repoA+"'", "heuristic", "2026-10-01 03:00:00")
	insert("d-none", "i-5", "NULL", "heuristic", "2026-10-01 03:00:00")
	chschema.Apply(ctx, t, instance)
	if recorded() == 0 || leftover() != 0 {
		t.Fatalf("the upgrade after the rollback: version rows %d, leftover tables %d", recorded(), leftover())
	}
	if got := count(`SELECT count() FROM work_graph_deployment_incident_edges FINAL WHERE deployment_id = 'd-one'`); got != 2 {
		t.Errorf("d-one holds %d rows after the upgrade, want 2 (one with a repository, one without)", got)
	}
	if got := count(`SELECT count() FROM work_graph_deployment_incident_edges FINAL WHERE deployment_id = 'd-none' AND repo_id IS NULL`); got != 1 {
		t.Errorf("the legacy row of d-none: %d rows without a repository, want 1", got)
	}
	if got := count(`SELECT count() FROM work_graph_deployment_incident_edges FINAL`); got != 6 {
		t.Errorf("the table holds %d rows after the upgrade, want 6", got)
	}
}
