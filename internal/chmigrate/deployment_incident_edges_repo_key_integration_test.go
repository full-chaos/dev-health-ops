//go:build integration

package chmigrate_test

import (
	"context"
	"strings"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Migration 131 keys work_graph_deployment_incident_edges by (org_id, ifNull(repo_id, zero uuid), deployment_id, incident_id).
// On the head: the key and the column type, one row for each repository when two repositories share a deployment
// id, the higher link tier winning a pair whatever the time, and a row without a repository kept apart from the
// same pair with one. (The rehearsal of the copy, the operator rollback and the upgrade after it is archived with
// 131, CHAOS-11751.)
func TestDeploymentIncidentEdgesRepoKey(t *testing.T) {
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
	if got := text(`SELECT arrayStringConcat(groupArray(toString(source)), ',') FROM work_graph_deployment_incident_edges FINAL WHERE deployment_id = 'd-pair'`); got != "native" {
		t.Errorf("the pair holds %s, want only the native row", got)
	}

	// A pair without a repository and the same pair with one are two rows (the key reads ifNull(repo_id, zero)).
	insert("d-one", "i-3", "NULL", "heuristic", "2026-10-01 03:00:00")
	insert("d-one", "i-3", "'"+repoA+"'", "heuristic", "2026-10-01 04:00:00")
	exec(`OPTIMIZE TABLE work_graph_deployment_incident_edges FINAL`)
	if got := count(`SELECT count() FROM work_graph_deployment_incident_edges FINAL WHERE deployment_id = 'd-one'`); got != 2 {
		t.Errorf("d-one holds %d rows, want 2 (one without a repository, one with)", got)
	}
}
