//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The work_items columns the view of migration 103 reads, as the chain types
// them (009 + 024).
const workItemsBeforeRelationsRead = `CREATE TABLE work_items (
    repo_id UUID, work_item_id String, provider String, project_id String,
    last_synced DateTime64(3), org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, work_item_id)`

// INVARIANT (CHAOS-8578): for every work item (org, id),
// max(relations_read_at) in work_item_relations_read is the latest
// last_synced of its work_items rows that are NOT github Projects v2 board
// rows (project_id 'ghprojv2:...'): the board pass reads no relations and
// never moves it. An item stored before the migration gets the latest such
// last_synced still stored. relation_writer exists on work_item_dependencies,
// is NULL unless a writer sets it, and is replaced with the row.
func TestRelationWriterAndReadMigrationKeepsTheLatestRelationRead(t *testing.T) {
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
	// readAt reads an item's read time the way the reader contract says: max
	// over the key. The empty string means no row.
	readAt := func(org, id string) string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx, `
SELECT if(count() = 0, '', toString(max(relations_read_at)))
FROM work_item_relations_read
WHERE org_id = ? AND work_item_id = ?`, org, id).Scan(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	const repo, noRepo = "11111111-1111-4111-8111-111111111111", "00000000-0000-0000-0000-000000000000"
	item := func(org, repoID, id, provider, project, lastSynced string) {
		t.Helper()
		exec(`INSERT INTO work_items (repo_id, work_item_id, provider, project_id, last_synced, org_id) VALUES ('` +
			repoID + `', '` + id + `', '` + provider + `', '` + project + `', '` + lastSynced + `', '` + org + `')`)
	}

	exec(workItemsBeforeRelationsRead)
	exec(dependenciesBeforeRelationStart)
	exec(`ALTER TABLE work_item_dependencies ADD COLUMN IF NOT EXISTS relation_started_at Nullable(DateTime64(3))`)
	// Before the migration: #7 read by the text pass on the 5th, then
	// written by the board pass on the 7th; #8 only by the board pass; a
	// jira item; #7 of another organization.
	item("org-a", repo, "gh:acme/api#7", "github", "acme/api", "2026-08-05 10:00:00.000")
	item("org-a", noRepo, "gh:acme/api#7", "github", "ghprojv2:acme#1", "2026-08-07 10:00:00.000")
	item("org-a", noRepo, "gh:acme/api#8", "github", "ghprojv2:acme#1", "2026-08-07 10:00:00.000")
	item("org-a", repo, "jira:OPS-1", "jira", "OPS", "2026-08-06 10:00:00.000")
	item("org-b", repo, "gh:acme/api#7", "github", "acme/api", "2026-08-02 10:00:00.000")

	sql, err := os.ReadFile("sql/103_work_item_relation_writer_and_read.sql")
	if err != nil {
		t.Fatal(err)
	}
	statements := chmigrate.SplitStatements(string(sql))
	if len(statements) != 4 {
		t.Fatalf("migration 103 has %d statements, want the column, the table, the view and the fill", len(statements))
	}
	for _, statement := range statements {
		exec(statement)
	}

	check := func(when string) {
		t.Helper()
		for _, tc := range []struct{ org, id, want string }{
			{"org-a", "gh:acme/api#7", "2026-08-09 10:00:00.000"},
			{"org-a", "gh:acme/api#8", ""},
			{"org-a", "jira:OPS-1", "2026-08-06 10:00:00.000"},
			{"org-b", "gh:acme/api#7", "2026-08-02 10:00:00.000"},
		} {
			if got := readAt(tc.org, tc.id); got != tc.want {
				t.Fatalf("%s: %s %s read at %q, want %q", when, tc.org, tc.id, got, tc.want)
			}
		}
	}
	// Items stored before the migration: the board row is not a read.
	if got := readAt("org-a", "gh:acme/api#7"); got != "2026-08-05 10:00:00.000" {
		t.Fatalf("org-a #7 stored before the migration: read at %q, want the text pass, not the later board pass", got)
	}
	// After it: a text pass moves the read time, a board pass does not.
	item("org-a", repo, "gh:acme/api#7", "github", "acme/api", "2026-08-09 10:00:00.000")
	item("org-a", noRepo, "gh:acme/api#7", "github", "ghprojv2:acme#1", "2026-08-12 10:00:00.000")
	item("org-a", noRepo, "gh:acme/api#8", "github", "ghprojv2:acme#1", "2026-08-12 10:00:00.000")
	check("after a text pass and a board pass")

	// Merges change nothing: the maximum per key survives them.
	exec(`OPTIMIZE TABLE work_item_relations_read FINAL`)
	exec(`OPTIMIZE TABLE work_items FINAL`)
	check("after merges")

	// The migration is re-runnable: every statement again, same answers.
	for _, statement := range statements {
		exec(statement)
	}
	check("after the migration ran again")

	// relation_writer: NULL unless a writer sets it, and replaced with the row.
	exec(`INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id, relationship_semantics_version) VALUES ('gitlab:a/r#5', 'gitlab:a/r#7', 'blocks', 'blocks', '2026-08-09 10:00:00.000', 'org-a', 'canonical-blocks.v2')`)
	var writer string
	if err := conn.QueryRow(ctx, `SELECT ifNull(relation_writer, 'NULL') FROM work_item_dependencies FINAL WHERE org_id = 'org-a' AND source_work_item_id = 'gitlab:a/r#5'`).Scan(&writer); err != nil {
		t.Fatal(err)
	}
	if writer != "NULL" {
		t.Fatalf("relation_writer of a row no writer set = %q, want NULL", writer)
	}
	exec(`INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id, relationship_semantics_version, relation_writer) VALUES ('gitlab:a/r#5', 'gitlab:a/r#7', 'blocks', 'blocks', '2026-08-10 10:00:00.000', 'org-a', 'canonical-blocks.v2', 'source')`)
	if err := conn.QueryRow(ctx, `SELECT ifNull(relation_writer, 'NULL') FROM work_item_dependencies FINAL WHERE org_id = 'org-a' AND source_work_item_id = 'gitlab:a/r#5'`).Scan(&writer); err != nil {
		t.Fatal(err)
	}
	if writer != "source" {
		t.Fatalf("relation_writer = %q, want the writer the newest row carries", writer)
	}
}
