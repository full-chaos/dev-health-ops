//go:build integration

package chmigrate_test

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// INVARIANT (CHAOS-8574): for every relation (org, source, target, type),
// min(first_seen_at) in work_item_dependency_first_seen is the earliest
// last_synced any insert of that relation carried -- a time a sync really
// wrote it -- and a later sync of the same relation does not move it.
// relation_started_at exists, is NULL unless a writer sets it, and is replaced
// with the row. (The fill of relations stored before migration 102 and its
// rerun are archived with it, CHAOS-11751.)
func TestRelationFirstSeenKeepsTheFirstSyncTimeOfEveryRelation(t *testing.T) {
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
	chschema.Apply(ctx, t, instance) // the head, migration 102 included
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
	// firstSeen reads the relation's first-seen time the way the reader
	// contract says: min over the key. The empty string means no row.
	firstSeen := func(org, source, target string) string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx, `
SELECT if(count() = 0, '', toString(min(first_seen_at)))
FROM work_item_dependency_first_seen
WHERE org_id = ? AND source_work_item_id = ? AND target_work_item_id = ? AND relationship_type = 'blocks'`,
			org, source, target).Scan(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	insert := func(org, source, target, lastSynced string) {
		t.Helper()
		exec(`INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id, relationship_semantics_version) VALUES ('` +
			source + `', '` + target + `', 'blocks', 'blocks', '` + lastSynced + `', '` + org + `', 'canonical-blocks.v2')`)
	}

	// A-1 -> A-2 is synced twice (two parts, not merged): the earlier sync is
	// its first-seen time. The same ids in org-b are another relation.
	insert("org-a", "jira:A-1", "jira:A-2", "2026-08-01 10:00:00.000")
	insert("org-a", "jira:A-1", "jira:A-2", "2026-08-05 10:00:00.000")
	insert("org-b", "jira:A-1", "jira:A-2", "2026-08-03 10:00:00.000")

	if got := firstSeen("org-a", "jira:A-1", "jira:A-2"); got != "2026-08-01 10:00:00.000" {
		t.Fatalf("org-a relation synced twice: first seen %q, want the earlier sync", got)
	}
	if got := firstSeen("org-b", "jira:A-1", "jira:A-2"); got != "2026-08-03 10:00:00.000" {
		t.Fatalf("org-b relation: first seen %q, want its own time (the same ids in another organization are another relation)", got)
	}

	// A later sync of a known relation does not move its first-seen time.
	insert("org-a", "jira:A-1", "jira:A-2", "2026-08-09 10:00:00.000")
	if got := firstSeen("org-a", "jira:A-1", "jira:A-2"); got != "2026-08-01 10:00:00.000" {
		t.Fatalf("after a later sync: first seen %q, want it unchanged", got)
	}
	// A relation written for the first time after the migration: the view
	// records the sync that wrote it, and the next sync does not move it.
	if got := firstSeen("org-a", "jira:A-3", "jira:A-2"); got != "" {
		t.Fatalf("a relation never written has first seen %q", got)
	}
	insert("org-a", "jira:A-3", "jira:A-2", "2026-08-09 10:00:00.000")
	insert("org-a", "jira:A-3", "jira:A-2", "2026-08-12 10:00:00.000")
	if got := firstSeen("org-a", "jira:A-3", "jira:A-2"); got != "2026-08-09 10:00:00.000" {
		t.Fatalf("a new relation synced twice: first seen %q, want the first sync", got)
	}

	// Merges change nothing: the minimum per key survives them.
	exec(`OPTIMIZE TABLE work_item_dependency_first_seen FINAL`)
	exec(`OPTIMIZE TABLE work_item_dependencies FINAL`)
	if got := firstSeen("org-a", "jira:A-1", "jira:A-2"); got != "2026-08-01 10:00:00.000" {
		t.Fatalf("after merges: first seen %q, want it unchanged", got)
	}
	if got := firstSeen("org-a", "jira:A-3", "jira:A-2"); got != "2026-08-09 10:00:00.000" {
		t.Fatalf("after merges: first seen of the new relation %q, want it unchanged", got)
	}

	// relation_started_at: NULL unless a writer sets it, and replaced with the row.
	var nulls, rows uint64
	if err := conn.QueryRow(ctx, `SELECT countIf(relation_started_at IS NULL), count() FROM work_item_dependencies FINAL`).Scan(&nulls, &rows); err != nil {
		t.Fatal(err)
	}
	if rows == 0 || nulls != rows {
		t.Fatalf("%d of %d relation rows have a NULL relation_started_at, want all of them: no writer set one", nulls, rows)
	}
	exec(`INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id, relationship_semantics_version, relation_started_at) VALUES ('jira:A-3', 'jira:A-2', 'blocks', 'blocks', '2026-08-15 10:00:00.000', 'org-a', 'canonical-blocks.v2', '2026-07-20 08:30:00.000')`)
	var started string
	if err := conn.QueryRow(ctx, `SELECT toString(relation_started_at) FROM work_item_dependencies FINAL WHERE org_id = 'org-a' AND source_work_item_id = 'jira:A-3'`).Scan(&started); err != nil {
		t.Fatal(err)
	}
	if started != "2026-07-20 08:30:00.000" {
		t.Fatalf("relation_started_at = %q, want the provider time the newest row carries", started)
	}
	if got := firstSeen("org-a", "jira:A-3", "jira:A-2"); got != "2026-08-09 10:00:00.000" {
		t.Fatalf("a provider time on the row moved first seen to %q", got)
	}
}
