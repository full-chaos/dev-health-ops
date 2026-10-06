//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// INVARIANT (CHAOS-8790): the real chain, applied to a fresh database, leaves
// work_item_interactions keyed by the provider comment id as the LAST part of
// the sorting key, so two comments of one work item in the same millisecond are
// two rows; the old writer's column list (no interaction_id) still inserts,
// as a legacy row with the empty id; and migration 107 is re-runnable: a
// second run of every statement is a no-op, as the chain's rule requires.
func TestWorkItemInteractionsCommentIDMigration(t *testing.T) {
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
	admin := openDatabase(t, instance.URI, "")
	name := scratchDatabase(t, admin)
	conn := openDatabase(t, instance.URI, name)
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("real chain: %v", err)
	}
	str := func(query string) string {
		t.Helper()
		var value string
		if err := conn.QueryRow(ctx, query).Scan(&value); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return value
	}
	count := func(query string) uint64 {
		t.Helper()
		var value uint64
		if err := conn.QueryRow(ctx, query).Scan(&value); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return value
	}

	got := str("SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'work_item_interactions'")
	const want = "org_id, work_item_id, occurred_at, interaction_type, interaction_id"
	if got != want {
		t.Fatalf("sorting key = %q, want %q", got, want)
	}

	// the migration file again, statement by statement, as the runner sends it
	file, err := os.ReadFile("sql/107_work_item_interactions_comment_id.sql")
	if err != nil {
		t.Fatal(err)
	}
	statements := chmigrate.SplitStatements(string(file))
	if len(statements) != 2 {
		t.Fatalf("migration 107 has %d statements, want the ALTER and the view", len(statements))
	}
	if !strings.HasPrefix(statements[0], "ALTER TABLE work_item_interactions") {
		t.Fatalf("the first statement is not the one ALTER: %.60s", statements[0])
	}
	for _, statement := range statements {
		if err := db.Exec(ctx, statement); err != nil {
			t.Fatalf("re-run: %v", err)
		}
	}

	insert := func(columns, values string) {
		t.Helper()
		if err := conn.Exec(ctx, "INSERT INTO work_item_interactions ("+columns+") VALUES "+values); err != nil {
			t.Fatal(err)
		}
	}
	const old = "work_item_id, provider, interaction_type, occurred_at, actor, body_length, last_synced, org_id"
	insert(old, "('wi-old', 'github', 'comment', '2026-10-01 10:00:00.123', 'a', 1, '2026-10-01 11:00:00.000', 'o1')")
	if id := str("SELECT interaction_id FROM work_item_interactions FINAL WHERE work_item_id = 'wi-old'"); id != "" {
		t.Fatalf("an old-writer row has interaction_id %q, want the empty legacy id", id)
	}

	const keyed = old + ", interaction_id"
	insert(keyed, "('wi-new', 'github', 'comment', '2026-10-01 10:00:00.123', 'a', 1, '2026-10-01 11:00:00.000', 'o1', 'c-1'), "+
		"('wi-new', 'github', 'comment', '2026-10-01 10:00:00.123', 'a', 1, '2026-10-01 11:00:00.000', 'o1', 'c-2')")
	if n := count("SELECT count() FROM work_item_interactions FINAL WHERE work_item_id = 'wi-new'"); n != 2 {
		t.Fatalf("two same-millisecond comments = %d rows, want 2", n)
	}
	if err := conn.Exec(ctx, "OPTIMIZE TABLE work_item_interactions FINAL"); err != nil {
		t.Fatal(err)
	}
	if n := count("SELECT count() FROM work_item_interactions WHERE work_item_id = 'wi-new'"); n != 2 {
		t.Fatalf("after OPTIMIZE FINAL the table holds %d rows of the pair, want 2", n)
	}
}
