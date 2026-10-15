//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Migration 132 makes work_graph_pr_commit keep the link of the highest tier for a pair, whatever the sync order
// (CHAOS-9572): a text row stamped after a native row does not replace it, the same tier still lets the newer row
// win, and a replay of an older native row does not replace a newer one.
func TestPRCommitNativeWinsWhateverTheSyncOrder(t *testing.T) {
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
	chschema.Apply(ctx, t, instance)

	const repo = "11111111-2222-4333-8444-555555555555"
	const org = "00000000-0000-4000-8000-000000000001"
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	insert := func(pr int, commit, provenance, evidence, at string) {
		t.Helper()
		exec(`INSERT INTO work_graph_pr_commit (repo_id, pr_number, commit_hash, confidence, provenance, evidence, last_synced, org_id) VALUES ('` +
			repo + `', ` + string(rune('0'+pr)) + `, '` + commit + `', 1, '` + provenance + `', '` + evidence + `', '` + at + `', '` + org + `')`)
	}
	served := func(pr int, commit string) (string, string, uint64) {
		t.Helper()
		exec(`OPTIMIZE TABLE work_graph_pr_commit FINAL`)
		var provenance, evidence string
		var rows uint64
		if err := conn.QueryRow(ctx, `SELECT any(provenance), any(evidence), count() FROM work_graph_pr_commit FINAL WHERE org_id = '`+org+
			`' AND pr_number = `+string(rune('0'+pr))+` AND commit_hash = '`+commit+`'`).Scan(&provenance, &evidence, &rows); err != nil {
			t.Fatal(err)
		}
		return provenance, evidence, rows
	}

	cases := []struct {
		name       string
		pr         int
		rows       [][3]string // provenance, evidence, last_synced
		wantTier   string
		wantSource string
	}{
		{"native first, text stamped later", 1, [][3]string{
			{"native", "api_pr_commits", "2026-10-15 10:00:01"}, {"explicit_text", "commit_message", "2026-10-15 10:00:02"}},
			"native", "api_pr_commits"},
		{"text first, native stamped older", 2, [][3]string{
			{"explicit_text", "commit_message", "2026-10-15 10:00:05"}, {"native", "api_pr_commits", "2026-10-15 10:00:01"}},
			"native", "api_pr_commits"},
		{"same tier, newer wins", 3, [][3]string{
			{"native", "old_evidence", "2026-10-15 10:00:01"}, {"native", "new_evidence", "2026-10-15 10:00:02"}},
			"native", "new_evidence"},
		{"replay of an older native row", 4, [][3]string{
			{"native", "new_evidence", "2026-10-15 10:00:09"}, {"native", "old_evidence", "2026-10-15 10:00:01"}},
			"native", "new_evidence"},
		{"text over heuristic whatever the time", 5, [][3]string{
			{"explicit_text", "commit_message", "2026-10-15 10:00:01"}, {"heuristic", "branch_name", "2026-10-15 10:00:09"}},
			"explicit_text", "commit_message"},
	}
	for _, c := range cases {
		for _, r := range c.rows {
			insert(c.pr, "abc", r[0], r[1], r[2])
		}
		provenance, evidence, rows := served(c.pr, "abc")
		if provenance != c.wantTier || evidence != c.wantSource || rows != 1 {
			t.Errorf("%s: served %s/%s rows=%d, want %s/%s rows=1", c.name, provenance, evidence, rows, c.wantTier, c.wantSource)
		}
	}
}

// The named limit of migration 132: a pair whose native row the old ReplacingMergeTree(last_synced) already merged
// away (native stamped before text) is not restored by the migration, which copies only the rows that remain. The
// repair is a data step: a provider re-sync (dho backfill run) re-emits the native link with a new last_synced, and
// the new version_rank then keeps it over the text row. A pair whose rows both remain physically is repaired by the copy.
func TestPRCommitMigrationCannotRestoreANativeRowTheOldEngineAlreadyMergedAway(t *testing.T) {
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
	chschema.Apply(ctx, t, instance)

	const repo = "11111111-2222-4333-8444-555555555555"
	const org = "00000000-0000-4000-8000-000000000001"
	run := func(path string) {
		t.Helper()
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, statement := range chmigrate.SplitStatements(string(body)) {
			if err := conn.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
		}
	}
	insert := func(pr int, provenance, evidence, at string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO work_graph_pr_commit (repo_id, pr_number, commit_hash, confidence, provenance, evidence, last_synced, org_id) VALUES (?, ?, 'abc', 1, ?, ?, ?, ?)`,
			repo, uint32(pr), provenance, evidence, at, org); err != nil {
			t.Fatal(err)
		}
	}
	served := func(pr int) string {
		t.Helper()
		var provenance string
		if err := conn.QueryRow(ctx, `SELECT any(provenance) FROM work_graph_pr_commit FINAL WHERE org_id = ? AND pr_number = ? AND commit_hash = 'abc'`,
			org, uint32(pr)).Scan(&provenance); err != nil {
			t.Fatal(err)
		}
		return provenance
	}

	run("rollback/132_pr_commit_native_wins.down.sql")
	insert(1, "native", "api_pr_commits", "2026-10-15 10:00:01")
	insert(1, "explicit_text", "commit_message", "2026-10-15 10:00:02")
	if err := conn.Exec(ctx, `OPTIMIZE TABLE work_graph_pr_commit FINAL`); err != nil {
		t.Fatal(err)
	}
	insert(2, "native", "api_pr_commits", "2026-10-15 10:00:01")
	insert(2, "explicit_text", "commit_message", "2026-10-15 10:00:02")
	if got := served(1); got != "explicit_text" {
		t.Fatalf("old engine after the merge serves %s, want explicit_text (the lost native row)", got)
	}

	run("sql/132_pr_commit_native_wins.sql")
	if got := served(1); got != "explicit_text" {
		t.Errorf("migration 132 restored a merged-away native row: serves %s; the limit changed, update the RISK-NOTES data step", got)
	}
	if got := served(2); got != "native" {
		t.Errorf("a pair whose rows both remained physically serves %s after the migration, want native", got)
	}

	insert(1, "native", "api_pr_commits", "2026-10-16 09:00:00")
	if got := served(1); got != "native" {
		t.Errorf("after the provider re-sync the lost pair serves %s, want native", got)
	}
}
