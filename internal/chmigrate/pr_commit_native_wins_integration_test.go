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
