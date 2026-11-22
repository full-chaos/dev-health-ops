//go:build integration

package chmigrate_test

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// Migration 170 on the table as main's baseline creates it, holding rows in more than one part: one ALTER adds
// tie_ordinal UInt32 to the end of the key, keeps every row and part (no mutation), and can run again. An older
// writer that names no tie_ordinal writes 0, the rank of the only timeline event of an instant, so its two rows at
// one instant still collapse as on main.
func TestMigration170AddsTieOrdinalToTheKeyInPlace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	create := ""
	for _, object := range baseline.Objects {
		if object.Name == "work_item_transitions" {
			create = object.Create
		}
	}
	if create == "" {
		t.Fatal("the baseline has no work_item_transitions")
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	alter := ""
	for _, file := range chain {
		if file.Version == "170_work_item_transitions_tie_ordinal.sql" {
			alter = file.SQL
		}
	}
	if alter == "" {
		t.Fatal("the chain has no 170_work_item_transitions_tie_ordinal.sql")
	}
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%v\n%s", err, statement)
		}
	}
	exec(create)
	exec(`SYSTEM STOP MERGES work_item_transitions`)
	legacy := `INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, from_status, to_status, last_synced, org_id) VALUES `
	exec(legacy + `('00000000-0000-0000-0000-000000000000', 'A', '2026-03-02 09:00:00', 'done', 'todo', '2026-11-01 00:00:00', 'o1')`)
	exec(legacy + `('00000000-0000-0000-0000-000000000000', 'B', '2026-03-03 09:00:00', 'todo', 'done', '2026-11-01 00:00:00', 'o1')`)
	state := func() (parts, rows uint64, key, kind string, mutations uint64) {
		t.Helper()
		if err := conn.QueryRow(ctx, `SELECT count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'work_item_transitions' AND active`).Scan(&parts, &rows); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, `SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'work_item_transitions'`).Scan(&key); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, `SELECT ifNull(any(type), '') FROM system.columns WHERE database = currentDatabase() AND table = 'work_item_transitions' AND name = 'tie_ordinal'`).Scan(&kind); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, `SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'work_item_transitions'`).Scan(&mutations); err != nil {
			t.Fatal(err)
		}
		return parts, rows, key, kind, mutations
	}
	parts0, rows0, _, _, _ := state()
	for run := 1; run <= 2; run++ {
		for _, statement := range chmigrate.SplitStatements(alter) {
			exec(statement)
		}
		parts, rows, key, kind, mutations := state()
		t.Logf("run %d: parts %d -> %d, rows %d -> %d, key %q, tie_ordinal %s, mutations %d", run, parts0, parts, rows0, rows, key, kind, mutations)
		if parts != parts0 || rows != rows0 || key != "org_id, repo_id, work_item_id, occurred_at, tie_ordinal" || kind != "UInt32" || mutations != 0 {
			t.Fatalf("run %d: want the parts and rows kept, the key ending with tie_ordinal UInt32 and no mutation", run)
		}
	}
	old := `INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, last_synced, org_id, source_id) VALUES `
	exec(old + `('00000000-0000-0000-0000-000000000000', 'C', '2026-03-04 09:00:00', '', 'done', 'todo', '', 'reopened', '', '2026-11-02 00:00:00', 'o1', NULL)`)
	exec(old + `('00000000-0000-0000-0000-000000000000', 'C', '2026-03-04 09:00:00', '', 'todo', 'in_progress', '', 'in progress', '', '2026-11-02 00:00:01', 'o1', NULL)`)
	exec(`SYSTEM START MERGES work_item_transitions`)
	exec(`OPTIMIZE TABLE work_item_transitions FINAL`)
	var rows uint64
	var ordinals, survivor string
	if err := conn.QueryRow(ctx, `SELECT count(), toString(groupUniqArray(tie_ordinal)), any(to_status) FROM work_item_transitions WHERE work_item_id = 'C'`).Scan(&rows, &ordinals, &survivor); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || ordinals != "[0]" || survivor != "in_progress" {
		t.Fatalf("old writer on the new table: %d rows, ordinals %s, survivor %q; want 1 row at ordinal 0 (the newer, as on main)", rows, ordinals, survivor)
	}
}
