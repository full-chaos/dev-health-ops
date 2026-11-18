//go:build integration

package chmigrate_test

import (
	"context"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// outOfOrderFixtures are the statements of 155 and 156 as their open pull
// requests hold them (the testops pipeline status counts at c3000b5b876c, the
// testops test-day retraction marker at a36502557277, numbered 156 in the merge
// order of the two pull requests), used only when the live chain does not hold
// that version yet.
var outOfOrderFixtures = []chmigrate.ChainFile{
	{Version: "156_testops_test_metrics_retracted.sql", SQL: `ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;`},
	{Version: "155_testops_pipeline_run_status_counts.sql", SQL: `ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS skipped_count Nullable(UInt32);
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS unfinished_count Nullable(UInt32);
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS other_count Nullable(UInt32);
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS unfinished_stale_count Nullable(UInt32);`},
}

// A migration with a higher number can reach main before two with lower
// numbers. On a scratch ClickHouse: the chain without 155 and 156 is applied
// (the head through main's 154, then 157), then 155 and 156 join the chain and
// the upgrade runs again. Want: the second upgrade applies exactly 155 and 156
// with no error, and schema_migrations then holds 155, 156 and 157.
func TestChainAppliesLowerNumbersThatArriveAfter157(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	live, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	late := map[string]chmigrate.ChainFile{}
	for _, fixture := range outOfOrderFixtures {
		late[fixture.Version] = fixture
	}
	var first []chmigrate.ChainFile
	for _, file := range live {
		if _, isLate := late[file.Version]; isLate {
			late[file.Version] = file
			continue
		}
		first = append(first, file)
	}
	if !slices.ContainsFunc(first, func(file chmigrate.ChainFile) bool { return file.Version == benchmarkingRetractedVersion }) {
		t.Fatalf("the live chain does not hold %s", benchmarkingRetractedVersion)
	}
	firstResult, err := chmigrate.Upgrade(ctx, db, baseline, first)
	t.Logf("first upgrade: action=%s head=%s applied=%v err=%v", firstResult.Action, firstResult.Head, firstResult.Applied, err)
	if err != nil {
		t.Fatalf("the chain without 155 and 156: %v", err)
	}
	for _, version := range []string{"154_drop_grafana_panel_views.sql", benchmarkingRetractedVersion} {
		if !slices.Contains(firstResult.Applied, version) {
			t.Fatalf("the first upgrade did not apply %s", version)
		}
	}

	second := append([]chmigrate.ChainFile(nil), first...)
	for _, file := range late {
		second = append(second, file)
	}
	sort.Slice(second, func(i, j int) bool { return second[i].Version < second[j].Version })
	secondResult, err := chmigrate.Upgrade(ctx, db, baseline, second)
	t.Logf("second upgrade: action=%s head=%s applied=%v err=%v", secondResult.Action, secondResult.Head, secondResult.Applied, err)
	if err != nil {
		t.Fatalf("the upgrade after 155 and 156 joined the chain: %v", err)
	}
	want := []string{"155_testops_pipeline_run_status_counts.sql", "156_testops_test_metrics_retracted.sql"}
	if !slices.Equal(secondResult.Applied, want) {
		t.Fatalf("the second upgrade applied %v, want %v", secondResult.Applied, want)
	}
	for _, version := range append(want, benchmarkingRetractedVersion) {
		var rows uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", version).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		t.Logf("schema_migrations %s rows=%d", version, rows)
		if rows == 0 {
			t.Errorf("schema_migrations holds no %s", version)
		}
	}
	third, err := chmigrate.Upgrade(ctx, db, baseline, second)
	if err != nil || len(third.Applied) != 0 {
		t.Errorf("a third upgrade = %+v, %v; want nothing to apply", third, err)
	}
}
