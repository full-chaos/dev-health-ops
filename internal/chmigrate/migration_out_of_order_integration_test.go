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

// outOfOrderFixtures are the statements of 154 and 155 as their open pull
// requests hold them (154_testops_test_metrics_retracted.sql at e72a51201e8a,
// 155_testops_pipeline_run_status_counts.sql at b2c31b14ad9e), used only when the
// live chain does not hold that version yet.
var outOfOrderFixtures = []chmigrate.ChainFile{
	{Version: "154_testops_test_metrics_retracted.sql", SQL: `ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0;`},
	{Version: "155_testops_pipeline_run_status_counts.sql", SQL: `ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS skipped_count Nullable(UInt32);
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS unfinished_count Nullable(UInt32);
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS other_count Nullable(UInt32);
ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS unfinished_stale_count Nullable(UInt32);`},
}

// A migration with a higher number can reach main before two with lower
// numbers. On a scratch ClickHouse: the chain without 154 and 155 is applied
// (the head through 153, then 156), then 154 and 155 join the chain and the
// upgrade runs again. Want: the second upgrade applies exactly 154 and 155
// with no error, and schema_migrations then holds 154, 155 and 156.
func TestChainAppliesLowerNumbersThatArriveAfter156(t *testing.T) {
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
		t.Fatalf("the chain without 154 and 155: %v", err)
	}
	if !slices.Contains(firstResult.Applied, benchmarkingRetractedVersion) {
		t.Fatalf("the first upgrade did not apply %s", benchmarkingRetractedVersion)
	}

	second := append([]chmigrate.ChainFile(nil), first...)
	for _, file := range late {
		second = append(second, file)
	}
	sort.Slice(second, func(i, j int) bool { return second[i].Version < second[j].Version })
	secondResult, err := chmigrate.Upgrade(ctx, db, baseline, second)
	t.Logf("second upgrade: action=%s head=%s applied=%v err=%v", secondResult.Action, secondResult.Head, secondResult.Applied, err)
	if err != nil {
		t.Fatalf("the upgrade after 154 and 155 joined the chain: %v", err)
	}
	want := []string{"154_testops_test_metrics_retracted.sql", "155_testops_pipeline_run_status_counts.sql"}
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
