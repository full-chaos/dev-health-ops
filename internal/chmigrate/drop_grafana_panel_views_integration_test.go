//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const dropGrafanaPanelViewsVersion = "154_drop_grafana_panel_views.sql"

// grafanaPanelViews are the two views of migration 008 that 154 drops; the tables they read stay.
var (
	grafanaPanelViews       = []string{"v_file_hotspots_windowed", "v_ic_landscape_points"}
	grafanaPanelViewSources = []string{"file_metrics_daily", "file_hotspot_daily", "ic_landscape_rolling_30d"}
)

// CHAOS-9041: the head baseline still creates the two views (it is the capture of the archived chain), 154 drops them, the
// tables they read stay, and the hand-run rollback re-creates them and its version row goes, so the next upgrade drops
// them again.
func TestGrafanaPanelViewsAreDroppedAfterTheHeadAndTheRollbackRestoresThem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	db, conn := startDropRetiredInvestmentClickHouse(t, ctx)
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	inChain := false
	for _, file := range chain {
		inChain = inChain || file.Version == dropGrafanaPanelViewsVersion
	}
	if !inChain {
		t.Fatalf("the chain holds no %s", dropGrafanaPanelViewsVersion)
	}
	present := func(names ...string) []uint64 {
		counts := countRetiredInvestmentTables(t, ctx, conn, names...)
		out := make([]uint64, 0, len(names))
		for _, name := range names {
			out = append(out, counts[name])
		}
		return out
	}
	versionRows := func() uint64 {
		var count uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM schema_migrations WHERE version = ?`,
			dropGrafanaPanelViewsVersion).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatal(err)
	}
	if got := present(grafanaPanelViews...); !reflect.DeepEqual(got, []uint64{0, 0}) || versionRows() == 0 {
		t.Fatalf("after the head: views %v, version rows %d; want both views gone and the version recorded", got, versionRows())
	}
	if got := present(grafanaPanelViewSources...); !reflect.DeepEqual(got, []uint64{1, 1, 1}) {
		t.Fatalf("a table the views read is missing after the head: %v", got)
	}
	// The migration can run again.
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("second upgrade: %v", err)
	}

	body, err := os.ReadFile("rollback/154_drop_grafana_panel_views.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if got := present(grafanaPanelViews...); !reflect.DeepEqual(got, []uint64{1, 1}) || versionRows() != 0 {
		t.Fatalf("after the rollback: views %v, version rows %d; want both views and no version row", got, versionRows())
	}
	for _, view := range grafanaPanelViews {
		var rows uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+view).Scan(&rows); err != nil {
			t.Fatalf("the restored view %s does not answer a select: %v", view, err)
		}
	}

	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatal(err)
	}
	if got := present(grafanaPanelViews...); !reflect.DeepEqual(got, []uint64{0, 0}) || versionRows() == 0 {
		t.Fatalf("after up-down-up: views %v, version rows %d; want both views gone again", got, versionRows())
	}
}
