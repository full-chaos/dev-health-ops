//go:build integration

package chmigrate_test

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const dropRetiredInvestmentVersion = "120_drop_retired_investment_daily_tables.sql"

var retiredInvestmentTables = []string{"investment_classifications_daily", "investment_metrics_daily"}

// retiredInvestmentObjects are the tables and the view that selects from one of them: a view left behind
// fails on every select.
var retiredInvestmentObjects = append(append([]string{}, retiredInvestmentTables...), "v_investment_flow_edges")

func startDropRetiredInvestmentClickHouse(t *testing.T, ctx context.Context) (chmigrate.DB, driver.Conn) {
	t.Helper()
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
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	return db, conn
}

func countRetiredInvestmentTables(t *testing.T, ctx context.Context, conn driver.Conn, names ...string) map[string]uint64 {
	t.Helper()
	out := map[string]uint64{}
	for _, name := range names {
		var count uint64
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?`, name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		out[name] = count
	}
	return out
}

// INVARIANT (CHAOS-9302): after the head neither table of the retired rule
// classifier exists on an empty database, and the table that replaced them
// stays. (The drop of tables with rows, migration 120, is archived with the
// chain it belongs to, CHAOS-11751.)
func TestRetiredInvestmentDailyTablesAreAbsentAfterTheHeadOnAnEmptyDatabase(t *testing.T) {
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
	found := false
	for _, version := range baseline.Versions {
		found = found || version == dropRetiredInvestmentVersion
	}
	if !found {
		t.Fatalf("the head baseline records no %s: the measurement did not happen", dropRetiredInvestmentVersion)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatal(err)
	}
	counts := countRetiredInvestmentTables(t, ctx, conn, append([]string{"work_item_completion_daily"}, retiredInvestmentObjects...)...)
	for _, name := range retiredInvestmentObjects {
		if counts[name] != 0 {
			t.Fatalf("retired object %s present after the head: %v", name, counts)
		}
	}
	if counts["work_item_completion_daily"] != 1 {
		t.Fatalf("work_item_completion_daily is missing after the head: %v", counts)
	}
}
