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

// INVARIANT (CHAOS-9302): after the head of the chain neither table of the
// retired rule classifier exists, on an empty database, and on a database that
// held the tables with rows. The table that replaced them stays.
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
	for _, file := range chain {
		found = found || file.Version == dropRetiredInvestmentVersion
	}
	if !found {
		t.Fatalf("the chain holds no %s: the measurement did not happen", dropRetiredInvestmentVersion)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatal(err)
	}
	counts := countRetiredInvestmentTables(t, ctx, conn,
		"investment_classifications_daily", "investment_metrics_daily", "work_item_completion_daily")
	if counts["investment_classifications_daily"] != 0 || counts["investment_metrics_daily"] != 0 {
		t.Fatalf("retired tables present after the head: %v", counts)
	}
	if counts["work_item_completion_daily"] != 1 {
		t.Fatalf("work_item_completion_daily is missing after the head: %v", counts)
	}
}

func TestRetiredInvestmentDailyTablesWithRowsAreGoneAfterTheMigration(t *testing.T) {
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
	var before []chmigrate.ChainFile
	for _, file := range chain {
		if file.Version != dropRetiredInvestmentVersion {
			before = append(before, file)
		}
	}
	if len(before) != len(chain)-1 {
		t.Fatalf("chain without the drop holds %d of %d files: the measurement did not happen", len(before), len(chain))
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, before); err != nil {
		t.Fatal(err)
	}
	for _, table := range retiredInvestmentTables {
		if err := conn.Exec(ctx, `INSERT INTO `+table+` (day, org_id) VALUES ('2026-10-04', 'org-a'), ('2026-10-05', 'org-b')`); err != nil {
			t.Fatalf("seed %s: %v", table, err)
		}
		var rows uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM `+table).Scan(&rows); err != nil || rows != 2 {
			t.Fatalf("seed %s: %d rows, err %v, want 2", table, rows, err)
		}
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatal(err)
	}
	counts := countRetiredInvestmentTables(t, ctx, conn, retiredInvestmentTables...)
	for table, present := range counts {
		if present != 0 {
			t.Fatalf("%s still exists after migration 120", table)
		}
	}
	// Applying the head again is a no-op, and the statements are re-runnable.
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatal(err)
	}
}
