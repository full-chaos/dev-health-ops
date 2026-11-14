//go:build integration

package chwrite

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const refsMigrationVersion = "149_work_unit_investment_refs.sql"

func refsRecord(unit, evidence string, computedAt time.Time) InvestmentRecord {
	return InvestmentRecord{
		WorkUnitID: unit, FromTS: computedAt.Add(-24 * time.Hour), ToTS: computedAt,
		EffortMetric: "fte_days", EffortValue: 1, ThemeDistribution: map[string]float64{}, SubcategoryDistribution: map[string]float64{},
		StructuralEvidenceJSON: evidence, EvidenceQuality: 0.5, EvidenceQualityBand: "moderate", CategorizationStatus: "ok",
		CategorizationErrorsJSON: "[]", CategorizationModelVersion: "v1", CategorizationInputHash: "h", CategorizationRunID: "run-refs",
		ComputedAt: computedAt,
	}
}

// refsStoredColumns counts the reference columns the part holding unit's row stores.
func refsStoredColumns(ctx context.Context, t *testing.T, conn driver.Conn, unit string) uint64 {
	t.Helper()
	part := scanString(t, ctx, conn, `SELECT _part FROM work_unit_investments WHERE org_id = ? AND work_unit_id = ?`, testOrgID, unit)
	if active := scanUint64(t, ctx, conn, `SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 'work_unit_investments' AND active AND name = ?`, part); active != 1 {
		t.Fatalf("the part %s of %s is not active", part, unit)
	}
	return scanUint64(t, ctx, conn, `SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'work_unit_investments' AND active AND name = ? AND column IN ('issue_refs', 'pr_refs')`, part)
}

func refsRead(ctx context.Context, t *testing.T, conn driver.Conn, unit string) (issues, prs []string) {
	t.Helper()
	if err := conn.QueryRow(ctx, `SELECT issue_refs, pr_refs FROM work_unit_investments WHERE org_id = ? AND work_unit_id = ?`,
		testOrgID, unit).Scan(&issues, &prs); err != nil {
		t.Fatalf("read the references of %s: %v", unit, err)
	}
	return issues, prs
}

// INVARIANT (CHAOS-10726): the writer names neither reference column, so the
// same build writes a database without 149 and one with it. On a database with
// 149 ClickHouse computes the columns from the row's evidence and stores them in
// the part the insert writes; a row written before 149 reads the same keys from
// its evidence.
func TestWriteInvestmentsBeforeAndAfterMigration149(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
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
	before := slices.DeleteFunc(slices.Clone(chain), func(file chmigrate.ChainFile) bool { return file.Version == refsMigrationVersion })
	if len(before) != len(chain)-1 {
		t.Fatalf("the live chain does not hold %s", refsMigrationVersion)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, before); err != nil {
		t.Fatalf("the chain before 149: %v", err)
	}
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	if columns := scanUint64(t, ctx, conn, `SELECT count() FROM system.columns
WHERE database = currentDatabase() AND table = 'work_unit_investments' AND name IN ('issue_refs', 'pr_refs')`); columns != 0 {
		t.Fatalf("before 149 work_unit_investments holds %d reference columns, want 0", columns)
	}
	// A merge would fill the columns of the part written before 149.
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES work_unit_investments"); err != nil {
		t.Fatal(err)
	}
	computedAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	evidence := `{"issues":["i-1","i-2"],"prs":["p-1"],"commits":["c-1"],"edges":["e-1"]}`
	if written, err := writer.WriteInvestments(ctx, testOrgID, []InvestmentRecord{refsRecord("wu-at-148", evidence, computedAt)}); err != nil || written != 1 {
		t.Fatalf("WriteInvestments at 148 = %d, %v; want 1 row", written, err)
	}

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != refsMigrationVersion {
		t.Fatalf("the upgrade = %+v, %v; want %s applied", result, err, refsMigrationVersion)
	}
	if stored := refsStoredColumns(ctx, t, conn, "wu-at-148"); stored != 0 {
		t.Fatalf("the part written before 149 stores %d reference columns, want 0 (149 rewrote it)", stored)
	}
	if issues, prs := refsRead(ctx, t, conn, "wu-at-148"); !slices.Equal(issues, []string{"i-1", "i-2"}) || !slices.Equal(prs, []string{"p-1"}) {
		t.Errorf("the row written before 149 reads issue_refs %q pr_refs %q, want [i-1 i-2] [p-1]", issues, prs)
	}

	records := []InvestmentRecord{
		refsRecord("wu-at-149", `{"issues":["i-3"],"prs":["p-2","p-3"],"commits":[],"edges":[]}`, computedAt),
		refsRecord("wu-at-149-empty", `{}`, computedAt),
	}
	if written, err := writer.WriteInvestments(ctx, testOrgID, records); err != nil || written != 2 {
		t.Fatalf("WriteInvestments at 149 = %d, %v; want 2 rows", written, err)
	}
	for unit, want := range map[string][2][]string{
		"wu-at-149":       {{"i-3"}, {"p-2", "p-3"}},
		"wu-at-149-empty": {{}, {}},
	} {
		if stored := refsStoredColumns(ctx, t, conn, unit); stored != 2 {
			t.Errorf("the part written at 149 for %s stores %d reference columns, want 2", unit, stored)
		}
		if issues, prs := refsRead(ctx, t, conn, unit); !slices.Equal(issues, want[0]) || !slices.Equal(prs, want[1]) {
			t.Errorf("%s reads issue_refs %q pr_refs %q, want %q %q", unit, issues, prs, want[0], want[1])
		}
	}
}
