//go:build integration

package remaining

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The stored compounding-risk rationale must carry the day of the persisted
// composite row the score was read from. The rule test feeds the day in a
// hand-built snapshot and the loader test reads the day directly; neither
// sees a day lost between the loader and the stored row. This drives the real
// loader, rule, write and readback on real ClickHouse.
func TestStoredCompoundingRiskRationaleCarriesTheAsOfDay(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	conn := openLoaderClickHouse(t, ctx, dsn)
	defer seedLoaderFixture(t, ctx, conn)()

	executor, err := newClickHouseOnlyExecutor(ctx, conn, loaderOrgID)
	if err != nil {
		t.Fatalf("construct executor: %v", err)
	}
	asOf := mustDate(t, "2026-08-31")
	now, _ := EvaluationInstant(&asOf, nil)
	if _, err := executor.ComputeOrg(ctx, loaderOrgID, now, 30, "v1", ""); err != nil {
		t.Fatalf("compute org: %v", err)
	}

	// The fixture's only persisted composite for team-alpha is the 2026-08-20 row.
	const wantDay = "2026-08-20"
	var fired bool
	var rationale string
	row := conn.QueryRow(ctx, `
        SELECT fired, rationale
        FROM recommendations_daily FINAL
        WHERE org_id = ? AND team_id = ? AND rule_id = 'compounding-risk'`,
		loaderOrgID, loaderTeamA)
	if err := row.Scan(&fired, &rationale); err != nil {
		t.Fatalf("read back the stored compounding-risk row: %v", err)
	}
	if !fired {
		t.Fatalf("compounding-risk did not fire for %s; the rationale under test is a tombstone's: %q", loaderTeamA, rationale)
	}
	if want := "Score as of " + wantDay + "."; !strings.Contains(rationale, want) {
		t.Errorf("stored rationale %q does not contain %q", rationale, want)
	}
}
