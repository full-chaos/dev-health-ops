//go:build integration

package syncadmin

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// clickhouse-go v2.50+ refuses a NULL scanned into a destination that cannot
// hold NULL. The diagnostics read scans count() and countIf() results, which are
// never NULL, also over a compounding_risk_daily row whose measures are all NULL.
func TestBackfillDiagnosticsCountOverNullMeasures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const org = "00000000-0000-4000-8000-000000014311"
	day := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	if err := conn.Exec(ctx, `INSERT INTO compounding_risk_daily (org_id, day, scope, scope_id, compounding_risk, rework_churn, computed_at)
		VALUES (?, ?, 'repo', 'r1', NULL, NULL, ?)`, org, day, day.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	out, err := clickhouseDiagnostics{conn: conn}.backfillDiagnostics(ctx, org, day, day)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("diagnostics = nil, want an object")
	}
}
