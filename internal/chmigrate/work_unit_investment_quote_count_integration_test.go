//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// INVARIANT (CHAOS-8788, migration 106): work_unit_investments gains a NULLABLE
// evidence_quote_count. A row written without it (every row written before the
// migration, and a run that did not persist snippets) reads back NULL, which the
// materializer treats as "complete"; a row written with a count reads it back.
// The migration is re-runnable.
func TestWorkUnitInvestmentQuoteCountIsNullableAndRerunnable(t *testing.T) {
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
	chschema.Apply(ctx, t, instance) // the whole chain, 106 included
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	var columnType string
	if err := conn.QueryRow(ctx, `SELECT type FROM system.columns
		WHERE database = currentDatabase() AND table = 'work_unit_investments' AND name = 'evidence_quote_count'`).Scan(&columnType); err != nil {
		t.Fatalf("migration 106 left no evidence_quote_count column: %v", err)
	}
	if columnType != "Nullable(UInt32)" {
		t.Fatalf("evidence_quote_count is %s, want Nullable(UInt32)", columnType)
	}

	insert := func(unit string, count string) {
		t.Helper()
		columns, values := "", ""
		if count != "" {
			columns, values = ", evidence_quote_count", ", "+count
		}
		if err := conn.Exec(ctx, `INSERT INTO work_unit_investments
			(work_unit_id, from_ts, to_ts, effort_metric, effort_value, structural_evidence_json, evidence_quality,
			 evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version,
			 categorization_input_hash, categorization_run_id, computed_at, org_id`+columns+`)
			VALUES ('`+unit+`', now(), now(), 'm', 1, '{}', 1, 'high', 'ok', '[]', 'v', 'h', 'r', now(), 'org'`+values+`)`); err != nil {
			t.Fatal(err)
		}
	}
	insert("old-row", "")
	insert("new-row", "2")
	read := func(unit string) *uint32 {
		t.Helper()
		var count *uint32
		if err := conn.QueryRow(ctx, `SELECT argMax(evidence_quote_count, computed_at) FROM work_unit_investments
			WHERE org_id = 'org' AND work_unit_id = ?`, unit).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	if got := read("old-row"); got != nil {
		t.Fatalf("a row written without the column reads %d, want NULL", *got)
	}
	if got := read("new-row"); got == nil || *got != 2 {
		t.Fatalf("a row written with a count reads %v, want 2", got)
	}

	sql, err := os.ReadFile("sql/106_work_unit_investment_quote_count.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(sql)) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("rerun of migration 106: %v", err)
		}
	}
	if got := read("new-row"); got == nil || *got != 2 {
		t.Fatalf("after the rerun the count reads %v, want 2", got)
	}
}
