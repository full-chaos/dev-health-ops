//go:build integration

package benchmarking

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestWriteBaselinesNullFieldsRoundTripAsRealClickHouseNull is the executed
// proof, against a REAL ClickHouse server, for the mechanism every unit test
// in this package can only assert on the Go side: a nil *float64 field on
// BenchmarkBaselineRecord (CHAOS-4806, ruling R73 -- see migration
// 090_testops_baselines_nullable_fields.sql) must land as a genuine SQL
// NULL, not a zero, not an error, and must never poison the OTHER fields on
// the same row or any other row in the same batch.
func TestWriteBaselinesNullFieldsRoundTripAsRealClickHouseNull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Minimal schema for the one table under test, matching migrations
	// 090_testops_baselines_nullable_fields.sql and
	// 092_testops_percentile_rank_nullable_field.sql's post-migration
	// shape exactly (every one of these six columns Nullable, per-column).
	if err := conn.Exec(ctx, `CREATE TABLE testops_metric_baselines (
    metric_name LowCardinality(String), scope_type LowCardinality(String), scope_key String,
    period_start Date, period_end Date, rolling_window_days UInt16,
    current_value Nullable(Float64), baseline_value Nullable(Float64), percentile_rank Nullable(Float64),
    p25_value Nullable(Float64), p50_value Nullable(Float64), p75_value Nullable(Float64), p90_value Nullable(Float64),
    sample_size UInt32, org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(period_end)
  ORDER BY (metric_name, scope_type, scope_key, period_end, rolling_window_days)`); err != nil {
		t.Fatal(err)
	}

	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}

	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	finiteValue := 42.5
	rows := []BenchmarkBaselineRecord{
		{
			MetricName: "test_null_roundtrip_metric", ScopeType: ScopeRepo, ScopeKey: "scope-nulled",
			PeriodStart: day, PeriodEnd: day, RollingWindowDays: 30,
			// R73's actual shape: only SOME fields on the row are
			// undefined -- CurrentValue and P25Value are nil, everything
			// else is a real, present value. The row still writes.
			CurrentValue: nil, BaselineValue: &finiteValue, PercentileRank: &finiteValue,
			P25Value: nil, P50Value: &finiteValue, P75Value: &finiteValue, P90Value: &finiteValue,
			SampleSize: 3, ComputedAt: day, OrgID: "org-null-roundtrip",
		},
	}

	written, err := writer.writeBaselines(ctx, rows, "org-null-roundtrip")
	if err != nil {
		t.Fatalf("writeBaselines: %v", err)
	}
	if written != 1 {
		t.Fatalf("writeBaselines reported %d rows written, want 1", written)
	}

	var currentValueIsNull, p25ValueIsNull uint8
	var baselineValue, p50Value float64
	if err := conn.QueryRow(ctx,
		`SELECT current_value IS NULL, p25_value IS NULL, baseline_value, p50_value
		 FROM testops_metric_baselines WHERE org_id = ? AND scope_key = ?`,
		"org-null-roundtrip", "scope-nulled",
	).Scan(&currentValueIsNull, &p25ValueIsNull, &baselineValue, &p50Value); err != nil {
		t.Fatal(err)
	}
	if currentValueIsNull != 1 {
		t.Errorf("current_value IS NULL = %d, want 1 (true) -- the nil CurrentValue field must land as a real ClickHouse NULL", currentValueIsNull)
	}
	if p25ValueIsNull != 1 {
		t.Errorf("p25_value IS NULL = %d, want 1 (true)", p25ValueIsNull)
	}
	if baselineValue != finiteValue {
		t.Errorf("baseline_value = %v, want %v -- a nil sibling field must not corrupt a present field on the SAME row", baselineValue, finiteValue)
	}
	if p50Value != finiteValue {
		t.Errorf("p50_value = %v, want %v", p50Value, finiteValue)
	}
}
