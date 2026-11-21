//go:build integration

package quadrant

import (
	"context"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// scanMetricRows skips a NULL value (no point, never a 0) and reads a NULL bucket, entity or label as its zero value.
func TestMetricRowsSkipANullValueAndKeepTheZeroValueOfOtherNulls(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	const nullValue = "toFloat64(1) / nullIf(toFloat64(0), 0) AS value"
	const nullKeys = "CAST(NULL AS Nullable(Date)) AS bucket, CAST(NULL AS Nullable(String)) AS entity_id, CAST(NULL AS Nullable(String)) AS entity_label"

	rows, err := scanMetricRows(ctx, client, "SELECT toDate('2026-01-02') AS bucket, 'e' AS entity_id, 'l' AS entity_label, "+nullValue, nil, "quadrant")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("NULL value: rows = %+v, want none", rows)
	}

	rows, err = scanMetricRows(ctx, client, "SELECT "+nullKeys+", toFloat64(7) AS value", nil, "quadrant")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !rows[0].Bucket.IsZero() || rows[0].EntityID != "" || rows[0].EntityLabel != "" || rows[0].Value != 7 {
		t.Fatalf("NULL keys, value 7: rows = %+v, want one row of zero keys and value 7", rows)
	}

	rows, err = scanMetricRows(ctx, client, "SELECT "+nullKeys+", "+nullValue, nil, "quadrant")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("all NULL: rows = %+v, want none", rows)
	}
}
