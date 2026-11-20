//go:build integration

package quadrant

import (
	"context"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// scanMetricRows reads a NULL bucket, entity or value (value = .. / nullIf(sum(owner_share), 0)
// on review_latency) as the v2.49 zero value.
func TestMetricRowsKeepTheZeroValueOfANull(t *testing.T) {
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

	rows, err := scanMetricRows(ctx, client, "SELECT CAST(NULL AS Nullable(Date)) AS bucket, CAST(NULL AS Nullable(String)) AS entity_id, CAST(NULL AS Nullable(String)) AS entity_label, toFloat64(1) / nullIf(toFloat64(0), 0) AS value", nil, "quadrant")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !rows[0].Bucket.IsZero() || rows[0].EntityID != "" || rows[0].EntityLabel != "" || rows[0].Value != 0 {
		t.Fatalf("rows = %+v, want one row of zero values", rows)
	}
}
