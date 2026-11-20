//go:build integration

package venueoracle

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHRows builds each destination from the column's scan type, which is a
// pointer for a Nullable column, so a NULL value scans on every driver version.
func TestCHRowsScansANullValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	out := CHRows(t, ctx, ch.URI, "SELECT CAST(NULL AS Nullable(String)) AS s, CAST(NULL AS Nullable(UInt32)) AS n, 'x' AS k")
	if !strings.Contains(out, "x") {
		t.Fatalf("rows = %q, want the row with k = x", out)
	}
}
