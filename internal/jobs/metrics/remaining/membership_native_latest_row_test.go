package remaining

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/latestrow"
)

func TestLatestDistributionsQueryUsesTheSharedLatestRowOrder(t *testing.T) {
	query := latestDistributionsQuery()
	for _, column := range []string{"theme_distribution_json", "subcategory_distribution_json", "categorization_status"} {
		if want := latestrow.ArgMax(column); !strings.Contains(query, want) {
			t.Errorf("latest-distribution query must pick %s with %s", column, want)
		}
	}
	if strings.Contains(query, ", computed_at)") {
		t.Errorf("latest-distribution query orders by computed_at alone: %s", query)
	}
}
