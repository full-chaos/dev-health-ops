package analytics

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const repoJoinDedupCollisionsMetric = "devhealth_query_api_investment_repo_join_dedup_collisions_total"

func repoJoinCollisionPoints(t *testing.T) (total int64, attributed int) {
	t.Helper()
	var resource metricdata.ResourceMetrics
	if err := realMeterReader.Collect(context.Background(), &resource); err != nil {
		t.Fatal(err)
	}
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != repoJoinDedupCollisionsMetric {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 sum", m.Name, m.Data)
			}
			for _, point := range sum.DataPoints {
				total += point.Value
				if point.Attributes.Len() != 0 {
					attributed++
				}
			}
		}
	}
	return total, attributed
}

func TestRepoJoinDedupCollisionsCounterCarriesNoOrganizationAttribute(t *testing.T) {
	before, _ := repoJoinCollisionPoints(t)

	defaultRecordInvestmentRepoJoinDedupCollisions(context.Background(), "org-label-probe", 2)

	after, attributed := repoJoinCollisionPoints(t)
	if after != before+1 {
		t.Fatalf("counter %d, want %d: the increment did not reach the shared reader", after, before+1)
	}
	if attributed != 0 {
		t.Fatalf("%d data points carry attributes, want none", attributed)
	}
}
