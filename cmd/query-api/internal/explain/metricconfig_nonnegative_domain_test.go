package explain

import "testing"

// nonNegativeSumColumns names every ClickHouse column a sum-aggregator
// metric config is currently known to read, vetted non-negative by
// construction: throughput's items_completed and deploy_freq's
// deployments_count are counts, churn's total_loc_touched is
// touched-lines, blocked_work's duration_hours is a duration -- none can
// go negative.
//
// This is the domain premise internal/goapiproof/restcorpus.go's own
// CHAOS-5818 KeyedDirectionShape entries (data.drivers.value,
// data.contributors.value) rest their direction claim on: a sum over
// N>=1 non-negative values is greater than or equal to their average,
// equal only at N=1. That inequality REVERSES for a signed quantity (the
// sum of negatives sits further below zero than their average), which is
// exactly the effect already observed on this metric family's own
// delta_pct field (dropped from that citation's Paths for precisely this
// reason -- see the entry's own doc comment).
//
// A metric config added later with Aggregator=="sum" and a column not
// listed here has an UNVERIFIED domain: TestSumAggregatorMetricColumnsAreVettedNonNegative
// fails the moment one is added, so the direction claim can never
// silently extend to a column nobody checked.
var nonNegativeSumColumns = map[string]bool{
	"items_completed":   true, // throughput -- a count (metricConfigs' own throughput entry).
	"deployments_count": true, // deploy_freq -- a count.
	"total_loc_touched": true, // churn -- touched lines, a magnitude.
	"duration_hours":    true, // blocked_work -- a duration.
}

// TestSumAggregatorMetricColumnsAreVettedNonNegative tests the premise
// internal/goapiproof's own CHAOS-5818 direction shape depends on against
// the producing config itself, not a fixture: every metricConfigs entry
// whose Aggregator is "sum" must read a column already vetted into
// nonNegativeSumColumns above, or this test fails. A new sum-aggregator
// metric whose column can go negative (or one that simply has not been
// checked yet) must fail here first -- extending the direction claim to
// an unverified column, silently, is exactly what this guards against.
func TestSumAggregatorMetricColumnsAreVettedNonNegative(t *testing.T) {
	for name, cfg := range metricConfigs {
		if cfg.Aggregator != "sum" {
			continue
		}
		if !nonNegativeSumColumns[cfg.Column] {
			t.Errorf("metric %q: sum-aggregator column %s.%s is not in nonNegativeSumColumns -- "+
				"internal/goapiproof's CHAOS-5818 KeyedDirectionShape entries assume every "+
				"sum-aggregator metric's column is non-negative (their claim only holds then); "+
				"vet %s.%s for negative values before adding it here, or the direction claim "+
				"silently covers a metric it was never checked against",
				name, cfg.Table, cfg.Column, cfg.Table, cfg.Column)
		}
	}
}
