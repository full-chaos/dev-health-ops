package remaining

import (
	"reflect"
	"testing"
)

func TestDistributionFromPairsPreservesOrder(t *testing.T) {
	distribution := distributionFromPairs(
		[]string{"maintenance", "feature_delivery"},
		[]float64{0.4, 0.6},
	)
	got := distribution.Categories()
	want := []string{"maintenance", "feature_delivery"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Categories() = %v, want %v (mapKeys/mapValues order must survive intact)", got, want)
	}
	if distribution.Weight("maintenance") != 0.4 {
		t.Errorf("Weight(maintenance) = %v, want 0.4", distribution.Weight("maintenance"))
	}
}

func TestDistributionFromPairsEmpty(t *testing.T) {
	distribution := distributionFromPairs(nil, nil)
	if distribution.Len() != 0 {
		t.Errorf("Len() = %d, want 0", distribution.Len())
	}
}

// TestDistributionFromPairsMismatchedLengthIsTruncated pins a defensive
// choice (this should never happen -- mapKeys/mapValues over the same Map
// value are always positionally aligned and equal length -- but a truncation
// is a safer failure than an out-of-range panic if ClickHouse's contract were
// ever violated).
func TestDistributionFromPairsMismatchedLengthIsTruncated(t *testing.T) {
	distribution := distributionFromPairs([]string{"a", "b", "c"}, []float64{1.0})
	if distribution.Len() != 1 {
		t.Errorf("Len() = %d, want 1 (truncated to the shorter slice)", distribution.Len())
	}
}

func TestSortedUniqueStrings(t *testing.T) {
	got := sortedUniqueStrings([]string{"repo-b", "repo-a", "repo-b", "repo-a"})
	want := []string{"repo-a", "repo-b"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sortedUniqueStrings = %v, want %v", got, want)
	}
}
