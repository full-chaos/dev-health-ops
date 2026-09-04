package investmentexplain

import (
	"reflect"
	"testing"
)

// TestZipDistributionOrderedPairsByIndex regresses CHAOS-4977 step 7's
// live-ClickHouse finding: work_unit_investments.theme_distribution_json/
// subcategory_distribution_json are Map(String, Float64) columns, not
// JSON-string columns despite the name -- read via mapKeys(...)/
// mapValues(...) into two parallel arrays (workunitreader.go), zipped
// here by index. This proves the zip itself: order is exactly the
// input order (no re-sorting), and keys/values pair up positionally.
func TestZipDistributionOrderedPairsByIndex(t *testing.T) {
	keys := []string{"low", "high", "moderate"}
	values := []float64{1, 2, 3}

	got := zipDistributionOrdered(keys, values)
	want := []keyValue{{Key: "low", Value: 1}, {Key: "high", Value: 2}, {Key: "moderate", Value: 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("zipDistributionOrdered(%v, %v) = %+v, want %+v", keys, values, got, want)
	}
}

// TestZipDistributionOrderedEmpty regresses the nil-vs-empty edge: an
// empty Map (a work unit with no theme distribution recorded at all)
// must produce a nil/empty slice, matching parseDistributionOrdered's
// own "" -> nil convention, not a slice of zero-valued keyValues.
func TestZipDistributionOrderedEmpty(t *testing.T) {
	if got := zipDistributionOrdered(nil, nil); len(got) != 0 {
		t.Fatalf("zipDistributionOrdered(nil, nil) = %+v, want empty", got)
	}
}

// TestZipDistributionBuildsLookupMap regresses the unordered sibling
// used by matchesCategoryFilter -- same pairing, map form.
func TestZipDistributionBuildsLookupMap(t *testing.T) {
	keys := []string{"velocity", "quality"}
	values := []float64{40, 10}

	got := zipDistribution(keys, values)
	want := map[string]float64{"velocity": 40, "quality": 10}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("zipDistribution(%v, %v) = %v, want %v", keys, values, got, want)
	}
}
