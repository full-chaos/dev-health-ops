package workersctl

import "testing"

// CHAOS-8710 F8: finalize-redrive opens ClickHouse (and reads its credential)
// only on the path that can write a marker.
func TestFinalizeRedriveOpensClickHouseOnlyWhenItCanWriteAMarker(t *testing.T) {
	for _, test := range []struct {
		dryRun, includeSucceeded, want bool
	}{
		{false, true, true},
		{false, false, false},
		{true, true, false},
		{true, false, false},
	} {
		if got := finalizeRedriveWritesMarker(test.dryRun, test.includeSucceeded); got != test.want {
			t.Fatalf("finalizeRedriveWritesMarker(dryRun=%v, includeSucceeded=%v) = %v, want %v",
				test.dryRun, test.includeSucceeded, got, test.want)
		}
	}
}
