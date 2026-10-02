package synccli

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// writeVenueProof records that the running test made a real comparison against the live Python producer:
// venueoracle.WriteProof writes the proof file the venue-oracles verb reads under the test's own name. Only a
// test that has not failed writes it. It stays until the last live-Python oracle of this package is frozen
// (CHAOS-7319 sub-issues b and c), then it goes.
func writeVenueProof(t *testing.T) {
	t.Helper()
	if t.Failed() {
		return
	}
	venueoracle.WriteProof(t)
}
