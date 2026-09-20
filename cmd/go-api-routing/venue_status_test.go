package main

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// A venue row and a waiver row are different states with different words;
// a store-proven row is "ok" whatever its evidence says.
func TestProofWordKeepsVenueAndWaiverRowsApart(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   statusReportOperation
		mis  bool
		want string
	}{
		{"store proven", statusReportOperation{DigestState: goapiproof.DigestMatch, Proven: true}, false, "ok"},
		{"store proven, stale venue text", statusReportOperation{DigestState: goapiproof.DigestMatch, Proven: true, VenueProof: goapiproof.VenueClassAdmin}, false, "ok"},
		{"waiver", statusReportOperation{DigestState: goapiproof.DigestMatch}, false, "UNPROVEN"},
		{"venue admin", statusReportOperation{DigestState: goapiproof.DigestMatch, VenueProof: goapiproof.VenueClassAdmin}, false, "VENUE-PROVEN(admin_only)"},
		{"venue no data", statusReportOperation{DigestState: goapiproof.DigestMatch, VenueProof: goapiproof.VenueClassNoData}, false, "VENUE-PROVEN(no_production_data)"},
		{"venue but digest mismatch", statusReportOperation{DigestState: goapiproof.DigestMatch, VenueProof: goapiproof.VenueClassAdmin, DeployedDigestState: "MISMATCH"}, false, "MISMATCH"},
		{"venue but schema mismatch", statusReportOperation{DigestState: goapiproof.DigestMatch, VenueProof: goapiproof.VenueClassAdmin}, true, "MISMATCH"},
		{"no row", statusReportOperation{VenueProof: goapiproof.VenueClassAdmin}, false, "-"},
	} {
		if got := proofWord(tc.op, tc.mis); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}
