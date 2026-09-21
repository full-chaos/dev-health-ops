package main

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// A named-limit row, a row the retired venue path wrote, and a row nothing
// admitted are different states with different words; a store-proven row is
// "ok" whatever its evidence says.
//
// REBASE RESOLUTION, two code paths merged (#2761 and CHAOS-6107). #2761
// extracted this function and kept the pre-existing `|| schemaMismatch`
// clause; CHAOS-6107 removed that clause and, with it, the parameter.
// Every case below is #2761's, unchanged, EXCEPT the one that tested the
// removed clause -- its expectation now states the new contract, and the
// case that still refuses (`venue but digest mismatch`, #2761's own) is
// what proves the removal did not widen the MISMATCH path generally.
func TestProofWordKeepsNamedLimitLegacyVenueAndUnprovenRowsApart(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   statusReportOperation
		want string
	}{
		{"store proven", statusReportOperation{DigestState: goapiproof.DigestMatch, Proven: true}, "ok"},
		{"store proven, stale venue text", statusReportOperation{DigestState: goapiproof.DigestMatch, Proven: true, VenueProof: goapiproof.VenueClassAdmin}, "ok"},
		{"store proven, stale named-limit text", statusReportOperation{DigestState: goapiproof.DigestMatch, Proven: true, NamedLimit: true}, "ok"},
		{"nothing admitted it", statusReportOperation{DigestState: goapiproof.DigestMatch}, "UNPROVEN"},
		{"named limit", statusReportOperation{DigestState: goapiproof.DigestMatch, NamedLimit: true}, "NAMED-LIMIT"},
		{"named limit but the DEPLOYED plane registers another document digest", statusReportOperation{DigestState: goapiproof.DigestMatch, NamedLimit: true, DeployedDigestState: "MISMATCH"}, "MISMATCH"},
		{"named limit, no row", statusReportOperation{NamedLimit: true}, "-"},
		{"venue admin", statusReportOperation{DigestState: goapiproof.DigestMatch, VenueProof: goapiproof.VenueClassAdmin}, "VENUE-PROVEN(admin_only)"},
		{"venue no data", statusReportOperation{DigestState: goapiproof.DigestMatch, VenueProof: goapiproof.VenueClassNoData}, "VENUE-PROVEN(no_production_data)"},
		{"venue but the DEPLOYED plane registers another document digest", statusReportOperation{DigestState: goapiproof.DigestMatch, VenueProof: goapiproof.VenueClassAdmin, DeployedDigestState: "MISMATCH"}, "MISMATCH"},
		{"venue but the DEPLOYED plane does not register it at all", statusReportOperation{DigestState: goapiproof.DigestMatch, VenueProof: goapiproof.VenueClassAdmin, DeployedDigestState: "UNREGISTERED"}, "MISMATCH"},
		// WAS: `{"venue but schema mismatch", …, true, "MISMATCH"}`. A
		// schema difference is a fact about the binary PRINTING this
		// table, not about the row: proof is looked up at the live
		// digest, so a live row's receipt stands whatever SDL this
		// binary carries, and forcing MISMATCH here marked every live
		// row MISMATCH for the whole duration of a `carry` window. The
		// difference is still reported -- the banner and
		// `planes_agree:false` -- as what it actually predicts, that
		// WRITES from this binary would land where nothing reads.
		{"venue, with this binary's SDL differing from the deployed one", statusReportOperation{DigestState: goapiproof.DigestMatch, VenueProof: goapiproof.VenueClassAdmin}, "VENUE-PROVEN(admin_only)"},
		{"no row", statusReportOperation{VenueProof: goapiproof.VenueClassAdmin}, "-"},
	} {
		if got := proofWord(tc.op); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}
