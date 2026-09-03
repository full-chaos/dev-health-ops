package issueprlinks

import (
	"testing"

	"github.com/google/uuid"
)

// The MAGNITUDE axis (CHAOS-4815, contributed by lane-4752-go's round 1: its
// corpus had no positive id above 42, and a port that bounded values at 2^31
// mislabelled large rows silently).
//
// `ParsePRSource`'s doc records a reason-divergence at `uint32`. Until now that
// divergence was DESCRIBED and never exercised — which is the same failure as a
// measured table with an unmeasured generalisation: a claim nobody can check.
// These tests measure it.
//
// Python's side, measured against the deployed interpreter:
//
//	'0'                       isdigit=True  int=0                     rejected (<= 0)
//	'0'*40                    isdigit=True  int=0                     rejected (<= 0)
//	'2147483647' (int32 max)  isdigit=True  int=2147483647            accepted
//	'2147483648'              isdigit=True  int=2147483648            accepted
//	'4294967295' (uint32 max) isdigit=True  int=4294967295            accepted
//	'4294967296' (2^32)       isdigit=True  int=4294967296            accepted
//	'9223372036854775808'     isdigit=True  int=9223372036854775808   accepted
//	'9'*22                    isdigit=True  int=9999999999999999999999 accepted
//
// Python's int() is unbounded, so it accepts every magnitude. Go stops at
// uint32. The OUTPUT still agrees — `git_pull_requests.number` is `UInt32` in
// the live schema, so a parsed value above that can never match a row and the
// reference drops it at the PR-existence gate — but the REJECTION REASON
// differs, and the reason is load-bearing telemetry. Asserted here so the
// divergence is a measurement rather than a paragraph.

func TestParsePRSourceAcrossMagnitudes(t *testing.T) {
	const maxUint32 = "4294967295"
	cases := []struct {
		name     string
		number   string
		accepted bool
		why      string
	}{
		{"zero", "0", false, "both planes reject: Python via `pr_number <= 0`, Go via parsed == 0"},
		{"forty zeros", "0000000000000000000000000000000000000000", false,
			"Python's int() collapses it to 0 and rejects; Go's ParseUint yields 0 and rejects"},
		{"one", "1", true, "both accept"},
		{"int32 max", "2147483647", true, "both accept — a port bounding at 2^31 would stop just here"},
		{"int32 max plus one", "2147483648", true,
			"both accept; this is the value that catches an int32-bounded port"},
		{"uint32 max", maxUint32, true, "both accept; the last value Go can represent"},
		{"uint32 max plus one", "4294967296", false,
			"DIVERGENCE (reason only): Python parses it, then the PR-existence gate drops it because " +
				"git_pull_requests.number is UInt32. Same output, different rejection reason."},
		{"int64 max plus one", "9223372036854775808", false, "same class as 2^32"},
		{"twenty-two nines", "9999999999999999999999", false, "same class; Python's int() is unbounded"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			source, ok := ParsePRSource("ghpr:owner/repo#" + testCase.number)
			if ok != testCase.accepted {
				t.Fatalf("ParsePRSource(#%s) accepted=%v, want %v — %s",
					testCase.number, ok, testCase.accepted, testCase.why)
			}
			if !ok {
				return
			}
			if got := uint64(source.PRNumber); got == 0 {
				t.Fatalf("ParsePRSource(#%s) returned pr_number 0", testCase.number)
			}
		})
	}
}

// TestDeriveCountsAnOversizedPRNumberAsUnparseable pins WHICH counter the
// divergence lands in, since the rejection reasons are the telemetry that
// explains the read-to-written gap. If a future change routed these to
// `pr_not_found_or_out_of_window` instead — matching Python's reason as well as
// its output — this test is where that decision gets made deliberately.
func TestDeriveCountsAnOversizedPRNumberAsUnparseable(t *testing.T) {
	inputs := baseInputs()
	inputs.Dependencies[0].SourceWorkItemID = "ghpr:" + testSlug + "#4294967296"

	result := Derive(inputs)
	if result.Written() != 0 {
		t.Fatalf("wrote %d links for a PR number above uint32", result.Written())
	}
	if got := result.Rejected[ReasonUnparseableSource]; got != 1 {
		t.Errorf("rejected[unparseable_source] = %d, want 1 (full breakdown %v)", got, result.Rejected)
	}
	if got := result.Rejected[ReasonPRNotFound]; got != 0 {
		t.Errorf("rejected[pr_not_found] = %d, want 0 — Go stops at the parse, Python at the gate", got)
	}
	if !result.Balanced() {
		t.Fatalf("accounting does not balance: %+v", result)
	}
}

// TestDeriveWritesAPRNumberAtTheUint32Boundary is the other half: the largest
// value both planes accept must actually produce a link, not be lost to an
// off-by-one in the bound.
func TestDeriveWritesAPRNumberAtTheUint32Boundary(t *testing.T) {
	const maxUint32 = 4294967295
	inputs := baseInputs()
	inputs.Dependencies[0].SourceWorkItemID = "ghpr:" + testSlug + "#4294967295"
	inputs.PullRequests = []PullRequestRow{{OrgID: testOrg, RepoID: testRepoID, Number: maxUint32}}

	result := Derive(inputs)
	if result.Written() != 1 {
		t.Fatalf("wrote %d links at the uint32 boundary, want 1 (rejections %v)",
			result.Written(), result.Rejected)
	}
	if got := result.Links[0].PRNumber; got != maxUint32 {
		t.Fatalf("pr_number = %d, want %d", got, uint32(maxUint32))
	}
	if result.Links[0].RepoID == uuid.Nil {
		t.Fatal("repo_id was lost")
	}
}
