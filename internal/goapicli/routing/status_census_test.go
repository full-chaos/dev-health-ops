package routing

import (
	"strings"
	"testing"
)

// The census line is read by an operator deciding whether to roll, and
// "live" is a property of the DEPLOYED PROCESS, not of the binary
// printing the line.
//
// Both directions are pinned, because either one alone can be satisfied
// by a marker that is simply always wrong the other way: run from an
// image built at the commit ABOUT TO roll, the rows carrying production
// traffic must not be labelled STALE and the rows nothing reads yet must
// not be labelled live.
func TestCensusMarkerFollowsTheDeployedProcessNotThisBinary(t *testing.T) {
	const (
		deployed = "sha256:29d509cd"
		local    = "sha256:898250a9"
	)
	deployedDigest := deployed

	preRoll := map[string]string{
		deployed: "live",
		local:    "NOT live yet",
	}
	for digest, want := range preRoll {
		got := censusMarker(digest, local, &deployedDigest)
		if !strings.Contains(got, want) {
			t.Fatalf("pre-roll marker for %s = %q, want it to say %q", digest, got, want)
		}
	}
	// The rows the fleet actually reads are never STALE.
	if strings.Contains(censusMarker(deployed, local, &deployedDigest), "STALE") {
		t.Fatal("the digest the deployed process computes was marked STALE")
	}
	// The digest this binary computes is not claimed live while nothing
	// serves it.
	if marker := censusMarker(local, local, &deployedDigest); strings.Contains(marker, "<- live") {
		t.Fatalf("this binary's own digest was marked live while the deployed process computes another: %q", marker)
	}

	// The ordinary case: both agree, and the marker stays the short one.
	if marker := censusMarker(deployed, deployed, &deployedDigest); marker != "  <- live" {
		t.Fatalf("with both planes agreeing the marker is %q, want the plain live marker", marker)
	}
	// A third digest nothing computes is the genuinely dead one.
	if marker := censusMarker("sha256:0000", local, &deployedDigest); !strings.Contains(marker, "STALE") {
		t.Fatalf("a digest neither plane computes is %q, want STALE", marker)
	}
}

// With the registry unreachable there is no authority on what is live.
// Saying STALE then would be evidence the rows are dead, which is
// precisely the claim this command cannot make -- and the same "two
// states, one silence" failure the whole surface exists to end.
func TestCensusMarkerClaimsNothingWhileTheRegistryIsUnreachable(t *testing.T) {
	const local = "sha256:898250a9"
	for _, digest := range []string{local, "sha256:29d509cd"} {
		marker := censusMarker(digest, local, nil)
		if strings.Contains(marker, "STALE") || strings.Contains(marker, "<- live") {
			t.Fatalf("marker for %s with query-api unreachable = %q, which claims something that cannot be known", digest, marker)
		}
		if !strings.Contains(marker, "unreachable") {
			t.Fatalf("marker %q does not say why it cannot classify", marker)
		}
	}
}

// The whole report, not just the helper: a mutation that stopped passing
// the deployed digest through would leave the helper's own tests green.
func TestStatusCensusPrintsTheDeployedMarker(t *testing.T) {
	const (
		deployed = "sha256:29d509cd"
		local    = "sha256:898250a9"
	)
	deployedDigest := deployed
	agree := false
	out, _, _ := captureStatusText(t, statusReport{
		LocalSchemaDigest:   local,
		GoPlaneSchemaDigest: &deployedDigest,
		PlanesAgree:         &agree,
		CatalogLoaded:       true,
		RowsBySchemaDigest:  map[string]int{deployed: 15, local: 15},
	}, local)

	for _, want := range []string{
		deployed + "  15  <- live",
		local + "  15  <- this binary's SDL, NOT live yet",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("census does not contain %q:\n%s", want, out)
		}
	}
}

func captureStatusText(t *testing.T, report statusReport, local string) (string, string, error) {
	t.Helper()
	var outBuf, errBuf strings.Builder
	savedOut, savedErr := stdout, stderr
	stdout, stderr = &outBuf, &errBuf
	t.Cleanup(func() { stdout, stderr = savedOut, savedErr })
	printStatusText(report, local)
	return outBuf.String(), errBuf.String(), nil
}
