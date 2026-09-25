package routing

import (
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
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

// CHAOS-6807: a reachable row that records a control no plane obeys is named
// on the row (JSON `not_enforced`) and in the text, and a clean row is
// reported with an empty list, never null.
func TestStatusNamesTheControlsNoPlaneObeys(t *testing.T) {
	rollout, clean := 50, 100
	eligible := `["org-a"]`
	flagged := goapiproof.OperationStatus{
		Operation: "featureFlags", DocumentDigest: "abc", DigestState: goapiproof.DigestMatch,
		Mode: "canary", CurrentCandidateBuild: "b", RolloutPercentage: &rollout, EligibleOrgs: &eligible,
	}
	got := toReportOperation(flagged, map[string]string{"featureFlags": "abc"}, false, false)
	if want := []string{"rollout_percentage=50", `eligible_orgs=["org-a"]`}; !reflect.DeepEqual(got.NotEnforced, want) {
		t.Fatalf("not_enforced = %v, want %v", got.NotEnforced, want)
	}
	cleanRow := flagged
	cleanRow.RolloutPercentage, cleanRow.EligibleOrgs = &clean, nil
	if got := toReportOperation(cleanRow, map[string]string{"featureFlags": "abc"}, false, false); got.NotEnforced == nil || len(got.NotEnforced) != 0 {
		t.Fatalf("a clean row must report an empty list, got %#v", got.NotEnforced)
	}
	stale := goapiproof.OperationStatus{Operation: "hotspots", DocumentDigest: "def", DigestState: goapiproof.DigestStale}
	if got := toReportOperation(stale, nil, false, false); got.NotEnforced == nil || len(got.NotEnforced) != 0 {
		t.Fatalf("a row with no live state must report an empty list, got %#v", got.NotEnforced)
	}

	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	out, _, _ := captureStatusText(t, statusReport{
		LocalSchemaDigest: digest, GoPlaneSchemaDigest: stringPtr(digest), PlanesAgree: boolPtr(true), CatalogLoaded: true,
		RowsBySchemaDigest: map[string]int{digest: 1},
		Operations:         []statusReportOperation{toReportOperation(flagged, map[string]string{"featureFlags": "abc"}, false, false)},
	}, digest)
	if !strings.Contains(out, "NOT ENFORCED: rollout_percentage=50, eligible_orgs=[\"org-a\"]") || !strings.Contains(out, "EVERY authenticated org") {
		t.Fatalf("the text does not name the unenforced controls:\n%s", out)
	}
	out, _, _ = captureStatusText(t, statusReport{
		LocalSchemaDigest: digest, GoPlaneSchemaDigest: stringPtr(digest), PlanesAgree: boolPtr(true), CatalogLoaded: true,
		RowsBySchemaDigest: map[string]int{digest: 1},
		Operations:         []statusReportOperation{toReportOperation(cleanRow, map[string]string{"featureFlags": "abc"}, false, false)},
	}, digest)
	if strings.Contains(out, "NOT ENFORCED") {
		t.Fatalf("a clean row must not carry the flag:\n%s", out)
	}
}

// CHAOS-6807: `enable` refuses a rollout no plane obeys, by name, before any
// other precondition; the default (100) is unchanged.
func TestEnableRefusesARolloutNoPlaneObeys(t *testing.T) {
	t.Setenv(bearerEnvVar, "")
	t.Setenv("POSTGRES_URI", "")
	// The rollout refusal comes before every other precondition, -mode included.
	if err := run([]string{"enable", "-rollout", "50"}); err == nil || !strings.Contains(err.Error(), "neither plane enforces") {
		t.Fatalf("enable -rollout 50 with no -mode = %v, want the named rollout refusal first", err)
	}
	for _, rollout := range []string{"0", "1", "50", "99"} {
		err := run([]string{"enable", "-mode", "canary", "-rollout", rollout})
		if err == nil || !strings.Contains(err.Error(), "neither plane enforces") {
			t.Fatalf("enable -rollout %s = %v, want the named refusal", rollout, err)
		}
		if got := exitCodeFor(err); got != 3 {
			t.Fatalf("enable -rollout %s exits %d, want 3", rollout, got)
		}
	}
	// 100 passes this check and is then refused by the NEXT precondition,
	// which proves the refusal above is the rollout's and nothing else's.
	if err := run([]string{"enable", "-mode", "canary", "-rollout", "100"}); err == nil || strings.Contains(err.Error(), "neither plane enforces") {
		t.Fatalf("enable -rollout 100 = %v, want the provenance refusal, not the rollout one", err)
	}
}
