package goapiproof

import (
	"reflect"
	"strings"
	"testing"
)

// The set of operations `enable` may write with no store proof is the set
// the compiled ledger names a written limit for, and nothing else can widen
// it. Pinned to the exact list so adding or dropping a limit is a reviewed
// edit of this line, not a side effect of a ledger edit.
func TestCompiledLedgerAdmitsExactlyTheDeclaredUnprovableOperations(t *testing.T) {
	ledger, err := DefaultGoServedLedger()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, operation := range ledger.Operations() {
		if _, ok := ledger.EnableLimitReason(operation); ok {
			got = append(got, operation)
		}
	}
	want := []string{
		"acrRepositoryScopes",
		"capacityCompletionDistribution",
		"capacityForecast",
		"capacityForecasts",
		"catalogValues",
		"cognitiveLoad",
		"complexityTimeseries",
		"compoundingRisk",
		"connectorsDataHealth",
		"coverageBaselines",
		"coverageScopeBaseline",
		"dataHealthIdentity",
		"featureFlagTimeseries",
		"home",
		"hotspots",
		"investmentBreakdown",
		"investmentEvidenceQuality",
		"investmentFull",
		"mappingCoverageHealth",
		"metricLineage",
		"productTelemetryPlatformDashboard",
		"recommendations",
		"reportRuns",
		"savedReport",
		"savedReports",
		"securityAlerts",
		"securityOverview",
		"sourceHealth",
		"testopsJobFailures",
		"throughputForecast",
		"workGraphArtifacts",
		"workGraphEdges",
		"workGraphFlow",
		"workItemTeamAttributions",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("operations the compiled ledger lets enable write with no proof run:\n got %v\nwant %v", got, want)
	}
}

func TestEnableLimitReasonDecisionTable(t *testing.T) {
	limit := strings.Repeat("l", minUnprovenReasonLength)
	twoPlane := strings.Repeat("a", 40)
	document := func(entries string) []byte {
		return []byte(`{"deletion_error_message":"{operation} moved","entries":[` + entries + `]}`)
	}
	guard := `"guards":[{"file":"f.go","test":"TestA"}]`
	ledger, err := ParseGoServedLedger(document(
		`{"operation":"proven","two_plane_ops_sha":"` + twoPlane + `",` + guard + `},` +
			`{"operation":"limited","two_plane_ops_sha":"` + twoPlane + `","enable_limit":"` + limit + `",` + guard + `},` +
			`{"operation":"unproven","unproven_reason":"` + limit + `",` + guard + `}`))
	if err != nil {
		t.Fatal(err)
	}
	for operation, want := range map[string]bool{
		"proven":    false, // two-plane proven, no written limit: only a proof run admits it
		"limited":   true,
		"unproven":  true,
		"not-there": false,
	} {
		if reason, ok := ledger.EnableLimitReason(operation); ok != want || (ok && reason != limit) {
			t.Errorf("%s: EnableLimitReason = %q, %t; want admitted=%t", operation, reason, ok, want)
		}
	}
	var nilLedger *GoServedLedger
	if _, ok := nilLedger.EnableLimitReason("limited"); ok {
		t.Error("a nil ledger admitted an operation")
	}
	if _, err := ParseGoServedLedger(document(
		`{"operation":"short","two_plane_ops_sha":"` + twoPlane + `","enable_limit":"todo",` + guard + `}`)); err == nil {
		t.Error("a placeholder enable_limit was accepted")
	}
}

func TestNamedLimitEvidenceCarriesTheReasonDigestAndTheOperatorsWords(t *testing.T) {
	evidence := NamedLimitEvidence("reason", "the operator's words")
	if !strings.HasPrefix(evidence, NamedLimitEvidencePrefix+NamedLimitDigest("reason")+" ") {
		t.Fatalf("evidence %q does not open with the prefix and the reason digest", evidence)
	}
	if !strings.HasSuffix(evidence, " the operator's words") {
		t.Fatalf("evidence %q lost the operator's words", evidence)
	}
}

// The written limit's length bound, at the boundary and one either side, on
// both fields that carry it; whitespace does not count toward it.
func TestEnableLimitLengthBoundary(t *testing.T) {
	guard := `"guards":[{"file":"f.go","test":"TestA"}]`
	twoPlane := strings.Repeat("a", 40)
	for name, tc := range map[string]struct {
		field  string
		reason string
		ok     bool
	}{
		"enable_limit 39":              {"enable_limit", strings.Repeat("l", minUnprovenReasonLength-1), false},
		"enable_limit 40":              {"enable_limit", strings.Repeat("l", minUnprovenReasonLength), true},
		"enable_limit 41":              {"enable_limit", strings.Repeat("l", minUnprovenReasonLength+1), true},
		"enable_limit whitespace only": {"enable_limit", strings.Repeat(" ", 80), false},
		"enable_limit padded to 40":    {"enable_limit", strings.Repeat(" ", 20) + strings.Repeat("l", 20), false},
	} {
		raw := []byte(`{"deletion_error_message":"{operation} moved","entries":[{"operation":"x","two_plane_ops_sha":"` +
			twoPlane + `","` + tc.field + `":"` + tc.reason + `",` + guard + `}]}`)
		_, err := ParseGoServedLedger(raw)
		if (err == nil) != tc.ok {
			t.Errorf("%s: parse error = %v, want ok=%t", name, err, tc.ok)
		}
	}
	for name, tc := range map[string]struct {
		reason string
		ok     bool
	}{
		"unproven_reason 39": {strings.Repeat("l", minUnprovenReasonLength-1), false},
		"unproven_reason 40": {strings.Repeat("l", minUnprovenReasonLength), true},
	} {
		raw := []byte(`{"deletion_error_message":"{operation} moved","entries":[{"operation":"x","unproven_reason":"` +
			tc.reason + `",` + guard + `}]}`)
		if _, err := ParseGoServedLedger(raw); (err == nil) != tc.ok {
			t.Errorf("%s: parse error = %v, want ok=%t", name, err, tc.ok)
		}
	}
}
