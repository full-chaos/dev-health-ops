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
		"connectorsDataHealth",
		"dataHealthIdentity",
		"featureFlagTimeseries",
		"mappingCoverageHealth",
		"metricLineage",
		"productTelemetryPlatformDashboard",
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

func TestNamedLimitEvidenceRoundTrips(t *testing.T) {
	evidence := NamedLimitEvidence("reason", "the operator's words")
	if !HasNamedLimitEvidence(evidence) {
		t.Fatalf("%q is not recognised as named-limit evidence", evidence)
	}
	if !strings.HasSuffix(evidence, " the operator's words") || !strings.Contains(evidence, NamedLimitDigest("reason")) {
		t.Fatalf("evidence %q lost the operator's words or the reason digest", evidence)
	}
	for name, text := range map[string]string{
		"empty":            "",
		"plain":            "the operator's words",
		"short digest":     "NAMED-LIMIT:abc the operator's words",
		"upper digest":     "NAMED-LIMIT:" + strings.ToUpper(NamedLimitDigest("reason")) + " x",
		"prefix not first": "x " + evidence,
		"legacy waiver":    "ACKNOWLEDGED-UNPROVEN: the operator's words",
	} {
		if HasNamedLimitEvidence(text) {
			t.Errorf("%s: %q was read as named-limit evidence", name, text)
		}
	}
}

// Rows the retired venue path wrote keep their own word in status.
func TestLegacyVenueEvidenceIsStillRead(t *testing.T) {
	digest := strings.Repeat("d", 64)
	for name, tc := range map[string]struct{ evidence, want string }{
		"admin":   {"VENUE-PROOF:" + digest + " why", VenueClassAdmin},
		"no data": {"NO-PROD-DATA:" + digest + " VENUE-PROOF:" + digest + " why", VenueClassNoData},
		"waiver":  {"ACKNOWLEDGED-UNPROVEN: why", ""},
		"named":   {NamedLimitEvidence("r", "why"), ""},
	} {
		if got := LegacyVenueEvidenceClass(tc.evidence); got != tc.want {
			t.Errorf("%s: class %q, want %q", name, got, tc.want)
		}
	}
}
