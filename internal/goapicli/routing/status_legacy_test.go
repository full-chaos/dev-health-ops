package routing

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// CHAOS-8649: a MATCH row under a registered LEGACY document digest is reported with its class, its own
// digest, mode and build, and as reachable; the fields are additive, null/[] wherever there is no MATCH row.
func TestStatusReportsTheDocumentClassOfALegacyRow(t *testing.T) {
	current := strings.Repeat("a", 64)
	legacyDigest := strings.Repeat("b", 64)
	stray := strings.Repeat("c", 64)
	rollout := 100
	legacyRow := goapiproof.OperationStatus{
		Operation: "capacityForecast", DocumentDigest: current, DigestState: goapiproof.DigestMatch,
		Mode: "canary", CurrentCandidateBuild: "legacy-build", RolloutPercentage: &rollout,
		DocumentClass: goapiproof.DocumentClassLegacy, RowDocumentDigest: legacyDigest,
		AcceptedDocumentDigests: []string{legacyDigest, current},
	}
	deployed := map[string]string{"capacityForecast": current, "hotspots": current}

	got := toReportOperation(legacyRow, deployed, false, false)
	if got.Reachable == nil || !*got.Reachable || got.ReachableReason != nil || got.DeployedDigestState != "AGREE" {
		t.Fatalf("legacy row: reachable=%v reason=%v deployed=%s, want reachable true, no reason, AGREE", got.Reachable, got.ReachableReason, got.DeployedDigestState)
	}
	if derefOr(got.DocumentClass, "") != "legacy" || derefOr(got.RowDocumentDigest, "") != legacyDigest ||
		derefOr(got.Mode, "") != "canary" || derefOr(got.CurrentCandidateBuild, "") != "legacy-build" || got.DocumentDigest != current {
		t.Fatalf("legacy row: class=%v row=%v mode=%v build=%v document=%s", got.DocumentClass, got.RowDocumentDigest, got.Mode, got.CurrentCandidateBuild, got.DocumentDigest)
	}
	if !reflect.DeepEqual(got.AcceptedDocumentDigests, []string{legacyDigest, current}) {
		t.Fatalf("accepted = %v", got.AcceptedDocumentDigests)
	}

	stale := goapiproof.OperationStatus{Operation: "hotspots", DocumentDigest: current, DigestState: goapiproof.DigestStale,
		UnreachableDocumentDigests: []string{stray}}
	staleJSON, err := json.Marshal(toReportOperation(stale, deployed, false, false))
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(staleJSON, &decoded); err != nil {
		t.Fatal(err)
	}
	accepted, isList := decoded["accepted_document_digests"].([]any)
	if v, present := decoded["document_class"]; !present || v != nil {
		t.Errorf("stale row: document_class = %v (present %t), want null", v, present)
	}
	if v, present := decoded["row_document_digest"]; !present || v != nil {
		t.Errorf("stale row: row_document_digest = %v (present %t), want null", v, present)
	}
	if !isList || len(accepted) != 0 || decoded["reachable"] != false {
		t.Errorf("stale row: accepted_document_digests=%v reachable=%v, want [] and false", decoded["accepted_document_digests"], decoded["reachable"])
	}

	const schema = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	report := func(operations ...goapiproof.OperationStatus) statusReport {
		out := statusReport{
			LocalSchemaDigest: schema, GoPlaneSchemaDigest: stringPtr(schema), PlanesAgree: boolPtr(true), CatalogLoaded: true,
			RowsBySchemaDigest: map[string]int{schema: len(operations)},
		}
		for _, operation := range operations {
			out.Operations = append(out.Operations, toReportOperation(operation, deployed, false, false))
		}
		return out
	}
	text, _, _ := captureStatusText(t, report(legacyRow), schema)
	for _, want := range []string{
		"LEGACY document digest the catalog registers for this operation (served alike, CHAOS-8000 dual accept): " + legacyDigest + ", build legacy-build",
		"under accepted documents (current and legacy), any one in canary/primary serves: [" + legacyDigest + " " + current + "]",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("status text does not contain %q:\n%s", want, text)
		}
	}
	currentRow := legacyRow
	currentRow.DocumentClass, currentRow.RowDocumentDigest, currentRow.AcceptedDocumentDigests = goapiproof.DocumentClassCurrent, current, []string{current}
	if text, _, _ := captureStatusText(t, report(currentRow), schema); strings.Contains(text, "LEGACY") || strings.Contains(text, "accepted documents") {
		t.Errorf("a lone current-class row must not carry the legacy lines:\n%s", text)
	}
}
