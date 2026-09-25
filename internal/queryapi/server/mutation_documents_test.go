package server

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
)

// Each registered saved-report mutation document is byte-identical to the wire
// text captured from the web client's real exchange chain, and is a mutation:
// a document edited by hand digest-misses every real request.
func TestRegisteredMutationDocumentsEqualTheirWireCaptures(t *testing.T) {
	t.Parallel()
	for fixture, document := range map[string]string{
		"create_saved_report_captured.graphql": registeredCreateSavedReportDocument,
		"update_saved_report_captured.graphql": registeredUpdateSavedReportDocument,
		"delete_saved_report_captured.graphql": registeredDeleteSavedReportDocument,
		"clone_saved_report_captured.graphql":  registeredCloneSavedReportDocument,
		"trigger_report_captured.graphql":      registeredTriggerReportDocument,
	} {
		captured, err := os.ReadFile("testdata/wire_capture/" + fixture)
		if err != nil {
			t.Fatal(err)
		}
		if string(captured) != document {
			t.Errorf("%s: the registered document differs from its capture\n captured:   %q\n registered: %q", fixture, captured, document)
		}
		kind, err := digest.DocumentKind(document)
		if err != nil || kind != digest.KindMutation {
			t.Errorf("%s: kind %q, %v; want a mutation", fixture, kind, err)
		}
	}
}
