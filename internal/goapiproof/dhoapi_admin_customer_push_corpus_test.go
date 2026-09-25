package goapiproof

import (
	"slices"
	"testing"
)

// TestAdminCustomerPushCorpusPinsItsRoutesAndCapturedStatuses pins batch 5
// (CHAOS-6618): the org-admin customer-push reads and the status both planes
// answered on the bigboy capture of record.
func TestAdminCustomerPushCorpusPinsItsRoutesAndCapturedStatuses(t *testing.T) {
	want := map[string]int{
		"REST:GET:/api/v1/admin/customer-push/schemas/list":                         200,
		"REST:GET:/api/v1/admin/customer-push/schemas/{schema_version}/v1":          200,
		"REST:GET:/api/v1/admin/customer-push/sources/list":                         200,
		"REST:GET:/api/v1/admin/customer-push/sources/{source_id}/missing":          404,
		"REST:GET:/api/v1/admin/customer-push/sources/{source_id}/produced":         200,
		"REST:GET:/api/v1/admin/customer-push/sources/{source_id}/batches/missing":  404,
		"REST:GET:/api/v1/admin/customer-push/sources/{source_id}/batches/produced": 200,
		"REST:GET:/api/v1/admin/customer-push/sources/{source_id}/tokens/missing":   404,
		"REST:GET:/api/v1/admin/customer-push/sources/{source_id}/tokens/produced":  200,
		"REST:GET:/api/v1/admin/customer-push/tokens/list":                          200,
		"REST:GET:/api/v1/admin/customer-push/batches/{ingestion_id}/missing":       404,
	}
	got := map[string]int{}
	for _, operation := range dhoAPIAdminCustomerPushRunOrder {
		if !slices.Contains(RESTRunOrderFor(RESTServiceDHOAPI), operation) {
			t.Errorf("%s is not in the dho-api run order", operation)
		}
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		if spec.Method != "GET" || spec.Credential != RESTCredentialOrgAdmin || spec.EffectiveService() != RESTServiceDHOAPI {
			t.Errorf("%s: method %s credential %q service %s, want an org-admin GET on the dho api", operation, spec.Method, spec.Credential, spec.EffectiveService())
		}
		for _, request := range spec.Requests {
			if request.WantCandidateStatus != request.WantBaselineStatus {
				t.Errorf("%s/%s: the planes must be held to the same status (%d vs %d)", operation, request.Name, request.WantCandidateStatus, request.WantBaselineStatus)
			}
			if request.BodyMode != RESTBodyModeJSON {
				t.Errorf("%s/%s: body mode %q, want json (the bodies were byte-identical on the capture)", operation, request.Name, request.BodyMode)
			}
			got[operation+"/"+request.Name] = request.WantBaselineStatus
		}
	}
	// The schema route is captured for the one published schema version.
	schema, _ := SpecForREST("REST:GET:/api/v1/admin/customer-push/schemas/{schema_version}")
	if got := schema.Requests[0].PathLiterals["schema_version"]; got != "external-ingest.v1" {
		t.Errorf("schema_version = %q, want external-ingest.v1 (the captured request)", got)
	}
	if len(got) != len(want) {
		t.Fatalf("customer-push cases = %d, want %d: %v", len(got), len(want), got)
	}
	for key, status := range want {
		if got[key] != status {
			t.Errorf("%s: status %d, want %d", key, got[key], status)
		}
	}
}

// TestAdminCustomerPushProducedSourceIDComesFromTheSourcesListRoot: the
// produced-id cases take the id from the root array of the sources list and
// bind it into the {source_id} path of each source-scoped route; no source
// means unresolved, never a placeholder on the wire.
func TestAdminCustomerPushProducedSourceIDComesFromTheSourcesListRoot(t *testing.T) {
	list, err := SpecForREST("REST:GET:/api/v1/admin/customer-push/sources")
	if err != nil {
		t.Fatal(err)
	}
	producer := list.Requests[0].Produces[0]
	const id = "22222222-2222-4222-8222-222222222222"
	if got, ok := ExtractRESTID([]any{map[string]any{"id": id}}, producer); !ok || got != id {
		t.Fatalf("produced id = %q, %v", got, ok)
	}
	if _, ok := ExtractRESTID([]any{}, producer); ok {
		t.Fatal("an org with no source must produce no id")
	}
	for _, operation := range []string{
		"REST:GET:/api/v1/admin/customer-push/sources/{source_id}",
		"REST:GET:/api/v1/admin/customer-push/sources/{source_id}/batches",
		"REST:GET:/api/v1/admin/customer-push/sources/{source_id}/tokens",
	} {
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		path, _, _, unresolved := ResolveRESTIDBindings(spec.Path, spec.Requests[1], map[string]string{adminCustomerPushSourceIDProducer: id})
		if len(unresolved) != 0 || path == spec.Path {
			t.Errorf("%s: resolved path %q (unresolved %v)", operation, path, unresolved)
		}
		if _, _, _, unresolved := ResolveRESTIDBindings(spec.Path, spec.Requests[1], map[string]string{}); len(unresolved) == 0 {
			t.Errorf("%s: without a produced source id the request must be unresolved", operation)
		}
	}
}
