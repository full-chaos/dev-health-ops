package goapiproof

import (
	"slices"
	"testing"
)

// TestAdminAccessCorpusPinsItsRoutesAndCapturedStatuses pins batch 1 (CHAOS-6603):
// the ten org-admin cases (nine routes) and the status each plane answered on the bigboy
// capture of record. Deleting an entry, changing a status, or sending one with
// another credential fails here.
func TestAdminAccessCorpusPinsItsRoutesAndCapturedStatuses(t *testing.T) {
	want := map[string]int{
		"REST:GET:/api/v1/admin/audit-logs/list":                                                         200,
		"REST:GET:/api/v1/admin/audit-logs/{log_id}/produced":                                            200,
		"REST:GET:/api/v1/admin/audit-logs/{log_id}/missing":                                             404,
		"REST:GET:/api/v1/admin/audit-logs/resource/{resource_type}/{resource_id}/resource_without_rows": 200,
		"REST:GET:/api/v1/admin/audit-logs/user/{user_id}/user_without_rows":                             200,
		"REST:GET:/api/v1/admin/ip-allowlist/list":                                                       200,
		"REST:GET:/api/v1/admin/ip-allowlist/{entry_id}/missing":                                         404,
		"REST:GET:/api/v1/admin/retention-policies/list":                                                 200,
		"REST:GET:/api/v1/admin/retention-policies/{policy_id}/missing":                                  404,
		"REST:GET:/api/v1/admin/impersonate/status/status":                                               200,
	}
	got := map[string]int{}
	for _, operation := range dhoAPIAdminAccessRunOrder {
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
	if len(got) != len(want) {
		t.Fatalf("admin access cases = %d, want %d: %v", len(got), len(want), got)
	}
	for key, status := range want {
		if got[key] != status {
			t.Errorf("%s: status %d, want %d", key, got[key], status)
		}
	}
	if !PlansCredentialKind(RESTServiceDHOAPI, RESTCredentialOrgAdmin) || PlansCredentialKind(RESTServiceQueryAPI, RESTCredentialOrgAdmin) {
		t.Fatal("only the dho-api service plans org-admin entries")
	}
}

// TestAdminAuditLogProducedIDReadsTheListItemsAndBindsTheLogPath: the
// produced-id case takes the id from items[0].id of the audit-logs list body
// and puts it in the {log_id} path; an empty trail leaves it unresolved.
func TestAdminAuditLogProducedIDReadsTheListItemsAndBindsTheLogPath(t *testing.T) {
	list, err := SpecForREST("REST:GET:/api/v1/admin/audit-logs")
	if err != nil {
		t.Fatal(err)
	}
	producer := list.Requests[0].Produces[0]
	body := map[string]any{"items": []any{map[string]any{"id": "4cff98ba-6953-47ae-859e-920450fc9bc0"}, map[string]any{"id": "second"}}, "total": 2.0}
	id, ok := ExtractRESTID(body, producer)
	if !ok || id != "4cff98ba-6953-47ae-859e-920450fc9bc0" {
		t.Fatalf("produced id = %q, %v", id, ok)
	}
	if _, ok := ExtractRESTID(map[string]any{"items": []any{}}, producer); ok {
		t.Fatal("an empty trail must produce no id")
	}
	one, err := SpecForREST("REST:GET:/api/v1/admin/audit-logs/{log_id}")
	if err != nil {
		t.Fatal(err)
	}
	path, _, _, unresolved := ResolveRESTIDBindings(one.Path, one.Requests[1], map[string]string{producer.Name: id})
	if len(unresolved) != 0 || path != "/api/v1/admin/audit-logs/"+id {
		t.Fatalf("resolved path = %q (unresolved %v)", path, unresolved)
	}
	if _, _, _, unresolved := ResolveRESTIDBindings(one.Path, one.Requests[1], map[string]string{}); len(unresolved) == 0 {
		t.Fatal("without a produced id the request must be unresolved, never sent with a placeholder")
	}
}
