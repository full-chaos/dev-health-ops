package goapiproof

import (
	"slices"
	"testing"
)

// TestAdminAccessCorpusPinsItsRoutesAndCapturedStatuses pins batch 1 (CHAOS-6603):
// the nine org-admin reads and the status each plane answered on the bigboy
// capture of record. Deleting an entry, changing a status, or sending one with
// another credential fails here.
func TestAdminAccessCorpusPinsItsRoutesAndCapturedStatuses(t *testing.T) {
	want := map[string]int{
		"REST:GET:/api/v1/admin/audit-logs/list":                                                         200,
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
