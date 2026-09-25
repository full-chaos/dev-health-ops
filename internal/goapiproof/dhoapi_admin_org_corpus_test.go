package goapiproof

import (
	"slices"
	"testing"
)

// TestAdminOrgCorpusPinsItsRoutesAndCapturedStatuses pins batch 2 (CHAOS-6610):
// the org-admin reads of users, org members and credentials, and the status
// both planes answered on the bigboy capture of record.
func TestAdminOrgCorpusPinsItsRoutesAndCapturedStatuses(t *testing.T) {
	want := map[string]int{
		"REST:GET:/api/v1/admin/users/list":                            200,
		"REST:GET:/api/v1/admin/users/{user_id}/missing":               404,
		"REST:GET:/api/v1/admin/users/{user_id}/produced":              200,
		"REST:GET:/api/v1/admin/orgs/{org_id}/members/own_org_members": 200,
		"REST:GET:/api/v1/admin/credentials/list":                      200,
		"REST:GET:/api/v1/admin/credentials/{provider}/{name}/missing": 404,
	}
	got := map[string]int{}
	for _, operation := range dhoAPIAdminOrgRunOrder {
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
		t.Fatalf("admin org cases = %d, want %d: %v", len(got), len(want), got)
	}
	for key, status := range want {
		if got[key] != status {
			t.Errorf("%s: status %d, want %d", key, got[key], status)
		}
	}
}

// TestAdminUsersProducedIDReadsTheRootArrayAndMembersBindsTheOperatorOrg:
// the users list answers a bare array (producer reads the root), and the
// members route takes the operator-supplied org id.
func TestAdminUsersProducedIDReadsTheRootArrayAndMembersBindsTheOperatorOrg(t *testing.T) {
	list, err := SpecForREST("REST:GET:/api/v1/admin/users")
	if err != nil {
		t.Fatal(err)
	}
	producer := list.Requests[0].Produces[0]
	id, ok := ExtractRESTID([]any{map[string]any{"id": "00000000-0000-4000-8000-00000000ad01"}}, producer)
	if !ok || id != "00000000-0000-4000-8000-00000000ad01" {
		t.Fatalf("produced id = %q, %v", id, ok)
	}
	if _, ok := ExtractRESTID([]any{}, producer); ok {
		t.Fatal("an empty list must produce no id")
	}
	members, err := SpecForREST("REST:GET:/api/v1/admin/orgs/{org_id}/members")
	if err != nil {
		t.Fatal(err)
	}
	path, _, _, unresolved := ResolveRESTIDBindings(members.Path, members.Requests[0], map[string]string{dhoAPIOrgIDProducer: "11111111-1111-4111-8111-111111111111"})
	if len(unresolved) != 0 || path != "/api/v1/admin/orgs/11111111-1111-4111-8111-111111111111/members" {
		t.Fatalf("resolved path = %q (unresolved %v)", path, unresolved)
	}
	credential, err := SpecForREST("REST:GET:/api/v1/admin/credentials/{provider}/{name}")
	if err != nil {
		t.Fatal(err)
	}
	path, _, _, unresolved = ResolveRESTIDBindings(credential.Path, credential.Requests[0], nil)
	if len(unresolved) != 0 || path != "/api/v1/admin/credentials/github/zz-missing" {
		t.Fatalf("resolved credential path = %q (unresolved %v)", path, unresolved)
	}
}
