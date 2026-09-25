package goapiproof

import (
	"slices"
	"strings"
	"testing"
)

// TestAdminPlatformCorpusPinsItsRoutesAndCapturedStatuses pins batch 6
// (CHAOS-6617): the platform-superadmin reads and the status both planes
// answered on the bigboy capture of record.
func TestAdminPlatformCorpusPinsItsRoutesAndCapturedStatuses(t *testing.T) {
	want := map[string]int{
		"REST:GET:/api/v1/admin/feature-flags/list":                      200,
		"REST:GET:/api/v1/admin/orgs/list":                               200,
		"REST:GET:/api/v1/admin/orgs/{org_id}/own_org":                   200,
		"REST:GET:/api/v1/admin/orgs/{org_id}/missing":                   404,
		"REST:GET:/api/v1/admin/orgs/{org_id}/feature-overrides/own_org": 200,
		"REST:GET:/api/v1/admin/platform/stats/stats":                    200,
		"REST:GET:/api/v1/admin/platform/audit-logs/list":                200,
	}
	got := map[string]int{}
	for _, operation := range dhoAPIAdminPlatformRunOrder {
		if !slices.Contains(RESTRunOrderFor(RESTServiceDHOAPI), operation) {
			t.Errorf("%s is not in the dho-api run order", operation)
		}
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		if spec.Method != "GET" || spec.Credential != RESTCredentialPlatformSuperadmin || spec.EffectiveService() != RESTServiceDHOAPI {
			t.Errorf("%s: method %s credential %q service %s, want a platform-superadmin GET on the dho api", operation, spec.Method, spec.Credential, spec.EffectiveService())
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
		t.Fatalf("platform cases = %d, want %d: %v", len(got), len(want), got)
	}
	for key, status := range want {
		if got[key] != status {
			t.Errorf("%s: status %d, want %d", key, got[key], status)
		}
	}
	if !PlansCredentialKind(RESTServiceDHOAPI, RESTCredentialPlatformSuperadmin) || PlansCredentialKind(RESTServiceQueryAPI, RESTCredentialPlatformSuperadmin) {
		t.Fatal("only the dho-api service plans platform-superadmin entries")
	}
}

// TestAdminPlatformOrgRoutesBindTheOperatorOrgAndNameTheMissingOne: own_org
// resolves through the operator-supplied org id (never a produced or
// invented one); the missing case addresses the zero uuid literally.
func TestAdminPlatformOrgRoutesBindTheOperatorOrgAndNameTheMissingOne(t *testing.T) {
	const org = "67f1add8-9fcb-4272-addb-044b70c442c8"
	for _, operation := range []string{"REST:GET:/api/v1/admin/orgs/{org_id}", "REST:GET:/api/v1/admin/orgs/{org_id}/feature-overrides"} {
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		path, _, _, unresolved := ResolveRESTIDBindings(spec.Path, spec.Requests[0], map[string]string{dhoAPIOrgIDProducer: org})
		if len(unresolved) != 0 || path == spec.Path || !strings.Contains(path, org) {
			t.Errorf("%s: resolved path %q (unresolved %v), want the operator org", operation, path, unresolved)
		}
		if _, _, _, unresolved := ResolveRESTIDBindings(spec.Path, spec.Requests[0], nil); len(unresolved) == 0 {
			t.Errorf("%s: without the operator org the request must be unresolved, never sent with a placeholder", operation)
		}
	}
	one, _ := SpecForREST("REST:GET:/api/v1/admin/orgs/{org_id}")
	if len(one.Requests) != 2 || one.Requests[1].PathLiterals["org_id"] != adminMissingID {
		t.Errorf("orgs/{org_id} must carry the zero-uuid missing case: %+v", one.Requests)
	}
}
