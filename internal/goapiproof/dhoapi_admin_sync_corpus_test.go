package goapiproof

import (
	"slices"
	"testing"
)

// TestAdminSyncCorpusPinsItsRoutesAndCapturedStatuses pins batches 3+4
// (CHAOS-6614): the org-admin sync reads,
// with the status both planes answered on the bigboy capture of record.
func TestAdminSyncCorpusPinsItsRoutesAndCapturedStatuses(t *testing.T) {
	want := map[string]int{
		"REST:GET:/api/v1/admin/sync-configs/list":                                  200,
		"REST:GET:/api/v1/admin/sync-configs/auto-import-capabilities/capabilities": 200,
		"REST:GET:/api/v1/admin/sync-configs/{config_id}/missing":                   404,
		"REST:GET:/api/v1/admin/sync-configs/{config_id}/coverage/missing":          404,
		"REST:GET:/api/v1/admin/sync-configs/{config_id}/jobs/missing":              404,
		"REST:GET:/api/v1/admin/sync-configs/{config_id}/repositories/missing":      404,
		"REST:GET:/api/v1/admin/sync-runs/{run_id}/missing":                         404,
		"REST:GET:/api/v1/admin/sync-targets/targets":                               200,
		"REST:GET:/api/v1/admin/backfill-jobs/list":                                 200,
	}
	got := map[string]int{}
	for _, operation := range dhoAPIAdminSyncRunOrder {
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
		t.Fatalf("admin sync cases = %d, want %d: %v", len(got), len(want), got)
	}
	for key, status := range want {
		if got[key] != status {
			t.Errorf("%s: status %d, want %d", key, got[key], status)
		}
	}
}

// TestAdminTeamMemberRoutesAreNotHereYet: at this stack's base the Go dho api
// does not mount these routes (they reached main later), so a case would be
// refused by the mounted-route test in internal/apiservice. They are added,
// missing-team only for discover-members / infer-members, once the stack is on
// main; delete this test with that change.
func TestAdminTeamMemberRoutesAreNotHereYet(t *testing.T) {
	for _, operation := range []string{
		"REST:GET:/api/v1/admin/teams/{team_id}",
		"REST:GET:/api/v1/admin/teams/{team_id}/discover-members",
		"REST:GET:/api/v1/admin/teams/{team_id}/infer-members",
	} {
		if _, err := SpecForREST(operation); err == nil {
			t.Errorf("%s is registered but the Go dho api does not mount it at this base", operation)
		}
	}
}

// TestAdminHeldOutRowsStayOutUntilTheTimestampFix: teams (list), a real team
// and identities (list) are red on the capture only by the datetime render,
// and a corpus row known red on main fails every STEP run, so they are not
// registered until the render fix lands (then this test is deleted with them).
func TestAdminHeldOutRowsStayOutUntilTheTimestampFix(t *testing.T) {
	for _, operation := range []string{
		"REST:GET:/api/v1/admin/teams",
		"REST:GET:/api/v1/admin/identities",
	} {
		if _, err := SpecForREST(operation); err == nil {
			t.Errorf("%s is registered but its body differs on the capture (datetime render); hold it out until the fix", operation)
		}
	}
}

// TestAdminMissingCasesNameTheZeroUUID: every "missing" case of every
// admin-credential entry addresses the row by the zero uuid (the id the capture
// was taken with), except the credential route, which is addressed by
// provider and name. A different literal would be a different, uncaptured
// request.
func TestAdminMissingCasesNameTheZeroUUID(t *testing.T) {
	checked := 0
	for _, operation := range RESTRunOrderFor(RESTServiceDHOAPI) {
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		if spec.Credential != RESTCredentialOrgAdmin {
			continue
		}
		for _, request := range spec.Requests {
			if request.Name != "missing" || len(request.PathLiterals) == 0 {
				continue
			}
			if _, isCredential := request.PathLiterals["provider"]; isCredential {
				continue
			}
			checked++
			for name, value := range request.PathLiterals {
				if value != adminMissingID {
					t.Errorf("%s/%s: %s = %q, want the zero uuid", operation, request.Name, name, value)
				}
			}
		}
	}
	if checked < 8 {
		t.Fatalf("checked %d missing cases, want at least 8 (audit-logs, ip-allowlist, retention-policies, users, sync-configs x4, sync-runs)", checked)
	}
}
