package goapiproof

import "testing"

// TestAdminSetupCorpusPinsSetupStatusAndThePreflightRequests pins the setup
// status read and the two PagerDuty preflight requests (CHAOS-6702) with the
// statuses both planes answered on the bigboy captures of record.
func TestAdminSetupCorpusPinsSetupStatusAndThePreflightRequests(t *testing.T) {
	status, err := SpecForREST("REST:GET:/api/v1/admin/setup/status")
	if err != nil || status.Credential != RESTCredentialOrgAdmin || len(status.Requests) != 1 || status.Requests[0].WantBaselineStatus != 200 || status.Requests[0].WantCandidateStatus != 200 {
		t.Fatalf("setup/status = %+v, %v", status, err)
	}
	pre, err := SpecForREST("REST:POST:/api/v1/admin/integrations/pagerduty/preflight")
	if err != nil || pre.Credential != RESTCredentialOrgAdmin || pre.Method != "POST" {
		t.Fatalf("preflight = %+v, %v", pre, err)
	}
	want := map[string]int{"missing_credential": 200, "unknown_dataset": 400}
	if len(pre.Requests) != len(want) {
		t.Fatalf("preflight requests = %d, want %d", len(pre.Requests), len(want))
	}
	for _, request := range pre.Requests {
		if status, ok := want[request.Name]; !ok || request.WantCandidateStatus != status || request.WantBaselineStatus != status || request.BodyMode != RESTBodyModeJSON {
			t.Errorf("preflight %q: %+v", request.Name, request)
		}
		body, ok := request.Body.(map[string]any)
		if !ok || body["credential_name"] != "zz-missing" {
			t.Errorf("preflight %q body = %v: only the missing credential name may be used (no real credential is ever probed)", request.Name, request.Body)
		}
	}
}
