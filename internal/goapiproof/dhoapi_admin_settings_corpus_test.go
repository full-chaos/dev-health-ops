package goapiproof

import (
	"net/http"
	"slices"
	"testing"
)

// TestAdminSettingsCorpusPinsItsRoutesAndCapturedStatuses pins the bigboy admin
// pass additions (CHAOS-6688) and the status both planes answered on the
// capture of record.
func TestAdminSettingsCorpusPinsItsRoutesAndCapturedStatuses(t *testing.T) {
	want := map[string]int{
		"REST:GET:/api/v1/admin/settings/{category}/categories":                   200,
		"REST:GET:/api/v1/admin/settings/{category}/empty_category":               200,
		"REST:GET:/api/v1/admin/settings/{category}/unknown_category":             200,
		"REST:GET:/api/v1/admin/settings/{category}/{key}/missing_key":            404,
		"REST:GET:/api/v1/admin/llm-settings/unconfigured":                        200,
		"REST:GET:/api/v1/admin/sync-runs/{run_id}/units/missing":                 404,
		"REST:GET:/api/v1/admin/backfill-jobs/{job_id}/missing":                   404,
		"REST:GET:/api/v1/admin/integrations/pagerduty/status/default":            200,
		"REST:GET:/api/v1/admin/integrations/pagerduty/status/missing_credential": 200,
		"REST:POST:/api/v1/admin/integrations/github/install-url/mint":            200,
	}
	got := map[string]int{}
	for _, operation := range dhoAPIAdminSettingsRunOrder {
		if !slices.Contains(RESTRunOrderFor(RESTServiceDHOAPI), operation) {
			t.Errorf("%s is not in the dho-api run order", operation)
		}
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		if spec.Credential != RESTCredentialOrgAdmin || spec.EffectiveService() != RESTServiceDHOAPI {
			t.Errorf("%s: credential %q service %s, want an org-admin entry on the dho api", operation, spec.Credential, spec.EffectiveService())
		}
		for _, request := range spec.Requests {
			if request.WantCandidateStatus != request.WantBaselineStatus {
				t.Errorf("%s/%s: the planes must be held to the same status (%d vs %d)", operation, request.Name, request.WantCandidateStatus, request.WantBaselineStatus)
			}
			wantMode := RESTBodyModeJSON
			if spec.Method == http.MethodPost {
				wantMode = RESTBodyModeStatusOnly // the signed state differs on every call
			}
			if request.BodyMode != wantMode {
				t.Errorf("%s/%s: body mode %q, want %q", operation, request.Name, request.BodyMode, wantMode)
			}
			got[operation+"/"+request.Name] = request.WantBaselineStatus
		}
	}
	if len(got) != len(want) {
		t.Fatalf("settings-pass cases = %d, want %d: %v", len(got), len(want), got)
	}
	for key, status := range want {
		if got[key] != status {
			t.Errorf("%s: status %d, want %d", key, got[key], status)
		}
	}
	// The literals of the captured requests.
	category, _ := SpecForREST("REST:GET:/api/v1/admin/settings/{category}")
	for i, literal := range []string{"categories", "general", "zz-missing-category"} {
		if got := category.Requests[i].PathLiterals["category"]; got != literal {
			t.Errorf("settings/{category} request %d category = %q, want %q", i, got, literal)
		}
	}
	key, _ := SpecForREST("REST:GET:/api/v1/admin/settings/{category}/{key}")
	if l := key.Requests[0].PathLiterals; l["category"] != "general" || l["key"] != "zz-missing-key" {
		t.Errorf("settings/{category}/{key} literals = %v", l)
	}
	pd, _ := SpecForREST("REST:GET:/api/v1/admin/integrations/pagerduty/status")
	if got := pd.Requests[1].Query.Get("credential_name"); got != "zz-missing" {
		t.Errorf("pagerduty missing credential_name = %q", got)
	}
}

// TestAdminPersistNothingPOSTAllowlistIsExactlyTheInstallURLMint: an admin
// credential may carry a non-GET only for the operations on the allowlist, each
// read in the Python handler to persist nothing; any other POST stays refused
// (the existing TestAdminCredentialEntriesMustBeDHOAPIReadOnlyGETs pins the
// refusal), and the allowlist does not grow silently.
func TestAdminPersistNothingPOSTAllowlistIsExactlyTheInstallURLMint(t *testing.T) {
	if len(adminPersistNothingPOSTs) != 1 || !adminPersistNothingPOSTs["REST:POST:/api/v1/admin/integrations/github/install-url"] {
		t.Fatalf("allowlist = %v, want exactly the GitHub install-url mint", adminPersistNothingPOSTs)
	}
	if err := ValidateRESTCorpus(); err != nil {
		t.Fatalf("the corpus with the allowlisted POST must validate: %v", err)
	}
	// The same POST under another path is refused.
	operation := "REST:POST:/api/v1/admin/integrations/github/install-callback"
	restEndpointSpecs[operation] = RESTEndpointSpec{
		Method: "POST", Path: "/api/v1/admin/integrations/github/install-callback", Service: RESTServiceDHOAPI, Credential: RESTCredentialOrgAdmin,
		Requests: []RESTRequest{{Name: "case", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: RESTBodyModeStatusOnly}},
	}
	restRunOrder = append(restRunOrder, operation)
	err := validateRESTCredentialKinds()
	delete(restEndpointSpecs, operation)
	restRunOrder = restRunOrder[:len(restRunOrder)-1]
	if err == nil {
		t.Fatal("an org-admin POST that is not on the allowlist must be refused")
	}
}

// TestAdminUnservedLLMSettingsReadsStayOut: on the capture the Go plane answers
// 404 for these where Python answers 200; they are a route gap for the porting
// ticket and must not enter the corpus as cases (a case would fail every STEP
// run). Delete this test when the routes are served and captured.
func TestAdminUnservedLLMSettingsReadsStayOut(t *testing.T) {
	for _, operation := range []string{
		"REST:GET:/api/v1/admin/llm-settings/status",
		"REST:GET:/api/v1/admin/llm-settings/budget",
		"REST:GET:/api/v1/admin/llm-settings/spend",
		"REST:POST:/api/v1/admin/integrations/pagerduty/preflight",
		"REST:POST:/api/v1/admin/integrations/github/install-callback",
	} {
		if _, err := SpecForREST(operation); err == nil {
			t.Errorf("%s is registered but was not captured as identical (or may reach an outside service)", operation)
		}
	}
}
