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
		"REST:GET:/api/v1/admin/sync-configs/{config_id}/produced":                  200,
		"REST:GET:/api/v1/admin/sync-configs/{config_id}/jobs/produced":             200,
		"REST:GET:/api/v1/admin/sync-configs/{config_id}/repositories/produced":     200,
		"REST:GET:/api/v1/admin/backfill-jobs/{job_id}/missing":                     404,
		"REST:GET:/api/v1/admin/backfill-jobs/{job_id}/produced":                    200,
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

// TestAdminSyncProducedIDsComeFromTheirListsAndStayUnresolvedWithoutRows: the
// sync-config and backfill-job produced cases bind the first id of their list
// (a bare array, and {items: [...]}); an org with no row leaves them unresolved
// (refused by name, never sent with a placeholder). coverage has no produced
// case: its status depends on the projection state.
func TestAdminSyncProducedIDsComeFromTheirListsAndStayUnresolvedWithoutRows(t *testing.T) {
	configs, _ := SpecForREST("REST:GET:/api/v1/admin/sync-configs")
	cfgProducer := configs.Requests[0].Produces[0]
	if id, ok := ExtractRESTID([]any{map[string]any{"id": "c-1"}, map[string]any{"id": "c-2"}}, cfgProducer); !ok || id != "c-1" {
		t.Fatalf("sync config producer = %q, %v", id, ok)
	}
	if _, ok := ExtractRESTID([]any{}, cfgProducer); ok {
		t.Fatal("an org without sync configs must produce no id")
	}
	jobs, _ := SpecForREST("REST:GET:/api/v1/admin/backfill-jobs")
	jobProducer := jobs.Requests[0].Produces[0]
	if id, ok := ExtractRESTID(map[string]any{"items": []any{map[string]any{"id": "j-1"}}}, jobProducer); !ok || id != "j-1" {
		t.Fatalf("backfill job producer = %q, %v", id, ok)
	}
	if _, ok := ExtractRESTID(map[string]any{"items": []any{}, "total": 0.0}, jobProducer); ok {
		t.Fatal("an org without backfill jobs must produce no id")
	}
	for operation, producer := range map[string]string{
		"REST:GET:/api/v1/admin/sync-configs/{config_id}":              cfgProducer.Name,
		"REST:GET:/api/v1/admin/sync-configs/{config_id}/jobs":         cfgProducer.Name,
		"REST:GET:/api/v1/admin/sync-configs/{config_id}/repositories": cfgProducer.Name,
		"REST:GET:/api/v1/admin/backfill-jobs/{job_id}":                jobProducer.Name,
	} {
		spec, _ := SpecForREST(operation)
		request := spec.Requests[len(spec.Requests)-1]
		if request.Name != "produced" {
			t.Fatalf("%s: last request is %q, want produced", operation, request.Name)
		}
		if path, _, _, unresolved := ResolveRESTIDBindings(spec.Path, request, map[string]string{producer: "the-id"}); len(unresolved) != 0 || path == spec.Path {
			t.Errorf("%s: resolved %q (unresolved %v)", operation, path, unresolved)
		}
		if _, _, _, unresolved := ResolveRESTIDBindings(spec.Path, request, map[string]string{}); len(unresolved) == 0 {
			t.Errorf("%s: without a produced id the request must be unresolved", operation)
		}
	}
	coverage, _ := SpecForREST("REST:GET:/api/v1/admin/sync-configs/{config_id}/coverage")
	for _, request := range coverage.Requests {
		if len(request.IDBindings) != 0 {
			t.Errorf("coverage has a produced case %q: its status depends on the projection state and stays out", request.Name)
		}
	}
}
