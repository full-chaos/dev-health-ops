package goapiproof

import (
	"slices"
	"testing"
)

// TestAdminTeamsCorpusPinsItsRoutesAndCapturedStatuses pins the org-admin team
// routes and the status both planes answered on the bigboy capture of record.
func TestAdminTeamsCorpusPinsItsRoutesAndCapturedStatuses(t *testing.T) {
	want := map[string]int{
		"REST:GET:/api/v1/admin/teams/{team_id}/missing":                       404,
		"REST:GET:/api/v1/admin/teams/{team_id}/discover-members/missing_team": 404,
		"REST:GET:/api/v1/admin/teams/{team_id}/infer-members/missing_team":    404,
	}
	got := map[string]int{}
	for _, operation := range dhoAPIAdminTeamsRunOrder {
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
				t.Errorf("%s/%s: body mode %q, want json", operation, request.Name, request.BodyMode)
			}
			got[operation+"/"+request.Name] = request.WantBaselineStatus
		}
	}
	if len(got) != len(want) {
		t.Fatalf("teams cases = %d, want %d: %v", len(got), len(want), got)
	}
	for key, status := range want {
		if got[key] != status {
			t.Errorf("%s: status %d, want %d", key, got[key], status)
		}
	}
}

// TestAdminMemberDiscoveryIsMissingTeamOnly is the safety pin for the two
// routes that reach a provider API once a team AND a credential exist: every
// request the corpus sends them names the zero-uuid team, never a produced or
// operator-supplied team id, and expects the 404.
func TestAdminMemberDiscoveryIsMissingTeamOnly(t *testing.T) {
	for _, operation := range []string{
		"REST:GET:/api/v1/admin/teams/{team_id}/discover-members",
		"REST:GET:/api/v1/admin/teams/{team_id}/infer-members",
	} {
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		if len(spec.Requests) != 1 {
			t.Fatalf("%s has %d requests, want exactly the missing-team case", operation, len(spec.Requests))
		}
		request := spec.Requests[0]
		if len(request.IDBindings) != 0 || len(request.Produces) != 0 || request.PathLiterals["team_id"] != adminMissingID ||
			request.WantCandidateStatus != 404 || request.WantBaselineStatus != 404 {
			t.Errorf("%s: %+v must be the zero-uuid team, no id binding, 404 on both planes", operation, request)
		}
	}
	discover, _ := SpecForREST("REST:GET:/api/v1/admin/teams/{team_id}/discover-members")
	if got := discover.Requests[0].Query.Get("provider"); got != "github" {
		t.Errorf("discover-members provider = %q, want github (the route requires one)", got)
	}
	// The teams/{team_id} entry has no real-team (200) case until the datetime
	// render fix.
	one, _ := SpecForREST("REST:GET:/api/v1/admin/teams/{team_id}")
	for _, request := range one.Requests {
		if request.WantCandidateStatus == 200 {
			t.Errorf("teams/{team_id} has a 200 case %q, held out with the datetime render", request.Name)
		}
	}
}
