package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestLinearReferenceCatalogNormalizesRetiredProjectAndLeadTeams(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	observed := time.Date(2026, 8, 10, 12, 34, 56, 789000000, time.UTC)
	archivedAt := "2026-08-09T00:00:00Z"
	row, err := normalizeLinearReferenceProject(claim, linearReferenceProjectPayload{
		ID:         "project-42",
		Name:       "Platform",
		Status:     linearReferenceProjectStatusPayload{Type: "completed"},
		Trashed:    false,
		ArchivedAt: &archivedAt,
		TargetDate: "2026-09-30",
		URL:        "https://linear.app/acme/project-42",
		Lead:       &linearReferenceIdentityPayload{ID: "user-7", Name: "Alice", Email: "alice@example.com"},
		Teams: linearReferenceProjectTeamsPayload{Nodes: []linearReferenceProjectTeamPayload{
			{ID: "team-2", Key: "OPS"},
			{ID: "team-1", Key: "ENG"},
		}},
	}, observed)
	if err != nil {
		t.Fatal(err)
	}
	if row.ID != "project-42" || row.OrgID != claim.OrgID || row.Provider != "linear" ||
		row.Name != "Platform" || row.IsActive != 0 || row.State != "completed" ||
		row.TargetDate == nil || row.TargetDate.Format("2006-01-02") != "2026-09-30" ||
		row.URL != "https://linear.app/acme/project-42" ||
		row.LeadID == nil || *row.LeadID != "user-7" || row.LeadName == nil ||
		*row.LeadName != "Alice" || row.LeadEmail == nil || *row.LeadEmail != "alice@example.com" ||
		len(row.TeamIDs) != 2 || row.TeamIDs[0] != "team-2" || row.TeamKeys[1] != "ENG" ||
		!row.UpdatedAt.Equal(observed) || !row.LastSynced.Equal(observed) {
		t.Fatalf("row=%+v", row)
	}
}

func TestLinearReferenceCatalogTreatsTrashedProjectAsRetired(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	row, err := normalizeLinearReferenceProject(claim, linearReferenceProjectPayload{
		ID: "project-trashed", Name: "Deleted", Trashed: true,
		Status: linearReferenceProjectStatusPayload{Type: "canceled"},
	}, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if row.IsActive != 0 {
		t.Fatalf("trashed project remained active: %+v", row)
	}
}

func TestLinearReferenceCatalogRejectsCrossScopeInputs(t *testing.T) {
	baseClaim := nativeTestClaim("linear", "work-items")
	baseRef := teamCatalogRefFromClaim(baseClaim)
	observed := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name       string
		ref        TeamCatalogReference
		credential providerfoundation.Credential
		client     func(*providerfoundation.HTTPClient)
	}{
		{
			name:       "invalid reference: missing org id",
			ref:        TeamCatalogReference{SyncRunID: baseRef.SyncRunID},
			credential: providerfoundation.Credential{Provider: "linear", ID: baseClaim.CredentialID},
		},
		{
			name:       "invalid reference: missing sync run id",
			ref:        TeamCatalogReference{OrgID: baseRef.OrgID},
			credential: providerfoundation.Credential{Provider: "linear", ID: baseClaim.CredentialID},
		},
		{
			name:       "wrong credential provider",
			ref:        baseRef,
			credential: providerfoundation.Credential{Provider: "github", ID: baseClaim.CredentialID},
		},
		{
			name:       "missing credential id",
			ref:        baseRef,
			credential: providerfoundation.Credential{Provider: "linear", ID: ""},
		},
		{
			name:       "wrong client provider",
			ref:        baseRef,
			credential: providerfoundation.Credential{Provider: "linear", ID: baseClaim.CredentialID},
			client:     func(client *providerfoundation.HTTPClient) { client.Provider = "github" },
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			doer := &linearWorkItemsDoer{responses: []string{}}
			client := linearWorkItemsClient(t, doer)
			if testCase.client != nil {
				testCase.client(client)
			}
			batch, err := (LinearReferenceCatalogRouteHandler{}).CollectReferenceCatalog(
				context.Background(), testCase.ref, testCase.credential, client,
				TeamCatalogSelections{Teams: true, Members: true, Projects: true}, observed,
			)
			if !errors.Is(err, ErrInvalidConfiguration) || batch.Result.Complete || len(doer.requests) != 0 {
				t.Fatalf("batch=%+v error=%v requests=%d", batch, err, len(doer.requests))
			}
		})
	}
}

// teamCatalogRefFromClaim lets existing tests keep building scenarios off
// nativeTestClaim's fixture data (CHAOS-4431 dropped the Claim parameter
// from CollectReferenceCatalog; it is now fed a claim-free reference).
func teamCatalogRefFromClaim(claim Claim) TeamCatalogReference {
	return TeamCatalogReference{
		OrgID: claim.OrgID, SyncRunID: claim.SyncRunID,
		IntegrationID: claim.IntegrationID, SourceID: claim.SourceID,
	}
}

func TestLinearReferenceCatalogRejectsForeignTenantCache(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = "org-a"
	rows := []LinearReferenceTeam{{
		OrgID: "org-b", Provider: "linear", ID: "team-1", Name: "Engineering", NativeTeamKey: "ENG",
	}}
	if _, ok := linearReferenceTeamPayloadForOrg(rows, claim.OrgID, "ENG"); ok {
		t.Fatal("foreign-tenant reference cache row resolved")
	}
	rows[0].OrgID = claim.OrgID
	if _, ok := linearReferenceTeamPayloadForOrg(rows, claim.OrgID, "ENG"); !ok {
		t.Fatal("tenant-matching reference cache row did not resolve")
	}
}

func TestLinearReferenceCatalogPrefersTenantScopedCache(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = "org-a"
	rows := []LinearReferenceTeam{
		{Provider: "linear", ID: "ENG", Name: "Provider default", NativeTeamKey: "ENG"},
		{OrgID: claim.OrgID, Provider: "linear", ID: "ENG", Name: "Tenant Engineering", NativeTeamKey: "ENG"},
	}
	team, ok := linearReferenceTeamPayloadForOrg(rows, claim.OrgID, "ENG")
	if !ok || team.Name != "Tenant Engineering" {
		t.Fatalf("team=%+v ok=%t", team, ok)
	}
}

func TestLinearReferenceCatalogBuildsConcreteEffects(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	now := time.Date(2026, 8, 10, 12, 34, 56, 0, time.UTC)
	rows := LinearReferenceCatalogRows{
		Teams:       []linearReferenceTeamRow{{OrgID: claim.OrgID, Provider: "linear", ID: "team-1", Name: "Engineering", NativeTeamKey: linearReferenceStringPtr("ENG"), UpdatedAt: now}},
		Members:     []linearReferenceMemberRow{{OrgID: claim.OrgID, MemberID: "linear:alice@example.com", Name: "Alice", ProviderIdentities: `{"linear": ["alice@example.com"]}`, IsActive: 1, UpdatedAt: now}},
		Memberships: []linearReferenceMembershipRow{{OrgID: claim.OrgID, Provider: "linear", TeamID: "team-1", MemberID: "linear:alice@example.com", Source: "native", IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: now, UpdatedAt: now}},
		Projects:    []linearReferenceProjectRow{{OrgID: claim.OrgID, Provider: "linear", ID: "project-1", Name: "Platform", IsActive: 1, UpdatedAt: now, LastSynced: now}},
		Ownership:   []linearReferenceOwnershipRow{{OrgID: claim.OrgID, Provider: "linear", TeamID: "team-1", ProjectID: "project-1", Source: "native", IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: now, UpdatedAt: now}},
		Sprints:     []linearSprintRow{{OrgID: claim.OrgID, Provider: "linear", SprintID: "linear:cycle:cycle-1", Name: linearReferenceStringPtr("Cycle 1"), State: linearReferenceStringPtr("active"), NativeTeamKey: linearReferenceStringPtr("ENG"), LastSynced: now}},
	}
	effects, err := BuildLinearReferenceCatalogEffects(rows)
	if err != nil {
		t.Fatal(err)
	}
	batches := effects.Batches()
	if len(batches) != linearReferenceCatalogDestinationCount {
		t.Fatalf("effects=%d", len(batches))
	}
	wantDestinations := []string{
		linearReferenceCatalogTeamsDestination,
		linearReferenceCatalogMembersDestination,
		linearReferenceCatalogMembershipsDestination,
		linearReferenceCatalogProjectsDestination,
		linearReferenceCatalogOwnershipDestination,
		linearReferenceCatalogSprintsDestination,
	}
	for index, want := range wantDestinations {
		if got := batches[index].Destination; got != want {
			t.Fatalf("destination[%d]=%q want=%q", index, got, want)
		}
	}
	for _, effect := range batches {
		if effect.Recovery != EffectReadbackRequired || len(effect.Rows) == 0 {
			t.Fatalf("effect=%+v", effect)
		}
	}
	_ = context.Background()
	_ = errors.Is
}

func TestLinearReferenceCatalogRejectsForeignProjectEffectRow(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	now := time.Date(2026, 8, 10, 12, 34, 56, 0, time.UTC)
	row := linearReferenceProjectRow{
		ID: "project-1", OrgID: "other-org", Provider: "linear", Name: "Platform",
		IsActive: 1, UpdatedAt: now, LastSynced: now,
	}
	if err := row.validate(claim); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign project row validation error=%v", err)
	}
}

func TestLinearReferenceCatalogCollectsTeamsMembersProjectsAndOwnership(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "workspace"
	observed := time.Date(2026, 8, 10, 12, 34, 56, 0, time.UTC)
	doer := &linearWorkItemsDoer{responses: []string{
		`{"data":{"teams":{"nodes":[{"id":"team-raw-1","key":"ENG","name":"Engineering","description":"Platform team","members":{"nodes":[{"id":"user-1","name":"Alice","email":"alice@example.com","active":true},{"id":"user-2","name":"Inactive","email":"inactive@example.com","active":false}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"projects":{"nodes":[{"id":"project-42","name":"Platform","description":"Platform project","status":{"id":"status-1","name":"Completed","type":"completed"},"trashed":false,"targetDate":"2026-09-30","archivedAt":null,"url":"https://linear.app/project-42","lead":{"id":"user-1","name":"Alice","email":"alice@example.com"},"teams":{"nodes":[{"id":"team-raw-1","key":"ENG"}]} }],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}}
	batch, err := (LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10}).CollectReferenceCatalog(
		context.Background(), teamCatalogRefFromClaim(claim),
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer),
		TeamCatalogSelections{Teams: true, Members: true, Projects: true}, observed,
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Failure != nil || !batch.Result.Complete ||
		batch.Evidence.Requests != 3 || batch.Evidence.Pages != 3 ||
		!batch.Evidence.TeamsComplete || !batch.Evidence.MembersComplete || !batch.Evidence.ProjectsComplete {
		t.Fatalf("batch=%+v", batch)
	}
	if batch.Result.Teams != 1 || batch.Result.Members != 1 || batch.Result.Memberships != 1 ||
		batch.Result.Projects != 2 || batch.Result.Ownership != 2 {
		t.Fatalf("result=%+v", batch.Result)
	}
	if batch.Rows.Projects[0].State != "completed" || batch.Rows.Projects[0].IsActive != 1 ||
		batch.Rows.Projects[0].LeadID == nil || *batch.Rows.Projects[0].LeadID != "user-1" {
		t.Fatalf("projects=%+v", batch.Rows.Projects)
	}
	if len(batch.Rows.Projects[1].TeamIDs) != 0 || len(batch.Rows.Projects[1].TeamKeys) != 0 {
		t.Fatalf("team-derived project diverged from Python row shape: %+v", batch.Rows.Projects[1])
	}
	if batch.Rows.Members[0].MemberID != "linear:alice@example.com" || batch.Rows.Members[0].IsActive != 1 ||
		len(batch.Rows.Memberships[0].IdentityFacets) != 2 ||
		batch.Rows.Memberships[0].IdentityFacets[0] != "linear:alice@example.com" ||
		batch.Rows.Memberships[0].IdentityFacets[1] != "alice@example.com" ||
		len(batch.Rows.Teams[0].Members) != 2 || batch.Rows.Teams[0].Members[0] != "linear:alice@example.com" {
		t.Fatalf("members=%+v", batch.Rows.Members)
	}
	var teamRequest, projectRequest struct {
		Query     string                             `json:"query"`
		Variables linearReferenceConnectionVariables `json:"variables"`
	}
	if err := json.NewDecoder(doer.requests[0].Body).Decode(&teamRequest); err != nil {
		t.Fatal(err)
	}
	// requests[1] is the unconditional per-team cycles/sprint fetch (CHAOS-4431
	// codex review P1) -- the projects request now lands at index 2.
	if err := json.NewDecoder(doer.requests[2].Body).Decode(&projectRequest); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(teamRequest.Query, "members(first: 10)") ||
		!projectRequest.Variables.IncludeArchived ||
		!strings.Contains(projectRequest.Query, "lead") || !strings.Contains(projectRequest.Query, "teams") {
		t.Fatalf("team=%+v project=%+v", teamRequest, projectRequest)
	}
}

func TestLinearReferenceCatalogDoesNotRetireFromCappedOrMalformedProjects(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "workspace"
	cases := []struct {
		name      string
		responses []string
		handler   LinearReferenceCatalogRouteHandler
		code      LinearReferenceCatalogFailureCode
		wantErr   error
	}{
		{
			name:      "teams cap",
			responses: []string{`{"data":{"teams":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"cursor-1"}}}}`},
			handler:   LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 1}, code: LinearReferenceCatalogPaginationCap, wantErr: ErrPaginationCapExceeded,
		},
		{
			name: "malformed project page",
			responses: []string{
				`{"data":{"teams":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
				`{"data":{"projects":{"nodes":[{"id":"project-1","name":"Broken"}],"pageInfo":{}}}}`,
			},
			handler: LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10}, code: LinearReferenceCatalogInvalidResponse, wantErr: providerfoundation.ErrPaginationInvalid,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			doer := &linearWorkItemsDoer{responses: testCase.responses}
			batch, err := testCase.handler.CollectReferenceCatalog(
				context.Background(), teamCatalogRefFromClaim(claim),
				providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
				linearWorkItemsClient(t, doer),
				TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
			)
			if err == nil || !errors.Is(err, testCase.wantErr) || batch.Failure == nil || batch.Failure.Code != testCase.code || batch.Effects.Batches()[0].Destination != "" {
				t.Fatalf("batch=%+v err=%v", batch, err)
			}
		})
	}
}

func TestLinearReferenceCatalogPaginatesLargeTeamMemberships(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "workspace"
	doer := &linearWorkItemsDoer{responses: []string{
		`{"data":{"teams":{"nodes":[{"id":"team-raw-1","key":"ENG","name":"Engineering","members":{"nodes":[{"id":"user-1","name":"Alice","email":"alice@example.com","active":true}],"pageInfo":{"hasNextPage":true,"endCursor":"members-1"}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"team":{"members":{"nodes":[{"id":"user-2","name":"Bob","email":"bob@example.com","active":true}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`,
		`{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"projects":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}}
	batch, err := (LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10}).CollectReferenceCatalog(
		context.Background(), teamCatalogRefFromClaim(claim),
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer),
		TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != nil || batch.Result.Members != 2 || batch.Evidence.Requests != 4 || !batch.Evidence.MembersComplete {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	// CHAOS-4431 codex review P1: the team roster must reflect BOTH pages of
	// members, not just the page-1 node returned alongside the teams query --
	// a roster built from page 1 alone would silently truncate to Alice only
	// (2 facets: "linear:alice@example.com" + "alice@example.com"), never
	// reaching Bob's page-2 facets at all.
	if len(batch.Rows.Teams) != 1 || len(batch.Rows.Teams[0].Members) != 4 ||
		!containsString(batch.Rows.Teams[0].Members, "linear:bob@example.com") {
		t.Fatalf("roster truncated to page 1: teams=%+v", batch.Rows.Teams)
	}
}

func TestLinearReferenceCatalogCountsMultiplePagesOnce(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "workspace"
	doer := &linearWorkItemsDoer{responses: []string{
		`{"data":{"teams":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"teams-1"}}}}`,
		`{"data":{"teams":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"projects":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}}
	batch, err := (LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10}).CollectReferenceCatalog(
		context.Background(), teamCatalogRefFromClaim(claim),
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer),
		TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != nil || batch.Evidence.Requests != 3 || batch.Evidence.Pages != 3 || !batch.Result.Complete {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

// TestLinearReferenceCatalogNonStrictCycleFailureKeepsOtherRows pins CHAOS-
// 4431 codex review round 2, P1: team_autoimport_linear.py:636-639's outer
// except only zeroes sprint_rows on a non-strict cycles failure ("if strict:
// raise; sprint_rows = []") -- it never discards teams/members/projects
// already fetched. The cycles fetch here returns a GraphQL error response so
// it fails with providerfoundation.ErrGraphQLResponse.
func TestLinearReferenceCatalogNonStrictCycleFailureKeepsOtherRows(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	doer := &linearWorkItemsDoer{responses: []string{
		`{"data":{"teams":{"nodes":[{"id":"team-raw-1","key":"ENG","name":"Engineering","members":{"nodes":[{"id":"user-1","name":"Alice","email":"alice@example.com","active":true}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"errors":[{"message":"boom"}]}`,
		`{"data":{"projects":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}}
	ref := teamCatalogRefFromClaim(claim)
	ref.Strict = false
	batch, err := (LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10}).CollectReferenceCatalog(
		context.Background(), ref,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer),
		TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("non-strict cycles failure must not abort the whole call, got err=%v", err)
	}
	if len(batch.Rows.Sprints) != 0 {
		t.Fatalf("sprints must be discarded after a non-strict cycles failure, got=%+v", batch.Rows.Sprints)
	}
	if len(batch.Rows.Teams) != 1 || len(batch.Rows.Members) != 1 {
		t.Fatalf("teams/members already fetched must survive a non-strict cycles failure: teams=%+v members=%+v", batch.Rows.Teams, batch.Rows.Members)
	}
}

// TestLinearReferenceCatalogStrictCycleFailureAbortsTheWholeCall is the
// strict-mode complement: reference discovery must still propagate a cycles
// failure as a hard error (Python's "if strict: raise"), never silently
// degrade.
func TestLinearReferenceCatalogStrictCycleFailureAbortsTheWholeCall(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	doer := &linearWorkItemsDoer{responses: []string{
		`{"data":{"teams":{"nodes":[{"id":"team-raw-1","key":"ENG","name":"Engineering","members":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"errors":[{"message":"boom"}]}`,
	}}
	ref := teamCatalogRefFromClaim(claim)
	ref.Strict = true
	batch, err := (LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10}).CollectReferenceCatalog(
		context.Background(), ref,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer),
		TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err == nil || !errors.Is(err, providerfoundation.ErrGraphQLResponse) {
		t.Fatalf("strict cycles failure must propagate as an error, got batch=%+v err=%v", batch, err)
	}
}

// TestLinearReferenceTeamRosterFromMembershipsExcludesRejectedMemberships
// pins CHAOS-4431 codex review round 2, P1: the roster LinearTeamCatalog
// Collector writes to `teams.members` must be built from the membership-
// conflict guard's KEPT rows only. A member the guard rejected (e.g.
// alice, who has an active manual pin to a different team) must never
// appear in the roster even though she was one of the raw discovered
// members -- mirroring Python's _apply_roster, which runs on memberships
// AFTER split_memberships_for_review already filtered them.
func TestLinearReferenceTeamRosterFromMembershipsExcludesRejectedMemberships(t *testing.T) {
	// Simulates: alice was discovered on ENG but the conflict guard rejected
	// her (kept out of the memberships passed here); bob was kept.
	keptMemberships := []linearReferenceMembershipRow{
		{TeamID: "ENG", MemberID: "linear:bob@example.com", IdentityFacets: []string{"linear:bob@example.com", "bob@example.com"}},
	}
	roster := linearReferenceTeamRosterFromMemberships("ENG", keptMemberships)
	if len(roster) != 2 || !containsString(roster, "linear:bob@example.com") || !containsString(roster, "bob@example.com") {
		t.Fatalf("roster=%+v want bob's facets only", roster)
	}
	if containsString(roster, "linear:alice@example.com") || containsString(roster, "alice@example.com") {
		t.Fatalf("roster=%+v must not contain a membership the conflict guard rejected", roster)
	}
}

// TestLinearReferenceTeamRosterFromMembershipsScopesByTeam proves the
// rebuild is per-team: a kept membership for a DIFFERENT team must not leak
// into this team's roster.
func TestLinearReferenceTeamRosterFromMembershipsScopesByTeam(t *testing.T) {
	keptMemberships := []linearReferenceMembershipRow{
		{TeamID: "OPS", MemberID: "linear:carol@example.com", IdentityFacets: []string{"linear:carol@example.com"}},
	}
	roster := linearReferenceTeamRosterFromMemberships("ENG", keptMemberships)
	if len(roster) != 0 {
		t.Fatalf("roster=%+v want empty -- the only kept membership is for a different team", roster)
	}
}

func linearReferenceStringPtr(value string) *string { return &value }
