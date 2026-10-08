package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// collectLinearTeamIDFixture runs the real Linear reference catalog route on
// one team (QA, one member) and one project owned by QA and by a team node
// with no key.
func collectLinearTeamIDFixture(t *testing.T) (Claim, LinearReferenceCatalogBatch) {
	t.Helper()
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "workspace"
	doer := &linearWorkItemsDoer{responses: []string{
		`{"data":{"teams":{"nodes":[` +
			`{"id":"team-raw-qa","key":"QA","name":"Quality","members":{"nodes":[{"id":"user-1","name":"Alice","email":"alice@example.com","active":true}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}` +
			`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"projects":{"nodes":[{"id":"7a6b5c4d-0000-4000-8000-00000000000a","name":"Q Project","description":"","status":{"id":"s","name":"Active","type":"started"},"trashed":false,"targetDate":"","archivedAt":null,"url":"","lead":null,"teams":{"nodes":[{"id":"team-raw-qa","key":"QA"},{"id":"team-raw-nokey","key":""}]}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}}
	batch, err := (LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10}).CollectReferenceCatalog(
		context.Background(), teamCatalogRefFromClaim(claim),
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, fakehttp.Client(doer)),
		TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("CollectReferenceCatalog: %v", err)
	}
	if batch.Failure != nil {
		t.Fatalf("failure: %+v", batch.Failure)
	}
	return claim, batch
}

// TestEveryLinearTeamIDWriteSiteWritesAPrefixedID asserts, on the rows the
// route itself emits, every Linear team-id write site: the team row (and the
// membership rows that take its id), the per-project ownership rows (which
// build the id on their own), and the team-key ownership row (whose
// project_key and project_id keep the NATIVE key).
func TestEveryLinearTeamIDWriteSiteWritesAPrefixedID(t *testing.T) {
	claim, batch := collectLinearTeamIDFixture(t)
	rows := batch.Rows

	teamIDs := map[string]bool{}
	if len(rows.Teams) != 1 {
		t.Fatalf("teams = %d, want 1", len(rows.Teams))
	}
	for _, team := range rows.Teams {
		if team.NativeTeamKey == nil || team.ID != "linear:"+*team.NativeTeamKey {
			t.Errorf("team row id %q is not linear:<native_team_key>", team.ID)
		}
		if want := uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+team.ID)).String(); team.TeamUUID != want {
			t.Errorf("team %q: team_uuid %s, want uuid5 of the prefixed id %s", team.ID, team.TeamUUID, want)
		}
		if err := validateLinearReferenceTeamRow(claim, team); err != nil {
			t.Errorf("team %q refused at write: %v", team.ID, err)
		}
		teamIDs[team.ID] = true
	}
	if !teamIDs["linear:QA"] {
		t.Fatalf("team ids = %v, want linear:QA", teamIDs)
	}

	if len(rows.Memberships) != 1 {
		t.Fatalf("memberships = %d, want 1", len(rows.Memberships))
	}
	for _, membership := range rows.Memberships {
		if membership.TeamID != "linear:QA" {
			t.Errorf("membership team_id %q, want linear:QA", membership.TeamID)
		}
		if err := validateLinearReferenceMembershipRow(claim, membership); err != nil {
			t.Errorf("membership refused at write: %v", err)
		}
	}

	projectRows, teamKeyRows := 0, map[string]bool{}
	for _, row := range rows.Ownership {
		if err := validateLinearReferenceOwnershipRow(claim, row); err != nil {
			t.Errorf("ownership row %+v refused at write: %v", row, err)
		}
		switch row.ProjectID {
		case "7a6b5c4d-0000-4000-8000-00000000000a":
			projectRows++
			if row.TeamID != "linear:QA" || row.ProjectKey != nil {
				t.Errorf("project ownership row: team_id %q project_key %v, want linear:QA and nil", row.TeamID, row.ProjectKey)
			}
		case claim.OrgID + ":linear:QA":
			key := row.ProjectID[len(claim.OrgID+":linear:"):]
			teamKeyRows[key] = true
			if row.TeamID != "linear:"+key || row.ProjectKey == nil || *row.ProjectKey != key {
				t.Errorf("team-key ownership row %s: team_id %q project_key %v, want linear:%s and the native key %s",
					row.ProjectID, row.TeamID, row.ProjectKey, key, key)
			}
		default:
			t.Errorf("unexpected ownership row %+v (a project_id built from a prefixed id, or a keyless team)", row)
		}
	}
	if projectRows != 1 || !teamKeyRows["QA"] || len(rows.Ownership) != 2 {
		t.Errorf("ownership rows = %+v (projects %d, evidence %+v), want one project row and one team-key row per team",
			rows.Ownership, len(rows.Projects), batch.Evidence)
	}
	if batch.Result.OwnershipTeamsWithoutKey != 1 {
		t.Errorf("ownership_teams_without_key = %d, want 1 (the keyless team node)", batch.Result.OwnershipTeamsWithoutKey)
	}
}

// TestTheLinearEffectsRefuseABareTeamID: the write-time validators refuse a
// Linear row whose team id has no "linear:" prefix, on every row class that
// carries a team id.
func TestTheLinearEffectsRefuseABareTeamID(t *testing.T) {
	claim, batch := collectLinearTeamIDFixture(t)
	team := batch.Rows.Teams[0]
	team.ID = *team.NativeTeamKey
	team.TeamUUID = uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+team.ID)).String()
	membership := batch.Rows.Memberships[0]
	membership.TeamID = "QA"
	ownership := batch.Rows.Ownership[0]
	ownership.TeamID = "QA"
	for name, err := range map[string]error{
		"team":       validateLinearReferenceTeamRow(claim, team),
		"membership": validateLinearReferenceMembershipRow(claim, membership),
		"ownership":  validateLinearReferenceOwnershipRow(claim, ownership),
	} {
		if !errors.Is(err, teamid.ErrBareTeamID) || !errors.Is(err, ErrInvalidConfiguration) {
			t.Errorf("%s row with a bare team id: err = %v, want a refusal", name, err)
		}
	}
}
