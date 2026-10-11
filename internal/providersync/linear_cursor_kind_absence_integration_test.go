//go:build integration

package providersync

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// TestLinearCursorKindsCloseAnAbsentRowOnlyWhenLinearSaysItIsGone (CHAOS-9124)
// runs the real collector against a real store that holds one open row of each
// Linear cursor kind: the ownership of a project ("keep", team QA) and the
// team-key row of a team ("OLD"). The walk is complete and holds neither, so
// each is a candidate. The end of a cursor walk is no proof (Linear states no
// contract for a list that changes while it is read): each row closes only on
// Linear's own answer for that one fact. The answers are the recorded ones
// (testdata/provider_answers/linear): the not-found bodies verbatim, the found
// ones built from the recorded shape.
func TestLinearCursorKindsCloseAnAbsentRowOnlyWhenLinearSaysItIsGone(t *testing.T) {
	const org = "linear-cursor-org"
	const end = `,"pageInfo":{"hasNextPage":false,"endCursor":null}`
	const oneTeam = `{"data":{"teams":{"nodes":[{"id":"team-raw-1","key":"QA","name":"Quality","members":{"nodes":[]` + end + `}}]` + end + `}}}`
	const cycles = `{"data":{"cycles":{"nodes":[]` + end + `}}}`
	// The walk lists one project ("other", of team QA) and no "keep": an empty project walk
	// closes nothing, so the candidate "keep" needs a project the run does hold.
	projects := `{"data":{"projects":{"nodes":[` + linearProjectNodeJSON("other", "QA") + `]` + end + `}}}`
	teamFound := loadRecordedAnswer(t, "linear", "teams_filter_key_found")
	projectFound := loadRecordedAnswer(t, "linear", "project_by_id_found")
	oldTeamKey := org + ":linear:OLD"

	for _, c := range []struct {
		name          string
		lookup        func(query string) string
		wantOpen      []string
		wantRetracted int
		wantLookups   int
	}{
		{"Linear says both are gone: both close (the control)", nil, []string{}, 2, 2},
		{"Linear says the team of the team-key row still exists: that row stays open",
			func(query string) string {
				if strings.Contains(query, "teams(filter") {
					return teamFound.foundBody(t, map[string]any{"key": "OLD"})
				}
				return linearRecordedProjectNotFound
			}, []string{oldTeamKey}, 1, 2},
		{"Linear says the project still has the team: the project row stays open",
			func(query string) string {
				if strings.Contains(query, "project(id") {
					return projectFound.foundBody(t, map[string]any{"nodes": []any{map[string]any{"key": "QA"}}})
				}
				return linearRecordedTeamsNotFound
			}, []string{"keep"}, 1, 2},
		{"Linear says the project exists without the team: the link is gone and closes",
			func(query string) string {
				if strings.Contains(query, "project(id") {
					return projectFound.foundBody(t, map[string]any{"nodes": []any{map[string]any{"key": "OPS"}}})
				}
				return linearRecordedTeamsNotFound
			}, []string{}, 2, 2},
		{"Linear answers a rate limit: nothing closes",
			func(string) string {
				return `{"errors":[{"message":"Rate limit exceeded","extensions":{"code":"RATELIMITED"}}],"data":null}`
			}, []string{"keep", oldTeamKey}, 0, 2},
		{"Linear answers something that is not JSON: nothing closes",
			func(string) string { return `<html>bad gateway</html>` }, []string{"keep", oldTeamKey}, 0, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, conn := newWorkItemEffectsConn(t)
			lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
			sink := LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}
			seededAt := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			projectRow := linearOwnershipTestRow(t, "linear:QA", "keep", seededAt)
			projectRow.OrgID = org
			teamKeyRow := linearOwnershipTestRow(t, "linear:OLD", "unused", seededAt)
			teamKeyRow.OrgID = org
			teamKeyRow.ProjectID = mustProjectID(t)(LinearTeamKeyProjectID(org, "OLD"))
			// The team QA's own team-key row is written by the run itself.
			if err := sink.writeOwnership(ctx, []linearReferenceOwnershipRow{projectRow, teamKeyRow}); err != nil {
				t.Fatal(err)
			}
			claim := nativeTestClaim("linear", "work-items")
			claim.OrgID = org
			ref := teamCatalogRefFromClaim(claim)
			ref.Strict = false
			doer := &linearWorkItemsDoer{responses: []string{oneTeam, cycles, projects}, lookup: c.lookup}
			collector := LinearTeamCatalogCollector{
				ScopeCensus: staticScopeCensus{},
				Handler:     LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10},
				Sink:        sink,
			}
			result, err := collector.CollectTeamCatalog(ctx, ref,
				providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
				linearWorkItemsClient(t, fakehttp.Client(doer)),
				TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatalf("collect: %v", err)
			}
			open := []string{}
			for _, row := range openGitLabOwnership(ctx, t, conn, org, "linear") {
				if row.Project != org+":linear:QA" && row.Project != "other" { // what the run itself holds and writes
					open = append(open, row.Project)
				}
			}
			sort.Strings(open)
			want := append([]string(nil), c.wantOpen...)
			sort.Strings(want)
			if strings.Join(open, ",") != strings.Join(want, ",") {
				t.Errorf("open rows after the run = %v, want %v", open, want)
			}
			if result.OwnershipRetracted != c.wantRetracted {
				t.Errorf("retracted = %d, want %d", result.OwnershipRetracted, c.wantRetracted)
			}
			lookups := 0
			for _, request := range doer.requests {
				body := requestBody(t, request)
				if linearLookupQuery(body) {
					lookups++
				}
			}
			if lookups != c.wantLookups {
				t.Errorf("lookups = %d, want %d: one question for each candidate", lookups, c.wantLookups)
			}
		})
	}
}
