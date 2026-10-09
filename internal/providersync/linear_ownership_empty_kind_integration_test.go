//go:build integration

package providersync

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// TestLinearCollectorEmptyAnswerOfAKindClosesNoRowOfThatKind runs the real
// collector against a real store that holds one open row of each of the
// writer's two fact kinds: the ownership of a real project ("keep", team QA)
// and the team-key row of a team ("OLD"). A kind closes only on its own
// answer: the team-key row of a present team does not make the project answer
// "not empty", and a project does not make the team answer "not empty". The
// control closes both, so a row left open below is the rule's, not a harness
// that cannot close.
func TestLinearCollectorEmptyAnswerOfAKindClosesNoRowOfThatKind(t *testing.T) {
	const org = "linear-kind-org"
	const end = `,"pageInfo":{"hasNextPage":false,"endCursor":null}`
	const oneTeam = `{"data":{"teams":{"nodes":[{"id":"team-raw-1","key":"QA","name":"Quality","members":{"nodes":[]` + end + `}}]` + end + `}}}`
	const noTeam = `{"data":{"teams":{"nodes":[]` + end + `}}}`
	const cycles = `{"data":{"cycles":{"nodes":[]` + end + `}}}`
	projects := func(nodes string) string { return `{"data":{"projects":{"nodes":[` + nodes + `]` + end + `}}}` }
	project := linearProjectNodeJSON("other", "QA")
	oldTeamKey := org + ":linear:OLD"

	for _, c := range []struct {
		name          string
		responses     []string
		wantOpen      []string
		wantRetracted int
		wantAbandoned bool
	}{
		{"control: a team and a project, both kinds close the row they lost",
			[]string{oneTeam, cycles, projects(project)}, []string{org + ":linear:QA", "other"}, 2, false},
		{"zero project nodes and a team present: the project row stays open",
			[]string{oneTeam, cycles, projects(``)}, []string{"keep", org + ":linear:QA"}, 1, true},
		{"zero teams and a project present: the team-key row stays open",
			[]string{noTeam, projects(project)}, []string{oldTeamKey, "other"}, 1, true},
		{"zero teams and zero project nodes: both rows stay open",
			[]string{noTeam, projects(``)}, []string{"keep", oldTeamKey}, 0, true},
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
			if err := sink.writeOwnership(ctx, []linearReferenceOwnershipRow{projectRow, teamKeyRow}); err != nil {
				t.Fatal(err)
			}
			claim := nativeTestClaim("linear", "work-items")
			claim.OrgID = org
			ref := teamCatalogRefFromClaim(claim)
			ref.Strict = false
			collector := LinearTeamCatalogCollector{
				Handler: LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10},
				Sink:    sink,
			}
			result, err := collector.CollectTeamCatalog(ctx, ref,
				providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
				linearWorkItemsClient(t, fakehttp.Client(&linearWorkItemsDoer{responses: c.responses})),
				TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatalf("collect: %v", err)
			}
			open := []string{}
			for _, row := range openGitLabOwnership(ctx, t, conn, org, "linear") {
				open = append(open, row.Project)
			}
			sort.Strings(open)
			want := append([]string(nil), c.wantOpen...)
			sort.Strings(want)
			if !reflect.DeepEqual(open, want) {
				t.Errorf("open rows after the run = %v, want %v", open, want)
			}
			if result.OwnershipRetracted != c.wantRetracted || result.OwnershipSnapshotIncomplete != c.wantAbandoned {
				t.Errorf("retracted=%d incomplete=%v, want %d and %v", result.OwnershipRetracted, result.OwnershipSnapshotIncomplete,
					c.wantRetracted, c.wantAbandoned)
			}
		})
	}
}
