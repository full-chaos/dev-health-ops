package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func linearOwnershipTestRow(t *testing.T, team, project string, at time.Time) linearReferenceOwnershipRow {
	t.Helper()
	id := mustProjectID(t)(LinearProjectID(project))
	return linearReferenceOwnershipRow{
		OrgID: "org-1", Provider: "linear", TeamID: team, ProjectID: id, Source: "native",
		IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: at, UpdatedAt: at,
	}
}

// TestLinearOwnershipSnapshotRule pins the Linear writer on the shared rule:
// a fact still held keeps its first-seen valid_from (no new open row at each
// sync), a fact a COMPLETE run no longer holds is closed, and an incomplete
// run closes nothing.
func TestLinearOwnershipSnapshotRule(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	t2 := t1.Add(time.Hour)
	kept := linearOwnershipTestRow(t, "linear:ENG", "p1", t0)
	lost := linearOwnershipTestRow(t, "linear:ENG", "p2", t0)
	open := []linearReferenceOwnershipRow{kept, lost}

	rows, closed := linearOwnershipSnapshot([]linearReferenceOwnershipRow{linearOwnershipTestRow(t, "linear:ENG", "p1", t1)}, open, t1, true)
	if closed != 1 || len(rows) != 2 {
		t.Fatalf("complete run: rows=%d closed=%d, want 2 and 1", len(rows), closed)
	}
	if !rows[0].ValidFrom.Equal(t0) || rows[0].ValidTo != nil {
		t.Errorf("held fact: valid_from %v valid_to %v, want the first-seen %v and open", rows[0].ValidFrom, rows[0].ValidTo, t0)
	}
	if rows[1].ProjectID.String() != "p2" || rows[1].ValidTo == nil || !rows[1].ValidTo.Equal(t1) {
		t.Errorf("lost fact not closed at the run time: %+v", rows[1])
	}

	rows, closed = linearOwnershipSnapshot([]linearReferenceOwnershipRow{linearOwnershipTestRow(t, "linear:ENG", "p1", t2)}, open, t2, false)
	if closed != 0 || len(rows) != 1 || !rows[0].ValidFrom.Equal(t0) {
		t.Errorf("incomplete run: rows=%d closed=%d first=%v, want 1, 0 and first-seen valid_from", len(rows), closed, rows[0].ValidFrom)
	}
}

// TestLinearProjectsCompleteIsFalseWhenANodeIsGivenUp pins fail-closed
// completeness for the production mode (Strict=false): a project node the walk
// cannot decode, and one it cannot normalize, each leave ProjectsComplete
// false, so the snapshot closes nothing for the projects after it.
func TestLinearProjectsCompleteIsFalseWhenANodeIsGivenUp(t *testing.T) {
	node := func(id string) string {
		return `{"id":` + id + `,"name":"P","description":"","status":{"id":"s","name":"Active","type":"started"},"trashed":false,"targetDate":"","archivedAt":null,"url":"","lead":null,"teams":{"nodes":[{"id":"team-raw-1","key":"QA"}]}}`
	}
	for name, bad := range map[string]string{
		"undecodable node":      node("5"),  // id is a number: json.Unmarshal fails
		"not normalizable node": node(`""`), // empty id: normalize refuses
	} {
		t.Run(name, func(t *testing.T) {
			claim := nativeTestClaim("linear", "work-items")
			claim.OrgID = chaos4530SyntheticOrgID
			claim.SourceExternalID = "workspace"
			doer := &linearWorkItemsDoer{responses: []string{
				`{"data":{"teams":{"nodes":[{"id":"team-raw-1","key":"QA","name":"Quality","members":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
				`{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
				`{"data":{"projects":{"nodes":[` + node(`"p1"`) + `,` + bad + `,` + node(`"p3"`) + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
			}}
			batch, err := (LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10}).CollectReferenceCatalog(
				context.Background(), teamCatalogRefFromClaim(claim),
				providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
				linearWorkItemsClient(t, fakehttp.Client(doer)),
				TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
			)
			if err != nil {
				t.Fatalf("non-strict walk must keep what it read: %v", err)
			}
			if batch.Evidence.ProjectsComplete {
				t.Fatalf("ProjectsComplete = true after a node was given up on; rows=%d", len(batch.Rows.Projects))
			}
			// The snapshot a collector builds from this walk closes nothing.
			open := []linearReferenceOwnershipRow{
				linearOwnershipTestRow(t, "linear:QA", "p1", time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)),
				linearOwnershipTestRow(t, "linear:QA", "p3", time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)),
			}
			_, closed := linearOwnershipSnapshot(batch.Rows.Ownership, open, time.Now().UTC(), batch.Evidence.TeamsComplete && batch.Evidence.ProjectsComplete)
			if closed != 0 {
				t.Fatalf("the snapshot closed %d rows after a given-up node: p3 must stay open", closed)
			}
		})
	}
}
