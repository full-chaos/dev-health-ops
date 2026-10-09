//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// TestLinearOwnershipThreeSyncsLeaveOneOpenRowPerFact runs the real sink three
// times (CHAOS-8885 shape: a new open row at every sync) and reads back what
// the readers see.
func TestLinearOwnershipThreeSyncsLeaveOneOpenRowPerFact(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}
	const org = "linear-snapshot-org"
	base := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)

	sync := func(n int, projects []string, complete bool) {
		t.Helper()
		at := base.Add(time.Duration(n) * time.Hour)
		var fresh []linearReferenceOwnershipRow
		for _, project := range projects {
			row := linearOwnershipTestRow(t, "linear:ENG", project, at)
			row.OrgID = org
			fresh = append(fresh, row)
		}
		rows, _, err := sink.SnapshotOwnership(ctx, org, fresh, at, linearOwnershipKindSnapshots(org, testSoleScope(), LinearReferenceCatalogEvidence{TeamsComplete: true, ProjectsComplete: complete}, LinearReferenceCatalogResult{})...)
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.writeOwnership(ctx, rows); err != nil {
			t.Fatal(err)
		}
	}
	open := func() map[string]int {
		t.Helper()
		out := map[string]int{}
		for _, row := range openGitLabOwnership(ctx, t, conn, org, "linear") {
			out[row.Project]++
		}
		return out
	}

	sync(0, []string{"p1", "p2"}, true)
	sync(1, []string{"p1", "p2"}, true)
	sync(2, []string{"p1", "p2"}, true)
	if got := open(); len(got) != 2 || got["p1"] != 1 || got["p2"] != 1 {
		t.Fatalf("after 3 syncs open rows = %v, want one per fact", got)
	}
	sync(3, []string{"p1"}, false)
	if got := open(); got["p2"] != 1 {
		t.Fatalf("an incomplete run closed a row: %v", got)
	}
	sync(4, []string{"p1"}, true)
	if got := open(); len(got) != 1 || got["p1"] != 1 {
		t.Fatalf("a complete run did not close the lost fact: %v", got)
	}
}

// runLinearCollectorOverSeededOwnership seeds one open ownership row for
// project "keep" and runs the real collector (Strict=false, as in
// production) over a workspace whose projects page is given.
func runLinearCollectorOverSeededOwnership(t *testing.T, projectNodes string, noTeams ...bool) (open map[string]int, result TeamCatalogResult) {
	t.Helper()
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}
	const org = "linear-collector-org"
	seed := linearOwnershipTestRow(t, "linear:QA", "keep", time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	seed.OrgID = org
	if err := sink.writeOwnership(ctx, []linearReferenceOwnershipRow{seed}); err != nil {
		t.Fatal(err)
	}
	responses := []string{
		`{"data":{"teams":{"nodes":[{"id":"team-raw-1","key":"QA","name":"Quality","members":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"projects":{"nodes":[` + projectNodes + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}
	if len(noTeams) > 0 && noTeams[0] {
		// A workspace with no team: the collector writes no team-key row either.
		responses = []string{
			`{"data":{"teams":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
			responses[2],
		}
	}
	doer := &linearWorkItemsDoer{responses: responses}
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = org
	ref := teamCatalogRefFromClaim(claim)
	ref.Strict = false
	collector := LinearTeamCatalogCollector{
		ScopeCensus: staticScopeCensus{},
		Handler:     LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10},
		Sink:        sink,
	}
	result, err := collector.CollectTeamCatalog(ctx, ref,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID}, linearWorkItemsClient(t, fakehttp.Client(doer)),
		TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	open = map[string]int{}
	for _, row := range openGitLabOwnership(ctx, t, conn, org, "linear") {
		open[row.Project]++
	}
	return open, result
}

func linearProjectNodeJSON(id, teamKey string) string {
	return `{"id":"` + id + `","name":"P","description":"","status":{"id":"s","name":"Active","type":"started"},"trashed":false,"targetDate":"","archivedAt":null,"url":"","lead":null,"teams":{"nodes":[{"id":"team-raw-1","key":"` + teamKey + `"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}`
}

// TestLinearCollectorClosesNothingUnlessTheSnapshotIsCompleteAndNotEmpty drives
// the collector's guards against a real store, one clause at a time. The
// control closes the seeded row, so a refusal below is the guard's, not a
// harness that cannot close.
func TestLinearCollectorClosesNothingUnlessTheSnapshotIsCompleteAndNotEmpty(t *testing.T) {
	t.Run("control: a complete run that no longer holds the seeded project closes it", func(t *testing.T) {
		open, result := runLinearCollectorOverSeededOwnership(t, linearProjectNodeJSON("other", "QA"))
		if open["keep"] != 0 || open["other"] != 1 || result.OwnershipRetracted != 1 {
			t.Fatalf("control: open=%v retracted=%d, want keep closed and other open", open, result.OwnershipRetracted)
		}
	})
	t.Run("a project node the walk gave up on", func(t *testing.T) {
		open, result := runLinearCollectorOverSeededOwnership(t, linearProjectNodeJSON("other", "QA")+`,{"id":5}`)
		if open["keep"] != 1 || result.OwnershipRetracted != 0 || !result.OwnershipSnapshotIncomplete {
			t.Fatalf("open=%v retracted=%d incomplete=%v, want keep open, 0 closed, incomplete", open, result.OwnershipRetracted, result.OwnershipSnapshotIncomplete)
		}
	})
	t.Run("a project whose teams page end is not stated", func(t *testing.T) {
		unstated := `{"id":"unstated","name":"P","description":"","status":{"id":"s","name":"Active","type":"started"},"trashed":false,"targetDate":"","archivedAt":null,"url":"","lead":null,"teams":{"nodes":[{"id":"team-raw-1","key":"QA"}]}}`
		open, result := runLinearCollectorOverSeededOwnership(t, linearProjectNodeJSON("other", "QA")+`,`+unstated)
		if open["keep"] != 1 || result.OwnershipRetracted != 0 || !result.OwnershipSnapshotIncomplete {
			t.Fatalf("open=%v retracted=%d incomplete=%v, want keep open, 0 closed, incomplete", open, result.OwnershipRetracted, result.OwnershipSnapshotIncomplete)
		}
	})
	t.Run("a project-team link without a key", func(t *testing.T) {
		open, result := runLinearCollectorOverSeededOwnership(t, linearProjectNodeJSON("other", "QA")+`,`+linearProjectNodeJSON("nokey", ""))
		if open["keep"] != 1 || result.OwnershipRetracted != 0 || !result.OwnershipSnapshotIncomplete {
			t.Fatalf("open=%v retracted=%d incomplete=%v, want keep open, 0 closed, incomplete", open, result.OwnershipRetracted, result.OwnershipSnapshotIncomplete)
		}
	})
	t.Run("a complete run with no ownership row at all", func(t *testing.T) {
		open, result := runLinearCollectorOverSeededOwnership(t, ``, true)
		if open["keep"] != 1 || result.OwnershipRetracted != 0 {
			t.Fatalf("open=%v retracted=%d, want keep open and 0 closed: an empty answer is an access change before it is a removal", open, result.OwnershipRetracted)
		}
	})
}
