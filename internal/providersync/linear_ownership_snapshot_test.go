package providersync

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

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
		return `{"id":` + id + `,"name":"P","description":"","status":{"id":"s","name":"Active","type":"started"},"trashed":false,"targetDate":"","archivedAt":null,"url":"","lead":null,"teams":{"nodes":[{"id":"team-raw-1","key":"QA"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}`
	}
	wantReason := map[string]string{
		"undecodable node":      "project_node_undecodable",
		"not normalizable node": "project_node_not_normalizable",
	}
	for name, bad := range map[string]string{
		"undecodable node":      node("5"),  // id is a number: json.Unmarshal fails
		"not normalizable node": node(`""`), // empty id: normalize refuses
	} {
		t.Run(name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			previous := otel.GetMeterProvider()
			otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
			t.Cleanup(func() { otel.SetMeterProvider(previous) })
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
			var collected metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &collected); err != nil {
				t.Fatal(err)
			}
			if got := linearIncompleteCount(collected, wantReason[name]); got != 1 {
				t.Fatalf("%s{reason=%q} = %d, want 1", linearOwnershipSnapshotIncompleteName, wantReason[name], got)
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

func linearIncompleteCount(collected metricdata.ResourceMetrics, reason string) int64 {
	var total int64
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != linearOwnershipSnapshotIncompleteName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				if value, found := point.Attributes.Value("reason"); found && value.AsString() == reason {
					total += point.Value
				}
			}
		}
	}
	return total
}

// TestLinearOwnershipSnapshotCompleteNeedsEveryTerm kills the three terms one
// at a time. A failed team walk returns an error before the collector reads
// the flag, so the TeamsComplete term cannot be driven false through the
// collector; this is its only observer.
func TestLinearOwnershipSnapshotCompleteNeedsEveryTerm(t *testing.T) {
	good := LinearReferenceCatalogEvidence{TeamsComplete: true, ProjectsComplete: true}
	if !linearOwnershipSnapshotComplete(good, LinearReferenceCatalogResult{}) {
		t.Fatal("a fully read run with no dropped link must be complete")
	}
	teams := good
	teams.TeamsComplete = false
	projects := good
	projects.ProjectsComplete = false
	for name, got := range map[string]bool{
		"teams not complete":    linearOwnershipSnapshotComplete(teams, LinearReferenceCatalogResult{}),
		"projects not complete": linearOwnershipSnapshotComplete(projects, LinearReferenceCatalogResult{}),
		"keyless link dropped":  linearOwnershipSnapshotComplete(good, LinearReferenceCatalogResult{OwnershipTeamsWithoutKey: 1}),
	} {
		if got {
			t.Errorf("%s: snapshot reported complete", name)
		}
	}
}

func runLinearCatalogWalk(t *testing.T, strict bool, teamsJSON, projectsJSON string) (LinearReferenceCatalogBatch, error) {
	t.Helper()
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = chaos4530SyntheticOrgID
	claim.SourceExternalID = "workspace"
	responses := []string{teamsJSON}
	if strings.Contains(teamsJSON, `"key":"QA"`) {
		responses = append(responses, `{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`)
	}
	responses = append(responses, projectsJSON)
	ref := teamCatalogRefFromClaim(claim)
	ref.Strict = strict
	return (LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10}).CollectReferenceCatalog(
		context.Background(), ref,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, fakehttp.Client(&linearWorkItemsDoer{responses: responses})),
		TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
}

const linearOneTeamJSON = `{"data":{"teams":{"nodes":[{"id":"team-raw-1","key":"QA","name":"Quality","members":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`

// TestLinearCatalogNeverReadsAnAbsentPageEndAsTheEnd pins one rule at every
// level: an absent or null page end is not an end. A page end counts only
// from an explicit false.
func TestLinearCatalogNeverReadsAnAbsentPageEndAsTheEnd(t *testing.T) {
	projectNode := func(teams string) string {
		return `{"id":"p1","name":"P","description":"","status":{"id":"s","name":"Active","type":"started"},"trashed":false,"targetDate":"","archivedAt":null,"url":"","lead":null,"teams":` + teams + `}`
	}
	page := func(nodes string, pageInfo string) string {
		return `{"data":{"projects":{"nodes":[` + nodes + `]` + pageInfo + `}}}`
	}
	end := `,"pageInfo":{"hasNextPage":false,"endCursor":null}`
	cases := map[string]string{
		"nested teams.pageInfo absent":        page(projectNode(`{"nodes":[{"id":"t","key":"QA"}]}`), end),
		"nested teams.pageInfo null":          page(projectNode(`{"nodes":[{"id":"t","key":"QA"}],"pageInfo":null}`), end),
		"nested teams.hasNextPage absent":     page(projectNode(`{"nodes":[{"id":"t","key":"QA"}],"pageInfo":{"endCursor":null}}`), end),
		"nested teams.hasNextPage null":       page(projectNode(`{"nodes":[{"id":"t","key":"QA"}],"pageInfo":{"hasNextPage":null}}`), end),
		"top-level projects pageInfo absent":  page(projectNode(`{"nodes":[{"id":"t","key":"QA"}],`+`"pageInfo":{"hasNextPage":false,"endCursor":null}}`), ``),
		"top-level projects hasNextPage null": page(projectNode(`{"nodes":[{"id":"t","key":"QA"}],`+`"pageInfo":{"hasNextPage":false,"endCursor":null}}`), `,"pageInfo":{"hasNextPage":null}`),
	}
	for name, projects := range cases {
		t.Run(name, func(t *testing.T) {
			batch, err := runLinearCatalogWalk(t, false, linearOneTeamJSON, projects)
			if err != nil {
				t.Fatalf("non-strict keeps what it read: %v", err)
			}
			if batch.Evidence.ProjectsComplete {
				t.Fatalf("ProjectsComplete = true on %s", name)
			}
			open := []linearReferenceOwnershipRow{linearOwnershipTestRow(t, "linear:QA", "keep", time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))}
			_, closed := linearOwnershipSnapshot(batch.Rows.Ownership, open, time.Now().UTC(), batch.Evidence.TeamsComplete && batch.Evidence.ProjectsComplete)
			if closed != 0 {
				t.Fatalf("the snapshot closed %d rows on %s", closed, name)
			}
		})
	}
	t.Run("explicit false end is complete", func(t *testing.T) {
		good := page(projectNode(`{"nodes":[{"id":"t","key":"QA"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}`), end)
		batch, err := runLinearCatalogWalk(t, false, linearOneTeamJSON, good)
		if err != nil || !batch.Evidence.ProjectsComplete {
			t.Fatalf("a stated end must be complete: err=%v complete=%v", err, batch.Evidence.ProjectsComplete)
		}
	})
	t.Run("strict: nested absent fails the run", func(t *testing.T) {
		_, err := runLinearCatalogWalk(t, true, linearOneTeamJSON, page(projectNode(`{"nodes":[]}`), end))
		if err == nil {
			t.Fatal("strict mode must fail on an unstated page end")
		}
	})
	t.Run("team members pageInfo absent fails the run", func(t *testing.T) {
		teams := `{"data":{"teams":{"nodes":[{"id":"team-raw-1","key":"QA","name":"Quality","members":{"nodes":[]}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
		if _, err := runLinearCatalogWalk(t, false, teams, page(``, end)); err == nil {
			t.Fatal("a roster with no stated end must fail, not pass as complete")
		}
	})
}
