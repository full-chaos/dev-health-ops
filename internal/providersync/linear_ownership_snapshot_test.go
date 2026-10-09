package providersync

import (
	"context"
	"reflect"
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

	kinds := func(complete bool) []KindSnapshot[OwnershipSnapshotRow] {
		return linearOwnershipKindSnapshots("org-1", testSoleScope(), LinearReferenceCatalogEvidence{TeamsComplete: true, ProjectsComplete: complete}, LinearReferenceCatalogResult{})
	}
	rows, plan := linearOwnershipSnapshot([]linearReferenceOwnershipRow{linearOwnershipTestRow(t, "linear:ENG", "p1", t1)}, open, t1, kinds(true)...)
	closed := len(plan.Retract)
	if closed != 1 || len(rows) != 2 {
		t.Fatalf("complete run: rows=%d closed=%d, want 2 and 1", len(rows), closed)
	}
	if !rows[0].ValidFrom.Equal(t0) || rows[0].ValidTo != nil {
		t.Errorf("held fact: valid_from %v valid_to %v, want the first-seen %v and open", rows[0].ValidFrom, rows[0].ValidTo, t0)
	}
	if rows[1].ProjectID.String() != "p2" || rows[1].ValidTo == nil || !rows[1].ValidTo.Equal(t1) {
		t.Errorf("lost fact not closed at the run time: %+v", rows[1])
	}

	rows, plan = linearOwnershipSnapshot([]linearReferenceOwnershipRow{linearOwnershipTestRow(t, "linear:ENG", "p1", t2)}, open, t2, kinds(false)...)
	closed = len(plan.Retract)
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
			_, plan := linearOwnershipSnapshot(batch.Rows.Ownership, open, time.Now().UTC(),
				linearOwnershipKindSnapshots(claim.OrgID, testSoleScope(), batch.Evidence, batch.Result)...)
			if closed := len(plan.Retract); closed != 0 {
				t.Fatalf("the snapshot closed %d rows after a given-up node: p3 must stay open", closed)
			}
			if got := plan.SnapshotReasons(); !reflect.DeepEqual(got, []string{linearSnapshotProjectsNotRead}) {
				t.Fatalf("abandon reasons = %v, want the project walk term", got)
			}
		})
	}
}

// linearIncompleteCounts reads the Linear incomplete-snapshot counter of the
// test binary's one meter reader, by reason.
func linearIncompleteCounts(t *testing.T) map[string]int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := meterReader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != linearOwnershipSnapshotIncompleteName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 sum", m.Name, m.Data)
			}
			for _, point := range sum.DataPoints {
				reason, _ := point.Attributes.Value("reason")
				out[reason.AsString()] += point.Value
			}
		}
	}
	return out
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

// TestLinearOwnershipKindSnapshotsGiveEachKindItsOwnTerms kills the three
// terms one at a time, and pins which kind each one gates: the team walk
// gates the team-key rows only, the project walk and the keyless link gate the
// project rows only. A failed team walk returns an error before the collector
// reads the flag, so the TeamsComplete term cannot be driven false through the
// collector; this is its only observer.
func TestLinearOwnershipKindSnapshotsGiveEachKindItsOwnTerms(t *testing.T) {
	const org = "org-1"
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	before := at.Add(-time.Hour)
	teamKey := func(key string) linearReferenceOwnershipRow {
		row := linearOwnershipTestRow(t, "linear:"+key, "unused", before)
		row.ProjectID = mustProjectID(t)(LinearTeamKeyProjectID(org, key))
		return row
	}
	open := []linearReferenceOwnershipRow{
		linearOwnershipTestRow(t, "linear:QA", "lost-project", before),
		teamKey("GONE"),
	}
	fresh := []linearReferenceOwnershipRow{
		linearOwnershipTestRow(t, "linear:QA", "held-project", at),
		teamKey("QA"),
	}
	good := LinearReferenceCatalogEvidence{TeamsComplete: true, ProjectsComplete: true}
	teams := good
	teams.TeamsComplete = false
	projects := good
	projects.ProjectsComplete = false
	for name, c := range map[string]struct {
		evidence   LinearReferenceCatalogEvidence
		result     LinearReferenceCatalogResult
		wantClosed []string
		wantReason map[string][]string
	}{
		"every term holds": {good, LinearReferenceCatalogResult{}, []string{"lost-project", org + ":linear:GONE"}, map[string][]string{}},
		"teams not complete": {teams, LinearReferenceCatalogResult{}, []string{"lost-project"},
			map[string][]string{"linear_team_key_ownership": {linearSnapshotTeamsNotRead}}},
		"projects not complete": {projects, LinearReferenceCatalogResult{}, []string{org + ":linear:GONE"},
			map[string][]string{"linear_project_ownership": {linearSnapshotProjectsNotRead}}},
		"keyless link dropped": {good, LinearReferenceCatalogResult{OwnershipTeamsWithoutKey: 1}, []string{org + ":linear:GONE"},
			map[string][]string{"linear_project_ownership": {linearSnapshotKeylessLink}}},
	} {
		rows, plan := linearOwnershipSnapshot(fresh, open, at, linearOwnershipKindSnapshots(org, testSoleScope(), c.evidence, c.result)...)
		closed := []string{}
		for _, row := range rows {
			if row.ValidTo != nil {
				closed = append(closed, row.ProjectID.String())
			}
		}
		if !reflect.DeepEqual(closed, c.wantClosed) {
			t.Errorf("%s: closed %v, want %v", name, closed, c.wantClosed)
		}
		reasons := map[string][]string{}
		for _, outcome := range plan.Abandoned() {
			reasons[outcome.Kind] = outcome.Abandoned
		}
		if !reflect.DeepEqual(reasons, c.wantReason) {
			t.Errorf("%s: abandoned %v, want %v", name, reasons, c.wantReason)
		}
	}
}

// Both Linear kinds take the scope gate's answer: with another active Linear
// integration in the organization, a complete and non-empty run closes no row
// of either kind and names scope_shared for both.
func TestLinearOwnershipKindSnapshotsCloseNothingOnASharedScope(t *testing.T) {
	const org = "org-1"
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	before := at.Add(-time.Hour)
	teamKey := func(key string, validFrom time.Time) linearReferenceOwnershipRow {
		row := linearOwnershipTestRow(t, "linear:"+key, "unused", validFrom)
		row.ProjectID = mustProjectID(t)(LinearTeamKeyProjectID(org, key))
		return row
	}
	open := []linearReferenceOwnershipRow{linearOwnershipTestRow(t, "linear:QA", "other-workspace-project", before), teamKey("OTHER", before)}
	fresh := []linearReferenceOwnershipRow{linearOwnershipTestRow(t, "linear:OPS", "this-workspace-project", at), teamKey("OPS", at)}
	good := LinearReferenceCatalogEvidence{TeamsComplete: true, ProjectsComplete: true}
	_, shared := linearOwnershipSnapshot(fresh, open, at, linearOwnershipKindSnapshots(org, testSharedScope(), good, LinearReferenceCatalogResult{})...)
	reasons := map[string][]string{}
	for _, outcome := range shared.Abandoned() {
		reasons[outcome.Kind] = outcome.Abandoned
	}
	want := map[string][]string{
		"linear_project_ownership":  {OwnershipCloseSkippedScopeShared},
		"linear_team_key_ownership": {OwnershipCloseSkippedScopeShared},
	}
	if len(shared.Retract) != 0 || !reflect.DeepEqual(reasons, want) {
		t.Fatalf("a shared scope: retract=%+v abandoned=%v, want nothing closed and %v", shared.Retract, reasons, want)
	}
	if _, sole := linearOwnershipSnapshot(fresh, open, at, linearOwnershipKindSnapshots(org, testSoleScope(), good, LinearReferenceCatalogResult{})...); len(sole.Retract) != 2 {
		t.Fatalf("control, the only integration: retract=%+v, want both rows of the lost workspace state closed", sole.Retract)
	}
}

// TestLinearEmptyAnswerOfOneKindClosesNoRowOfThatKind runs the real walk and
// the real kinds: a kind whose own answer is empty closes none of its rows,
// whatever the other kind holds. Zero project nodes with a team present keeps
// the open project rows (the team-key row of the team does not make the
// project kind "not empty"), and zero teams with a project present keeps the
// open team-key rows.
func TestLinearEmptyAnswerOfOneKindClosesNoRowOfThatKind(t *testing.T) {
	before := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	const org = chaos4530SyntheticOrgID
	projectRow := linearOwnershipTestRow(t, "linear:QA", "keep", before)
	teamKeyRow := linearOwnershipTestRow(t, "linear:OLD", "unused", before)
	teamKeyRow.ProjectID = mustProjectID(t)(LinearTeamKeyProjectID(org, "OLD"))
	open := []linearReferenceOwnershipRow{projectRow, teamKeyRow}
	end := `,"pageInfo":{"hasNextPage":false,"endCursor":null}`
	project := `{"id":"other","name":"P","description":"","status":{"id":"s","name":"Active","type":"started"},"trashed":false,"targetDate":"","archivedAt":null,"url":"","lead":null,"teams":{"nodes":[{"id":"team-raw-1","key":"QA"}]` + end + `}}`
	noTeams := `{"data":{"teams":{"nodes":[]` + end + `}}}`
	projects := func(nodes string) string { return `{"data":{"projects":{"nodes":[` + nodes + `]` + end + `}}}` }
	for name, c := range map[string]struct {
		teams, projects string
		wantOpen        []string
		wantAbandoned   map[string][]string
	}{
		"control: a team and a project, both kinds close what they lost": {linearOneTeamJSON, projects(project),
			[]string{}, map[string][]string{}},
		"zero projects and a team present: the project row stays open": {linearOneTeamJSON, projects(``),
			[]string{"keep"}, map[string][]string{"linear_project_ownership": {SnapshotEmptyAnswer}}},
		"zero teams and a project present: the team-key row stays open": {noTeams, projects(project),
			[]string{org + ":linear:OLD"}, map[string][]string{"linear_team_key_ownership": {SnapshotEmptyAnswer}}},
		"zero teams and zero projects: both stay open": {noTeams, projects(``),
			[]string{"keep", org + ":linear:OLD"},
			map[string][]string{"linear_project_ownership": {SnapshotEmptyAnswer}, "linear_team_key_ownership": {SnapshotEmptyAnswer}}},
	} {
		t.Run(name, func(t *testing.T) {
			batch, err := runLinearCatalogWalk(t, false, c.teams, c.projects)
			if err != nil {
				t.Fatal(err)
			}
			if !batch.Evidence.TeamsComplete || !batch.Evidence.ProjectsComplete {
				t.Fatalf("the walk must be complete for this test to measure the empty rule: %+v", batch.Evidence)
			}
			rows, plan := linearOwnershipSnapshot(batch.Rows.Ownership, open, time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC),
				linearOwnershipKindSnapshots(org, testSoleScope(), batch.Evidence, batch.Result)...)
			closed := map[string]bool{}
			for _, row := range rows {
				if row.ValidTo != nil {
					closed[row.ProjectID.String()] = true
				}
			}
			stillOpen := []string{}
			for _, row := range open {
				if !closed[row.ProjectID.String()] {
					stillOpen = append(stillOpen, row.ProjectID.String())
				}
			}
			if !reflect.DeepEqual(stillOpen, c.wantOpen) {
				t.Errorf("open after the run = %v, want %v", stillOpen, c.wantOpen)
			}
			abandoned := map[string][]string{}
			for _, outcome := range plan.Abandoned() {
				abandoned[outcome.Kind] = outcome.Abandoned
			}
			if !reflect.DeepEqual(abandoned, c.wantAbandoned) {
				t.Errorf("abandoned = %v, want %v", abandoned, c.wantAbandoned)
			}
		})
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
	// Each case names the reason label the walk counts and logs the abandoned
	// close with.
	const nestedNotStated, pagesNotRead = "project_teams_page_end_not_stated", "project_pages_not_read_to_the_end"
	cases := map[string][2]string{
		"nested teams.pageInfo absent":        {page(projectNode(`{"nodes":[{"id":"t","key":"QA"}]}`), end), nestedNotStated},
		"nested teams.pageInfo null":          {page(projectNode(`{"nodes":[{"id":"t","key":"QA"}],"pageInfo":null}`), end), nestedNotStated},
		"nested teams.hasNextPage absent":     {page(projectNode(`{"nodes":[{"id":"t","key":"QA"}],"pageInfo":{"endCursor":null}}`), end), nestedNotStated},
		"nested teams.hasNextPage null":       {page(projectNode(`{"nodes":[{"id":"t","key":"QA"}],"pageInfo":{"hasNextPage":null}}`), end), nestedNotStated},
		"top-level projects pageInfo absent":  {page(projectNode(`{"nodes":[{"id":"t","key":"QA"}],`+`"pageInfo":{"hasNextPage":false,"endCursor":null}}`), ``), pagesNotRead},
		"top-level projects hasNextPage null": {page(projectNode(`{"nodes":[{"id":"t","key":"QA"}],`+`"pageInfo":{"hasNextPage":false,"endCursor":null}}`), `,"pageInfo":{"hasNextPage":null}`), pagesNotRead},
	}
	for name, c := range cases {
		projects, wantReason := c[0], c[1]
		t.Run(name, func(t *testing.T) {
			counted := linearIncompleteCounts(t)
			batch, err := runLinearCatalogWalk(t, false, linearOneTeamJSON, projects)
			if err != nil {
				t.Fatalf("non-strict keeps what it read: %v", err)
			}
			moved := map[string]int64{}
			for reason, n := range linearIncompleteCounts(t) {
				if d := n - counted[reason]; d != 0 {
					moved[reason] = d
				}
			}
			if !reflect.DeepEqual(moved, map[string]int64{wantReason: 1}) {
				t.Fatalf("%s moved %v, want {%s: 1}", linearOwnershipSnapshotIncompleteName, moved, wantReason)
			}
			if batch.Evidence.ProjectsComplete {
				t.Fatalf("ProjectsComplete = true on %s", name)
			}
			open := []linearReferenceOwnershipRow{linearOwnershipTestRow(t, "linear:QA", "keep", time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))}
			_, plan := linearOwnershipSnapshot(batch.Rows.Ownership, open, time.Now().UTC(),
				linearOwnershipKindSnapshots(chaos4530SyntheticOrgID, testSoleScope(), batch.Evidence, batch.Result)...)
			if closed := len(plan.Retract); closed != 0 {
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
