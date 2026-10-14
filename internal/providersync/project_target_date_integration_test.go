//go:build integration

package providersync

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// storedProjectTarget is the six target columns of one project as a reader
// gets them: date|state|kind|source|ref|name|other_open|fetched_at (ms).
func storedProjectTarget(t *testing.T, ctx context.Context, conn driver.Conn, orgID, provider, projectID string) string {
	t.Helper()
	var value string
	if err := conn.QueryRow(ctx, `SELECT concat(ifNull(toString(target_date), 'NULL'), '|', target_state, '|', target_kind, '|', target_source, '|', target_ref, '|',
		target_ref_name, '|', ifNull(toString(target_other_open), 'NULL'), '|', ifNull(toString(toUnixTimestamp64Milli(target_fetched_at)), 'NULL'))
		FROM projects FINAL WHERE org_id = ? AND provider = ? AND id = ?`, orgID, provider, projectID).Scan(&value); err != nil {
		t.Fatalf("read the target of %s: %v", projectID, err)
	}
	return value
}

// A Jira project's target follows its open versions through the real catalog
// write: the earliest open dated version, even overdue; the next one once it
// is released; none once nothing open is dated. A body that was not read, or
// that states no list, keeps what the key holds and says unknown.
func TestJiraProjectTargetFollowsOpenVersionsThroughTheCatalogWrite(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	orgID := uuid.NewString()
	start := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)

	sync := func(step int, opsBody string, opsStatus int) {
		t.Helper()
		newDoer := func() *jiraTeamCatalogFixtureDoer {
			return &jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
				jiraTeamCatalogProjectSearchURI: {body: `{"values":[{"id":"10001","key":"OPS","name":"Ops"},{"id":"20001","key":"NODATE","name":"No date"}],"isLast":true}`},
				"/rest/api/3/project/OPS":       {status: opsStatus, body: opsBody},
				"/rest/api/3/project/NODATE": {body: `{"projectTypeKey":"business","versions":[
				{"id":"9","released":false,"archived":false},{"id":"8","released":true,"releaseDate":"2026-01-01"}]}`},
				"/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0": {body: `{"values":[],"isLast":true}`},
			}}
		}
		sink := JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}
		ref := TeamCatalogReference{OrgID: orgID, SyncRunID: fmt.Sprintf("run-%d", step), IntegrationID: "integration-a"}
		credential := providerfoundation.Credential{Provider: "jira"}
		syncAt := start.Add(time.Duration(step) * time.Hour)
		_, err := JiraTeamCatalogCollector{ScopeCensus: staticScopeCensus{}, Handler: JiraTeamCatalogRouteHandler{}, Sink: sink}.CollectTeamCatalog(
			ctx, ref, credential, jiraTeamCatalogTestClient(t, fakehttp.Client(newDoer())), TeamCatalogSelections{Projects: true}, syncAt)
		if err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		// The recovery readback of the same answer finds the rows exactly as written.
		batch, err := JiraTeamCatalogRouteHandler{}.CollectTeamCatalog(
			ctx, ref, credential, jiraTeamCatalogTestClient(t, fakehttp.Client(newDoer())), TeamCatalogSelections{Projects: true}, syncAt)
		if err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		effect, err := effectBatchFromValues(jiraTeamCatalogProjectsDestination, EffectReadbackRequired, batch.Rows.Projects)
		if err != nil {
			t.Fatal(err)
		}
		claim := Claim{Unit: Unit{OrgID: orgID, Provider: "jira"}}
		if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
			t.Fatalf("step %d: readback %s %v, want exact", step, inspection, err)
		}
	}
	at := func(step int) int64 { return start.Add(time.Duration(step) * time.Hour).UnixMilli() }
	// readStep is the step whose answer NODATE's target is: the walk reads the
	// project bodies in order, so a failed body stops it and the projects after it
	// are unread that run (held).
	want := func(step, readStep int, ops string) {
		t.Helper()
		if got := storedProjectTarget(t, ctx, conn, orgID, "jira", "10001"); got != ops {
			t.Fatalf("step %d: OPS target = %s, want %s", step, got, ops)
		}
		state := "none_dated"
		if readStep != step {
			state = "held"
		}
		if got, none := storedProjectTarget(t, ctx, conn, orgID, "jira", "20001"), fmt.Sprintf("NULL|%s|||||NULL|%d", state, at(readStep)); got != none {
			t.Fatalf("step %d: NODATE target = %s, want %s", step, got, none)
		}
	}

	sync(1, `{"projectTypeKey":"software","versions":[
		{"id":"101","name":"2.0","released":false,"archived":false,"releaseDate":"2026-12-15"},
		{"id":"102","name":"1.9","released":false,"archived":false,"releaseDate":"2026-09-30"},
		{"id":"103","name":"1.8","released":true,"archived":false,"releaseDate":"2026-08-01"},
		{"id":"104","name":"1.7","released":false,"archived":true,"releaseDate":"2026-07-01"},
		{"id":"105","name":"3.0","released":false,"archived":false}]}`, http.StatusOK)
	want(1, 1, fmt.Sprintf("2026-09-30|stated|version|jira.version.releaseDate|102|1.9|1|%d", at(1)))

	// An ensure row of the Jira work-items route (a work item moved into the
	// project) is newer than the catalog row and names no target: it keeps the
	// target columns and says held.
	ensure := projectmembership.EnsureProjectsRow(orgID, "jira", "10001", "OPS", "Ops", start.Add(90*time.Minute)).
		WithState(projectmembership.JiraProjectState(projectmembership.JiraStatusLive))
	ensureEffect, err := effectBatchFromValues("projects", EffectReadbackRequired, []projectmembership.CatalogRow{ensure})
	if err != nil {
		t.Fatal(err)
	}
	if err := (JiraProjectCatalogClickHouseAdapter{Delegate: GitHubProjectCatalogClickHouseAdapter{Conn: conn}}).WriteGitHubWorkItemEffect(ctx,
		GitHubWorkItemEffectIdentity{OrgID: orgID, Provider: "jira", Dataset: "work-items", Destination: "projects", RowCount: len(ensureEffect.Rows)}, ensureEffect); err != nil {
		t.Fatal(err)
	}
	want(1, 1, fmt.Sprintf("2026-09-30|held|version|jira.version.releaseDate|102|1.9|1|%d", at(1)))

	sync(2, `{"projectTypeKey":"software","versions":[
		{"id":"101","name":"2.0","released":false,"archived":false,"releaseDate":"2026-12-15"},
		{"id":"102","name":"1.9","released":true,"archived":false,"releaseDate":"2026-09-30"}]}`, http.StatusOK)
	want(2, 2, fmt.Sprintf("2026-12-15|stated|version|jira.version.releaseDate|101|2.0|0|%d", at(2)))

	held := fmt.Sprintf("2026-12-15|held|version|jira.version.releaseDate|101|2.0|0|%d", at(2))
	sync(3, `{"projectTypeKey":"software"}`, http.StatusOK)
	want(3, 3, held)

	sync(4, `{}`, http.StatusInternalServerError)
	want(4, 3, held)

	sync(5, `{"projectTypeKey":"software","versions":[
		{"id":"101","name":"2.0","released":true,"archived":false,"releaseDate":"2026-12-15"}]}`, http.StatusOK)
	want(5, 5, fmt.Sprintf("NULL|none_dated|||||NULL|%d", at(5)))
}

type gitlabMilestoneServer struct {
	mu         sync.Mutex
	milestones string
	status     int
}

func (server *gitlabMilestoneServer) set(status int, milestones string) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.status, server.milestones = status, milestones
}

// GitLab's project target through the real catalog write, with the same
// held/stated/cleared rules and the milestone named by its sprints row key.
func TestGitLabProjectTargetFollowsOpenMilestonesThroughTheCatalogWrite(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := uuid.NewString()
	start := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	state := &gitlabMilestoneServer{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.EscapedPath() {
		case "/api/v4/groups/org":
			_, _ = w.Write([]byte(`{"id":1,"full_path":"org","name":"Org"}`))
		case "/api/v4/groups/org/subgroups":
			_, _ = w.Write([]byte(`[]`))
		case "/api/v4/groups/org/projects":
			_, _ = w.Write([]byte(`[{"id":100,"path_with_namespace":"org/team-a/api","name":"api","archived":false,"web_url":"u"}]`))
		case "/api/v4/projects/100/milestones":
			state.mu.Lock()
			status, body := state.status, state.milestones
			state.mu.Unlock()
			if status != http.StatusOK {
				w.WriteHeader(status)
				return
			}
			_, _ = w.Write([]byte(body))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	projectID := ""
	sync := func(step int, status int, milestones string) {
		t.Helper()
		state.set(status, milestones)
		collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
			Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		}, ScopeCensus: staticScopeCensus{}}
		ref := TeamCatalogReference{OrgID: orgID, SyncRunID: fmt.Sprintf("run-%d", step), IntegrationID: "integration-a"}
		credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
		syncAt := start.Add(time.Duration(step) * time.Hour)
		if _, err := collector.CollectTeamCatalog(ctx, ref, credential, gitlabTeamCatalogTestClient(t, server.URL),
			TeamCatalogSelections{Projects: true}, syncAt); err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		// The recovery readback of the same answer finds the rows exactly as written.
		batch, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(ctx, ref, credential,
			gitlabTeamCatalogTestClient(t, server.URL), TeamCatalogSelections{Projects: true}, syncAt)
		if err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		claim := Claim{Unit: Unit{OrgID: orgID, Provider: "gitlab"}}
		if inspection, err := collector.Sink.InspectEffect(ctx, claim, *batch.Effects.Projects); err != nil || inspection != EffectExact {
			t.Fatalf("step %d: readback %s %v, want exact", step, inspection, err)
		}
		if projectID == "" {
			if err := conn.QueryRow(ctx, `SELECT id FROM projects FINAL WHERE org_id = ? AND provider = 'gitlab'`, orgID).Scan(&projectID); err != nil {
				t.Fatalf("step %d: the project row: %v", step, err)
			}
		}
	}
	at := func(step int) int64 { return start.Add(time.Duration(step) * time.Hour).UnixMilli() }
	ref := func(id string) string { return gitlabMilestoneSprintID("org/team-a/api", id) }
	want := func(step int, expected string) {
		t.Helper()
		if got := storedProjectTarget(t, ctx, conn, orgID, "gitlab", projectID); got != expected {
			t.Fatalf("step %d: target = %s, want %s", step, got, expected)
		}
	}

	sync(1, http.StatusOK, `[{"id":7,"title":"Q4","state":"active","due_date":"2026-12-31"},
		{"id":6,"title":"Q3","state":"active","due_date":"2026-09-30"},{"id":5,"title":"Undated","state":"active","due_date":null}]`)
	want(1, fmt.Sprintf("2026-09-30|stated|milestone|gitlab.milestone.due_date|%s|Q3|1|%d", ref("6"), at(1)))

	sync(2, http.StatusOK, `[{"id":7,"title":"Q4","state":"active","due_date":"2026-12-31"}]`)
	want(2, fmt.Sprintf("2026-12-31|stated|milestone|gitlab.milestone.due_date|%s|Q4|0|%d", ref("7"), at(2)))

	sync(3, http.StatusInternalServerError, ``)
	want(3, fmt.Sprintf("2026-12-31|held|milestone|gitlab.milestone.due_date|%s|Q4|0|%d", ref("7"), at(2)))

	sync(4, http.StatusOK, `[{"id":7,"title":"Q4","state":"closed","due_date":"2026-12-31"}]`)
	want(4, fmt.Sprintf("NULL|none_dated|||||NULL|%d", at(4)))
}

// legacyLinearProjectsWrite is the Linear projects writer as it was before the
// stored-version contract: the row appended as it stands, in the columns that
// existed then.
func legacyLinearProjectsWrite(t *testing.T, ctx context.Context, conn driver.Conn, rows []linearReferenceProjectRow) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, state, state_source, target_date, url, team_ids, team_keys, lead_id, lead_name, lead_email, updated_at, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := batch.Append(row.ID, row.OrgID, row.Provider, row.ProjectKey, row.Name, row.IsActive, row.State, row.StateSource, linearReferenceTargetDate(row.TargetDate), row.URL, row.TeamIDs, row.TeamKeys, row.LeadID, row.LeadName, row.LeadEmail, row.UpdatedAt, row.LastSynced); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

// storedLinearProjects are the columns that existed before the contract, by
// value, one string per row.
func storedLinearProjects(t *testing.T, ctx context.Context, conn driver.Conn, orgID string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString((id, provider, ifNull(project_key, 'NULL'), name, is_active, state, state_source,
		ifNull(toString(target_date), 'NULL'), url, team_ids, team_keys, ifNull(lead_id, 'NULL'), ifNull(lead_name, 'NULL'),
		ifNull(lead_email, 'NULL'), updated_at, last_synced)) FROM projects FINAL WHERE org_id = ? AND provider = 'linear' ORDER BY id`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var stored []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		stored = append(stored, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return stored
}

// The stored-version contract covers Linear's writer as well. Linear states
// every project's target date, a date or none, so the stored rows are the same
// by value, in every column that existed before, as the rows the writer left
// before the contract: the same fixture through both paths, a date, a changed
// date, then a removed date. The new columns say the date is Linear's own.
func TestLinearProjectRowsAreTheSameByValueBeforeAndAfterTheContract(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	sink := LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	now := time.Date(2026, 8, 10, 12, 34, 56, 789000000, time.UTC)
	afterOrg, beforeOrg := uuid.NewString(), uuid.NewString()
	afterClaim := nativeTestClaim("linear", "work-items")
	afterClaim.OrgID = afterOrg

	changed := linearReferenceDate("2026-11-30")
	steps := []struct {
		apply func(row *linearReferenceProjectRow)
		want  string
		// legacy is how the row the old writer left reads: Linear's own date as
		// stated (fetch time NULL: it was not read under the new rule), no date as unknown.
		legacy string
	}{
		{func(*linearReferenceProjectRow) {}, "2026-09-30|stated|project|linear.project.targetDate|||0|%d", "2026-09-30|stated|project|linear.project.targetDate|||NULL|NULL"},
		{func(row *linearReferenceProjectRow) { row.TargetDate = &changed; row.UpdatedAt = now.Add(time.Hour) }, "2026-11-30|stated|project|linear.project.targetDate|||0|%d", "2026-11-30|stated|project|linear.project.targetDate|||NULL|NULL"},
		{func(row *linearReferenceProjectRow) { row.TargetDate = nil; row.UpdatedAt = now.Add(2 * time.Hour) }, "NULL|none_dated|||||NULL|%d", "NULL|unknown|||||NULL|NULL"},
	}
	for index, step := range steps {
		rows := linearReferenceCatalogIntegrationRows(afterClaim, now).Projects
		step.apply(&rows[0])
		effect, err := effectBatchFromValues(linearReferenceCatalogProjectsDestination, EffectReadbackRequired, rows)
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.WriteEffect(ctx, afterClaim, effect); err != nil {
			t.Fatalf("step %d: write: %v", index, err)
		}
		if inspection, err := sink.InspectEffect(ctx, afterClaim, effect); err != nil || inspection != EffectExact {
			t.Fatalf("step %d: readback %s %v", index, inspection, err)
		}

		beforeRows := linearReferenceCatalogIntegrationRows(afterClaim, now).Projects
		step.apply(&beforeRows[0])
		beforeRows[0].OrgID = beforeOrg
		legacyLinearProjectsWrite(t, ctx, conn, beforeRows)

		after, before := storedLinearProjects(t, ctx, conn, afterOrg), storedLinearProjects(t, ctx, conn, beforeOrg)
		if len(after) != 1 || fmt.Sprint(after) != fmt.Sprint(before) {
			t.Fatalf("step %d: stored rows differ.\nafter  the contract: %v\nbefore the contract: %v", index, after, before)
		}
		// Linear's date is its own: no version or milestone behind it, no other
		// entry, and read when the row was written.
		if got, want := storedProjectTarget(t, ctx, conn, afterOrg, "linear", "project-1"), fmt.Sprintf(step.want, now.UnixMilli()); got != want {
			t.Fatalf("step %d: Linear target = %s, want %s", index, got, want)
		}
		if legacy := storedProjectTarget(t, ctx, conn, beforeOrg, "linear", "project-1"); legacy != step.legacy {
			t.Fatalf("step %d: a row written before the contract reads %s, want %s", index, legacy, step.legacy)
		}
	}
}

// The recovery readback of a project row is exact in the target columns too:
// a stored row whose target provenance differs from what the write gave it is
// a conflict, not an exact match, for every writer of projects. A row an older
// writer left (no fetch time) is still accepted for Linear.
func TestProjectReadbackDetectsTargetProvenanceDrift(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	now := time.Date(2026, 8, 10, 12, 34, 56, 789000000, time.UTC)
	tamper := func(orgID, provider string) {
		t.Helper()
		if err := conn.Exec(ctx, `ALTER TABLE projects UPDATE target_source = 'tampered' WHERE org_id = ? AND provider = ? SETTINGS mutations_sync = 2`, orgID, provider); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("linear", func(t *testing.T) {
		sink := LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
		claim := nativeTestClaim("linear", "work-items")
		claim.OrgID = uuid.NewString()
		effect, err := effectBatchFromValues(linearReferenceCatalogProjectsDestination, EffectReadbackRequired, linearReferenceCatalogIntegrationRows(claim, now).Projects)
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatal(err)
		}
		if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
			t.Fatalf("readback of the written row = %s %v, want exact", inspection, err)
		}
		tamper(claim.OrgID, "linear")
		if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectConflict {
			t.Fatalf("readback after the provenance changed = %s %v, want conflict", inspection, err)
		}

		// A row of the writer before the contract carries no target columns: its readback stays exact.
		legacyClaim := claim
		legacyClaim.OrgID = uuid.NewString()
		legacyRows := linearReferenceCatalogIntegrationRows(legacyClaim, now).Projects
		legacyLinearProjectsWrite(t, ctx, conn, legacyRows)
		legacyEffect, err := effectBatchFromValues(linearReferenceCatalogProjectsDestination, EffectReadbackRequired, legacyRows)
		if err != nil {
			t.Fatal(err)
		}
		if inspection, err := sink.InspectEffect(ctx, legacyClaim, legacyEffect); err != nil || inspection != EffectExact {
			t.Fatalf("readback of a row the old writer left = %s %v, want exact", inspection, err)
		}
	})

	t.Run("github ensure row", func(t *testing.T) {
		adapter := GitHubProjectCatalogClickHouseAdapter{Conn: conn}
		orgID := uuid.NewString()
		ensure := projectmembership.EnsureProjectsRow(orgID, "github", "ghprojv2:acme#1", "", "Board", now)
		effect, err := effectBatchFromValues("projects", EffectReadbackRequired, []projectmembership.CatalogRow{ensure})
		if err != nil {
			t.Fatal(err)
		}
		identity := GitHubWorkItemEffectIdentity{OrgID: orgID, Provider: "github", Dataset: "work-items", Destination: "projects", RowCount: len(effect.Rows)}
		if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
			t.Fatal(err)
		}
		if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectExact {
			t.Fatalf("readback of the written row = %s %v, want exact", inspection, err)
		}
		tamper(orgID, "github")
		if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectConflict {
			t.Fatalf("readback after the provenance changed = %s %v, want conflict", inspection, err)
		}
	})
}
