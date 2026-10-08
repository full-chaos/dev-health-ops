//go:build integration

package providersync

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitlabOwnershipFact struct {
	TeamID, Project, Source string
}

type gitlabOwnershipOpenRow struct {
	gitlabOwnershipFact
	ValidFrom time.Time
}

// openGitLabOwnership reads what the readers see: the newest version of each
// row key whose valid_to is empty or in the future.
func openGitLabOwnership(ctx context.Context, t *testing.T, conn driver.Conn, orgID, provider string) []gitlabOwnershipOpenRow {
	t.Helper()
	result, err := conn.Query(ctx,
		`SELECT team_id, project_id, toString(source), valid_from FROM team_project_ownership FINAL `+
			`WHERE org_id = ? AND provider = ? AND (valid_to IS NULL OR valid_to > now64(3, 'UTC'))`,
		orgID, provider)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	var out []gitlabOwnershipOpenRow
	for result.Next() {
		var row gitlabOwnershipOpenRow
		if err := result.Scan(&row.TeamID, &row.Project, &row.Source, &row.ValidFrom); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := result.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TeamID+out[i].Project+out[i].Source < out[j].TeamID+out[j].Project+out[j].Source
	})
	return out
}

func requireGitLabFacts(t *testing.T, label string, got []gitlabOwnershipOpenRow, want ...gitlabOwnershipFact) {
	t.Helper()
	sort.Slice(want, func(i, j int) bool {
		return want[i].TeamID+want[i].Project+want[i].Source < want[j].TeamID+want[j].Project+want[j].Source
	})
	facts := make([]gitlabOwnershipFact, len(got))
	for index := range got {
		facts[index] = got[index].gitlabOwnershipFact
	}
	if fmt.Sprint(facts) != fmt.Sprint(want) {
		t.Fatalf("%s: open rows = %+v, want %+v", label, facts, want)
	}
}

func countGitLabOwnershipRows(ctx context.Context, t *testing.T, conn driver.Conn, orgID string) uint64 {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM team_project_ownership FINAL WHERE org_id = ?`, orgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// gitlabSnapshotServer is a GitLab group tree the test edits between runs.
type gitlabSnapshotServer struct {
	*httptest.Server
	mu          sync.Mutex
	subgroups   []string            // full paths below "org"
	projects    map[string][]string // group full path -> project paths
	failGroup   string              // group whose /projects answers 500
	fullPageFor string              // group whose /projects never ends (page cap)
}

func newGitLabSnapshotServer(t *testing.T) *gitlabSnapshotServer {
	t.Helper()
	fake := &gitlabSnapshotServer{projects: map[string][]string{}}
	group := func(id int, fullPath string) map[string]any {
		return map[string]any{"id": id, "full_path": fullPath, "name": fullPath, "description": nil}
	}
	projectPayload := func(path string) map[string]any {
		return map[string]any{"id": len(path) + 1000, "path_with_namespace": path, "name": path, "archived": false,
			"web_url": "https://gitlab.example.com/" + path}
	}
	fake.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		path := r.URL.EscapedPath()
		switch {
		case path == "/api/v4/groups/org":
			writeGitLabTeamCatalogJSON(t, w, group(1, "org"))
		case path == "/api/v4/groups/org/subgroups":
			out := []map[string]any{}
			for index, sub := range fake.subgroups {
				out = append(out, group(10+index, sub))
			}
			writeGitLabTeamCatalogJSON(t, w, out)
		case strings.HasSuffix(path, "/projects"):
			groupPath := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(path, "/api/v4/groups/"), "/projects"), "%2F", "/")
			if r.URL.Query().Get("include_subgroups") == "true" {
				out := []map[string]any{}
				for _, list := range fake.projects {
					for _, project := range list {
						out = append(out, projectPayload(project))
					}
				}
				writeGitLabTeamCatalogJSON(t, w, out)
				return
			}
			if groupPath == fake.failGroup {
				http.Error(w, "simulated projects fetch failure", http.StatusInternalServerError)
				return
			}
			out := []map[string]any{}
			if groupPath == fake.fullPageFor {
				for index := 0; index < 100; index++ {
					out = append(out, projectPayload(fmt.Sprintf("%s/filler-%d", groupPath, index)))
				}
			} else {
				for _, project := range fake.projects[groupPath] {
					out = append(out, projectPayload(project))
				}
			}
			writeGitLabTeamCatalogJSON(t, w, out)
		case strings.HasSuffix(path, "/members"):
			writeGitLabTeamCatalogJSON(t, w, []map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)
	return fake
}

func (fake *gitlabSnapshotServer) set(failGroup, fullPageFor string, subgroups []string, projects map[string][]string) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.failGroup, fake.fullPageFor, fake.subgroups, fake.projects = failGroup, fullPageFor, subgroups, projects
}

func gitlabSnapshotRun(
	ctx context.Context, t *testing.T, conn driver.Conn, orgID string, fake *gitlabSnapshotServer,
	selections TeamCatalogSelections, strict bool, at time.Time,
) (TeamCatalogResult, error) {
	t.Helper()
	collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}}
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	ref := TeamCatalogReference{OrgID: orgID, SyncRunID: "run", Strict: strict}
	return collector.CollectTeamCatalog(ctx, ref, credential, gitlabTeamCatalogTestClient(t, fake.URL), selections, at)
}

func TestGitLabTeamCatalogClosesProviderAccessRowsGitLabNoLongerReturns(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	t0 := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	projectsOnly := TeamCatalogSelections{Projects: true}
	rootSvc := gitlabOwnershipFact{TeamID: "gl:org", Project: "org/root-svc", Source: "provider_access"}
	rootOld := gitlabOwnershipFact{TeamID: "gl:org", Project: "org/old-svc", Source: "provider_access"}
	teamAApi := gitlabOwnershipFact{TeamID: "gl:org/team-a", Project: "org/team-a/api", Source: "provider_access"}
	teamAWeb := gitlabOwnershipFact{TeamID: "gl:org/team-a", Project: "org/team-a/web", Source: "provider_access"}
	seedTree := func(fake *gitlabSnapshotServer) {
		fake.set("", "", []string{"org/team-a"}, map[string][]string{
			"org":        {"org/root-svc", "org/old-svc"},
			"org/team-a": {"org/team-a/api", "org/team-a/web"},
		})
	}

	t.Run("a project dropped from the listing is closed and the rest stay open", func(t *testing.T) {
		org, fake := "gl-snap-dropped", newGitLabSnapshotServer(t)
		seedTree(fake)
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
			t.Fatal(err)
		}
		requireGitLabFacts(t, "after seed", openGitLabOwnership(ctx, t, conn, org, "gitlab"), rootSvc, rootOld, teamAApi, teamAWeb)
		fake.set("", "", []string{"org/team-a"}, map[string][]string{
			"org":        {"org/root-svc"},
			"org/team-a": {"org/team-a/api", "org/team-a/web"},
		})
		result, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if result.OwnershipWritten != 3 {
			t.Fatalf("OwnershipWritten = %d, want the 3 grants GitLab returned", result.OwnershipWritten)
		}
		requireGitLabFacts(t, "after drop", openGitLabOwnership(ctx, t, conn, org, "gitlab"), rootSvc, teamAApi, teamAWeb)
	})

	t.Run("a project held by a group and its subgroup is closed only where it was dropped", func(t *testing.T) {
		org, fake := "gl-snap-two-holders", newGitLabSnapshotServer(t)
		fake.set("", "", []string{"org/team-a"}, map[string][]string{
			"org": {"org/shared"}, "org/team-a": {"org/shared"},
		})
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
			t.Fatal(err)
		}
		fake.set("", "", []string{"org/team-a"}, map[string][]string{"org": {"org/shared"}, "org/team-a": {}})
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		requireGitLabFacts(t, "after drop in the subgroup", openGitLabOwnership(ctx, t, conn, org, "gitlab"),
			gitlabOwnershipFact{TeamID: "gl:org", Project: "org/shared", Source: "provider_access"})
	})

	t.Run("a listed group with no project at all has its rows closed", func(t *testing.T) {
		org, fake := "gl-snap-empty-group", newGitLabSnapshotServer(t)
		seedTree(fake)
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
			t.Fatal(err)
		}
		fake.set("", "", []string{"org/team-a"}, map[string][]string{"org": {"org/root-svc", "org/old-svc"}, "org/team-a": {}})
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		requireGitLabFacts(t, "after empty listing", openGitLabOwnership(ctx, t, conn, org, "gitlab"), rootSvc, rootOld)
	})

	t.Run("a group GitLab no longer lists is not listed, so its rows stay open", func(t *testing.T) {
		org, fake := "gl-snap-unlisted-group", newGitLabSnapshotServer(t)
		seedTree(fake)
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
			t.Fatal(err)
		}
		fake.set("", "", nil, map[string][]string{"org": {"org/root-svc", "org/old-svc"}})
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		requireGitLabFacts(t, "after the subgroup vanished", openGitLabOwnership(ctx, t, conn, org, "gitlab"),
			rootSvc, rootOld, teamAApi, teamAWeb)
	})

	t.Run("a failed project listing closes nothing, under non-strict and strict", func(t *testing.T) {
		org, fake := "gl-snap-failed-listing", newGitLabSnapshotServer(t)
		seedTree(fake)
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
			t.Fatal(err)
		}
		rows := countGitLabOwnershipRows(ctx, t, conn, org)
		fake.set("org/team-a", "", []string{"org/team-a"}, map[string][]string{"org": {"org/root-svc"}, "org/team-a": {}})
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour)); err != nil {
			t.Fatalf("non-strict skips the walk and must not error: %v", err)
		}
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, true, t0.Add(2*time.Hour)); err == nil {
			t.Fatal("strict run with a failed listing returned no error")
		}
		requireGitLabFacts(t, "after failed listings", openGitLabOwnership(ctx, t, conn, org, "gitlab"), rootSvc, rootOld, teamAApi, teamAWeb)
		if got := countGitLabOwnershipRows(ctx, t, conn, org); got != rows {
			t.Fatalf("rows = %d, want %d: a failed listing wrote something", got, rows)
		}
	})

	t.Run("a listing that hit the page cap closes nothing", func(t *testing.T) {
		org, fake := "gl-snap-capped", newGitLabSnapshotServer(t)
		seedTree(fake)
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
			t.Fatal(err)
		}
		rows := countGitLabOwnershipRows(ctx, t, conn, org)
		fake.set("", "org/team-a", []string{"org/team-a"}, map[string][]string{"org": {"org/root-svc"}})
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour)); !errors.Is(err, ErrPaginationCapExceeded) {
			t.Fatalf("err = %v, want ErrPaginationCapExceeded", err)
		}
		requireGitLabFacts(t, "after capped listing", openGitLabOwnership(ctx, t, conn, org, "gitlab"), rootSvc, rootOld, teamAApi, teamAWeb)
		if got := countGitLabOwnershipRows(ctx, t, conn, org); got != rows {
			t.Fatalf("rows = %d, want %d: a capped listing wrote something", got, rows)
		}
	})

	t.Run("a failed read of the open rows fails the run and writes nothing", func(t *testing.T) {
		org, fake := "gl-snap-read-failed", newGitLabSnapshotServer(t)
		seedTree(fake)
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
			t.Fatal(err)
		}
		rows := countGitLabOwnershipRows(ctx, t, conn, org)
		fake.set("", "", []string{"org/team-a"}, map[string][]string{"org": {"org/root-svc"}, "org/team-a": {}})
		_, err := gitlabSnapshotRun(ctx, t, openOwnershipReadFailingConn{conn}, org, fake, projectsOnly, false, t0.Add(time.Hour))
		if err == nil {
			t.Fatal("run with a failed open-row read returned no error")
		}
		requireGitLabFacts(t, "after failed read", openGitLabOwnership(ctx, t, conn, org, "gitlab"), rootSvc, rootOld, teamAApi, teamAWeb)
		if got := countGitLabOwnershipRows(ctx, t, conn, org); got != rows {
			t.Fatalf("rows = %d, want %d: a failed read still wrote", got, rows)
		}
	})

	t.Run("a run without projects selected closes nothing", func(t *testing.T) {
		org, fake := "gl-snap-no-projects", newGitLabSnapshotServer(t)
		seedTree(fake)
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
			t.Fatal(err)
		}
		fake.set("", "", []string{"org/team-a"}, map[string][]string{"org": {}, "org/team-a": {}})
		for name, selections := range map[string]TeamCatalogSelections{
			"teams only": {Teams: true}, "members only": {Members: true}, "teams and members": {Teams: true, Members: true},
		} {
			if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, selections, false, t0.Add(time.Hour)); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		}
		requireGitLabFacts(t, "after runs without projects", openGitLabOwnership(ctx, t, conn, org, "gitlab"), rootSvc, rootOld, teamAApi, teamAWeb)
	})

	t.Run("another org, source or provider is untouched", func(t *testing.T) {
		org, other, fake := "gl-snap-scope", "gl-snap-scope-other", newGitLabSnapshotServer(t)
		seedTree(fake)
		sink := GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
		older := t0.Add(-48 * time.Hour)
		// Same team id and project id in every row below: only org, source or
		// provider differ, each planted with an older valid_from than the run.
		otherOrg := normalizeGitLabOwnershipRow(other, "gl:org", "org/old-svc", gitlabTeamCatalogBaseSpecificity, older)
		otherSource := normalizeGitLabOwnershipRow(org, "gl:org", "org/old-svc", gitlabTeamCatalogBaseSpecificity, older)
		otherSource.Source = "manual"
		if err := sink.writeOwnership(ctx, []gitlabTeamCatalogOwnershipRow{otherOrg, otherSource}); err != nil {
			t.Fatal(err)
		}
		jiraRow := normalizeJiraOwnershipRow(org, "gl:org", "org/old-svc", "OLD", older)
		if err := (JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: sink.Lease}).writeOwnership(ctx, []jiraTeamCatalogOwnershipRow{jiraRow}); err != nil {
			t.Fatal(err)
		}
		linearKey := "org/old-svc"
		linearRow := linearReferenceOwnershipRow{
			OrgID: org, Provider: "linear", TeamID: "gl:org", ProjectID: "org/old-svc", ProjectKey: &linearKey,
			Source: "provider_access", Specificity: 1, Priority: 1, ValidFrom: older, UpdatedAt: older,
		}
		if err := (LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: sink.Lease}).writeOwnership(ctx, []linearReferenceOwnershipRow{linearRow}); err != nil {
			t.Fatal(err)
		}
		githubRow, err := normalizeGitHubTeamRepoOwnership(org, "org", "org/old-svc", older)
		if err != nil {
			t.Fatal(err)
		}
		if err := (GitHubTeamCatalogClickHouseEffects{Conn: conn}).WriteTeamRepoOwnership(ctx, org, []githubTeamRepoOwnershipRow{githubRow}); err != nil {
			t.Fatal(err)
		}

		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
			t.Fatal(err)
		}
		fake.set("", "", []string{"org/team-a"}, map[string][]string{"org": {"org/root-svc"}, "org/team-a": {}})
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		requireGitLabFacts(t, "gitlab in the run's org", openGitLabOwnership(ctx, t, conn, org, "gitlab"),
			rootSvc, gitlabOwnershipFact{TeamID: "gl:org", Project: "org/old-svc", Source: "manual"})
		requireGitLabFacts(t, "gitlab in another org", openGitLabOwnership(ctx, t, conn, other, "gitlab"),
			gitlabOwnershipFact{TeamID: "gl:org", Project: "org/old-svc", Source: "provider_access"})
		requireGitLabFacts(t, "jira", openGitLabOwnership(ctx, t, conn, org, "jira"),
			gitlabOwnershipFact{TeamID: "gl:org", Project: "org/old-svc", Source: jiraTeamCatalogSource})
		requireGitLabFacts(t, "linear", openGitLabOwnership(ctx, t, conn, org, "linear"),
			gitlabOwnershipFact{TeamID: "gl:org", Project: "org/old-svc", Source: "provider_access"})
		var githubOpen uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND provider = 'github' `+
			`AND (valid_to IS NULL OR valid_to > now64(3, 'UTC'))`, org).Scan(&githubOpen); err != nil {
			t.Fatal(err)
		}
		if githubOpen != 1 {
			t.Fatalf("open github repo rows = %d, want the 1 planted", githubOpen)
		}
	})

	t.Run("a repeat run adds no row and keeps valid_from; a closed row is not read as open", func(t *testing.T) {
		org, fake := "gl-snap-repeat", newGitLabSnapshotServer(t)
		seedTree(fake)
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
			t.Fatal(err)
		}
		rows := countGitLabOwnershipRows(ctx, t, conn, org)
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if got := countGitLabOwnershipRows(ctx, t, conn, org); got != rows {
			t.Fatalf("rows after a repeat run = %d, want %d", got, rows)
		}
		for _, row := range openGitLabOwnership(ctx, t, conn, org, "gitlab") {
			if !row.ValidFrom.Equal(t0) {
				t.Fatalf("%+v: valid_from moved from %v", row.gitlabOwnershipFact, t0)
			}
		}
		// Drop, repeat the drop (the closed row keeps its valid_to), then re-grant.
		fake.set("", "", []string{"org/team-a"}, map[string][]string{"org": {"org/root-svc"}, "org/team-a": {"org/team-a/api", "org/team-a/web"}})
		for _, at := range []time.Time{t0.Add(2 * time.Hour), t0.Add(3 * time.Hour)} {
			if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, at); err != nil {
				t.Fatal(err)
			}
		}
		requireGitLabFacts(t, "after drop twice", openGitLabOwnership(ctx, t, conn, org, "gitlab"), rootSvc, teamAApi, teamAWeb)
		seedTree(fake)
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(4*time.Hour)); err != nil {
			t.Fatal(err)
		}
		requireGitLabFacts(t, "after re-grant", openGitLabOwnership(ctx, t, conn, org, "gitlab"), rootSvc, rootOld, teamAApi, teamAWeb)
	})

	t.Run("an older open duplicate of a held grant is closed", func(t *testing.T) {
		org, fake := "gl-snap-duplicate", newGitLabSnapshotServer(t)
		fake.set("", "", nil, map[string][]string{"org": {"org/root-svc"}})
		sink := GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
		// The writer before this change stamped each run's own valid_from.
		for _, at := range []time.Time{t0, t0.Add(time.Hour)} {
			row := normalizeGitLabOwnershipRow(org, "gl:org", "org/root-svc", gitlabTeamCatalogBaseSpecificity, at)
			if err := sink.writeOwnership(ctx, []gitlabTeamCatalogOwnershipRow{row}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(2*time.Hour)); err != nil {
			t.Fatal(err)
		}
		open := openGitLabOwnership(ctx, t, conn, org, "gitlab")
		requireGitLabFacts(t, "after dedupe", open, rootSvc)
		if !open[0].ValidFrom.Equal(t0) {
			t.Fatalf("kept valid_from = %v, want the earliest %v", open[0].ValidFrom, t0)
		}
	})
}

// openOwnershipReadFailingConn fails only the read of open
// team_project_ownership rows (the snapshot's read); every write and every
// other read goes to the real connection.
type openOwnershipReadFailingConn struct {
	driver.Conn
}

func (conn openOwnershipReadFailingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, "FROM team_project_ownership FINAL") && strings.Contains(query, "valid_to > now64") {
		return nil, errors.New("injected team_project_ownership read failure")
	}
	return conn.Conn.Query(ctx, query, args...)
}
