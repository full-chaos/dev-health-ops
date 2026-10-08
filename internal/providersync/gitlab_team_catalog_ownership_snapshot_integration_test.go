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
	// nextPage, when set for a group, replaces GitLab's end signal (an empty
	// X-Next-Page) on that group's first /projects page: "absent" sends no
	// header, any other value is sent as is. page2 is what page 2 returns.
	nextPage map[string]string
	page2    map[string][]string
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
				writeGitLabTeamCatalogJSON(t, w, out)
				return
			}
			listed := fake.projects[groupPath]
			if page := r.URL.Query().Get("page"); page != "" && page != "1" {
				listed = fake.page2[groupPath]
			} else if next, ok := fake.nextPage[groupPath]; ok {
				if next != "absent" {
					w.Header().Set("X-Next-Page", next)
				}
				for _, project := range listed {
					out = append(out, projectPayload(project))
				}
				writeGitLabTeamCatalogJSON(t, w, out)
				return
			}
			for _, project := range listed {
				out = append(out, projectPayload(project))
			}
			// GitLab's end of an offset listing: X-Next-Page sent and empty.
			w.Header()["X-Next-Page"] = []string{""}
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
	return gitlabSnapshotRunAs(ctx, t, conn, orgID, fake, selections, strict, at, "integration-a", staticScopeCensus{})
}

// gitlabSnapshotRunAs runs one collection as the given integration, with the
// given census of the org's other active GitLab integrations.
func gitlabSnapshotRunAs(
	ctx context.Context, t *testing.T, conn driver.Conn, orgID string, fake *gitlabSnapshotServer,
	selections TeamCatalogSelections, strict bool, at time.Time, integrationID string, census OwnershipScopeCensus,
) (TeamCatalogResult, error) {
	t.Helper()
	collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}, ScopeCensus: census}
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	ref := TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: integrationID, Strict: strict}
	return collector.CollectTeamCatalog(ctx, ref, credential, gitlabTeamCatalogTestClient(t, fake.URL), selections, at)
}

func (fake *gitlabSnapshotServer) setPaging(nextPage map[string]string, page2 map[string][]string) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.nextPage, fake.page2 = nextPage, page2
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

	late := gitlabOwnershipFact{TeamID: "gl:org", Project: "org/late", Source: "provider_access"}
	for _, test := range []struct{ name, nextPage string }{
		{"a malformed X-Next-Page", "not-a-page"},
		{"a non-positive X-Next-Page", "0"},
		{"no X-Next-Page at all (an end inferred from a short page)", "absent"},
	} {
		t.Run(test.name+" leaves the listing unproven: nothing closes and the run says so", func(t *testing.T) {
			org, fake := "gl-snap-unproven-"+strings.ReplaceAll(test.nextPage, "-", ""), newGitLabSnapshotServer(t)
			fake.set("", "", nil, map[string][]string{"org": {"org/root-svc", "org/late"}})
			if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
				t.Fatal(err)
			}
			// Page 1 stops short of org/late, which GitLab still lists on page 2.
			fake.set("", "", nil, map[string][]string{"org": {"org/root-svc"}})
			fake.setPaging(map[string]string{"org": test.nextPage}, map[string][]string{"org": {"org/late"}})
			result, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			requireGitLabFacts(t, "after the unproven listing", openGitLabOwnership(ctx, t, conn, org, "gitlab"), rootSvc, late)
			if got := closeLegReasons(result.DegradedLegs); len(got) != 1 || got[0] != OwnershipCloseSkippedListingIncomplete {
				t.Fatalf("degraded close legs = %v, want [%s]", got, OwnershipCloseSkippedListingIncomplete)
			}
		})
	}

	t.Run("an unproven subgroup listing keeps only that subgroup's rows open; the proven root still closes", func(t *testing.T) {
		org, fake := "gl-snap-partly-proven", newGitLabSnapshotServer(t)
		seedTree(fake)
		if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
			t.Fatal(err)
		}
		fake.set("", "", []string{"org/team-a"}, map[string][]string{"org": {"org/root-svc"}, "org/team-a": {"org/team-a/api"}})
		fake.setPaging(map[string]string{"org/team-a": "not-a-page"}, map[string][]string{"org/team-a": {"org/team-a/web"}})
		result, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		requireGitLabFacts(t, "after the partly proven run", openGitLabOwnership(ctx, t, conn, org, "gitlab"), rootSvc, teamAApi, teamAWeb)
		if got := closeLegReasons(result.DegradedLegs); len(got) != 1 || got[0] != OwnershipCloseSkippedListingIncomplete {
			t.Fatalf("degraded close legs = %v, want [%s]", got, OwnershipCloseSkippedListingIncomplete)
		}
	})

	t.Run("two active integrations of the org on the same group path never close each other's rows", func(t *testing.T) {
		org := "gl-snap-two-integrations"
		a, b := newGitLabSnapshotServer(t), newGitLabSnapshotServer(t)
		a.set("", "", nil, map[string][]string{"org": {"org/kept"}})
		b.set("", "", nil, map[string][]string{"org": {}})
		// Each census names the other integration on the same group path, as the
		// org's integrations table would.
		censusOfA := staticScopeCensus{siblings: []OwnershipSiblingIntegration{{IntegrationID: "integration-b", SyncOptions: map[string]any{"group_path": "org"}}}}
		censusOfB := staticScopeCensus{siblings: []OwnershipSiblingIntegration{{IntegrationID: "integration-a", SyncOptions: map[string]any{"group_path": "org"}}}}
		if _, err := gitlabSnapshotRunAs(ctx, t, conn, org, a, projectsOnly, false, t0, "integration-a", censusOfA); err != nil {
			t.Fatal(err)
		}
		result, err := gitlabSnapshotRunAs(ctx, t, conn, org, b, projectsOnly, false, t0.Add(time.Hour), "integration-b", censusOfB)
		if err != nil {
			t.Fatal(err)
		}
		kept := gitlabOwnershipFact{TeamID: "gl:org", Project: "org/kept", Source: "provider_access"}
		requireGitLabFacts(t, "after integration B listed nothing", openGitLabOwnership(ctx, t, conn, org, "gitlab"), kept)
		if got := closeLegReasons(result.DegradedLegs); len(got) != 1 || got[0] != OwnershipCloseSkippedScopeShared {
			t.Fatalf("degraded close legs = %v, want [%s]", got, OwnershipCloseSkippedScopeShared)
		}
		// A's own repeat run keeps its first valid_from: no new row per run.
		if _, err := gitlabSnapshotRunAs(ctx, t, conn, org, a, projectsOnly, false, t0.Add(2*time.Hour), "integration-a", censusOfA); err != nil {
			t.Fatal(err)
		}
		open := openGitLabOwnership(ctx, t, conn, org, "gitlab")
		requireGitLabFacts(t, "after A again", open, kept)
		if !open[0].ValidFrom.Equal(t0) {
			t.Fatalf("valid_from = %v, want the first-seen %v", open[0].ValidFrom, t0)
		}
	})

	t.Run("an active integration on another group path does not stop the close", func(t *testing.T) {
		org, fake := "gl-snap-other-path", newGitLabSnapshotServer(t)
		seedTree(fake)
		census := staticScopeCensus{siblings: []OwnershipSiblingIntegration{{IntegrationID: "integration-b", SyncOptions: map[string]any{"group_path": "other-org"}}}}
		if _, err := gitlabSnapshotRunAs(ctx, t, conn, org, fake, projectsOnly, false, t0, "integration-a", census); err != nil {
			t.Fatal(err)
		}
		fake.set("", "", []string{"org/team-a"}, map[string][]string{"org": {"org/root-svc"}, "org/team-a": {"org/team-a/api", "org/team-a/web"}})
		result, err := gitlabSnapshotRunAs(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour), "integration-a", census)
		if err != nil {
			t.Fatal(err)
		}
		requireGitLabFacts(t, "after drop", openGitLabOwnership(ctx, t, conn, org, "gitlab"), rootSvc, teamAApi, teamAWeb)
		if len(result.DegradedLegs) != 0 {
			t.Fatalf("degraded legs = %+v, want none", result.DegradedLegs)
		}
	})

	for _, test := range []struct {
		name, integrationID, reason string
		census                      OwnershipScopeCensus
	}{
		{"a sibling whose group path is not known", "integration-a", OwnershipCloseSkippedScopeShared,
			staticScopeCensus{siblings: []OwnershipSiblingIntegration{{IntegrationID: "integration-b"}}}},
		{"a failed census read", "integration-a", OwnershipCloseSkippedCensusFailed,
			staticScopeCensus{err: errors.New("integrations read failed")}},
		{"no census", "integration-a", OwnershipCloseSkippedCensusUnavailable, nil},
		{"a run without an integration", "", OwnershipCloseSkippedCensusUnavailable, staticScopeCensus{}},
	} {
		t.Run(test.name+" closes nothing and the run says so", func(t *testing.T) {
			org, fake := "gl-snap-census-"+strings.ReplaceAll(test.reason, "_", "")+test.integrationID, newGitLabSnapshotServer(t)
			seedTree(fake)
			if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
				t.Fatal(err)
			}
			fake.set("", "", []string{"org/team-a"}, map[string][]string{"org": {}, "org/team-a": {}})
			result, err := gitlabSnapshotRunAs(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour), test.integrationID, test.census)
			if err != nil {
				t.Fatal(err)
			}
			requireGitLabFacts(t, "after the empty listing", openGitLabOwnership(ctx, t, conn, org, "gitlab"), rootSvc, rootOld, teamAApi, teamAWeb)
			if got := closeLegReasons(result.DegradedLegs); len(got) != 1 || got[0] != test.reason {
				t.Fatalf("degraded close legs = %v, want [%s]", got, test.reason)
			}
		})
	}
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
