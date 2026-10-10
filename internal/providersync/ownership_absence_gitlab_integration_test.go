//go:build integration

package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// A group's project listing is read by page number (page, per_page,
// X-Next-Page). When a project leaves the group between two page requests,
// every later project moves one place up and one project that is still in the
// group is on no page, while the listing ends with GitLab's own end signal. So
// the end of a listing of more than one response does not prove that a grant
// is gone: the run asks the same endpoint for that one project
// (GET /groups/:id/projects?search=<name>) and closes the row only when that
// one response does not hold it.
//
// The real GitLab team collector and the real ClickHouse writer; the provider
// is a scripted answer in GitLab's shape.

// gitlabGrantServer is one GitLab group and its projects.
type gitlabGrantServer struct {
	*httptest.Server
	mu       sync.Mutex
	projects []string // project names of the group "org"
	// afterPageOne runs once, directly after page 1 of the listing is built.
	afterPageOne func(projects []string) []string
	// searchStatus, when not 0, is the status every search for one project answers.
	searchStatus int
	searches     int
}

func newGitLabGrantServer(t *testing.T, projects []string) *gitlabGrantServer {
	t.Helper()
	fake := &gitlabGrantServer{projects: projects}
	payload := func(name string) map[string]any {
		return map[string]any{"id": 1000 + len(name), "path_with_namespace": "org/" + name, "name": name, "archived": false,
			"web_url": "https://gitlab.example.com/org/" + name}
	}
	fake.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		switch path := r.URL.EscapedPath(); path {
		case "/api/v4/groups/org":
			writeGitLabTeamCatalogJSON(t, w, map[string]any{"id": 1, "full_path": "org", "name": "org", "description": nil})
		case "/api/v4/groups/org/subgroups", "/api/v4/groups/org/members":
			writeGitLabTeamCatalogJSON(t, w, []map[string]any{})
		case "/api/v4/groups/org/projects":
			query := r.URL.Query()
			if search := query.Get("search"); search != "" {
				fake.searches++
				if fake.searchStatus != 0 {
					http.Error(w, "simulated failure", fake.searchStatus)
					return
				}
				out := []map[string]any{}
				for _, name := range fake.projects {
					if strings.Contains(name, search) {
						out = append(out, payload(name))
					}
				}
				w.Header()["X-Next-Page"] = []string{""}
				writeGitLabTeamCatalogJSON(t, w, out)
				return
			}
			page, _ := strconv.Atoi(query.Get("page"))
			perPage, _ := strconv.Atoi(query.Get("per_page"))
			if page < 1 {
				page = 1
			}
			if perPage < 1 {
				perPage = 20
			}
			start, end := (page-1)*perPage, page*perPage
			if start > len(fake.projects) {
				start = len(fake.projects)
			}
			if end >= len(fake.projects) {
				end = len(fake.projects)
				// GitLab's end of an offset listing: X-Next-Page sent and empty.
				w.Header()["X-Next-Page"] = []string{""}
			} else {
				w.Header().Set("X-Next-Page", strconv.Itoa(page+1))
			}
			out := make([]map[string]any, 0, end-start)
			for _, name := range fake.projects[start:end] {
				out = append(out, payload(name))
			}
			if page == 1 && fake.afterPageOne != nil {
				fake.projects = fake.afterPageOne(fake.projects)
				fake.afterPageOne = nil
			}
			writeGitLabTeamCatalogJSON(t, w, out)
		default:
			t.Errorf("unexpected GitLab request %s", r.URL)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)
	return fake
}

func (fake *gitlabGrantServer) set(mutate func(fake *gitlabGrantServer)) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	mutate(fake)
}

func gitlabGrantProjects(count int) []string {
	projects := make([]string, 0, count)
	for index := 0; index < count; index++ {
		projects = append(projects, fmt.Sprintf("project-%03d", index))
	}
	return projects
}

// gitlabGrantRun is one sync of the group: the result, the searches for one
// project it made, and the WARN lines of the absence rule.
func gitlabGrantRun(
	ctx context.Context, t *testing.T, conn driver.Conn, orgID string, fake *gitlabGrantServer, at time.Time,
) (result TeamCatalogResult, searches int, warnings []map[string]any) {
	t.Helper()
	fake.set(func(fake *gitlabGrantServer) { fake.searches = 0 })
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}, ScopeCensus: staticScopeCensus{}}
	result, err := collector.CollectTeamCatalog(ctx,
		TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
		providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}},
		gitlabTeamCatalogTestClient(t, fake.URL), TeamCatalogSelections{Projects: true}, at)
	slog.SetDefault(previous)
	if err != nil {
		t.Fatalf("sync at %s: %v", at.Format(time.RFC3339), err)
	}
	for _, raw := range strings.Split(logged.String(), "\n") {
		var entry map[string]any
		if json.Unmarshal([]byte(raw), &entry) == nil && entry["msg"] == SnapshotAbsenceNotProvenLog {
			warnings = append(warnings, entry)
		}
	}
	fake.set(func(fake *gitlabGrantServer) { searches = fake.searches })
	return result, searches, warnings
}

// gitlabGrantOpen is the open grants of the group, by project path, with
// their valid_from.
func gitlabGrantOpen(ctx context.Context, t *testing.T, conn driver.Conn, orgID string) map[string]time.Time {
	t.Helper()
	open := map[string]time.Time{}
	for _, row := range openGitLabOwnership(ctx, t, conn, orgID, "gitlab") {
		open[row.Project] = row.ValidFrom.UTC()
	}
	return open
}

func TestGitLabGroupProjectGrantIsNotClosedByAListingThatChangedWhileItWasRead(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	t0 := time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC)
	const pages = 3 * gitlabTeamCatalogListPerPage / 2 // one page and a half

	// project-000 leaves the group after page 1 is read. The project at the
	// page boundary is on no page and is STILL in the group: its row stays
	// open on its first valid_from.
	t.Run("a project leaves the group between two pages", func(t *testing.T) {
		const orgID = "org-gl-grant-page-shift"
		fake := newGitLabGrantServer(t, gitlabGrantProjects(pages))
		boundary := fmt.Sprintf("org/project-%03d", gitlabTeamCatalogListPerPage)
		if _, searches, _ := gitlabGrantRun(ctx, t, conn, orgID, fake, t0); searches != 0 || len(gitlabGrantOpen(ctx, t, conn, orgID)) != pages {
			t.Fatalf("the first sync made %d search(es) and holds %d open grants, want 0 and %d: the case is not set",
				searches, len(gitlabGrantOpen(ctx, t, conn, orgID)), pages)
		}
		fake.set(func(fake *gitlabGrantServer) {
			fake.afterPageOne = func(projects []string) []string { return withoutRepo(projects, "project-000") }
		})
		result, searches, warnings := gitlabGrantRun(ctx, t, conn, orgID, fake, t0.Add(time.Hour))
		open := gitlabGrantOpen(ctx, t, conn, orgID)
		if from, isOpen := open[boundary]; !isOpen || !from.Equal(t0) {
			t.Errorf("%s is still in the group and was on no page: open %v valid_from %s, want an open row from %s",
				boundary, isOpen, from.Format(time.RFC3339), t0.Format(time.RFC3339))
		}
		if searches != 1 {
			t.Errorf("the sync made %d search(es) for one project, want 1 (the one grant the listing did not hold)", searches)
		}
		if len(warnings) != 1 || warnings[0]["kind"] != "gitlab_group_project_grants" || warnings[0]["rows_still_held"] != float64(1) ||
			warnings[0]["rows_closed"] != float64(0) {
			t.Errorf("the WARN line of the absence rule is %v, want one line for gitlab_group_project_grants with 1 row still held and 0 closed", warnings)
		}
		if reasons := absenceLegReasons(result); len(reasons) != 1 || reasons[0] != OwnershipAbsenceListingWrong {
			t.Errorf("the result names the absence legs %v, want [%s]", reasons, OwnershipAbsenceListingWrong)
		}
		// The next clean sync closes the project that did leave.
		gitlabGrantRun(ctx, t, conn, orgID, fake, t0.Add(2*time.Hour))
		open = gitlabGrantOpen(ctx, t, conn, orgID)
		if _, isOpen := open["org/project-000"]; isOpen {
			t.Errorf("org/project-000 left the group and its row is still open after a clean sync")
		}
		if from, isOpen := open[boundary]; !isOpen || !from.Equal(t0) {
			t.Errorf("%s after the clean sync: open %v valid_from %s, want the open row from %s", boundary, isOpen, from.Format(time.RFC3339), t0.Format(time.RFC3339))
		}
	})

	// A project that left a group of more than one page: closed in the same
	// sync, on GitLab's answer for that project.
	t.Run("a project left a group of more than one page", func(t *testing.T) {
		const orgID = "org-gl-grant-paged-removal"
		fake := newGitLabGrantServer(t, gitlabGrantProjects(pages))
		gitlabGrantRun(ctx, t, conn, orgID, fake, t0)
		fake.set(func(fake *gitlabGrantServer) { fake.projects = withoutRepo(fake.projects, "project-007") })
		result, searches, warnings := gitlabGrantRun(ctx, t, conn, orgID, fake, t0.Add(time.Hour))
		if _, isOpen := gitlabGrantOpen(ctx, t, conn, orgID)["org/project-007"]; isOpen {
			t.Errorf("org/project-007 left the group and GitLab says so: its row is still open")
		}
		if searches != 1 || len(warnings) != 0 || len(absenceLegReasons(result)) != 0 {
			t.Errorf("the sync made %d search(es), wrote %d WARN line(s) and the legs %v; want 1, none and none", searches, len(warnings), absenceLegReasons(result))
		}
	})

	// A listing of one response cannot move: no search is made.
	t.Run("a project left a group of one page", func(t *testing.T) {
		const orgID = "org-gl-grant-one-page"
		fake := newGitLabGrantServer(t, gitlabGrantProjects(3))
		gitlabGrantRun(ctx, t, conn, orgID, fake, t0)
		fake.set(func(fake *gitlabGrantServer) { fake.projects = withoutRepo(fake.projects, "project-001") })
		_, searches, warnings := gitlabGrantRun(ctx, t, conn, orgID, fake, t0.Add(time.Hour))
		open := gitlabGrantOpen(ctx, t, conn, orgID)
		if _, isOpen := open["org/project-001"]; isOpen || len(open) != 2 {
			t.Errorf("after the sync the open grants are %v, want project-000 and project-002", open)
		}
		if searches != 0 || len(warnings) != 0 {
			t.Errorf("a listing of one response made %d search(es) and %d WARN line(s), want none", searches, len(warnings))
		}
	})

	// The search for the one project fails: the row stays open, loudly.
	t.Run("the search for the one project fails", func(t *testing.T) {
		const orgID = "org-gl-grant-lookup-fails"
		fake := newGitLabGrantServer(t, gitlabGrantProjects(pages))
		gitlabGrantRun(ctx, t, conn, orgID, fake, t0)
		fake.set(func(fake *gitlabGrantServer) {
			fake.projects = withoutRepo(fake.projects, "project-007")
			fake.searchStatus = http.StatusInternalServerError
		})
		result, _, warnings := gitlabGrantRun(ctx, t, conn, orgID, fake, t0.Add(time.Hour))
		if _, isOpen := gitlabGrantOpen(ctx, t, conn, orgID)["org/project-007"]; !isOpen {
			t.Errorf("the search failed and the row of org/project-007 is closed: a failed answer proves nothing")
		}
		if len(warnings) != 1 || warnings[0]["rows_absence_not_proven"] != float64(1) || warnings[0]["rows_closed"] != float64(0) {
			t.Errorf("the WARN line is %v, want 1 row with no proof of absence and 0 closed", warnings)
		}
		if reasons := absenceLegReasons(result); len(reasons) != 1 || reasons[0] != OwnershipAbsenceNotProven {
			t.Errorf("the result names the absence legs %v, want [%s]", reasons, OwnershipAbsenceNotProven)
		}
	})
}
