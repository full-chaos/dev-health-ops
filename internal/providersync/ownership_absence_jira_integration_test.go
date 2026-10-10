//go:build integration

package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// The Jira project search is read by offset (startAt). When a project leaves
// the live answer between two page requests, every later project moves one
// place up and one live project is on no page, while the search ends with the
// provider's own end signal. The catalog reads the live search twice (before
// and after the archived search) and takes the union, so one change hides
// nothing; a project is lost only when BOTH walks lose it (a project that
// leaves, comes back and leaves again while the catalog reads). The ownership
// rows of that project are then not in the run's answer. So a project that a
// search of more than one response does not hold is a candidate only: the run
// asks the search for that one project
// (GET /rest/api/3/project/search?id=<native id>) and closes its rows only
// when that one response does not hold it.
//
// The real Jira team catalog collector and the real ClickHouse writer; the
// provider is a scripted answer in Jira's shape (values, startAt, isLast).

// jiraProjectServer is the live and the archived projects of one Jira site.
type jiraProjectServer struct {
	mu       sync.Mutex
	projects []string // live project keys, in the provider's order
	archived []string // archived project keys, in the provider's order
	// afterArchivedPageOne runs once, directly after page 1 of the archived
	// search is built: a change of the archived projects between two requests.
	afterArchivedPageOne func(archived []string) []string
	// flap, when set, is a project key that leaves the live answer directly
	// after page 1 of EACH live walk is built, and is live again when the
	// archived search is read (between the two live walks).
	flap string
	// lookupStatus, when not 0, is the status every search for one id answers.
	lookupStatus int
	lookups      int
}

type jiraProjectDoer func(*http.Request) (*http.Response, error)

func (doer jiraProjectDoer) Do(request *http.Request) (*http.Response, error) { return doer(request) }

// jiraProjectNativeID is the native id of a project key of the server.
func jiraProjectNativeID(key string) string { return "10" + strings.TrimPrefix(key, "K") }

func (server *jiraProjectServer) doer(t *testing.T) jiraProjectDoer {
	entry := func(key string) string {
		return `{"id":"` + jiraProjectNativeID(key) + `","key":"` + key + `","name":"Project ` + key + `"}`
	}
	return func(request *http.Request) (*http.Response, error) {
		server.mu.Lock()
		defer server.mu.Unlock()
		respond := func(status int, body string) (*http.Response, error) {
			return &http.Response{StatusCode: status, Status: strconv.Itoa(status) + " " + http.StatusText(status),
				Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
		}
		path, query := request.URL.Path, request.URL.Query()
		switch {
		case path == "/rest/api/3/project/search" && query.Get("id") != "":
			// The search for one project. As the provider does, it answers for
			// the states `status` names, and for live projects only when the
			// request names none.
			server.lookups++
			if server.lookupStatus != 0 {
				return respond(server.lookupStatus, `{"errorMessages":["simulated failure"]}`)
			}
			statuses := query["status"]
			if len(statuses) == 0 {
				statuses = []string{"live"}
			}
			for _, status := range statuses {
				list := server.projects
				if status == "archived" {
					list = server.archived
				}
				for _, key := range list {
					if jiraProjectNativeID(key) == query.Get("id") {
						return respond(200, `{"values":[`+entry(key)+`],"startAt":0,"isLast":true,"total":1}`)
					}
				}
			}
			return respond(200, `{"values":[],"startAt":0,"isLast":true,"total":0}`)
		case path == "/rest/api/3/project/search" && query.Get("status") == "archived":
			if server.flap != "" {
				server.projects = append([]string{server.flap}, withoutRepo(server.projects, server.flap)...)
			}
			startAt, _ := strconv.Atoi(query.Get("startAt"))
			maxResults, _ := strconv.Atoi(query.Get("maxResults"))
			if maxResults < 1 {
				maxResults = 50
			}
			start, end := startAt, startAt+maxResults
			if start > len(server.archived) {
				start = len(server.archived)
			}
			last := end >= len(server.archived)
			if last {
				end = len(server.archived)
			}
			values := make([]string, 0, end-start)
			for _, key := range server.archived[start:end] {
				values = append(values, entry(key))
			}
			total := len(server.archived)
			if startAt == 0 && server.afterArchivedPageOne != nil {
				server.archived = server.afterArchivedPageOne(server.archived)
				server.afterArchivedPageOne = nil
			}
			return respond(200, fmt.Sprintf(`{"values":[%s],"startAt":%d,"maxResults":%d,"isLast":%t,"total":%d}`,
				strings.Join(values, ","), startAt, maxResults, last, total))
		case path == "/rest/api/3/project/search":
			startAt, _ := strconv.Atoi(query.Get("startAt"))
			maxResults, _ := strconv.Atoi(query.Get("maxResults"))
			if maxResults < 1 {
				maxResults = 50
			}
			start, end := startAt, startAt+maxResults
			if start > len(server.projects) {
				start = len(server.projects)
			}
			last := end >= len(server.projects)
			if last {
				end = len(server.projects)
			}
			values := make([]string, 0, end-start)
			for _, key := range server.projects[start:end] {
				values = append(values, entry(key))
			}
			total := len(server.projects)
			if startAt == 0 && server.flap != "" {
				server.projects = withoutRepo(server.projects, server.flap)
			}
			return respond(200, fmt.Sprintf(`{"values":[%s],"startAt":%d,"maxResults":%d,"isLast":%t,"total":%d}`,
				strings.Join(values, ","), startAt, maxResults, last, total))
		case strings.HasPrefix(path, "/rest/api/3/project/"):
			return respond(200, `{"projectTypeKey":"business"}`)
		}
		t.Errorf("unexpected Jira request %s", request.URL)
		return respond(500, `{}`)
	}
}

func (server *jiraProjectServer) set(mutate func(server *jiraProjectServer)) {
	server.mu.Lock()
	defer server.mu.Unlock()
	mutate(server)
}

func jiraProjectKeys(count int) []string {
	keys := make([]string, 0, count)
	for index := 0; index < count; index++ {
		keys = append(keys, fmt.Sprintf("K%03d", index))
	}
	return keys
}

// jiraArchivedKeys are project keys whose native ids do not meet those of
// jiraProjectKeys.
func jiraArchivedKeys(count int) []string {
	keys := make([]string, 0, count)
	for index := 0; index < count; index++ {
		keys = append(keys, fmt.Sprintf("K9%03d", index))
	}
	return keys
}

// jiraProjectRun is one sync of the site: the result, the searches for one
// project it made, and the WARN lines of the absence rule.
func jiraProjectRun(
	ctx context.Context, t *testing.T, conn driver.Conn, orgID string, server *jiraProjectServer, at time.Time,
) (result TeamCatalogResult, lookups int, warnings []map[string]any) {
	t.Helper()
	server.set(func(server *jiraProjectServer) { server.lookups = 0 })
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	result, err := JiraTeamCatalogCollector{
		Handler: JiraTeamCatalogRouteHandler{},
		Sink: JiraTeamCatalogClickHouseEffects{Conn: conn,
			Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })},
		ScopeCensus: staticScopeCensus{},
	}.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
		providerfoundation.Credential{Provider: "jira"}, jiraTeamCatalogTestClient(t, fakehttp.Client(server.doer(t))),
		TeamCatalogSelections{Projects: true}, at)
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
	server.set(func(server *jiraProjectServer) { lookups = server.lookups })
	return result, lookups, warnings
}

// jiraLegacyOpen is the open legacy ownership rows of the site, by native
// project id, with their valid_from.
func jiraLegacyOpen(ctx context.Context, t *testing.T, conn driver.Conn, orgID string) map[string]time.Time {
	t.Helper()
	open := map[string]time.Time{}
	for _, row := range openGitLabOwnership(ctx, t, conn, orgID, "jira") {
		if row.Source == jiraTeamCatalogLegacySource {
			open[row.Project] = row.ValidFrom.UTC()
		}
	}
	return open
}

func TestJiraLegacyOwnershipIsNotClosedByAProjectSearchThatChangedWhileItWasRead(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	t0 := time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC)
	const count = 3 * jiraTeamCatalogProjectSearchMaxResults / 2 // one page and a half
	// link gives every project of the site a legacy link to one team.
	link := func(orgID string, keys []string) {
		t.Helper()
		for _, key := range keys {
			if err := conn.Exec(ctx, `INSERT INTO jira_project_ops_team_links (org_id, project_key, ops_team_id, project_name, ops_team_name, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
				orgID, key, "ops-team-a", "Project "+key, "Ops Team", t0.Add(-time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
	}

	// The first project leaves the live answer after page 1 of each live walk
	// is read (it is live again in between). The project at the page boundary
	// is on no page of either walk and is STILL live: its ownership row stays
	// open on its first valid_from.
	t.Run("a project leaves the live answer between two pages of both walks", func(t *testing.T) {
		orgID := uuid.NewString()
		keys := jiraProjectKeys(count)
		link(orgID, keys)
		server := &jiraProjectServer{projects: keys}
		boundary := jiraProjectNativeID(fmt.Sprintf("K%03d", jiraTeamCatalogProjectSearchMaxResults))
		if _, lookups, _ := jiraProjectRun(ctx, t, conn, orgID, server, t0); lookups != 0 || len(jiraLegacyOpen(ctx, t, conn, orgID)) != count {
			t.Fatalf("the first sync made %d search(es) for one project and holds %d open rows, want 0 and %d: the case is not set",
				lookups, len(jiraLegacyOpen(ctx, t, conn, orgID)), count)
		}
		server.set(func(server *jiraProjectServer) { server.flap = "K000" })
		result, lookups, warnings := jiraProjectRun(ctx, t, conn, orgID, server, t0.Add(time.Hour))
		server.set(func(server *jiraProjectServer) { server.flap = "" })
		open := jiraLegacyOpen(ctx, t, conn, orgID)
		if from, isOpen := open[boundary]; !isOpen || !from.Equal(t0) {
			t.Errorf("project %s is still live and was on no page: open %v valid_from %s, want an open row from %s",
				boundary, isOpen, from.Format(time.RFC3339), t0.Format(time.RFC3339))
		}
		if lookups != 1 {
			t.Errorf("the sync made %d search(es) for one project, want 1 (the one project the search did not hold)", lookups)
		}
		if len(warnings) != 1 || warnings[0]["kind"] != "jira_legacy_ownership" || warnings[0]["rows_still_held"] != float64(1) ||
			warnings[0]["rows_closed"] != float64(0) {
			t.Errorf("the WARN line of the absence rule is %v, want one line for jira_legacy_ownership with 1 row still held and 0 closed", warnings)
		}
		if reasons := absenceLegReasons(result); len(reasons) != 1 || reasons[0] != OwnershipAbsenceListingWrong {
			t.Errorf("the result names the absence legs %v, want [%s]", reasons, OwnershipAbsenceListingWrong)
		}
		// The next clean sync closes the project that did leave.
		jiraProjectRun(ctx, t, conn, orgID, server, t0.Add(2*time.Hour))
		open = jiraLegacyOpen(ctx, t, conn, orgID)
		if _, isOpen := open[jiraProjectNativeID("K000")]; isOpen {
			t.Errorf("project K000 left the live answer and its row is still open after a clean sync")
		}
		if from, isOpen := open[boundary]; !isOpen || !from.Equal(t0) {
			t.Errorf("project %s after the clean sync: open %v valid_from %s, want the open row from %s", boundary, isOpen, from.Format(time.RFC3339), t0.Format(time.RFC3339))
		}
	})

	// A project that left a site of more than one page: closed in the same
	// sync, on the provider's answer for that project.
	t.Run("a project left a site of more than one page", func(t *testing.T) {
		orgID := uuid.NewString()
		keys := jiraProjectKeys(count)
		link(orgID, keys)
		server := &jiraProjectServer{projects: keys}
		jiraProjectRun(ctx, t, conn, orgID, server, t0)
		server.set(func(server *jiraProjectServer) { server.projects = withoutRepo(server.projects, "K007") })
		result, lookups, warnings := jiraProjectRun(ctx, t, conn, orgID, server, t0.Add(time.Hour))
		if _, isOpen := jiraLegacyOpen(ctx, t, conn, orgID)[jiraProjectNativeID("K007")]; isOpen {
			t.Errorf("project K007 left the live answer and the provider says so: its row is still open")
		}
		if lookups != 1 || len(warnings) != 0 || len(absenceLegReasons(result)) != 0 {
			t.Errorf("the sync made %d search(es) for one project, wrote %d WARN line(s) and the legs %v; want 1, none and none",
				lookups, len(warnings), absenceLegReasons(result))
		}
	})

	// The project is in the search answer and its legacy LINK is gone: the
	// links are one read of the store, so the row closes with no request,
	// whatever the number of pages of the search.
	t.Run("the legacy link of a live project is removed", func(t *testing.T) {
		orgID := uuid.NewString()
		keys := jiraProjectKeys(count)
		link(orgID, keys)
		server := &jiraProjectServer{projects: keys}
		jiraProjectRun(ctx, t, conn, orgID, server, t0)
		if err := conn.Exec(ctx, `ALTER TABLE jira_project_ops_team_links DELETE WHERE org_id = ? AND project_key = 'K007' SETTINGS mutations_sync = 2`, orgID); err != nil {
			t.Fatal(err)
		}
		_, lookups, warnings := jiraProjectRun(ctx, t, conn, orgID, server, t0.Add(time.Hour))
		open := jiraLegacyOpen(ctx, t, conn, orgID)
		if _, isOpen := open[jiraProjectNativeID("K007")]; isOpen || len(open) != count-1 {
			t.Errorf("the link of K007 is removed: its row open %v, %d open rows; want closed and %d", isOpen, len(open), count-1)
		}
		if lookups != 0 || len(warnings) != 0 {
			t.Errorf("a removed link made %d search(es) for one project and %d WARN line(s), want none", lookups, len(warnings))
		}
	})

	// A search of one response cannot move: no search for one project.
	t.Run("a project left a site of one page", func(t *testing.T) {
		orgID := uuid.NewString()
		keys := jiraProjectKeys(3)
		link(orgID, keys)
		server := &jiraProjectServer{projects: keys}
		jiraProjectRun(ctx, t, conn, orgID, server, t0)
		server.set(func(server *jiraProjectServer) { server.projects = withoutRepo(server.projects, "K001") })
		_, lookups, warnings := jiraProjectRun(ctx, t, conn, orgID, server, t0.Add(time.Hour))
		open := jiraLegacyOpen(ctx, t, conn, orgID)
		if _, isOpen := open[jiraProjectNativeID("K001")]; isOpen || len(open) != 2 {
			t.Errorf("after the sync the open rows are %v, want the rows of K000 and K002", open)
		}
		if lookups != 0 || len(warnings) != 0 {
			t.Errorf("a search of one response made %d search(es) for one project and %d WARN line(s), want none", lookups, len(warnings))
		}
	})

	// The search for the one project fails: the row stays open, loudly.
	t.Run("the search for the one project fails", func(t *testing.T) {
		orgID := uuid.NewString()
		keys := jiraProjectKeys(count)
		link(orgID, keys)
		server := &jiraProjectServer{projects: keys}
		jiraProjectRun(ctx, t, conn, orgID, server, t0)
		server.set(func(server *jiraProjectServer) {
			server.projects = withoutRepo(server.projects, "K007")
			server.lookupStatus = http.StatusInternalServerError
		})
		result, _, warnings := jiraProjectRun(ctx, t, conn, orgID, server, t0.Add(time.Hour))
		if _, isOpen := jiraLegacyOpen(ctx, t, conn, orgID)[jiraProjectNativeID("K007")]; !isOpen {
			t.Errorf("the search for the one project failed and its row is closed: a failed answer proves nothing")
		}
		if len(warnings) != 1 || warnings[0]["rows_absence_not_proven"] != float64(1) || warnings[0]["rows_closed"] != float64(0) {
			t.Errorf("the WARN line is %v, want 1 row with no proof of absence and 0 closed", warnings)
		}
		if reasons := absenceLegReasons(result); len(reasons) != 1 || reasons[0] != OwnershipAbsenceNotProven {
			t.Errorf("the result names the absence legs %v, want [%s]", reasons, OwnershipAbsenceNotProven)
		}
	})
}

// An ARCHIVED project keeps its ownership: the catalog leaves the open rows of
// every project the archived search returns as they are. The archived search
// is its own walk by offset, and it feeds the same held set as the two live
// walks. When an archived project is deleted between two of its page
// requests, another project that is STILL archived is on no page. Its row must
// stay open: one response of the live search does not prove it gone, and an
// answer for live projects only would call it gone.
func TestJiraLegacyOwnershipOfAnArchivedProjectIsNotClosedByAnArchivedSearchThatChangedWhileItWasRead(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	t0 := time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC)
	const paged = 3 * jiraTeamCatalogProjectSearchMaxResults / 2 // one page and a half
	link := func(orgID string, keys []string) {
		t.Helper()
		for _, key := range keys {
			if err := conn.Exec(ctx, `INSERT INTO jira_project_ops_team_links (org_id, project_key, ops_team_id, project_name, ops_team_name, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
				orgID, key, "ops-team-a", "Project "+key, "Ops Team", t0.Add(-time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, tc := range []struct {
		name string
		live int
	}{
		{"the live search is one response", 3},
		{"the live search is two responses", paged},
	} {
		t.Run("an archived project is deleted between two pages of the archived search; "+tc.name, func(t *testing.T) {
			orgID := uuid.NewString()
			live, later := jiraProjectKeys(tc.live), jiraArchivedKeys(paged)
			every := append(append([]string{}, live...), later...)
			link(orgID, every)
			// Run 1: every project is live. Run 2: the later ones are archived.
			server := &jiraProjectServer{projects: every}
			jiraProjectRun(ctx, t, conn, orgID, server, t0)
			server.set(func(server *jiraProjectServer) { server.projects, server.archived = live, later })
			jiraProjectRun(ctx, t, conn, orgID, server, t0.Add(time.Hour))
			if open := jiraLegacyOpen(ctx, t, conn, orgID); len(open) != len(every) {
				t.Fatalf("after the archive %d rows are open, want %d (an archived project keeps its rows): the case is not set", len(open), len(every))
			}
			first := later[0]
			boundary := jiraProjectNativeID(later[jiraTeamCatalogProjectSearchMaxResults])
			server.set(func(server *jiraProjectServer) {
				server.afterArchivedPageOne = func(archived []string) []string { return withoutRepo(archived, first) }
			})
			result, lookups, warnings := jiraProjectRun(ctx, t, conn, orgID, server, t0.Add(2*time.Hour))
			open := jiraLegacyOpen(ctx, t, conn, orgID)
			if from, isOpen := open[boundary]; !isOpen || !from.Equal(t0) {
				t.Errorf("archived project %s still exists and was on no page of the archived search: open %v valid_from %s, want an open row from %s",
					boundary, isOpen, from.Format(time.RFC3339), t0.Format(time.RFC3339))
			}
			if lookups != 1 {
				t.Errorf("the sync made %d search(es) for one project, want 1 (the one project no walk held)", lookups)
			}
			if len(warnings) != 1 || warnings[0]["rows_still_held"] != float64(1) || warnings[0]["rows_closed"] != float64(0) {
				t.Errorf("the WARN line of the absence rule is %v, want 1 row still held and 0 closed", warnings)
			}
			if reasons := absenceLegReasons(result); len(reasons) != 1 || reasons[0] != OwnershipAbsenceListingWrong {
				t.Errorf("the result names the absence legs %v, want [%s]", reasons, OwnershipAbsenceListingWrong)
			}
			// The next clean sync closes the project that WAS deleted, on the
			// provider's answer (neither live nor archived), and leaves the
			// archived one as it is.
			if _, lookups, _ := jiraProjectRun(ctx, t, conn, orgID, server, t0.Add(3*time.Hour)); lookups != 1 {
				t.Errorf("the clean sync made %d search(es) for one project, want 1 (the deleted project)", lookups)
			}
			open = jiraLegacyOpen(ctx, t, conn, orgID)
			if _, isOpen := open[jiraProjectNativeID(first)]; isOpen {
				t.Errorf("the deleted project %s is still open after a clean sync", first)
			}
			if from, isOpen := open[boundary]; !isOpen || !from.Equal(t0) {
				t.Errorf("archived project %s after the clean sync: open %v valid_from %s, want the open row from %s",
					boundary, isOpen, from.Format(time.RFC3339), t0.Format(time.RFC3339))
			}
		})
	}

	// Every walk is one response: a deleted archived project is closed by the
	// walks themselves, with no request.
	t.Run("an archived project is deleted and every walk is one response", func(t *testing.T) {
		orgID := uuid.NewString()
		live, later := jiraProjectKeys(3), jiraArchivedKeys(3)
		every := append(append([]string{}, live...), later...)
		link(orgID, every)
		server := &jiraProjectServer{projects: every}
		jiraProjectRun(ctx, t, conn, orgID, server, t0)
		server.set(func(server *jiraProjectServer) { server.projects, server.archived = live, later })
		jiraProjectRun(ctx, t, conn, orgID, server, t0.Add(time.Hour))
		server.set(func(server *jiraProjectServer) { server.archived = withoutRepo(server.archived, later[1]) })
		_, lookups, warnings := jiraProjectRun(ctx, t, conn, orgID, server, t0.Add(2*time.Hour))
		open := jiraLegacyOpen(ctx, t, conn, orgID)
		if _, isOpen := open[jiraProjectNativeID(later[1])]; isOpen || len(open) != len(every)-1 {
			t.Errorf("the deleted archived project is open %v and %d rows are open; want closed and %d", isOpen, len(open), len(every)-1)
		}
		if lookups != 0 || len(warnings) != 0 {
			t.Errorf("walks of one response each made %d search(es) for one project and %d WARN line(s), want none", lookups, len(warnings))
		}
	})

	// The live search is one response and the ARCHIVED search is two: a live
	// project that left is a candidate (one walk of the held set could have
	// moved), and it is closed on the provider's answer.
	t.Run("a live project left while the archived search took two responses", func(t *testing.T) {
		orgID := uuid.NewString()
		live, later := jiraProjectKeys(3), jiraArchivedKeys(paged)
		every := append(append([]string{}, live...), later...)
		link(orgID, every)
		server := &jiraProjectServer{projects: every}
		jiraProjectRun(ctx, t, conn, orgID, server, t0)
		server.set(func(server *jiraProjectServer) { server.projects, server.archived = live, later })
		jiraProjectRun(ctx, t, conn, orgID, server, t0.Add(time.Hour))
		server.set(func(server *jiraProjectServer) { server.projects = withoutRepo(server.projects, "K001") })
		_, lookups, warnings := jiraProjectRun(ctx, t, conn, orgID, server, t0.Add(2*time.Hour))
		open := jiraLegacyOpen(ctx, t, conn, orgID)
		if _, isOpen := open[jiraProjectNativeID("K001")]; isOpen || len(open) != len(every)-1 {
			t.Errorf("the live project that left is open %v and %d rows are open; want closed and %d", isOpen, len(open), len(every)-1)
		}
		if lookups != 1 || len(warnings) != 0 {
			t.Errorf("the sync made %d search(es) for one project and %d WARN line(s), want 1 and none", lookups, len(warnings))
		}
	})
}
