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

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// A team's repository listing is read by page number. When a repository
// leaves the team between two page requests, every later repository moves one
// place up and the repository at the page boundary is on NO page, while the
// listing still ends with GitHub's own end signal. So the end of a listing of
// more than one response does not prove that a grant is gone: the run asks
// GitHub for that one grant (GET /orgs/{org}/teams/{slug}/repos/{owner}/{repo})
// and closes the row only on "not found".
//
// The real GitHub team collector and the real ClickHouse writer; the provider
// is a scripted answer in GitHub's shape (page and per_page, the Link header,
// 204 and 404 of the direct check).

// githubGrantServer is one GitHub team and its repositories.
type githubGrantServer struct {
	mu    sync.Mutex
	repos []string
	// afterPageOne runs once, directly after page 1 of the listing is built:
	// a change of the team between two page requests.
	afterPageOne func(repos []string) []string
	// lookupStatus, when not 0, is the status every direct check answers.
	lookupStatus int
	lookups      int
}

type githubGrantDoer func(*http.Request) (*http.Response, error)

func (doer githubGrantDoer) Do(request *http.Request) (*http.Response, error) { return doer(request) }

func (server *githubGrantServer) doer(t *testing.T) githubGrantDoer {
	return func(request *http.Request) (*http.Response, error) {
		server.mu.Lock()
		defer server.mu.Unlock()
		respond := func(status int, body string, header http.Header) (*http.Response, error) {
			if header == nil {
				header = http.Header{}
			}
			header.Set("Content-Type", "application/json")
			return &http.Response{StatusCode: status, Status: strconv.Itoa(status) + " " + http.StatusText(status), Header: header,
				Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
		}
		const listing = "/orgs/acme/teams/platform/repos"
		switch path := request.URL.Path; {
		case path == "/orgs/acme/teams":
			return respond(200, `[{"slug":"platform","name":"Platform"}]`, nil)
		case path == listing:
			page, _ := strconv.Atoi(request.URL.Query().Get("page"))
			perPage, _ := strconv.Atoi(request.URL.Query().Get("per_page"))
			if page < 1 {
				page = 1
			}
			if perPage < 1 {
				perPage = 100
			}
			start, end := (page-1)*perPage, page*perPage
			if start > len(server.repos) {
				start = len(server.repos)
			}
			header := http.Header{}
			if end >= len(server.repos) {
				end = len(server.repos)
				header.Set("Link", `<https://api.github.com/orgs/acme/teams/platform/repos?per_page=100&page=1>; rel="first"`)
			} else {
				header.Set("Link", fmt.Sprintf(`<https://api.github.com/orgs/acme/teams/platform/repos?per_page=%d&page=%d>; rel="next"`, perPage, page+1))
			}
			nodes := make([]string, 0, end-start)
			for _, name := range server.repos[start:end] {
				nodes = append(nodes, `{"name":"`+name+`"}`)
			}
			if page == 1 && server.afterPageOne != nil {
				server.repos = server.afterPageOne(server.repos)
				server.afterPageOne = nil
			}
			return respond(200, `[`+strings.Join(nodes, ",")+`]`, header)
		case strings.HasPrefix(path, listing+"/acme/"):
			server.lookups++
			if server.lookupStatus != 0 {
				return respond(server.lookupStatus, `{"message":"Server Error"}`, nil)
			}
			name := strings.TrimPrefix(path, listing+"/acme/")
			for _, repo := range server.repos {
				if repo == name {
					return respond(204, "", nil)
				}
			}
			return respond(404, `{"message":"Not Found","documentation_url":"https://docs.github.com/rest/teams/teams#check-team-permissions-for-a-repository","status":"404"}`, nil)
		}
		t.Errorf("unexpected request %s", request.URL)
		return respond(500, `{}`, nil)
	}
}

func (server *githubGrantServer) set(mutate func(server *githubGrantServer)) {
	server.mu.Lock()
	defer server.mu.Unlock()
	mutate(server)
}

func githubGrantRepos(count int) []string {
	repos := make([]string, 0, count)
	for index := 0; index < count; index++ {
		repos = append(repos, fmt.Sprintf("repo-%03d", index))
	}
	return repos
}

func withoutRepo(repos []string, names ...string) []string {
	gone := map[string]bool{}
	for _, name := range names {
		gone[name] = true
	}
	out := make([]string, 0, len(repos))
	for _, repo := range repos {
		if !gone[repo] {
			out = append(out, repo)
		}
	}
	return out
}

// githubGrantRow is the newest state of one team_repo_ownership fact.
type githubGrantRow struct {
	rows      int
	open      bool
	validFrom time.Time
}

func githubGrantRows(ctx context.Context, t *testing.T, conn driver.Conn, orgID string) map[string]githubGrantRow {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT repo_full_name, valid_from, valid_to FROM team_repo_ownership FINAL WHERE org_id = ? ORDER BY repo_full_name, valid_from`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]githubGrantRow{}
	for rows.Next() {
		var repo string
		var from time.Time
		var to *time.Time
		if err := rows.Scan(&repo, &from, &to); err != nil {
			t.Fatal(err)
		}
		row := out[repo]
		row.rows++
		row.open = to == nil
		row.validFrom = from.UTC()
		out[repo] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// githubGrantRun is one sync of the team: the result, the lookups it made and
// the WARN lines of the absence rule.
func githubGrantRun(
	ctx context.Context, t *testing.T, conn driver.Conn, orgID string, server *githubGrantServer, at time.Time,
) (result TeamCatalogResult, lookups int, warnings []map[string]any) {
	t.Helper()
	server.set(func(server *githubGrantServer) { server.lookups = 0 })
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
	result, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
		providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}},
		githubTeamCatalogAdapterClient(t, fakehttp.Client(server.doer(t))), TeamCatalogSelections{Teams: true}, at)
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
	server.set(func(server *githubGrantServer) { lookups = server.lookups })
	return result, lookups, warnings
}

func absenceLegReasons(result TeamCatalogResult) []string {
	var reasons []string
	for _, leg := range result.DegradedLegs {
		if leg.Leg == ownershipAbsenceLeg {
			reasons = append(reasons, leg.Reason)
		}
	}
	return reasons
}

func TestGitHubRepoGrantIsNotClosedByAListingThatChangedWhileItWasRead(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	t0 := time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC)

	// The reproduced case: 150 repositories (pages of 100); repo-000 leaves
	// the team after page 1 is read. repo-100 is on no page and is STILL
	// granted. Its row must stay open, on its first valid_from.
	t.Run("a repository leaves the team between two pages", func(t *testing.T) {
		const orgID = "org-grant-page-shift"
		server := &githubGrantServer{repos: githubGrantRepos(150)}
		if first, lookups, _ := githubGrantRun(ctx, t, conn, orgID, server, t0); first.RepoOwnershipWritten != 150 || lookups != 0 {
			t.Fatalf("the first sync wrote %d grants with %d direct checks, want 150 and 0: the case is not set", first.RepoOwnershipWritten, lookups)
		}
		server.set(func(server *githubGrantServer) {
			server.afterPageOne = func(repos []string) []string { return withoutRepo(repos, "repo-000") }
		})
		result, lookups, warnings := githubGrantRun(ctx, t, conn, orgID, server, t0.Add(time.Hour))
		if result.RepoOwnershipWritten != 149 {
			t.Fatalf("the sync read %d grants, want 149 (repo-100 on no page): the case is not set", result.RepoOwnershipWritten)
		}
		rows := githubGrantRows(ctx, t, conn, orgID)
		if got := rows["acme/repo-100"]; !got.open || got.rows != 1 || !got.validFrom.Equal(t0) {
			t.Errorf("acme/repo-100 is still granted and was on no page: rows %d open %v valid_from %s, want one open row from %s",
				got.rows, got.open, got.validFrom.Format(time.RFC3339), t0.Format(time.RFC3339))
		}
		// GitHub was asked for that one grant, and said it holds.
		if lookups != 1 {
			t.Errorf("the sync made %d direct check(s), want 1 (the one grant the listing did not hold)", lookups)
		}
		if len(warnings) != 1 || warnings[0]["kind"] != "github_team_repo_grants" || warnings[0]["rows_still_held"] != float64(1) ||
			warnings[0]["rows_closed"] != float64(0) || warnings[0]["level"] != "WARN" {
			t.Errorf("the WARN line of the absence rule is %v, want one line for github_team_repo_grants with 1 row still held and 0 closed", warnings)
		}
		if reasons := absenceLegReasons(result); len(reasons) != 1 || reasons[0] != OwnershipAbsenceListingWrong {
			t.Errorf("the result names the absence legs %v, want [%s]", reasons, OwnershipAbsenceListingWrong)
		}
		// The repository that DID leave was on page 1 of this walk: its row is
		// open for this run. The next clean sync closes it, on GitHub's "not
		// found", and repo-100 is untouched.
		if got := rows["acme/repo-000"]; !got.open {
			t.Fatalf("acme/repo-000 was on page 1 of this walk and its row is closed: the case is not set")
		}
		if _, lookups, _ := githubGrantRun(ctx, t, conn, orgID, server, t0.Add(2*time.Hour)); lookups != 1 {
			t.Errorf("the clean sync made %d direct check(s), want 1 (the repository that left)", lookups)
		}
		rows = githubGrantRows(ctx, t, conn, orgID)
		if got := rows["acme/repo-000"]; got.open {
			t.Errorf("acme/repo-000 left the team and its row is still open after a clean sync")
		}
		if got := rows["acme/repo-100"]; !got.open || got.rows != 1 || !got.validFrom.Equal(t0) {
			t.Errorf("acme/repo-100 after the clean sync: rows %d open %v valid_from %s, want the one open row from %s",
				got.rows, got.open, got.validFrom.Format(time.RFC3339), t0.Format(time.RFC3339))
		}
	})

	// A repository that leaves a team of more than one page, with nothing
	// moving during the walk: closed in the same sync, on GitHub's answer.
	t.Run("a repository left a team of more than one page", func(t *testing.T) {
		const orgID = "org-grant-paged-removal"
		server := &githubGrantServer{repos: githubGrantRepos(150)}
		githubGrantRun(ctx, t, conn, orgID, server, t0)
		server.set(func(server *githubGrantServer) { server.repos = withoutRepo(server.repos, "repo-120") })
		result, lookups, warnings := githubGrantRun(ctx, t, conn, orgID, server, t0.Add(time.Hour))
		if got := githubGrantRows(ctx, t, conn, orgID)["acme/repo-120"]; got.open {
			t.Errorf("acme/repo-120 left the team and GitHub says so: its row is still open")
		}
		if lookups != 1 || len(warnings) != 0 || len(absenceLegReasons(result)) != 0 {
			t.Errorf("the sync made %d direct check(s), wrote %d WARN line(s) and the legs %v; want 1, none and none",
				lookups, len(warnings), absenceLegReasons(result))
		}
	})

	// A listing that is ONE response cannot move: what it does not hold is
	// gone, and no direct check is made.
	t.Run("a repository left a team of one page", func(t *testing.T) {
		const orgID = "org-grant-one-page"
		server := &githubGrantServer{repos: githubGrantRepos(3)}
		githubGrantRun(ctx, t, conn, orgID, server, t0)
		server.set(func(server *githubGrantServer) { server.repos = withoutRepo(server.repos, "repo-001") })
		_, lookups, warnings := githubGrantRun(ctx, t, conn, orgID, server, t0.Add(time.Hour))
		rows := githubGrantRows(ctx, t, conn, orgID)
		if rows["acme/repo-001"].open || !rows["acme/repo-000"].open || !rows["acme/repo-002"].open {
			t.Errorf("after the sync: repo-001 open %v (want closed), repo-000 open %v, repo-002 open %v (want open)",
				rows["acme/repo-001"].open, rows["acme/repo-000"].open, rows["acme/repo-002"].open)
		}
		if lookups != 0 || len(warnings) != 0 {
			t.Errorf("a listing of one response made %d direct check(s) and %d WARN line(s), want none", lookups, len(warnings))
		}
	})

	// The direct check fails: nothing proves the grant gone, so the row stays
	// open, loudly. The next sync, with an answer, closes it.
	t.Run("the direct check fails", func(t *testing.T) {
		const orgID = "org-grant-lookup-fails"
		server := &githubGrantServer{repos: githubGrantRepos(150)}
		githubGrantRun(ctx, t, conn, orgID, server, t0)
		server.set(func(server *githubGrantServer) {
			server.repos = withoutRepo(server.repos, "repo-120")
			server.lookupStatus = http.StatusInternalServerError
		})
		result, _, warnings := githubGrantRun(ctx, t, conn, orgID, server, t0.Add(time.Hour))
		if got := githubGrantRows(ctx, t, conn, orgID)["acme/repo-120"]; !got.open {
			t.Errorf("the direct check failed and the row of acme/repo-120 is closed: a failed answer proves nothing")
		}
		if len(warnings) != 1 || warnings[0]["rows_absence_not_proven"] != float64(1) || warnings[0]["rows_closed"] != float64(0) {
			t.Errorf("the WARN line is %v, want 1 row with no proof of absence and 0 closed", warnings)
		}
		if reasons := absenceLegReasons(result); len(reasons) != 1 || reasons[0] != OwnershipAbsenceNotProven {
			t.Errorf("the result names the absence legs %v, want [%s]", reasons, OwnershipAbsenceNotProven)
		}
		server.set(func(server *githubGrantServer) { server.lookupStatus = 0 })
		githubGrantRun(ctx, t, conn, orgID, server, t0.Add(2*time.Hour))
		if got := githubGrantRows(ctx, t, conn, orgID)["acme/repo-120"]; got.open {
			t.Errorf("the next sync, with GitHub's answer, left the row of acme/repo-120 open")
		}
	})

	// More candidates than the budget of direct checks of one run: the run
	// closes the ones it asked for and leaves the rest open, loudly; the next
	// run goes on.
	t.Run("more removals than the budget of direct checks", func(t *testing.T) {
		const orgID = "org-grant-budget"
		over := 20
		server := &githubGrantServer{repos: githubGrantRepos(250)}
		githubGrantRun(ctx, t, conn, orgID, server, t0)
		removed := githubGrantRepos(OwnershipAbsenceLookupBudget + over)
		server.set(func(server *githubGrantServer) { server.repos = withoutRepo(server.repos, removed...) })
		result, lookups, warnings := githubGrantRun(ctx, t, conn, orgID, server, t0.Add(time.Hour))
		open := 0
		for _, name := range removed {
			if githubGrantRows(ctx, t, conn, orgID)["acme/"+name].open {
				open++
			}
			if open > over {
				break
			}
		}
		if lookups != OwnershipAbsenceLookupBudget || open != over {
			t.Errorf("the sync made %d direct check(s) and left %d of the %d removed grants open, want %d and %d",
				lookups, open, len(removed), OwnershipAbsenceLookupBudget, over)
		}
		if len(warnings) != 1 || warnings[0]["rows_over_lookup_budget"] != float64(over) || warnings[0]["rows_closed"] != float64(OwnershipAbsenceLookupBudget) {
			t.Errorf("the WARN line is %v, want %d rows over the budget and %d closed", warnings, over, OwnershipAbsenceLookupBudget)
		}
		if reasons := absenceLegReasons(result); len(reasons) != 1 || reasons[0] != OwnershipAbsenceOverBudget {
			t.Errorf("the result names the absence legs %v, want [%s]", reasons, OwnershipAbsenceOverBudget)
		}
		if _, lookups, warnings := githubGrantRun(ctx, t, conn, orgID, server, t0.Add(2*time.Hour)); lookups != over || len(warnings) != 0 {
			t.Errorf("the next sync made %d direct check(s) and %d WARN line(s), want %d and none", lookups, len(warnings), over)
		}
		for _, name := range removed {
			if githubGrantRows(ctx, t, conn, orgID)["acme/"+name].open {
				t.Errorf("acme/%s left the team and is still open after the second sync", name)
				break
			}
		}
	})
}
