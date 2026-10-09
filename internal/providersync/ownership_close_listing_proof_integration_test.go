//go:build integration

package providersync

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// headerFixtureDoer answers GitHub requests by path (page 2 as "?page=2"),
// with any number of Link field lines per path.
type headerFixtureDoer struct {
	t      *testing.T
	byPath map[string]string
	links  map[string][]string
}

func (doer *headerFixtureDoer) Do(request *http.Request) (*http.Response, error) {
	key := request.URL.Path
	if page := request.URL.Query().Get("page"); page != "" && page != "1" {
		key += "?page=" + page
	}
	body, ok := doer.byPath[key]
	if !ok {
		doer.t.Fatalf("unexpected GitHub request %q", key)
	}
	header := http.Header{"Content-Type": []string{"application/json"}}
	for _, link := range doer.links[key] {
		header.Add("Link", link)
	}
	return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func githubListingProofRun(ctx context.Context, t *testing.T, conn driver.Conn, orgID string, doer providerfoundation.HTTPDoer, at time.Time) (TeamCatalogResult, error) {
	t.Helper()
	adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	return adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
		credential, githubTeamCatalogAdapterClient(t, doer), TeamCatalogSelections{Teams: true}, at)
}

// TestGitHubOwnershipCloseFollowsEveryLinkFormItReadsAsNext: the walker that
// decides to follow rel="next" is the one that decides the end. A next page
// announced in a second Link field line, in a rel list, or in another case is
// read, so its repo stays open, and the repo GitHub no longer lists closes.
func TestGitHubOwnershipCloseFollowsEveryLinkFormItReadsAsNext(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	t0 := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	const (
		teams = `[{"slug":"platform","name":"Platform"}]`
		repos = "/orgs/acme/teams/platform/repos"
		page1 = "https://api.github.com/orgs/acme/teams/platform/repos?page=1"
		page2 = "https://api.github.com/orgs/acme/teams/platform/repos?page=2"
	)
	lastPage := []string{`<` + page1 + `>; rel="prev", <` + page1 + `>; rel="first"`}
	for _, test := range []struct {
		name  string
		links []string
	}{
		{"rel=next in one field line", []string{`<` + page2 + `>; rel="next"`}},
		{"rel=next in a second Link field line", []string{`<` + page1 + `>; rel="first"`, `<` + page2 + `>; rel="next"`}},
		{"a rel list holding next", []string{`<` + page2 + `>; rel="next last"`}},
		{"rel=NEXT in upper case", []string{`<` + page2 + `>; rel="NEXT"`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			org := "gh-proof-" + strings.NewReplacer(" ", "", "=", "", `"`, "").Replace(test.name)
			seed := &headerFixtureDoer{t: t, byPath: map[string]string{"/orgs/acme/teams": teams, repos: `[{"name":"api"},{"name":"late"},{"name":"old"}]`}}
			if _, err := githubListingProofRun(ctx, t, conn, org, seed, t0); err != nil {
				t.Fatal(err)
			}
			doer := &headerFixtureDoer{t: t,
				byPath: map[string]string{"/orgs/acme/teams": teams, repos: `[{"name":"api"}]`, repos + "?page=2": `[{"name":"late"}]`},
				links:  map[string][]string{repos: test.links, repos + "?page=2": lastPage},
			}
			result, err := githubListingProofRun(ctx, t, conn, org, doer, t0.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			requireRepoFacts(t, test.name, openRepoOwnership(ctx, t, conn, org, "github"),
				repoOwnershipFact{TeamID: "gh:platform", Repo: "acme/api", Source: "provider_access"},
				repoOwnershipFact{TeamID: "gh:platform", Repo: "acme/late", Source: "provider_access"})
			if got := closeLegReasons(result.DegradedLegs); len(got) != 0 {
				t.Fatalf("degraded close legs = %v, want none", got)
			}
		})
	}
}

// TestOwnershipCloseNeverReadsANullOrObjectPageAsAnEmptyListing: a 200 body
// that is not a JSON array is not a page with zero items, on GitHub and on
// GitLab; no row closes.
func TestOwnershipCloseNeverReadsANullOrObjectPageAsAnEmptyListing(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	t0 := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	for _, body := range []string{`null`, ` null `, `{}`} {
		t.Run("github "+body, func(t *testing.T) {
			org := "gh-proof-body-" + strings.TrimSpace(strings.NewReplacer("{", "o", "}", "o").Replace(body))
			const teams = `[{"slug":"platform","name":"Platform"}]`
			seed := &headerFixtureDoer{t: t, byPath: map[string]string{"/orgs/acme/teams": teams, "/orgs/acme/teams/platform/repos": `[{"name":"api"}]`}}
			if _, err := githubListingProofRun(ctx, t, conn, org, seed, t0); err != nil {
				t.Fatal(err)
			}
			doer := &headerFixtureDoer{t: t, byPath: map[string]string{"/orgs/acme/teams": teams, "/orgs/acme/teams/platform/repos": body}}
			result, err := githubListingProofRun(ctx, t, conn, org, doer, t0.Add(time.Hour))
			t.Logf("second run: err=%v legs=%v", err, closeLegReasons(result.DegradedLegs))
			requireRepoFacts(t, "after a "+body+" repos page", openRepoOwnership(ctx, t, conn, org, "github"),
				repoOwnershipFact{TeamID: "gh:platform", Repo: "acme/api", Source: "provider_access"})
		})
		t.Run("gitlab "+body, func(t *testing.T) {
			org := "gl-proof-body-" + strings.TrimSpace(strings.NewReplacer("{", "o", "}", "o").Replace(body))
			second := false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header()["X-Next-Page"] = []string{""}
				switch path := r.URL.EscapedPath(); {
				case path == "/api/v4/groups/org":
					_, _ = io.WriteString(w, `{"id":1,"full_path":"org","name":"org","description":null}`)
				case strings.HasSuffix(path, "/projects") && second:
					_, _ = io.WriteString(w, body)
				case strings.HasSuffix(path, "/projects"):
					_, _ = io.WriteString(w, `[{"id":7,"path_with_namespace":"org/kept","name":"kept","archived":false,"web_url":"https://x/org/kept"}]`)
				default:
					_, _ = io.WriteString(w, `[]`)
				}
			}))
			t.Cleanup(srv.Close)
			fake := &gitlabSnapshotServer{Server: srv}
			projectsOnly := TeamCatalogSelections{Projects: true}
			if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
				t.Fatal(err)
			}
			second = true
			result, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour))
			t.Logf("second run: err=%v legs=%v", err, closeLegReasons(result.DegradedLegs))
			requireGitLabFacts(t, "after a "+body+" projects page", openGitLabOwnership(ctx, t, conn, org, "gitlab"),
				gitlabOwnershipFact{TeamID: "gl:org", Project: "org/kept", Source: "provider_access"})
		})
	}
}

// TestGitLabOwnershipCloseNeedsNoLinkNextBesideAnEmptyXNextPage: an empty
// X-Next-Page is GitLab's end only when no Link header announces a next page.
func TestGitLabOwnershipCloseNeedsNoLinkNextBesideAnEmptyXNextPage(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	t0 := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	org := "gl-proof-link-next"
	second := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header()["X-Next-Page"] = []string{""}
		switch path := r.URL.EscapedPath(); {
		case path == "/api/v4/groups/org":
			_, _ = io.WriteString(w, `{"id":1,"full_path":"org","name":"org","description":null}`)
		case strings.HasSuffix(path, "/projects") && second:
			w.Header().Add("Link", `<https://gitlab.example/api/v4/groups/org/projects?page=1>; rel="first"`)
			w.Header().Add("Link", `<https://gitlab.example/api/v4/groups/org/projects?cursor=x>; rel="next"`)
			_, _ = io.WriteString(w, `[{"id":7,"path_with_namespace":"org/kept","name":"kept","archived":false,"web_url":"https://x/org/kept"}]`)
		case strings.HasSuffix(path, "/projects"):
			_, _ = io.WriteString(w, `[{"id":7,"path_with_namespace":"org/kept","name":"kept","archived":false,"web_url":"https://x/org/kept"},`+
				`{"id":8,"path_with_namespace":"org/late","name":"late","archived":false,"web_url":"https://x/org/late"}]`)
		default:
			_, _ = io.WriteString(w, `[]`)
		}
	}))
	t.Cleanup(srv.Close)
	fake := &gitlabSnapshotServer{Server: srv}
	projectsOnly := TeamCatalogSelections{Projects: true}
	if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
		t.Fatal(err)
	}
	second = true
	result, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	requireGitLabFacts(t, "after a page whose Link announces a next page", openGitLabOwnership(ctx, t, conn, org, "gitlab"),
		gitlabOwnershipFact{TeamID: "gl:org", Project: "org/kept", Source: "provider_access"},
		gitlabOwnershipFact{TeamID: "gl:org", Project: "org/late", Source: "provider_access"})
	if got := closeLegReasons(result.DegradedLegs); len(got) != 1 || got[0] != OwnershipCloseSkippedListingIncomplete {
		t.Fatalf("degraded close legs = %v, want [%s]", got, OwnershipCloseSkippedListingIncomplete)
	}
}

// TestOwnershipCloseNeverReadsALinkEntryWithoutARelationAsTheEnd: a Link entry
// whose rel names no relation (empty, blank, no value), or that has text
// between its URL and its parameters, does not prove the end of an empty page,
// on GitHub and on GitLab: no row closes and the run says so.
func TestOwnershipCloseNeverReadsALinkEntryWithoutARelationAsTheEnd(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	t0 := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	for index, form := range []string{`rel=""`, `rel=" "`, `rel=`, `junk; rel="prev"`} {
		t.Run("github "+form, func(t *testing.T) {
			org := "gh-proof-rel-" + strings.Repeat("x", index)
			const teams = `[{"slug":"platform","name":"Platform"}]`
			repos := "/orgs/acme/teams/platform/repos"
			seed := &headerFixtureDoer{t: t, byPath: map[string]string{"/orgs/acme/teams": teams, repos: `[{"name":"api"},{"name":"late"}]`}}
			if _, err := githubListingProofRun(ctx, t, conn, org, seed, t0); err != nil {
				t.Fatal(err)
			}
			link := `<https://api.github.com` + repos + `?page=2>`
			if strings.HasPrefix(form, "rel") {
				link += "; "
			} else {
				link += " "
			}
			doer := &headerFixtureDoer{t: t, byPath: map[string]string{"/orgs/acme/teams": teams, repos: `[]`},
				links: map[string][]string{repos: {link + form}}}
			result, err := githubListingProofRun(ctx, t, conn, org, doer, t0.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			requireRepoFacts(t, "after an empty page with Link "+link+form, openRepoOwnership(ctx, t, conn, org, "github"),
				repoOwnershipFact{TeamID: "gh:platform", Repo: "acme/api", Source: "provider_access"},
				repoOwnershipFact{TeamID: "gh:platform", Repo: "acme/late", Source: "provider_access"})
			if got := closeLegReasons(result.DegradedLegs); len(got) != 1 || got[0] != OwnershipCloseSkippedListingIncomplete {
				t.Fatalf("degraded close legs = %v, want [%s]", got, OwnershipCloseSkippedListingIncomplete)
			}
		})
		t.Run("gitlab "+form, func(t *testing.T) {
			org := "gl-proof-rel-" + strings.Repeat("x", index)
			link := `<https://gitlab.example/api/v4/groups/org/projects?page=2>`
			if strings.HasPrefix(form, "rel") {
				link += "; "
			} else {
				link += " "
			}
			second := false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header()["X-Next-Page"] = []string{""}
				switch path := r.URL.EscapedPath(); {
				case path == "/api/v4/groups/org":
					_, _ = io.WriteString(w, `{"id":1,"full_path":"org","name":"org","description":null}`)
				case strings.HasSuffix(path, "/projects") && second:
					w.Header().Set("Link", link+form)
					_, _ = io.WriteString(w, `[]`)
				case strings.HasSuffix(path, "/projects"):
					_, _ = io.WriteString(w, `[{"id":7,"path_with_namespace":"org/kept","name":"kept","archived":false,"web_url":"https://x/org/kept"}]`)
				default:
					_, _ = io.WriteString(w, `[]`)
				}
			}))
			t.Cleanup(srv.Close)
			fake := &gitlabSnapshotServer{Server: srv}
			projectsOnly := TeamCatalogSelections{Projects: true}
			if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0); err != nil {
				t.Fatal(err)
			}
			second = true
			result, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, t0.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			requireGitLabFacts(t, "after an empty page with Link "+link+form, openGitLabOwnership(ctx, t, conn, org, "gitlab"),
				gitlabOwnershipFact{TeamID: "gl:org", Project: "org/kept", Source: "provider_access"})
			if got := closeLegReasons(result.DegradedLegs); len(got) != 1 || got[0] != OwnershipCloseSkippedListingIncomplete {
				t.Fatalf("degraded close legs = %v, want [%s]", got, OwnershipCloseSkippedListingIncomplete)
			}
		})
	}
}
