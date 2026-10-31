package providersync

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type versionedMilestoneCase struct {
	name       string
	versionURL string // body of GET /api/v4/version; "" answers HTTP 500
	wantParam  string
}

var versionedMilestoneCases = []versionedMilestoneCase{
	{"old 16.6.9", `{"version":"16.6.9","revision":"abc"}`, "include_parent_milestones"},
	{"old 15.11.13-ee", `{"version":"15.11.13-ee","revision":"abc"}`, "include_parent_milestones"},
	{"cutoff 16.7.0", `{"version":"16.7.0","revision":"abc"}`, "include_ancestors"},
	{"new 17.2.1-ee", `{"version":"17.2.1-ee","revision":"abc"}`, "include_ancestors"},
	{"probe refused", ``, "include_ancestors"},
	{"probe garbage", `not json`, "include_ancestors"},
	{"probe unparseable version", `{"version":"nightly"}`, "include_ancestors"},
}

func assertMilestoneQuery(t *testing.T, query url.Values, wantParam string) {
	t.Helper()
	other := "include_parent_milestones"
	if wantParam == other {
		other = "include_ancestors"
	}
	if query.Get(wantParam) != "true" || query.Has(other) {
		t.Fatalf("milestone query %v, want %s=true alone (never both: GitLab >= 16.7 answers HTTP 400)", query, wantParam)
	}
}

func TestGitLabWorkItemsMilestonesParamFollowsTheGitLabVersion(t *testing.T) {
	for _, tc := range versionedMilestoneCases {
		t.Run(tc.name, func(t *testing.T) {
			responses := gitLabWorkItemResponses()
			if tc.versionURL != "" {
				responses["/api/v4/version"] = []string{tc.versionURL}
			}
			doer := &gitLabWorkItemsDoer{responses: responses}
			claim := nativeTestClaim("gitlab", "work-items")
			client := gitLabWorkItemsClient(t, fakehttp.Client(doer))
			if _, err := (GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 3, MaxPages: 10, NestedMaxPages: 10}).Collect(
				context.Background(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID}, client,
				time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
			); err != nil {
				t.Fatal(err)
			}
			reads, probes := 0, 0
			for _, request := range doer.requests {
				switch {
				case strings.HasSuffix(request.URL.Path, "/milestones"):
					reads++
					assertMilestoneQuery(t, request.URL.Query(), tc.wantParam)
				case request.URL.Path == "/api/v4/version":
					probes++
				}
			}
			if reads < 1 || probes != 1 {
				t.Fatalf("milestone reads = %d, version probes = %d, want >= 1 and exactly 1", reads, probes)
			}
		})
	}
}

func TestGitLabWorkItemsVersionProbeFailureNamesTheUnit(t *testing.T) {
	var logged lockedLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	doer := &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses(), statusByPath: map[string]int{"/api/v4/version": http.StatusInternalServerError}}
	claim := nativeTestClaim("gitlab", "work-items")
	if _, err := (GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 3, MaxPages: 10, NestedMaxPages: 10}).Collect(
		context.Background(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID}, gitLabWorkItemsClient(t, fakehttp.Client(doer)),
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	); err != nil {
		t.Fatal(err)
	}
	line := ""
	for _, candidate := range strings.Split(logged.String(), "\n") {
		if strings.Contains(candidate, "gitlab_version_probe_failed") {
			line = candidate
		}
	}
	if !strings.Contains(line, "level=ERROR") || !strings.Contains(line, "org_id="+claim.OrgID) || !strings.Contains(line, "unit_id="+claim.ID) {
		t.Fatalf("a failed version probe logged %q, want an ERROR line naming org_id and unit_id", line)
	}
}

func versionedMilestoneFront(t *testing.T, versionBody string, probes *atomic.Int32, projects, milestones map[string]string) (string, *[]string) {
	t.Helper()
	inner, requests := gitlabMilestoneFixture(t, projects, milestones)
	target, err := url.Parse(inner.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v4/version" {
			proxy.ServeHTTP(w, r)
			return
		}
		probes.Add(1)
		if versionBody == "" {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(versionBody))
	}))
	t.Cleanup(front.Close)
	return front.URL, requests
}

func TestGitLabTeamCatalogMilestonesParamFollowsTheGitLabVersion(t *testing.T) {
	for _, tc := range versionedMilestoneCases {
		t.Run(tc.name, func(t *testing.T) {
			var probes atomic.Int32
			frontURL, requests := versionedMilestoneFront(t, tc.versionURL, &probes,
				map[string]string{"100": "org/a", "101": "org/b"},
				map[string]string{
					"100": `[{"id":9,"title":"A","state":"active","due_date":"2026-09-30"}]`,
					"101": `[{"id":10,"title":"B","state":"active","due_date":"2026-10-30"}]`,
				})
			if _, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(context.Background(),
				TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1"},
				providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}},
				gitlabTeamCatalogTestClient(t, frontURL), TeamCatalogSelections{Projects: true}, targetFetchedAt); err != nil {
				t.Fatal(err)
			}
			reads := 0
			for _, request := range *requests {
				if strings.Contains(request, "/milestones") {
					reads++
					parsed, err := url.ParseRequestURI(request)
					if err != nil {
						t.Fatal(err)
					}
					assertMilestoneQuery(t, parsed.Query(), tc.wantParam)
				}
			}
			if reads != 2 || probes.Load() != 1 {
				t.Fatalf("milestone reads = %d, version probes = %d, want 2 and exactly 1 for two projects", reads, probes.Load())
			}
		})
	}
}
