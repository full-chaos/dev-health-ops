package providersync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-9761 structural guard: every GitHub and GitLab team catalog and
// membership route is driven through all three error kinds (HTTP status,
// malformed body, transport failure) with a planted org, group, team and login
// in the request path. The planted names must appear in neither the returned
// error nor the captured log. A new route in github_team_catalog_route.go,
// gitlab_team_catalog_route.go or membership_absence.go needs a row in
// catalogGuardRoutes.
type catalogGuardRoute struct {
	name  string
	route string // empty: the error is swallowed by the caller, so no route is visible
	run   func(*testing.T, *providerfoundation.HTTPClient) error
	match func(*http.Request) bool
}

func catalogPathIs(path string) func(*http.Request) bool {
	return func(request *http.Request) bool { return request.URL.Path == path }
}

func catalogGuardRoutes() []catalogGuardRoute {
	github := func(t *testing.T, client *providerfoundation.HTTPClient) error {
		collector := GitHubTeamCatalogRouteHandler{
			Client: client, OrgName: "secret-org", Now: func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) },
			ResolveEmail: true, Strict: true,
		}
		_, _, err := collector.Collect(context.Background(), "org-1", true, true)
		return err
	}
	gitlab := func(t *testing.T, client *providerfoundation.HTTPClient) error {
		credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "secret-group"}}
		_, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(context.Background(),
			TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: true}, credential, client,
			TeamCatalogSelections{Teams: true, Projects: true, Members: true}, time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC))
		return err
	}
	githubAbsence := func(t *testing.T, client *providerfoundation.HTTPClient) error {
		if got := (githubMembershipAbsence{client: client, org: "secret-org"}).Absence(context.Background(), "gh:secret-team", "gh:secret-login"); got != AbsenceUnproven {
			return fmt.Errorf("absence = %v, want unproven", got)
		}
		return nil
	}
	gitlabAbsence := func(t *testing.T, client *providerfoundation.HTTPClient) error {
		prover := gitlabMembershipAbsence{client: client, groupByTeam: map[string]string{"gl:secret-group": "/api/v4/groups/secret-group"}}
		if got := prover.Absence(context.Background(), "gl:secret-group", "gl:secret-login"); got != AbsenceUnproven {
			return fmt.Errorf("absence = %v, want unproven", got)
		}
		return nil
	}
	return []catalogGuardRoute{
		{"github catalog: team list", "/orgs/{org}/teams", github, catalogPathIs("/orgs/secret-org/teams")},
		{"github catalog: team repos", "/orgs/{org}/teams/{slug}/repos", github, catalogPathIs("/orgs/secret-org/teams/secret-team/repos")},
		{"github catalog: team members", "/orgs/{org}/teams/{slug}/members", github, catalogPathIs("/orgs/secret-org/teams/secret-team/members")},
		{"github catalog: user", "/users/{login}", github, catalogPathIs("/users/secret-login")},
		{"github membership absence", "", githubAbsence, catalogPathIs("/orgs/secret-org/teams/secret-team/memberships/secret-login")},
		{"gitlab catalog: root group", "/groups/{group}", gitlab, catalogPathIs("/api/v4/groups/secret-group")},
		{"gitlab catalog: subgroups", "/groups/{group}/subgroups", gitlab, catalogPathIs("/api/v4/groups/secret-group/subgroups")},
		{"gitlab catalog: projects", "/groups/{group}/projects", gitlab, catalogPathIs("/api/v4/groups/secret-group/projects")},
		{"gitlab catalog: members", "/groups/{group}/members", gitlab, catalogPathIs("/api/v4/groups/secret-group/members")},
		{"gitlab membership absence", "", gitlabAbsence, catalogPathIs("/api/v4/groups/secret-group/members")},
	}
}

var catalogGuardOK = map[string]string{
	"/orgs/secret-org/teams":                                      `[{"slug":"secret-team","name":"T"}]`,
	"/orgs/secret-org/teams/secret-team/repos":                    `[]`,
	"/orgs/secret-org/teams/secret-team/members":                  `[{"login":"secret-login"}]`,
	"/orgs/secret-org/teams/secret-team/memberships/secret-login": `{"state":"active"}`,
	"/users/secret-login":                                         `{"login":"secret-login","email":"e@example.test"}`,
	"/api/v4/groups/secret-group":                                 `{"id":1,"full_path":"secret-group","name":"G"}`,
	"/api/v4/groups/secret-group/subgroups":                       `[]`,
	"/api/v4/groups/secret-group/projects":                        `[]`,
	"/api/v4/groups/secret-group/members":                         `[{"id":5,"username":"secret-login","name":"N"}]`,
}

func TestCatalogRoutesNeverCarryPlantedNamesInErrorsOrLogs(t *testing.T) {
	forbidden := []string{"secret-org", "secret-team", "secret-login", "secret-group", "/api/v4/groups/", "/orgs/secret"}
	for _, route := range catalogGuardRoutes() {
		for _, kind := range []string{"status", "decode", "transport"} {
			t.Run(route.name+"/"+kind, func(t *testing.T) {
				var logs bytes.Buffer
				previous := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
				t.Cleanup(func() { slog.SetDefault(previous) })
				hit := false
				doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
					respond := func(status int, body string) (*http.Response, error) {
						return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
							Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
					}
					if route.match(request) {
						hit = true
						switch kind {
						case "status":
							return respond(http.StatusBadRequest, `{"message":"bad"}`)
						case "decode":
							return respond(http.StatusOK, `"not a list or an object"`)
						default:
							return nil, fmt.Errorf("Get %q: %w", request.URL.String(), context.Canceled)
						}
					}
					body, ok := catalogGuardOK[request.URL.Path]
					if !ok {
						return respond(http.StatusNotFound, `{}`)
					}
					return respond(http.StatusOK, body)
				})
				provider := "github"
				base := "https://api.github.com"
				if strings.HasPrefix(route.name, "gitlab") {
					provider, base = "gitlab", "https://gitlab.example.com"
				}
				client, err := providerfoundation.NewHTTPClient(provider, base, fakehttp.Client(doer),
					func(*http.Request) error { return nil },
					providerfoundation.RetryPolicy{MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
					providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
				if err != nil {
					t.Fatal(err)
				}
				runErr := route.run(t, client)
				if !hit {
					t.Fatal("route was never requested: the row does not drive it")
				}
				text := ""
				if runErr != nil {
					text = runErr.Error()
				}
				if route.route != "" && kind != "decode" && !strings.Contains(text+logs.String(), route.route) {
					t.Fatalf("neither the error nor the log names the route template %q:\nerr=%s\nlog=%s", route.route, text, logs.String())
				}
				for _, name := range forbidden {
					if strings.Contains(text, name) || strings.Contains(logs.String(), name) {
						t.Fatalf("%q reached the error or log:\nerr=%s\nlog=%s", name, text, logs.String())
					}
				}
			})
		}
	}
}
