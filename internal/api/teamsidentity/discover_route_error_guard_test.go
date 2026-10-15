package teamsidentity

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-9761 structural guard: every GitHub and GitLab discovery and member
// discovery route is driven through all three error kinds (HTTP status,
// malformed body, transport failure) with a planted org, team, group and login
// in the request path. The planted names must appear in neither the returned
// error text (handlers.internal logs err.Error() verbatim) nor the captured
// log. A new route in discover_github.go, discover_gitlab.go or
// members_discover.go needs a row in guardRoutes.
type guardRoute struct {
	name  string
	run   func(context.Context) error
	match func(*http.Request) bool
}

func pathIs(path string) func(*http.Request) bool {
	return func(request *http.Request) bool { return request.URL.Path == path }
}

func guardRoutes() []guardRoute {
	token := secrets.NewValue("token")
	githubDiscover := func(ctx context.Context) error {
		_, err := discoverGitHub(ctx, githubTestCredential(), "secret-org")
		return err
	}
	githubMembers := func(ctx context.Context) error {
		_, err := discoverMembersGitHub(ctx, token, "secret-org", "secret-team")
		return err
	}
	gitlabDiscover := func(ctx context.Context) error {
		_, _, _, err := discoverGitLab(ctx, gitlabTestCredential(), "secret/group")
		return err
	}
	gitlabMembers := func(ctx context.Context) error {
		_, err := discoverMembersGitLab(ctx, token, "", "secret/group")
		return err
	}
	flat := func(request *http.Request) bool {
		return request.URL.Path == "/api/v4/groups/7/projects" && strings.Contains(request.URL.RawQuery, "include_subgroups")
	}
	rootProjects := func(request *http.Request) bool {
		return request.URL.Path == "/api/v4/groups/7/projects" && !strings.Contains(request.URL.RawQuery, "include_subgroups")
	}
	return []guardRoute{
		{"github discover: team list", githubDiscover, pathIs("/orgs/secret-org/teams")},
		{"github discover: team repos", githubDiscover, pathIs("/orgs/secret-org/teams/secret-team/repos")},
		{"github discover: team detail", githubDiscover, pathIs("/orgs/secret-org/teams/secret-team")},
		{"github members: team detail", githubMembers, pathIs("/orgs/secret-org/teams/secret-team")},
		{"github members: member list", githubMembers, pathIs("/orgs/secret-org/teams/secret-team/members")},
		{"github members: user", githubMembers, pathIs("/users/secret-login")},
		{"gitlab discover: group", gitlabDiscover, pathIs("/api/v4/groups/secret/group")},
		{"gitlab discover: subgroups", gitlabDiscover, pathIs("/api/v4/groups/secret/group/subgroups")},
		{"gitlab discover: root projects", gitlabDiscover, rootProjects},
		{"gitlab discover: subgroup projects", gitlabDiscover, pathIs("/api/v4/groups/8/projects")},
		{"gitlab discover: all projects", gitlabDiscover, flat},
		{"gitlab members: group", gitlabMembers, pathIs("/api/v4/groups/secret/group")},
		{"gitlab members: member list", gitlabMembers, pathIs("/api/v4/groups/7/members")},
	}
}

var guardOK = map[string]string{
	"/orgs/secret-org/teams":                     `[{"slug":"secret-team","name":"T"}]`,
	"/orgs/secret-org/teams/secret-team/repos":   `[]`,
	"/orgs/secret-org/teams/secret-team":         `{"members_count":1}`,
	"/orgs/secret-org/teams/secret-team/members": `[{"login":"secret-login"}]`,
	"/users/secret-login":                        `{"login":"secret-login","name":"N","email":"e@example.test"}`,
	"/api/v4/groups/secret/group":                `{"id":7,"full_path":"secret/group","name":"G"}`,
	"/api/v4/groups/secret/group/subgroups":      `[{"id":8,"full_path":"secret/group/sub","name":"S"}]`,
	"/api/v4/groups/7/projects":                  `[{"id":1,"path_with_namespace":"secret/group/p"}]`,
	"/api/v4/groups/8/projects":                  `[{"id":2,"path_with_namespace":"secret/group/sub/p"}]`,
	"/api/v4/groups/7/members":                   `[{"username":"secret-login","name":"N"}]`,
}

type guardDoer struct {
	kind  string
	match func(*http.Request) bool
	hit   bool
}

func (d *guardDoer) Do(request *http.Request) (*http.Response, error) {
	respond := func(status int, body string) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
	if d.match(request) {
		d.hit = true
		switch d.kind {
		case "status":
			return respond(http.StatusBadRequest, `{"message":"bad"}`)
		case "decode":
			return respond(http.StatusOK, `"not a list or an object"`)
		default:
			return nil, fmt.Errorf("Get %q: %w", request.URL.String(), context.Canceled)
		}
	}
	body, ok := guardOK[request.URL.Path]
	if !ok {
		return respond(http.StatusNotFound, `{}`)
	}
	return respond(http.StatusOK, body)
}

func TestDiscoveryRoutesNeverCarryPlantedNamesInErrorsOrLogs(t *testing.T) {
	forbidden := []string{"secret-org", "secret-team", "secret-login", "secret/group", "secret%2Fgroup", "/api/v4/groups/", "/orgs/secret"}
	for _, route := range guardRoutes() {
		for _, kind := range []string{"status", "decode", "transport"} {
			t.Run(route.name+"/"+kind, func(t *testing.T) {
				var logs bytes.Buffer
				previous := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
				t.Cleanup(func() { slog.SetDefault(previous) })
				doer := &guardDoer{kind: kind, match: route.match}
				withDiscoveryClient(t, fakehttp.Client(doer))
				err := route.run(context.Background())
				if !doer.hit {
					t.Fatalf("route was never requested: the row does not drive it")
				}
				if err == nil {
					t.Fatalf("want a %s error", kind)
				}
				text := err.Error()
				for _, name := range forbidden {
					if strings.Contains(text, name) || strings.Contains(logs.String(), name) {
						t.Fatalf("%q reached the error or log:\nerr=%s\nlog=%s", name, text, logs.String())
					}
				}
			})
		}
	}
}
