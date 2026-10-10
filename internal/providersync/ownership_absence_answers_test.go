package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type absenceAnswerDoer func(*http.Request) (*http.Response, error)

func (doer absenceAnswerDoer) Do(request *http.Request) (*http.Response, error) { return doer(request) }

// The Jira direct answer, clause by clause: the question uses the identifier
// the row was built from (the native id, or the KEY of a row of the retired
// key-built form), it asks for live and archived projects, and only the
// provider's plain "no such project" (an empty answer marked as the whole
// answer) proves an absence. A 400, an error body, an entry of another project
// and an empty answer with no end signal prove nothing.
func TestTheJiraAnswerAsksWithTheIdentifierTheRowWasBuiltFrom(t *testing.T) {
	const org = "org-1"
	native := OwnershipSnapshotRow{TeamID: "T", ProjectID: testPID("10001"), Source: jiraTeamCatalogLegacySource}
	keyBuilt := OwnershipSnapshotRow{TeamID: "T", ProjectID: testPID(jiraKeyBuiltProjectIDPrefix(org) + "OPS"), Source: jiraTeamCatalogLegacySource}
	for _, test := range []struct {
		name      string
		row       OwnershipSnapshotRow
		status    int
		body      string
		want      SnapshotAbsence
		parameter string // the parameter that must name the project
		value     string
	}{
		{"native id, the project is there", native, 200, `{"values":[{"id":"10001","key":"OPS","name":"Ops"}],"isLast":true,"total":1}`, SnapshotFactStillHeld, "id", "10001"},
		{"native id, no such project (isLast)", native, 200, `{"values":[],"isLast":true}`, SnapshotAbsenceProven, "id", "10001"},
		{"native id, no such project (total 0)", native, 200, `{"values":[],"total":0}`, SnapshotAbsenceProven, "id", "10001"},
		{"native id, an empty answer with no end signal", native, 200, `{"values":[]}`, SnapshotAbsenceNotProven, "id", "10001"},
		{"native id, an empty answer that is not the last page", native, 200, `{"values":[],"isLast":false,"total":3}`, SnapshotAbsenceNotProven, "id", "10001"},
		{"native id, the answer holds another project", native, 200, `{"values":[{"id":"10002","key":"WEB","name":"Web"}],"isLast":true,"total":1}`, SnapshotAbsenceNotProven, "id", "10001"},
		{"native id, an error body under 200", native, 200, `{"errorMessages":["no"],"values":[],"isLast":true}`, SnapshotAbsenceNotProven, "id", "10001"},
		{"native id, 400", native, 400, `{"errorMessages":["The value is not a valid project id"]}`, SnapshotAbsenceNotProven, "id", "10001"},
		{"native id, 500", native, 500, `{}`, SnapshotAbsenceNotProven, "id", "10001"},
		{"key-built id, the project is there under its native id", keyBuilt, 200, `{"values":[{"id":"10001","key":"OPS","name":"Ops"}],"isLast":true,"total":1}`, SnapshotFactStillHeld, "keys", "OPS"},
		{"key-built id, no such project", keyBuilt, 200, `{"values":[],"isLast":true,"total":0}`, SnapshotAbsenceProven, "keys", "OPS"},
		{"key-built id, the answer holds another key", keyBuilt, 200, `{"values":[{"id":"10002","key":"OPSX","name":"Other"}],"isLast":true,"total":1}`, SnapshotAbsenceNotProven, "keys", "OPS"},
		{"key-built id, 400", keyBuilt, 400, `{"errorMessages":["bad"]}`, SnapshotAbsenceNotProven, "keys", "OPS"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var asked *http.Request
			client := jiraTeamCatalogTestClient(t, absenceAnswerDoer(func(request *http.Request) (*http.Response, error) {
				asked = request
				return &http.Response{StatusCode: test.status, Header: http.Header{"Content-Type": []string{"application/json"}},
					Body: io.NopCloser(strings.NewReader(test.body)), Request: request}, nil
			}))
			got := jiraProjectAbsence{client: client, orgID: org}.OwnershipAbsence(context.Background(), test.row)
			if got != test.want {
				t.Errorf("the answer is %d, want %d", got, test.want)
			}
			if asked == nil {
				t.Fatal("no request was made")
			}
			query := asked.URL.Query()
			if asked.URL.Path != "/rest/api/3/project/search" || query.Get(test.parameter) != test.value {
				t.Errorf("the request is %s, want the project search with %s=%s", asked.URL, test.parameter, test.value)
			}
			// One identifier only, and never the organization id.
			other := map[string]string{"id": "keys", "keys": "id"}[test.parameter]
			if query.Has(other) || strings.Contains(asked.URL.RawQuery, org) {
				t.Errorf("the request %s names the project a second way or carries the organization id", asked.URL.RawQuery)
			}
			if statuses := query["status"]; len(statuses) != 2 || statuses[0] != "live" || statuses[1] != "archived" {
				t.Errorf("the request asks for the states %v, want live and archived", statuses)
			}
		})
	}
	// A row with no project id, and a prover with no client, ask nothing.
	if got := (jiraProjectAbsence{orgID: org}).OwnershipAbsence(context.Background(), native); got != SnapshotAbsenceNotProven {
		t.Errorf("a prover with no client answers %d, want not proven", got)
	}
}

// The GitHub direct answer: GitHub's own 404 is "the team does not have the
// repository"; 204 and 200 say it has; any other answer proves nothing. A 404
// whose body is not GitHub's error answer comes from something in front of
// the host (a gateway, a proxy) and proves nothing.
func TestTheGitHubAnswerForOneGrant(t *testing.T) {
	row := OwnershipSnapshotRow{TeamID: "gh:platform", ProjectID: testPID("acme/api"), Source: githubTeamCatalogSource}
	for _, test := range []struct {
		name   string
		status int
		body   string
		want   SnapshotAbsence
	}{
		{"204", 204, ``, SnapshotFactStillHeld}, {"200 with the repository", 200, `{}`, SnapshotFactStillHeld},
		{"404 of the provider", 404, `{"message":"Not Found","documentation_url":"https://docs.github.com/rest","status":"404"}`, SnapshotAbsenceProven},
		{"404 with a page of a gateway", 404, `<html><body>404 Not Found</body></html>`, SnapshotAbsenceNotProven},
		{"404 with an empty object", 404, `{}`, SnapshotAbsenceNotProven},
		{"404 with no body", 404, ``, SnapshotAbsenceNotProven},
		{"404 with another message", 404, `{"message":"no route"}`, SnapshotAbsenceNotProven},
		{"404 with a body that is not an object", 404, `"Not Found"`, SnapshotAbsenceNotProven},
		{"403", 403, `{"message":"Not Found"}`, SnapshotAbsenceNotProven}, {"500", 500, `{"message":"Not Found"}`, SnapshotAbsenceNotProven},
	} {
		t.Run(test.name, func(t *testing.T) {
			var path string
			client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com",
				fakehttp.Client(absenceAnswerDoer(func(request *http.Request) (*http.Response, error) {
					path = request.URL.Path
					return &http.Response{StatusCode: test.status, Header: http.Header{"Content-Type": []string{"application/json"}},
						Body: io.NopCloser(strings.NewReader(test.body)), Request: request}, nil
				})), func(*http.Request) error { return nil },
				providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: 1, MaxWait: 1},
				providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
			if err != nil {
				t.Fatal(err)
			}
			if got := (githubRepoGrantAbsence{client: client, org: "acme"}).OwnershipAbsence(context.Background(), row); got != test.want {
				t.Errorf("the answer for HTTP %d is %d, want %d", test.status, got, test.want)
			}
			if path != "/orgs/acme/teams/platform/repos/acme/api" {
				t.Errorf("the request path is %q", path)
			}
		})
	}
}

// The GitLab direct answer: a search answer that does not hold the project
// proves the absence only when it is the WHOLE answer by the paginator's own
// reading of GitLab's end signal. An answer that announces a next page in any
// way the paginator knows (X-Next-Page, a Link to a next page, a full page)
// leaves the project possibly on a page that was not asked for.
func TestTheGitLabAnswerForOneProject(t *testing.T) {
	row := OwnershipSnapshotRow{TeamID: "gl:acme/platform", ProjectID: testPID("acme/platform/api"), Source: gitlabTeamCatalogSource}
	other := `[{"id":7,"path_with_namespace":"acme/platform/api-gateway"}]`
	full := "[" + strings.TrimSuffix(strings.Repeat(`{"id":7,"path_with_namespace":"acme/platform/api-gateway"},`, gitlabAbsenceSearchPerPage), ",") + "]"
	for _, test := range []struct {
		name   string
		header http.Header
		body   string
		want   SnapshotAbsence
	}{
		{"the project is in the answer", http.Header{"X-Next-Page": {""}}, `[{"id":1,"path_with_namespace":"acme/platform/api"}]`, SnapshotFactStillHeld},
		{"an empty answer with the end signal", http.Header{"X-Next-Page": {""}}, `[]`, SnapshotAbsenceProven},
		{"other projects only, with the end signal", http.Header{"X-Next-Page": {""}}, other, SnapshotAbsenceProven},
		{"an empty answer with the end signal and a Link to a next page", http.Header{"X-Next-Page": {""},
			"Link": {`<https://gitlab.example/api/v4/groups/1/projects?page=2>; rel="next"`}}, `[]`, SnapshotAbsenceNotProven},
		{"an empty answer with the end signal and a Link that does not parse", http.Header{"X-Next-Page": {""}, "Link": {`<<<`}}, `[]`, SnapshotAbsenceNotProven},
		{"an empty answer with a Link to the first page only", http.Header{"X-Next-Page": {""},
			"Link": {`<https://gitlab.example/api/v4/groups/1/projects?page=1>; rel="first"`}}, `[]`, SnapshotAbsenceProven},
		{"an empty answer with no end signal", http.Header{}, `[]`, SnapshotAbsenceNotProven},
		{"an empty answer with a next page number", http.Header{"X-Next-Page": {"2"}}, `[]`, SnapshotAbsenceNotProven},
		{"an empty answer with two end headers", http.Header{"X-Next-Page": {"", ""}}, `[]`, SnapshotAbsenceNotProven},
		{"a full page of other projects with the end signal", http.Header{"X-Next-Page": {""}}, full, SnapshotAbsenceNotProven},
		{"a body that is not a list", http.Header{"X-Next-Page": {""}}, `{"message":"404"}`, SnapshotAbsenceNotProven},
		{"null", http.Header{"X-Next-Page": {""}}, `null`, SnapshotAbsenceNotProven},
	} {
		t.Run(test.name, func(t *testing.T) {
			var query string
			client, err := providerfoundation.NewHTTPClient("gitlab", "https://gitlab.example",
				fakehttp.Client(absenceAnswerDoer(func(request *http.Request) (*http.Response, error) {
					query = request.URL.RawQuery
					header := test.header.Clone()
					header.Set("Content-Type", "application/json")
					return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(strings.NewReader(test.body)), Request: request}, nil
				})), func(*http.Request) error { return nil },
				providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: 1, MaxWait: 1},
				providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
			if err != nil {
				t.Fatal(err)
			}
			if got := (gitlabGroupProjectAbsence{client: client}).OwnershipAbsence(context.Background(), row); got != test.want {
				t.Errorf("the answer is %d, want %d", got, test.want)
			}
			if query != "per_page=100&search=api" {
				t.Errorf("the request query is %q", query)
			}
		})
	}
}
