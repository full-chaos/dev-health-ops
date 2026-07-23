package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestNativeRESTHandlerCoversReferenceAndWorkItemDatasetMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		provider, dataset string
		records, requests int
	}{
		{"github", "repo-metadata", 1, 1},
		{"github", "work-items", 1, 1},
		{"github", "work-item-labels", 1, 1},
		{"github", "work-item-projects", 1, 1},
		{"github", "work-item-history", 1, 2},
		{"github", "work-item-comments", 1, 2},
		{"gitlab", "repo-metadata", 1, 1},
		{"gitlab", "work-items", 2, 2},
		{"gitlab", "work-item-labels", 1, 1},
		{"gitlab", "work-item-projects", 1, 1},
		{"gitlab", "work-item-history", 4, 6},
		{"gitlab", "work-item-comments", 2, 4},
	}
	for _, test := range tests {
		test := test
		t.Run(test.provider+"/"+test.dataset, func(t *testing.T) {
			t.Parallel()
			doer := &fixtureDoer{t: t, provider: test.provider}
			client, err := providerfoundation.NewHTTPClient(
				test.provider,
				"https://fixture.test",
				doer,
				func(*http.Request) error { return nil },
				providerfoundation.RetryPolicy{
					MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
				},
				providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
			)
			if err != nil {
				t.Fatal(err)
			}
			claim := nativeTestClaim(test.provider, test.dataset)
			handler := NativeRESTHandler{Now: func() time.Time {
				return time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
			}}
			result, err := handler.Fetch(context.Background(), claim, client)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Envelopes) != test.records ||
				result.Evidence.Records != test.records ||
				result.Evidence.Requests != test.requests ||
				result.Evidence.Pages != test.requests ||
				doer.requests != test.requests ||
				result.Evidence.CapReached {
				t.Fatalf("result=%+v actual_requests=%d", result.Evidence, doer.requests)
			}
			for _, envelope := range result.Envelopes {
				if err := envelope.Validate(); err != nil {
					t.Fatal(err)
				}
				if envelope.Provider != test.provider || envelope.OrgID != "org-acme" ||
					envelope.IntegrationID != firstIntegrationID {
					t.Fatalf("envelope=%+v", envelope)
				}
			}
		})
	}
}

func TestNativeRESTHandlerRequiresPythonCompatibilityForCodeDataset(t *testing.T) {
	t.Parallel()
	handler := NativeRESTHandler{}
	claim := nativeTestClaim("github", "commits")
	client, err := providerfoundation.NewHTTPClient(
		"github", "https://example.test", noRequestDoer{},
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handler.Fetch(context.Background(), claim, client); err != ErrCompatibilityRequired {
		t.Fatalf("error=%v", err)
	}
}

func nativeTestClaim(provider, dataset string) Claim {
	capability, _ := Capability(provider, dataset)
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 7, 31, 23, 59, 59, 0, time.UTC)
	externalID, sourceName := "acme/api", "acme/api"
	if provider == "gitlab" {
		externalID = "123"
	}
	return Claim{
		Unit: Unit{
			ID: firstUnitID, SyncRunID: firstRunID, OrgID: "org-acme",
			IntegrationID: firstIntegrationID, SourceID: firstSourceID,
			SourceExternalID: externalID, SourceName: sourceName,
			Provider: provider, Dataset: dataset, CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before,
			CredentialID: firstCredentialID, AuthSource: "integration_credential",
		},
		Owner: "66666666-6666-4666-8666-666666666666", Attempt: 1,
		LeaseExpiresAt: time.Date(2026, 7, 23, 13, 0, 0, 0, time.UTC),
	}
}

func assertWindowQuery(t *testing.T, provider string, request *http.Request) {
	t.Helper()
	if !strings.HasSuffix(request.URL.Path, "/issues") &&
		!strings.HasSuffix(request.URL.Path, "/merge_requests") {
		return
	}
	query := request.URL.Query()
	if provider == "github" {
		if query.Get("since") != "2026-07-01T00:00:00Z" {
			t.Errorf("GitHub since query=%q", request.URL.RawQuery)
		}
		return
	}
	if query.Get("updated_after") != "2026-07-01T00:00:00Z" ||
		query.Get("updated_before") != "2026-07-31T23:59:59Z" {
		t.Errorf("GitLab window query=%q", request.URL.RawQuery)
	}
}

func fixtureResponse(t *testing.T, provider, path string) string {
	t.Helper()
	if provider == "github" {
		switch {
		case path == "/repos/acme/api":
			return `{"id":1,"name":"api","full_name":"acme/api","html_url":"https://github.example/acme/api","default_branch":"main","archived":false,"updated_at":"2026-07-20T10:00:00Z"}`
		case path == "/repos/acme/api/issues":
			return `[
				{"number":42,"title":"Bound retries","state":"open","created_at":"2026-07-10T10:00:00Z","updated_at":"2026-07-20T10:00:00Z","labels":[{"name":"in progress"},{"name":"bug"}]},
				{"number":43,"title":"Issue endpoint PR","state":"open","created_at":"2026-07-11T10:00:00Z","updated_at":"2026-07-21T10:00:00Z","pull_request":{"url":"https://api.github.example/pulls/43"},"labels":[]}
			]`
		case path == "/repos/acme/api/pulls":
			return `[{"number":43,"title":"Ship retry bound","state":"open","draft":false,"created_at":"2026-07-11T10:00:00Z","updated_at":"2026-07-21T10:00:00Z","merged_at":null,"labels":[]}]`
		case path == "/repos/acme/api/labels":
			return `[{"id":10,"name":"bug","color":"d73a4a"}]`
		case path == "/repos/acme/api/milestones":
			return `[{"id":20,"number":3,"title":"July","state":"open","updated_at":"2026-07-19T10:00:00Z","due_on":"2026-07-31T00:00:00Z"}]`
		case path == "/repos/acme/api/issues/42/events":
			return `[{"id":30,"event":"closed","created_at":"2026-07-21T10:00:00Z"}]`
		case path == "/repos/acme/api/issues/42/comments":
			return `[{"id":40,"body":"done","created_at":"2026-07-21T11:00:00Z","updated_at":"2026-07-21T11:01:00Z"}]`
		}
	}
	if provider == "gitlab" {
		switch {
		case path == "/api/v4/projects/123":
			return `{"id":123,"name":"api","path_with_namespace":"acme/api","web_url":"https://gitlab.example/acme/api","default_branch":"main","archived":false,"last_activity_at":"2026-07-20T10:00:00Z"}`
		case path == "/api/v4/projects/123/issues":
			return `[{"iid":7,"title":"Bound retries","state":"opened","created_at":"2026-07-10T10:00:00Z","updated_at":"2026-07-20T10:00:00Z","labels":["bug"]}]`
		case path == "/api/v4/projects/123/merge_requests":
			return `[{"iid":8,"title":"Ship retry bound","state":"merged","created_at":"2026-07-11T10:00:00Z","updated_at":"2026-07-22T10:00:00Z","merged_at":"2026-07-22T10:00:00Z","labels":[]}]`
		case path == "/api/v4/projects/123/labels":
			return `[{"id":10,"name":"bug","color":"#d73a4a"}]`
		case path == "/api/v4/projects/123/milestones":
			return `[{"id":20,"iid":3,"title":"July","state":"active","updated_at":"2026-07-19T10:00:00Z","due_date":"2026-07-31"}]`
		case strings.HasSuffix(path, "/resource_state_events"):
			return `[{"id":30,"state":"closed","created_at":"2026-07-21T10:00:00Z"}]`
		case strings.HasSuffix(path, "/resource_label_events"):
			return `[{"id":31,"action":"add","created_at":"2026-07-21T10:01:00Z","label":{"name":"done"}}]`
		case strings.HasSuffix(path, "/notes"):
			return `[{"id":40,"body":"done","system":false,"created_at":"2026-07-21T11:00:00Z","updated_at":"2026-07-21T11:01:00Z"}]`
		}
	}
	t.Fatalf("unexpected fixture request %s %s", provider, path)
	return ""
}

func TestGitHubWorkItemsHonorFrozenSyncPRsFlag(t *testing.T) {
	t.Parallel()
	handler := NativeRESTHandler{}
	fetch := func(syncPRs bool) (FetchResult, int) {
		t.Helper()
		doer := &fixtureDoer{t: t, provider: "github"}
		client, err := providerfoundation.NewHTTPClient(
			"github", "https://fixture.test", doer,
			func(*http.Request) error { return nil },
			providerfoundation.RetryPolicy{
				MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
			},
			providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		)
		if err != nil {
			t.Fatal(err)
		}
		claim := nativeTestClaim("github", "work-items")
		claim.ProcessorFlags = map[string]bool{"sync_prs": syncPRs}
		result, err := handler.Fetch(context.Background(), claim, client)
		if err != nil {
			t.Fatal(err)
		}
		return result, doer.requests
	}
	issuesOnly, issuesRequests := fetch(false)
	if issuesRequests != 1 || len(issuesOnly.Envelopes) != 1 ||
		issuesOnly.Envelopes[0].SourceID != "gh:acme/api#42" {
		t.Fatalf("issues_only=%+v requests=%d", issuesOnly, issuesRequests)
	}
	withPRs, withPRRequests := fetch(true)
	if withPRRequests != 2 || len(withPRs.Envelopes) != 2 ||
		withPRs.Envelopes[1].SourceID != "ghpr:acme/api#43" ||
		withPRs.Envelopes[1].Attributes["type"] != "pr" {
		t.Fatalf("with_prs=%+v requests=%d", withPRs, withPRRequests)
	}
}

type noRequestDoer struct{}

func (noRequestDoer) Do(*http.Request) (*http.Response, error) {
	panic("provider request was not expected")
}

type fixtureDoer struct {
	t             *testing.T
	provider      string
	authorization string
	requests      int
}

func (doer *fixtureDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests++
	if doer.authorization != "" && request.Header.Get("Authorization") != doer.authorization {
		doer.t.Errorf("authorization=%q", request.Header.Get("Authorization"))
	}
	assertWindowQuery(doer.t, doer.provider, request)
	body := fixtureResponse(doer.t, doer.provider, request.URL.Path)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}
