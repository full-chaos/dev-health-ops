package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitLabSecurityRouteDoer struct {
	responses map[string]gitLabSecurityHTTPResponse
	requests  []*http.Request
}

type gitLabSecurityHTTPResponse struct {
	status  int
	headers http.Header
	body    string
}

func (doer *gitLabSecurityRouteDoer) Do(request *http.Request) (*http.Response, error) {
	doer.requests = append(doer.requests, request)
	response, ok := doer.responses[request.URL.Path]
	if !ok {
		return nil, errors.New("unexpected GitLab security request: " + request.URL.String())
	}
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	headers := response.headers
	if headers == nil {
		headers = http.Header{"Content-Type": []string{"application/json"}}
	}
	return &http.Response{
		StatusCode: status,
		Header:     headers,
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Request:    request,
	}, nil
}

func gitLabSecurityRouteClient(t *testing.T, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"gitlab", "https://gitlab.example", doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func gitLabSecurityRouteFixtures() map[string]gitLabSecurityHTTPResponse {
	return map[string]gitLabSecurityHTTPResponse{
		"/api/v4/projects/123": {body: `{"id":123,"name":"api","path_with_namespace":"acme/api"}`},
		"/api/v4/projects/123/vulnerability_findings": {headers: http.Header{"X-Next-Page": []string{"2"}}, body: `[
            {"id":1,"severity":"low","state":"detected","name":"old","created_at":"2026-07-21T10:00:00Z"},
            {"id":2,"severity":"high","state":"detected","name":"in-window","created_at":"2026-07-22T10:00:00Z","identifiers":[{"type":"other","name":"x"},{"type":"cve","name":"CVE-2026-0002"}],"links":{"url":"https://gitlab.example/a/2"}},
            {"id":3,"severity":"critical","state":"detected","name":"after","created_at":"2026-07-23T13:00:00Z"}
        ]`},
		"/api/v4/projects/123/dependencies": {headers: http.Header{"X-Next-Page": []string{"2"}}, body: `[
            {"name":"widget","vulnerabilities":[{"id":4,"severity":"medium","name":"dep","url":"https://gitlab.example/d/4"}]}
        ]`},
	}
}

func TestGitLabSecurityRouteMirrorsPythonSinglePageWindowAndEvidence(t *testing.T) {
	t.Parallel()
	doer := &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()}
	claim := nativeTestClaim("gitlab", "security")
	since := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim.SinceAt, claim.BeforeAt = &since, &before
	normalizedAt := time.Date(2026, 7, 23, 12, 30, 0, 987654321, time.UTC)

	batch, err := (GitLabSecurityRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabSecurityRouteClient(t, doer), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 3 || batch.Evidence.Requests != 3 || batch.Evidence.Pages != 2 ||
		batch.Evidence.Records != 1 || batch.Evidence.CapReached {
		t.Fatalf("requests=%d evidence=%+v", len(doer.requests), batch.Evidence)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v", batch.Watermark)
	}
	for _, request := range doer.requests[1:] {
		if request.URL.Query().Get("page") != "" || request.URL.Query().Get("per_page") != "100" {
			t.Fatalf("single-page request=%s", request.URL.String())
		}
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "security_alerts" ||
		batch.Effects[0].Recovery != EffectReadbackRequired || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row gitLabSecurityAlertRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row.AlertID != "gitlab_vuln:2" || row.Source != "gitlab_vulnerability" ||
		row.OrgID != claim.OrgID || row.PackageName != nil || row.CVEID == nil ||
		*row.CVEID != "CVE-2026-0002" || row.URL == nil ||
		!row.CreatedAt.Equal(time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)) ||
		!row.LastSynced.Equal(normalizedAt.UTC().Truncate(time.Millisecond)) {
		t.Fatalf("row=%+v", row)
	}
}

func TestGitLabSecurityRouteSlicesCombinedAlertsBeforeWindowFiltering(t *testing.T) {
	t.Parallel()
	doer := &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()}
	claim := nativeTestClaim("gitlab", "security")
	since := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	claim.SinceAt = &since
	batch, err := (GitLabSecurityRouteHandler{MaxAlerts: 1}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabSecurityRouteClient(t, doer), time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 0 {
		// The first vulnerability is before the claim window; Python applies
		// max_alerts in the code client before processors applies `since`.
		t.Fatalf("effects=%+v", batch.Effects)
	}
}

func TestGitLabSecurityRouteDegradesPlainOptionalErrors(t *testing.T) {
	t.Parallel()
	for path, status := range map[string]int{
		"/api/v4/projects/123/vulnerability_findings": http.StatusForbidden,
		"/api/v4/projects/123/dependencies":           http.StatusNotFound,
	} {
		doer := &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()}
		doer.responses[path] = gitLabSecurityHTTPResponse{status: status, body: `{"message":"unavailable"}`}
		batch, err := (GitLabSecurityRouteHandler{}).Collect(
			context.Background(), nativeTestClaim("gitlab", "security"), providerfoundation.Credential{},
			gitLabSecurityRouteClient(t, doer), time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
		)
		if err != nil {
			t.Fatalf("path=%s error=%v", path, err)
		}
		if len(batch.Effects) != 1 || batch.Watermark == nil {
			t.Fatalf("path=%s batch=%+v", path, batch)
		}
		if path == "/api/v4/projects/123/vulnerability_findings" &&
			(len(batch.Effects[0].Rows) != 1 || !strings.Contains(string(batch.Effects[0].Rows[0]), "gitlab_dep:4")) {
			t.Fatalf("path=%s rows=%s", path, batch.Effects[0].Rows)
		}
		if path == "/api/v4/projects/123/dependencies" && len(batch.Effects[0].Rows) != 3 {
			t.Fatalf("path=%s rows=%d", path, len(batch.Effects[0].Rows))
		}
	}
}

func TestGitLabSecurityRoutePropagatesRateLimitWithoutWatermark(t *testing.T) {
	t.Parallel()
	doer := &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()}
	doer.responses["/api/v4/projects/123/dependencies"] = gitLabSecurityHTTPResponse{
		status: http.StatusTooManyRequests, body: `{"message":"slow down"}`,
	}
	batch, err := (GitLabSecurityRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "security"), providerfoundation.Credential{},
		gitLabSecurityRouteClient(t, doer), time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("error=%v", err)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("rate-limited batch advanced state: %+v", batch)
	}
}

func TestGitLabSecurityRoutePropagatesHeaderQualifiedForbiddenRateLimit(t *testing.T) {
	t.Parallel()
	doer := &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()}
	doer.responses["/api/v4/projects/123/dependencies"] = gitLabSecurityHTTPResponse{
		status:  http.StatusForbidden,
		headers: http.Header{"RateLimit-Remaining": []string{"0"}},
		body:    `{"message":"throttled"}`,
	}
	batch, err := (GitLabSecurityRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "security"), providerfoundation.Credential{},
		gitLabSecurityRouteClient(t, doer), time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorAuthentication {
		t.Fatalf("error=%v", err)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("header-qualified forbidden batch advanced state: %+v", batch)
	}
}

func TestGitLabSecurityRouteMirrorsCoreFailureAsEmptySuccess(t *testing.T) {
	t.Parallel()
	doer := &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()}
	doer.responses["/api/v4/projects/123/vulnerability_findings"] = gitLabSecurityHTTPResponse{
		status: http.StatusBadRequest, body: `{"message":"invalid"}`,
	}
	batch, err := (GitLabSecurityRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "security"), providerfoundation.Credential{},
		gitLabSecurityRouteClient(t, doer), time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err != nil || len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 0 ||
		batch.Evidence.Requests != 2 || batch.Watermark == nil {
		t.Fatalf("error=%v batch=%+v", err, batch)
	}
}

func TestGitLabSecurityRouteRejectsProjectMismatchAndLeaseBeforeRequests(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		project    string
		leaseError error
		want       error
	}{
		{name: "project mismatch", project: `{"id":999,"path_with_namespace":"acme/api"}`, want: providerfoundation.ErrNormalizationInvalid},
		{name: "lease lost", project: `{"id":123,"path_with_namespace":"acme/api"}`, leaseError: providerfoundation.ErrLeaseLost, want: providerfoundation.ErrLeaseLost},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()}
			doer.responses["/api/v4/projects/123"] = gitLabSecurityHTTPResponse{body: test.project}
			client := gitLabSecurityRouteClient(t, doer)
			if test.leaseError != nil {
				client.Lease = providerfoundation.LeaseGuardFunc(func(context.Context) error { return test.leaseError })
			}
			batch, err := (GitLabSecurityRouteHandler{}).Collect(
				context.Background(), nativeTestClaim("gitlab", "security"), providerfoundation.Credential{},
				client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
			if len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("incomplete batch=%+v", batch)
			}
			if test.leaseError != nil && len(doer.requests) != 0 {
				t.Fatalf("lease made requests=%d", len(doer.requests))
			}
		})
	}
}

func TestGitLabSecurityRouteRejectsInvalidConfigurationBeforeRequests(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	for _, test := range []struct {
		name       string
		claim      Claim
		client     *providerfoundation.HTTPClient
		normalized time.Time
		maxAlerts  int
	}{
		{
			name: "wrong_claim_provider",
			claim: func() Claim {
				claim := nativeTestClaim("gitlab", "security")
				claim.Provider = "github"
				return claim
			}(),
			client: gitLabSecurityRouteClient(t, &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()}),
		},
		{
			name:   "wrong_claim_dataset",
			claim:  nativeTestClaim("gitlab", "commits"),
			client: gitLabSecurityRouteClient(t, &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()}),
		},
		{
			name:   "nil_client",
			claim:  nativeTestClaim("gitlab", "security"),
			client: nil,
		},
		{
			name:  "wrong_client_provider",
			claim: nativeTestClaim("gitlab", "security"),
			client: func() *providerfoundation.HTTPClient {
				client := gitLabSecurityRouteClient(t, &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()})
				client.Provider = "github"
				return client
			}(),
		},
		{
			name:  "nil_client_base_URL",
			claim: nativeTestClaim("gitlab", "security"),
			client: func() *providerfoundation.HTTPClient {
				client := gitLabSecurityRouteClient(t, &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()})
				client.BaseURL = nil
				return client
			}(),
		},
		{
			name:       "zero_normalized_at",
			claim:      nativeTestClaim("gitlab", "security"),
			client:     gitLabSecurityRouteClient(t, &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()}),
			normalized: time.Time{},
		},
		{
			name:      "negative_max_alerts",
			claim:     nativeTestClaim("gitlab", "security"),
			client:    gitLabSecurityRouteClient(t, &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()}),
			maxAlerts: -1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := test.normalized
			if now.IsZero() && test.name != "zero_normalized_at" {
				now = normalizedAt
			}
			batch, err := (GitLabSecurityRouteHandler{MaxAlerts: test.maxAlerts}).Collect(
				context.Background(), test.claim, providerfoundation.Credential{}, test.client, now,
			)
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v", err)
			}
			if len(batch.Effects) != 0 || len(batch.Evidence.Provider) != 0 {
				t.Fatalf("invalid configuration returned batch=%+v", batch)
			}
			if test.client != nil {
				if doer, ok := test.client.Doer.(*gitLabSecurityRouteDoer); ok && len(doer.requests) != 0 {
					t.Fatalf("invalid configuration made %d requests", len(doer.requests))
				}
			}
		})
	}
}

func TestGitLabSecurityRouteNormalizesDependencyCreatedAtAndPackage(t *testing.T) {
	t.Parallel()
	doer := &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()}
	claim := nativeTestClaim("gitlab", "security")
	normalizedAt := time.Date(2026, 7, 23, 12, 30, 0, 987654321, time.UTC)
	batch, err := (GitLabSecurityRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabSecurityRouteClient(t, doer), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range batch.Effects[0].Rows {
		var row gitLabSecurityAlertRow
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		if row.AlertID != "gitlab_dep:4" {
			continue
		}
		if row.PackageName == nil || *row.PackageName != "widget" || row.State != nil ||
			!row.CreatedAt.Equal(normalizedAt.UTC().Truncate(time.Millisecond)) {
			t.Fatalf("dependency row=%+v", row)
		}
		return
	}
	t.Fatal("dependency row was not emitted")
}
