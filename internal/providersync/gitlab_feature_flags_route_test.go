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

type gitLabFeatureFlagsResponse struct {
	status  int
	body    string
	headers http.Header
}

type gitLabFeatureFlagsDoer struct {
	t         *testing.T
	responses []gitLabFeatureFlagsResponse
	requests  []*http.Request
}

func (doer *gitLabFeatureFlagsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request)
	if len(doer.responses) == 0 {
		doer.t.Fatalf("unexpected request %s", request.URL.RequestURI())
	}
	response := doer.responses[0]
	doer.responses = doer.responses[1:]
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Header:     response.headers,
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Request:    request,
	}, nil
}

func gitLabFeatureFlagsClient(
	t *testing.T,
	doer providerfoundation.HTTPDoer,
	retry providerfoundation.RetryPolicy,
) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"gitlab", "https://gitlab.example", doer,
		func(*http.Request) error { return nil }, retry,
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestGitLabFeatureFlagsRouteRejectsInvalidConfigurationBeforeRequests(t *testing.T) {
	validAt := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	validRetry := providerfoundation.RetryPolicy{
		MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	}

	tests := []struct {
		name       string
		ctx        context.Context
		claim      func() Claim
		client     func(t *testing.T, doer *gitLabFeatureFlagsDoer) *providerfoundation.HTTPClient
		normalized time.Time
		handler    GitLabFeatureFlagsRouteHandler
	}{
		{name: "nil context", ctx: nil},
		{name: "invalid claim", claim: func() Claim {
			claim := nativeTestClaim("gitlab", "feature-flags")
			claim.OrgID = ""
			return claim
		}},
		{name: "wrong claim provider", claim: func() Claim {
			return nativeTestClaim("github", "feature-flags")
		}},
		{name: "wrong claim dataset", claim: func() Claim {
			return nativeTestClaim("gitlab", "commits")
		}},
		{name: "nil client", client: func(*testing.T, *gitLabFeatureFlagsDoer) *providerfoundation.HTTPClient {
			return nil
		}},
		{name: "wrong client provider", client: func(t *testing.T, doer *gitLabFeatureFlagsDoer) *providerfoundation.HTTPClient {
			client := gitLabFeatureFlagsClient(t, doer, validRetry)
			client.Provider = "github"
			return client
		}},
		{name: "nil client base URL", client: func(t *testing.T, doer *gitLabFeatureFlagsDoer) *providerfoundation.HTTPClient {
			client := gitLabFeatureFlagsClient(t, doer, validRetry)
			client.BaseURL = nil
			return client
		}},
		{name: "nil client doer", client: func(t *testing.T, doer *gitLabFeatureFlagsDoer) *providerfoundation.HTTPClient {
			client := gitLabFeatureFlagsClient(t, doer, validRetry)
			client.Doer = nil
			return client
		}},
		{name: "zero normalized time", normalized: time.Time{}},
		{name: "blank source id", claim: func() Claim {
			claim := nativeTestClaim("gitlab", "feature-flags")
			claim.SourceExternalID = "  "
			return claim
		}},
		{name: "negative max pages", handler: GitLabFeatureFlagsRouteHandler{MaxPages: -1}},
		{name: "excessive max pages", handler: GitLabFeatureFlagsRouteHandler{MaxPages: gitLabFeatureFlagsMaxPages + 1}},
		{name: "negative page size", handler: GitLabFeatureFlagsRouteHandler{PerPage: -1}},
		{name: "excessive page size", handler: GitLabFeatureFlagsRouteHandler{PerPage: gitLabFeatureFlagsDefaultPerPage + 1}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			doer := &gitLabFeatureFlagsDoer{t: t}
			ctx := test.ctx
			if test.name != "nil context" {
				ctx = context.Background()
			}
			claim := nativeTestClaim("gitlab", "feature-flags")
			if test.claim != nil {
				claim = test.claim()
			}
			client := gitLabFeatureFlagsClient(t, doer, validRetry)
			if test.client != nil {
				client = test.client(t, doer)
			}
			normalizedAt := test.normalized
			if test.name != "zero normalized time" {
				normalizedAt = validAt
			}

			batch, err := test.handler.Collect(
				ctx, claim, providerfoundation.Credential{}, client, normalizedAt,
			)
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v want ErrInvalidConfiguration", err)
			}
			if len(doer.requests) != 0 || len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("invalid configuration reached provider or effects: requests=%d batch=%+v", len(doer.requests), batch)
			}
		})
	}
}

func TestGitLabFeatureFlagsRouteMirrorsPythonOrderScopesAndEffects(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 8, 10, 12, 0, 0, 123456789, time.UTC)
	doer := &gitLabFeatureFlagsDoer{t: t, responses: []gitLabFeatureFlagsResponse{
		{body: `[
			{"name":"checkout","active":true,"version":"new_version_flag","created_at":"2026-08-01T10:00:00Z","updated_at":"2026-08-09T10:00:00Z","strategies":[{"scopes":[{"environment_scope":"production"},{"environment_scope":"staging"}]}]},
			{"name":"search","active":false,"created_at":"2026-08-02T10:00:00Z","updated_at":"2026-08-09T11:00:00Z","strategies":[{"scopes":[{"environment_scope":"*"}]}]}
		]`, headers: http.Header{"X-Next-Page": {"2"}}},
		{body: `[{"key":"billing","active":true,"updated_at":"2026-08-09T12:00:00Z","strategies":[]}]`},
		{body: `{"path_with_namespace":"","path":"acme/api"}`},
	}}
	claim := nativeTestClaim("gitlab", "feature-flags")
	claim.SourceExternalID = "group/project"
	claim.SourceName = "group/project"
	batch, err := (GitLabFeatureFlagsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabFeatureFlagsClient(t, doer, providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		}), normalizedAt,
	)
	if err != nil {
		t.Fatalf("collect error=%v requests=%v", err, requestURLs(doer.requests))
	}
	if len(batch.Effects) != 3 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	byDestination := make(map[string]EffectBatch, len(batch.Effects))
	for _, effect := range batch.Effects {
		byDestination[effect.Destination] = effect
	}
	for _, destination := range []string{"feature_flag", "feature_flag_event", "work_graph_edges"} {
		if _, ok := byDestination[destination]; !ok {
			t.Fatalf("missing destination %q: %+v", destination, byDestination)
		}
	}
	wantRecovery := map[string]EffectRecoveryPolicy{
		"feature_flag":       EffectReplaySafe,
		"feature_flag_event": EffectReadbackRequired,
		"work_graph_edges":   EffectReplaySafe,
	}
	for destination, want := range wantRecovery {
		if got := byDestination[destination].Recovery; got != want {
			t.Fatalf("destination %q recovery=%q want=%q", destination, got, want)
		}
	}
	if _, ok := byDestination["feature_flag_link"]; ok {
		t.Fatal("GitLab route emitted LaunchDarkly-only feature_flag_link effect")
	}
	if len(byDestination["feature_flag"].Rows) != 4 ||
		len(byDestination["feature_flag_event"].Rows) != 4 ||
		len(byDestination["work_graph_edges"].Rows) != 8 {
		t.Fatalf("row counts flags=%d events=%d edges=%d", len(byDestination["feature_flag"].Rows), len(byDestination["feature_flag_event"].Rows), len(byDestination["work_graph_edges"].Rows))
	}
	if batch.Evidence.Requests != 3 || batch.Evidence.Pages != 2 ||
		batch.Evidence.Records != 16 || batch.Evidence.CapReached {
		t.Fatalf("evidence=%+v", batch.Evidence)
	}
	if batch.Watermark == nil || claim.BeforeAt == nil ||
		!batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v claim_before=%v", batch.Watermark, claim.BeforeAt)
	}
	if got := batch.Result["project_key"]; got != "acme/api" {
		t.Fatalf("project_key=%v", got)
	}
	if got := batch.Result["project_id_or_path"]; got != claim.SourceExternalID {
		t.Fatalf("project_id_or_path=%v want=%q", got, claim.SourceExternalID)
	}
	if batch.Evidence.Provider != claim.Provider || batch.Evidence.Dataset != claim.Dataset {
		t.Fatalf("evidence identity provider=%q dataset=%q want provider=%q dataset=%q", batch.Evidence.Provider, batch.Evidence.Dataset, claim.Provider, claim.Dataset)
	}
	wantRequests := []string{
		"/api/v4/projects/group%2Fproject/feature_flags?page=1&per_page=100",
		"/api/v4/projects/group%2Fproject/feature_flags?page=2&per_page=100",
		"/api/v4/projects/group%2Fproject",
	}
	if len(doer.requests) != len(wantRequests) {
		t.Fatalf("requests=%d want=%d paths=%v", len(doer.requests), len(wantRequests), requestURLs(doer.requests))
	}
	for index, want := range wantRequests {
		if got := doer.requests[index].URL.RequestURI(); got != want {
			t.Fatalf("request[%d]=%q want=%q", index, got, want)
		}
	}
	var flag launchDarklyFlagRow
	if err := json.Unmarshal(byDestination["feature_flag"].Rows[0], &flag); err != nil {
		t.Fatal(err)
	}
	if flag.Provider != "gitlab" || flag.ProjectKey != "acme/api" ||
		flag.OrgID != claim.OrgID || flag.LastSynced.IsZero() {
		t.Fatalf("flag=%+v", flag)
	}
}

func TestGitLabFeatureFlagsRouteFallsBackForNonObjectProject(t *testing.T) {
	doer := &gitLabFeatureFlagsDoer{t: t, responses: []gitLabFeatureFlagsResponse{
		{body: `[{"name":"checkout","active":true,"strategies":[]}]`},
		{body: `[]`},
	}}
	claim := nativeTestClaim("gitlab", "feature-flags")
	batch, err := (GitLabFeatureFlagsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabFeatureFlagsClient(t, doer, providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		}), time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Result["project_key"] != claim.SourceExternalID || batch.Evidence.Requests != 2 {
		t.Fatalf("batch=%+v", batch)
	}
}

func TestGitLabFeatureFlagsRouteFailsClosedOnNonListFlags(t *testing.T) {
	doer := &gitLabFeatureFlagsDoer{t: t, responses: []gitLabFeatureFlagsResponse{
		{body: `{"items":[]}`},
	}}
	claim := nativeTestClaim("gitlab", "feature-flags")
	batch, err := (GitLabFeatureFlagsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabFeatureFlagsClient(t, doer, providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		}), time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, providerfoundation.ErrPaginationInvalid) &&
		!errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
		t.Fatalf("error=%v want invalid flags payload", err)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("failed collection returned effects/watermark: %+v", batch)
	}
}

func TestGitLabFeatureFlagsRouteFailsClosedOnPaginationCap(t *testing.T) {
	doer := &gitLabFeatureFlagsDoer{t: t, responses: []gitLabFeatureFlagsResponse{
		{body: `[{"name":"checkout","strategies":[]}]`, headers: http.Header{"X-Next-Page": {"2"}}},
	}}
	claim := nativeTestClaim("gitlab", "feature-flags")
	batch, err := (GitLabFeatureFlagsRouteHandler{MaxPages: 1}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabFeatureFlagsClient(t, doer, providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		}), time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("error=%v want ErrPaginationCapExceeded", err)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil || len(doer.requests) != 1 {
		t.Fatalf("failed capped collection batch=%+v requests=%d", batch, len(doer.requests))
	}
}

func TestGitLabFeatureFlagsRouteClassifiesCaseInsensitiveQualified403(t *testing.T) {
	doer := &gitLabFeatureFlagsDoer{t: t, responses: []gitLabFeatureFlagsResponse{
		{status: http.StatusForbidden, body: `{}`, headers: http.Header{"ratelimit-remaining": {"0"}}},
	}}
	claim := nativeTestClaim("gitlab", "feature-flags")
	_, err := (GitLabFeatureFlagsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabFeatureFlagsClient(t, doer, providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		}), time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("error=%v want case-insensitive qualified rate limit", err)
	}
}

func TestGitLabFeatureFlagsRouteClassifiesPlain403AsUnavailableDataset(t *testing.T) {
	doer := &gitLabFeatureFlagsDoer{t: t, responses: []gitLabFeatureFlagsResponse{
		{status: http.StatusForbidden, body: `{"message":"403 Forbidden"}`},
	}}
	claim := nativeTestClaim("gitlab", "feature-flags")
	_, err := (GitLabFeatureFlagsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabFeatureFlagsClient(t, doer, providerfoundation.RetryPolicy{
			MaxAttempts: 5, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		}), time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.Is(err, ErrProviderDatasetUnavailable) ||
		!errors.As(err, &providerErr) ||
		providerErr.Class != providerfoundation.ErrorAuthentication ||
		providerErr.StatusCode != http.StatusForbidden {
		t.Fatalf("error=%v want unavailable dataset retaining forbidden provider evidence", err)
	}
	if len(doer.requests) != 1 {
		t.Fatalf("plain forbidden requests=%d want=1", len(doer.requests))
	}
}

func TestGitLabFeatureFlagsRouteKeepsProjectLookup403AsAuthentication(t *testing.T) {
	doer := &gitLabFeatureFlagsDoer{t: t, responses: []gitLabFeatureFlagsResponse{
		{body: `[]`},
		{status: http.StatusForbidden, body: `{"message":"403 Forbidden"}`},
	}}
	claim := nativeTestClaim("gitlab", "feature-flags")
	claim.SourceExternalID = "group/project"
	_, err := (GitLabFeatureFlagsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabFeatureFlagsClient(t, doer, providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		}), time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if errors.Is(err, ErrProviderDatasetUnavailable) ||
		!errors.As(err, &providerErr) ||
		providerErr.Class != providerfoundation.ErrorAuthentication ||
		providerErr.StatusCode != http.StatusForbidden {
		t.Fatalf("error=%v want unwrapped project lookup authentication error", err)
	}
	if len(doer.requests) != 2 {
		t.Fatalf("project forbidden requests=%d want=2", len(doer.requests))
	}
}

func TestGitLabFeatureFlagsRouteAcceptsEmptyInventory(t *testing.T) {
	doer := &gitLabFeatureFlagsDoer{t: t, responses: []gitLabFeatureFlagsResponse{
		{body: `[]`},
		{body: `{"path_with_namespace":"acme/api"}`},
	}}
	claim := nativeTestClaim("gitlab", "feature-flags")
	claim.SourceExternalID = "group/project"
	batch, err := (GitLabFeatureFlagsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabFeatureFlagsClient(t, doer, providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		}), time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("empty inventory error=%v", err)
	}
	if batch.Result["flags_synced"] != 0 || batch.Result["events_synced"] != 0 ||
		batch.Watermark == nil || len(doer.requests) != 2 {
		t.Fatalf("empty inventory batch=%+v requests=%d", batch, len(doer.requests))
	}
	for _, effect := range batch.Effects {
		if len(effect.Rows) != 0 {
			t.Fatalf("empty inventory effect %q rows=%d", effect.Destination, len(effect.Rows))
		}
	}
}
