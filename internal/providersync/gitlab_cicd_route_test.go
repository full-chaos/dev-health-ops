package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestGitLabCICDRouteMatchesPythonWindowOrderingAndRows(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 10, 0, 0, 987654321, time.UTC)
	doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
		{body: gitLabRepositoryFixture},
		{body: `[
			{"id":"missing-created","status":"success","created_at":null},
			{"id":901,"status":"running","created_at":"2026-07-20T10:00:00.123Z","started_at":null,"finished_at":null}
		]`, headers: http.Header{"X-Next-Page": []string{"2"}}},
		{body: `[
			{"id":902,"status":"success","created_at":"2026-07-01T00:00:00Z","started_at":"2026-07-01T00:00:01.456Z","finished_at":"2026-07-01T00:02:03.999Z"},
			{"id":903,"status":"failed","created_at":"2026-06-30T23:59:59Z"}
		]`},
	}}
	claim := nativeTestClaim("gitlab", "cicd")
	batch, err := (GitLabCICDRouteHandler{PerPage: 2, MaxPipelines: 4}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 3 || doer.requests[1].URL.Query().Get("order_by") != "updated_at" ||
		doer.requests[1].URL.Query().Get("sort") != "desc" ||
		doer.requests[1].URL.Query().Get("per_page") != "2" ||
		doer.requests[2].URL.Query().Get("page") != "2" {
		t.Fatalf("requests=%v", doer.requests)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "ci_pipeline_runs" ||
		batch.Effects[0].Recovery != EffectReadbackRequired || len(batch.Effects[0].Rows) != 2 ||
		batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) ||
		batch.Evidence.Requests != 3 || batch.Evidence.Pages != 2 || batch.Evidence.Records != 2 {
		t.Fatalf("batch=%+v", batch)
	}
	var first, second gitLabCICDPipelineRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(batch.Effects[0].Rows[1], &second); err != nil {
		t.Fatal(err)
	}
	if first.RunID != "901" || first.Status == nil || *first.Status != "running" ||
		first.QueuedAt == nil || !first.StartedAt.Equal(*first.QueuedAt) || first.FinishedAt != nil ||
		first.RetryCount != 0 || !first.LastSynced.Equal(now.Truncate(time.Millisecond)) ||
		second.RunID != "902" || second.FinishedAt == nil {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
}

func TestGitLabCICDRouteAcceptsFullPythonWindowButRejectsIncompleteTraversal(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	valid := func(id string) string {
		return `{"id":"` + id + `","created_at":"2026-07-20T00:00:00Z"}`
	}
	for _, test := range []struct {
		name      string
		responses []gitLabCommitsResponse
		wantErr   error
		wantRows  int
		wantPages int
		wantCap   bool
	}{
		{
			name: "intentional max window",
			responses: []gitLabCommitsResponse{
				{body: gitLabRepositoryFixture},
				{body: `[` + valid("1") + `,` + valid("2") + `]`, headers: http.Header{"X-Next-Page": []string{"2"}}},
				{body: `[` + valid("3") + `,` + valid("4") + `]`, headers: http.Header{"X-Next-Page": []string{"3"}}},
			},
			wantRows: 3, wantPages: 2, wantCap: true,
		},
		{
			name: "completed short traversal",
			responses: []gitLabCommitsResponse{
				{body: gitLabRepositoryFixture},
				{body: `[` + valid("1") + `,` + valid("2") + `]`, headers: http.Header{"X-Next-Page": []string{"2"}}},
				{body: `[]`},
			},
			wantRows: 2, wantPages: 2,
		},
		{
			name: "incomplete before max window",
			responses: []gitLabCommitsResponse{
				{body: gitLabRepositoryFixture},
				{body: `[` + valid("1") + `]`, headers: http.Header{"X-Next-Page": []string{"2"}}},
				{body: `[` + valid("2") + `]`, headers: http.Header{"X-Next-Page": []string{"3"}}},
			},
			wantErr: ErrPaginationCapExceeded,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &gitLabCommitsDoer{t: t, responses: test.responses}
			batch, err := (GitLabCICDRouteHandler{PerPage: 2, MaxPipelines: 3}).Collect(
				context.Background(), nativeTestClaim("gitlab", "cicd"),
				providerfoundation.Credential{},
				gitLabRepositoryClient(t, doer, "https://gitlab.example"), now,
			)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error=%v want=%v", err, test.wantErr)
			}
			if test.wantErr != nil {
				if len(batch.Effects) != 0 || batch.Watermark != nil {
					t.Fatalf("incomplete traversal leaked state: %+v", batch)
				}
			} else if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != test.wantRows ||
				batch.Evidence.Requests != test.wantPages+1 || batch.Evidence.Pages != test.wantPages ||
				batch.Evidence.Records != test.wantRows || batch.Evidence.CapReached != test.wantCap {
				t.Fatalf("batch=%+v", batch)
			}
		})
	}
}

func TestNormalizeGitLabCICDPipelinesComparesBoundariesBeforeMillisecondStorage(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "cicd")
	since := time.Date(2026, 7, 1, 0, 0, 0, 123400000, time.UTC)
	before := since
	claim.SinceAt = &since
	claim.BeforeAt = &before
	createdAccepted := "2026-07-01T00:00:00.123456Z"
	startedAfterBefore := "2026-07-01T00:00:00.123456Z"
	startedAtBoundary := "2026-07-01T00:00:00.123400Z"
	rows, err := normalizeGitLabCICDPipelines(
		claim,
		"a6a5cafb-6680-a10a-9e41-a5ef763ca016",
		[]gitLabCICDPipelinePayload{
			{ID: "started-after", CreatedAt: &createdAccepted, StartedAt: &startedAfterBefore},
			{ID: "accepted", CreatedAt: &createdAccepted, StartedAt: &startedAtBoundary},
		},
		claim.SinceAt,
		time.Date(2026, 8, 3, 10, 0, 0, 987654321, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].RunID != "accepted" {
		t.Fatalf("rows=%+v", rows)
	}
	stored := time.Date(2026, 7, 1, 0, 0, 0, 123000000, time.UTC)
	if rows[0].QueuedAt == nil || !rows[0].QueuedAt.Equal(stored) ||
		!rows[0].StartedAt.Equal(stored) {
		t.Fatalf("row timestamps were not canonicalized at persistence boundary: %+v", rows[0])
	}
}

func TestGitLabCICDRouteClassifiesPythonSoftAndControlPlaneFailures(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		status int
		fatal  bool
		class  providerfoundation.ErrorClass
	}{
		{"transient Python soft", http.StatusServiceUnavailable, false, providerfoundation.ErrorTransient},
		{"not found Python soft", http.StatusNotFound, false, providerfoundation.ErrorNotFound},
		{"authentication fatal", http.StatusUnauthorized, true, providerfoundation.ErrorAuthentication},
		{"rate fatal", http.StatusTooManyRequests, true, providerfoundation.ErrorRateLimited},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
				{body: gitLabRepositoryFixture}, {status: test.status, body: `{}`},
			}}
			batch, err := (GitLabCICDRouteHandler{}).Collect(
				context.Background(), nativeTestClaim("gitlab", "cicd"),
				providerfoundation.Credential{},
				gitLabRepositoryClient(t, doer, "https://gitlab.example"), now,
			)
			if test.fatal {
				var providerErr *providerfoundation.ProviderError
				if !errors.As(err, &providerErr) || providerErr.Class != test.class ||
					len(batch.Effects) != 0 || batch.Watermark != nil {
					t.Fatalf("error=%v batch=%+v", err, batch)
				}
				return
			}
			if err != nil || len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 0 ||
				batch.Watermark == nil || batch.Result["soft_failure"] != true {
				t.Fatalf("soft error=%v batch=%+v", err, batch)
			}
			if batch.Evidence.Requests != len(doer.requests) || batch.Evidence.Requests != 2 {
				t.Fatalf("soft evidence requests=%d doer attempts=%d", batch.Evidence.Requests, len(doer.requests))
			}
		})
	}
}

func TestGitLabCICDRouteCountsLaterPageAttemptOnPythonSoftFailure(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
		{body: gitLabRepositoryFixture},
		{body: `[{"id":"901","created_at":"2026-07-20T00:00:00Z"}]`, headers: http.Header{"X-Next-Page": []string{"2"}}},
		{status: http.StatusServiceUnavailable, body: `{}`},
	}}
	batch, err := (GitLabCICDRouteHandler{PerPage: 2, MaxPipelines: 4}).Collect(
		context.Background(), nativeTestClaim("gitlab", "cicd"),
		providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), now,
	)
	if err != nil || len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 0 ||
		batch.Watermark == nil || batch.Evidence.Requests != len(doer.requests) ||
		batch.Evidence.Requests != 3 {
		t.Fatalf("error=%v attempts=%d batch=%+v", err, len(doer.requests), batch)
	}
}

func TestGitLabCICDRouteCountsPhysicalAttemptOnSuccessfulRetry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
		{body: gitLabRepositoryFixture},
		{status: http.StatusServiceUnavailable, body: `{}`},
		{body: `[{"id":"901","created_at":"2026-07-20T00:00:00Z"}]`},
	}}
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
	client.Retry = providerfoundation.RetryPolicy{
		MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	}
	batch, err := (GitLabCICDRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "cicd"),
		providerfoundation.Credential{}, client, now,
	)
	if err != nil || len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 ||
		batch.Evidence.Requests != len(doer.requests) || batch.Evidence.Requests != 3 ||
		batch.Evidence.Pages != 1 || batch.Evidence.Records != 1 {
		t.Fatalf("error=%v attempts=%d batch=%+v", err, len(doer.requests), batch)
	}
}

func TestGitLabCICDRouteCountsPhysicalProjectAttemptOnSuccessfulRetry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
		{status: http.StatusServiceUnavailable, body: `{}`},
		{body: gitLabRepositoryFixture},
		{body: `[{"id":"901","created_at":"2026-07-20T00:00:00Z"}]`},
	}}
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
	client.Retry = providerfoundation.RetryPolicy{
		MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	}
	batch, err := (GitLabCICDRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "cicd"),
		providerfoundation.Credential{}, client, now,
	)
	if err != nil || len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 ||
		batch.Evidence.Requests != len(doer.requests) || batch.Evidence.Requests != 3 ||
		batch.Evidence.Pages != 1 || batch.Evidence.Records != 1 {
		t.Fatalf("error=%v attempts=%d batch=%+v", err, len(doer.requests), batch)
	}
}

func TestGitLabCICDRouteRejectsCrossScopeAndProjectMismatch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	baseClaim := nativeTestClaim("gitlab", "cicd")
	wrongProviderClaim := baseClaim
	wrongProviderClaim.Provider = "github"
	for _, test := range []struct {
		name       string
		claim      Claim
		clientName string
		project    string
	}{
		{name: "wrong claim provider", claim: wrongProviderClaim},
		{name: "wrong claim dataset", claim: nativeTestClaim("gitlab", "commits")},
		{name: "wrong client provider", claim: baseClaim, clientName: "github"},
		{name: "project id mismatch", claim: baseClaim, project: `{"id":124,"name":"api","path_with_namespace":"acme/api"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			responses := []gitLabCommitsResponse{}
			if test.project != "" {
				responses = append(responses, gitLabCommitsResponse{body: test.project})
			}
			doer := &gitLabCommitsDoer{t: t, responses: responses}
			client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
			if test.clientName != "" {
				client.Provider = test.clientName
			}
			batch, err := (GitLabCICDRouteHandler{}).Collect(
				context.Background(), test.claim, providerfoundation.Credential{}, client, now,
			)
			if err == nil || len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("error=%v batch=%+v", err, batch)
			}
		})
	}
}

func TestGitLabCICDRouteFailsClosedOnBudgetLeaseContextAndMalformedIdentity(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("gitlab", "cicd")

	t.Run("budget", func(t *testing.T) {
		doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{{body: gitLabRepositoryFixture}}}
		client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
		client.Budget = &gitLabCommitStatsBudget{failAt: 2}
		batch, err := (GitLabCICDRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
		if !errors.Is(err, providerfoundation.ErrBudgetUnavailable) || len(batch.Effects) != 0 || batch.Watermark != nil {
			t.Fatalf("error=%v batch=%+v", err, batch)
		}
	})

	t.Run("lease", func(t *testing.T) {
		doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{{body: gitLabRepositoryFixture}}}
		client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
		assertions := 0
		client.Lease = providerfoundation.LeaseGuardFunc(func(context.Context) error {
			assertions++
			if assertions == 3 {
				return providerfoundation.ErrLeaseLost
			}
			return nil
		})
		batch, err := (GitLabCICDRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
		if !errors.Is(err, providerfoundation.ErrLeaseLost) || len(batch.Effects) != 0 || batch.Watermark != nil {
			t.Fatalf("assertions=%d error=%v batch=%+v", assertions, err, batch)
		}
	})

	t.Run("context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		doer := &gitLabCommitsDoer{t: t}
		batch, err := (GitLabCICDRouteHandler{}).Collect(ctx, claim, providerfoundation.Credential{}, gitLabRepositoryClient(t, doer, "https://gitlab.example"), now)
		if !errors.Is(err, context.Canceled) || len(batch.Effects) != 0 || batch.Watermark != nil {
			t.Fatalf("error=%v batch=%+v", err, batch)
		}
	})

	t.Run("malformed identity", func(t *testing.T) {
		doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
			{body: gitLabRepositoryFixture}, {body: `[{"created_at":"2026-07-20T00:00:00Z"}]`},
		}}
		batch, err := (GitLabCICDRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, gitLabRepositoryClient(t, doer, "https://gitlab.example"), now)
		if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) || len(batch.Effects) != 0 || batch.Watermark != nil {
			t.Fatalf("error=%v batch=%+v", err, batch)
		}
	})

	t.Run("malformed status", func(t *testing.T) {
		doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
			{body: gitLabRepositoryFixture}, {body: `[{"id":"901","status":7,"created_at":"2026-07-20T00:00:00Z"}]`},
		}}
		batch, err := (GitLabCICDRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, gitLabRepositoryClient(t, doer, "https://gitlab.example"), now)
		if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) || len(batch.Effects) != 0 || batch.Watermark != nil {
			t.Fatalf("error=%v batch=%+v", err, batch)
		}
	})
}

func TestGitLabCICDFatalListErrorClassification(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		err   error
		fatal bool
	}{
		{"authentication", &providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication}, true},
		{"rate", &providerfoundation.ProviderError{Class: providerfoundation.ErrorRateLimited}, true},
		{"provider cancelled", &providerfoundation.ProviderError{Class: providerfoundation.ErrorCancelled}, true},
		{"context", context.Canceled, true},
		{"budget", providerfoundation.ErrBudgetUnavailable, true},
		{"lease", providerfoundation.ErrLeaseLost, true},
		{"unknown class", &providerfoundation.ProviderError{Class: "future-control-plane"}, true},
		{"joined transient and budget", errors.Join(&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient}, providerfoundation.ErrBudgetUnavailable), true},
		{"joined transient and lease", errors.Join(&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient}, providerfoundation.ErrLeaseLost), true},
		{"joined transient and context", errors.Join(&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient}, context.Canceled), true},
		{"joined transient and provider cancel", errors.Join(&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient}, &providerfoundation.ProviderError{Class: providerfoundation.ErrorCancelled}), true},
		{"joined allowed provider leaves", errors.Join(&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient}, &providerfoundation.ProviderError{Class: providerfoundation.ErrorNotFound}), false},
		{"transient", &providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient}, false},
		{"not found", &providerfoundation.ProviderError{Class: providerfoundation.ErrorNotFound}, false},
		{"conflict", &providerfoundation.ProviderError{Class: providerfoundation.ErrorConflict}, false},
		{"permanent", &providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := gitLabCICDFatalListError(context.Background(), test.err)
			if (got != nil) != test.fatal {
				t.Fatalf("fatal error=%v want=%t", got, test.fatal)
			}
		})
	}
}
