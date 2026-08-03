package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestGitLabIncidentsRouteEmitsCompleteCanonicalBatch(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 987654321, time.UTC)
	doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
		{body: gitLabRepositoryFixture},
		{body: `[
			{"id":9001,"iid":7,"issue_type":"incident","state":"opened","title":"API unavailable","description":"edge <down>","created_at":"2026-07-20T10:00:00.123456Z","updated_at":"2026-07-21T11:00:00.654321Z","web_url":"https://gitlab.example/Acme/API/-/issues/7","severity":"sev-1"},
			{"id":9001,"iid":7,"issue_type":"incident","state":"closed","title":"duplicate loses","created_at":"2026-07-19T10:00:00Z","updated_at":"2026-07-22T10:00:00Z"},
			{"id":5,"iid":8,"issue_type":"issue","state":"opened","title":"ordinary issue","created_at":"2026-07-20T10:00:00Z"},
			{"id":"9002","iid":"8","issue_type":"INCIDENT","state":"closed","title":"Queue lag","created_at":"2026-07-22T10:00:00Z","updated_at":"2026-07-23T10:00:00Z","closed_at":"2026-07-23T11:00:00Z","url":"https://gitlab.example/fallback","severity":"medium"}
		]`},
	}}
	claim := nativeTestClaim("gitlab", "incidents")
	batch, err := (GitLabIncidentsRouteHandler{PerPage: 5, MaxIssues: 10}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://GITLAB.example:443"), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 2 {
		t.Fatalf("requests=%d", len(doer.requests))
	}
	query := doer.requests[1].URL.Query()
	if query.Get("issue_type") != "incident" || query.Get("state") != "all" ||
		query.Get("updated_after") != "2026-07-01T00:00:00Z" ||
		query.Get("updated_before") != "2026-07-31T23:59:59Z" ||
		query.Get("order_by") != "updated_at" || query.Get("sort") != "desc" ||
		query.Get("page") != "1" || query.Get("per_page") != "5" {
		t.Fatalf("query=%s", query.Encode())
	}
	descriptor := CompleteRouteDescriptor{
		Provider: "gitlab", RequestedDataset: "incidents", RouteDataset: "incidents",
		Destinations: []string{"operational_services", "operational_service_repository_mappings", "operational_incidents"},
	}
	if err := batch.validate(descriptor); err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) ||
		batch.Evidence.Requests != 2 || batch.Evidence.Pages != 2 ||
		batch.Evidence.Records != 2 {
		t.Fatalf("watermark=%v evidence=%+v", batch.Watermark, batch.Evidence)
	}
	byDestination := map[string]EffectBatch{}
	for _, effect := range batch.Effects {
		byDestination[effect.Destination] = effect
	}
	if len(byDestination["operational_services"].Rows) != 1 ||
		len(byDestination["operational_service_repository_mappings"].Rows) != 1 ||
		len(byDestination["operational_incidents"].Rows) != 2 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var service gitLabOperationalServiceRow
	if err := json.Unmarshal(byDestination["operational_services"].Rows[0], &service); err != nil {
		t.Fatal(err)
	}
	if service.ProviderInstanceID != "gitlab.example" || service.ExternalID != "Acme/API" ||
		service.Name != "Acme/API" || service.ServiceType == nil || *service.ServiceType != "repository" ||
		!service.SourceVersionAt.Equal(time.Date(2026, 7, 21, 11, 0, 0, 654321000, time.UTC)) ||
		service.SourceRevision == nil || service.IngestRevision == nil || service.OrderingContract != 2 {
		t.Fatalf("service=%+v", service)
	}
	var mapping gitLabServiceRepositoryMappingRow
	if err := json.Unmarshal(byDestination["operational_service_repository_mappings"].Rows[0], &mapping); err != nil {
		t.Fatal(err)
	}
	if mapping.ServiceID != service.ID || mapping.RepoID == nil ||
		mapping.RepoFullName == nil || *mapping.RepoFullName != "Acme/API" ||
		mapping.RelationshipProvenance == nil || *mapping.RelationshipProvenance != "native_repository_context" ||
		mapping.RelationshipConfidence == nil || *mapping.RelationshipConfidence != 1 || !mapping.IsActive {
		t.Fatalf("mapping=%+v", mapping)
	}
	incidents := make(map[string]jiraIncidentRow)
	for _, raw := range byDestination["operational_incidents"].Rows {
		var row jiraIncidentRow
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		incidents[row.ExternalID] = row
	}
	first := incidents["9001"]
	if first.Title != "API unavailable" || first.SourceEventID == nil || *first.SourceEventID != "7" ||
		first.NormalizedStatus == nil || *first.NormalizedStatus != "open" ||
		first.NormalizedSeverity == nil || *first.NormalizedSeverity != "critical" ||
		first.ServiceID == nil || *first.ServiceID != service.ID || first.ResolvedAt != nil {
		t.Fatalf("first=%+v", first)
	}
	second := incidents["9002"]
	if second.SourceURL == nil || *second.SourceURL != "https://gitlab.example/fallback" ||
		second.NormalizedStatus == nil || *second.NormalizedStatus != "resolved" ||
		second.ResolvedAt == nil {
		t.Fatalf("second=%+v", second)
	}
}

func TestGitLabIncidentsRouteEmitsThreeEmptyEffects(t *testing.T) {
	t.Parallel()
	doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
		{body: gitLabRepositoryFixture}, {body: `[]`},
	}}
	batch, err := (GitLabIncidentsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "incidents"),
		providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"),
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 3 || batch.Evidence.Records != 0 {
		t.Fatalf("batch=%+v", batch)
	}
	wantDestinations := []string{
		"operational_services", "operational_service_repository_mappings",
		"operational_incidents",
	}
	for index, effect := range batch.Effects {
		if effect.Destination != wantDestinations[index] {
			t.Fatalf("effect[%d].destination=%s want=%s", index, effect.Destination, wantDestinations[index])
		}
		if len(effect.Rows) != 0 || effect.Recovery != EffectReadbackRequired {
			t.Fatalf("effect=%+v", effect)
		}
	}
}

func TestGitLabIncidentsRouteCountsPhysicalRetryAttempts(t *testing.T) {
	t.Parallel()
	doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
		{body: gitLabRepositoryFixture},
		{status: http.StatusServiceUnavailable, body: `{}`},
		{body: `[{"id":1,"iid":1,"issue_type":"incident","state":"opened","title":"one","created_at":"2026-07-20T10:00:00Z"}]`},
	}}
	client, err := providerfoundation.NewHTTPClient(
		"gitlab", "https://gitlab.example", doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{
			MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := (GitLabIncidentsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "incidents"),
		providerfoundation.Credential{}, client,
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Evidence.Requests != 3 || batch.Evidence.Pages != 2 || len(doer.requests) != 3 {
		t.Fatalf("evidence=%+v physical requests=%d", batch.Evidence, len(doer.requests))
	}
	firstAttempt, retry := doer.requests[1], doer.requests[2]
	if firstAttempt.URL.String() != retry.URL.String() ||
		!strings.HasSuffix(retry.URL.Path, "/api/v4/projects/123/issues") ||
		retry.URL.Query().Get("issue_type") != "incident" ||
		retry.URL.Query().Get("page") != "1" {
		t.Fatalf("first attempt=%s retry=%s", firstAttempt.URL, retry.URL)
	}
}

func TestGitLabIncidentsRouteFailsClosedOnIncompleteOrMalformedInventory(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	valid := `{"id":1,"iid":1,"issue_type":"incident","state":"opened","title":"one","created_at":"2026-07-20T10:00:00Z"}`
	// The legacy Python producer silently skips a missing/bad created_at and
	// falls back from malformed updated_at to created_at. The native route
	// deliberately fails the whole inventory instead: a silent skip followed
	// by watermark advancement permanently loses that incident.
	for name, test := range map[string]struct {
		responses []gitLabCommitsResponse
		handler   GitLabIncidentsRouteHandler
		want      error
	}{
		"saturated accepted-row cap": {
			responses: []gitLabCommitsResponse{{body: gitLabRepositoryFixture}, {body: `[` + valid + `]`}},
			handler:   GitLabIncidentsRouteHandler{MaxIssues: 1, PerPage: 1}, want: ErrPaginationCapExceeded,
		},
		"page cap": {
			responses: []gitLabCommitsResponse{{body: gitLabRepositoryFixture}, {body: `[{"id":2,"issue_type":"issue"}]`}},
			handler:   GitLabIncidentsRouteHandler{MaxIssues: 2, PerPage: 1, MaxPages: 1}, want: ErrPaginationCapExceeded,
		},
		"non-list body": {
			responses: []gitLabCommitsResponse{{body: gitLabRepositoryFixture}, {body: `{}`}},
			handler:   GitLabIncidentsRouteHandler{}, want: providerfoundation.ErrNormalizationInvalid,
		},
		"missing id": {
			responses: []gitLabCommitsResponse{{body: gitLabRepositoryFixture}, {body: `[{"issue_type":"incident","created_at":"2026-07-20T10:00:00Z"}]`}},
			handler:   GitLabIncidentsRouteHandler{}, want: providerfoundation.ErrNormalizationInvalid,
		},
		"bad created_at": {
			responses: []gitLabCommitsResponse{{body: gitLabRepositoryFixture}, {body: `[{"id":1,"issue_type":"incident","created_at":"bad"}]`}},
			handler:   GitLabIncidentsRouteHandler{}, want: providerfoundation.ErrNormalizationInvalid,
		},
		"bad updated_at": {
			responses: []gitLabCommitsResponse{{body: gitLabRepositoryFixture}, {body: `[{"id":1,"issue_type":"incident","created_at":"2026-07-20T10:00:00Z","updated_at":"bad"}]`}},
			handler:   GitLabIncidentsRouteHandler{}, want: providerfoundation.ErrNormalizationInvalid,
		},
		"partial traversal": {
			responses: []gitLabCommitsResponse{{body: gitLabRepositoryFixture}, {body: `[` + valid + `]`}, {status: http.StatusServiceUnavailable, body: `{}`}},
			handler:   GitLabIncidentsRouteHandler{MaxIssues: 10, PerPage: 1}, want: &providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient},
		},
	} {
		t.Run(name, func(t *testing.T) {
			doer := &gitLabCommitsDoer{t: t, responses: test.responses}
			batch, err := test.handler.Collect(
				context.Background(), nativeTestClaim("gitlab", "incidents"),
				providerfoundation.Credential{},
				gitLabRepositoryClient(t, doer, "https://gitlab.example"), now,
			)
			if err == nil || batch.Watermark != nil || len(batch.Effects) != 0 {
				t.Fatalf("batch=%+v error=%v", batch, err)
			}
			var wantProvider *providerfoundation.ProviderError
			if errors.As(test.want, &wantProvider) {
				var gotProvider *providerfoundation.ProviderError
				if !errors.As(err, &gotProvider) || gotProvider.Class != wantProvider.Class {
					t.Fatalf("error=%v want class=%s", err, wantProvider.Class)
				}
			} else if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
		})
	}
}
