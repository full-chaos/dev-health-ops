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

type pagerDutyTeamsResponse struct {
	status int
	body   string
}

type pagerDutyTeamsDoer struct {
	t         *testing.T
	responses []pagerDutyTeamsResponse
	requests  []*http.Request
}

func (doer *pagerDutyTeamsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request)
	if len(doer.responses) == 0 {
		doer.t.Fatalf("unexpected PagerDuty request %s", request.URL.RequestURI())
	}
	response := doer.responses[0]
	doer.responses = doer.responses[1:]
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Request:    request,
	}, nil
}

func TestPagerDutyTeamsRouteUsesOffsetPaginationAndCanonicalRow(t *testing.T) {
	t.Parallel()
	doer := &pagerDutyTeamsDoer{t: t, responses: []pagerDutyTeamsResponse{
		{body: `{"teams":[
			{"id":"PT1","type":"team","name":"Platform","description":"Platform response","updated_at":"2026-08-01T10:00:00.123456Z","html_url":"https://acme.pagerduty.com/teams/PT1"},
			{"id":"PT2","type":"team","summary":"Support","created_at":"2026-07-31T09:00:00Z","self":"/teams/PT2"}],"more":true}`},
		{body: `{"teams":[{"id":"PT3","type":"team"}],"more":false}`},
	}}
	client := pagerDutyTeamsTestClient(t, doer, providerfoundation.RetryPolicy{
		MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	})
	claim := nativeTestClaim("pagerduty", "teams")
	credential := providerfoundation.Credential{
		Provider: "pagerduty", Config: map[string]string{"subdomain": " Acme "},
	}
	normalizedAt := time.Date(2026, 8, 9, 12, 0, 0, 987654321, time.FixedZone("PDT", -7*60*60))
	batch, err := (PagerDutyTeamsRouteHandler{Entitlement: allowIncidentEntitlement, MaxPages: 10}).Collect(
		context.Background(), claim, credential, client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "operational_teams" ||
		batch.Effects[0].Recovery != EffectReadbackRequired || len(batch.Effects[0].Rows) != 3 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	if batch.Evidence.Requests != 2 || batch.Evidence.Pages != 2 || batch.Evidence.Records != 3 ||
		batch.Evidence.CapReached || batch.Watermark != nil {
		t.Fatalf("batch=%+v", batch)
	}
	if got := doer.requests[0].URL.RawQuery; got != "limit=100&offset=0" {
		t.Fatalf("first query=%q", got)
	}
	if got := doer.requests[1].URL.RawQuery; got != "limit=100&offset=2" {
		t.Fatalf("second query=%q", got)
	}
	row := mustPagerDutyTeamRow(t, batch.Effects[0].Rows[0])
	if row.Provider != "pagerduty" || row.ProviderInstanceID != "acme" ||
		row.SourceEntityType != "team" || row.ExternalID != "PT1" ||
		row.Name != "Platform" || row.Description == nil || *row.Description != "Platform response" ||
		row.SourceURL == nil || *row.SourceURL != "https://acme.pagerduty.com/teams/PT1" ||
		!row.SourceVersionAt.Equal(time.Date(2026, 8, 1, 10, 0, 0, 123456000, time.UTC)) ||
		!row.ObservedAt.Equal(time.Date(2026, 8, 9, 19, 0, 0, 987654000, time.UTC)) ||
		row.SourceRevision == nil || row.IngestRevision == nil || row.OrderingContract != 2 {
		t.Fatalf("row=%+v", row)
	}
	if second := mustPagerDutyTeamRow(t, batch.Effects[0].Rows[1]); second.Name != "Support" ||
		second.Description != nil || second.SourceURL == nil || *second.SourceURL != "/teams/PT2" {
		t.Fatalf("second=%+v", second)
	}
	if third := mustPagerDutyTeamRow(t, batch.Effects[0].Rows[2]); third.Name != "PT3" ||
		!third.SourceVersionAt.Equal(row.ObservedAt) {
		t.Fatalf("third=%+v", third)
	}
}

func TestPagerDutyTeamsRoutePreservesRetryAndPermanentErrorSemantics(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("pagerduty", "teams")
	credential := providerfoundation.Credential{
		Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"},
	}
	clientRetryDoer := &pagerDutyTeamsDoer{t: t, responses: []pagerDutyTeamsResponse{
		{status: http.StatusTooManyRequests, body: `{"message":"slow down"}`},
		{body: `{"teams":[],"more":false}`},
	}}
	retryClient := pagerDutyTeamsTestClient(t, clientRetryDoer, providerfoundation.RetryPolicy{
		MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	})
	batch, err := (PagerDutyTeamsRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		context.Background(), claim, credential, retryClient, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if err != nil || len(clientRetryDoer.requests) != 2 || len(batch.Effects) != 1 {
		t.Fatalf("retry batch=%+v error=%v requests=%d", batch, err, len(clientRetryDoer.requests))
	}

	authDoer := &pagerDutyTeamsDoer{t: t, responses: []pagerDutyTeamsResponse{
		{status: http.StatusUnauthorized, body: `{"message":"bad token"}`},
	}}
	authClient := pagerDutyTeamsTestClient(t, authDoer, providerfoundation.RetryPolicy{
		MaxAttempts: 3, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	})
	_, err = (PagerDutyTeamsRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		context.Background(), claim, credential, authClient, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorAuthentication ||
		len(authDoer.requests) != 1 {
		t.Fatalf("auth error=%v requests=%d", err, len(authDoer.requests))
	}
}

func TestPagerDutyTeamsRouteClassifiesUnavailableAccountAbility(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("pagerduty", "teams")
	credential := providerfoundation.Credential{
		Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"},
	}
	doer := &pagerDutyTeamsDoer{t: t, responses: []pagerDutyTeamsResponse{{
		status: http.StatusPaymentRequired,
		body:   `{"error":{"code":2014,"message":"Required abilities are unavailable"}}`,
	}}}
	client := pagerDutyTeamsTestClient(t, doer, providerfoundation.RetryPolicy{
		MaxAttempts: 3, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	})

	_, err := (PagerDutyTeamsRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		context.Background(), claim, credential, client,
		time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrProviderDatasetUnavailable) || len(doer.requests) != 1 {
		t.Fatalf("error=%v requests=%d", err, len(doer.requests))
	}
}

func TestPagerDutyTeamsRouteFailsClosedOnPaginationCapAndWrongDataset(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("pagerduty", "teams")
	client := pagerDutyTeamsTestClient(t, &pagerDutyTeamsDoer{
		t: t, responses: []pagerDutyTeamsResponse{{body: `{"teams":[{"id":"one"}],"more":true}`}},
	}, providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond})
	credential := providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}}
	_, err := (PagerDutyTeamsRouteHandler{Entitlement: allowIncidentEntitlement, MaxPages: 1}).Collect(
		context.Background(), claim, credential, client, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("cap error=%v", err)
	}
	wrongClaim := nativeTestClaim("pagerduty", "users")
	_, err = (PagerDutyTeamsRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		context.Background(), wrongClaim, credential, client, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong dataset error=%v", err)
	}
}

func TestPagerDutyTeamsRouteStopsWhenLeaseExpiresBetweenPages(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("pagerduty", "teams")
	doer := &pagerDutyTeamsDoer{t: t, responses: []pagerDutyTeamsResponse{
		{body: `{"teams":[{"id":"one"}],"more":true}`},
		{body: `{"teams":[],"more":false}`},
	}}
	asserts := 0
	client, err := providerfoundation.NewHTTPClient(
		"pagerduty", "https://api.pagerduty.com", doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error {
			asserts++
			if asserts > 2 {
				return providerfoundation.ErrLeaseLost
			}
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = (PagerDutyTeamsRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		client, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, providerfoundation.ErrLeaseLost) || len(doer.requests) != 1 {
		t.Fatalf("lease error=%v requests=%d asserts=%d", err, len(doer.requests), asserts)
	}
}

func pagerDutyTeamsTestClient(
	t *testing.T, doer providerfoundation.HTTPDoer, retry providerfoundation.RetryPolicy,
) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"pagerduty", "https://api.pagerduty.com", doer,
		func(*http.Request) error { return nil }, retry,
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func mustPagerDutyTeamRow(t *testing.T, raw []byte) pagerDutyTeamRow {
	t.Helper()
	var row pagerDutyTeamRow
	if err := json.Unmarshal(raw, &row); err != nil {
		t.Fatal(err)
	}
	return row
}
