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

type pagerDutyBusinessServicesResponse struct {
	status  int
	body    string
	headers http.Header
}

type pagerDutyBusinessServicesDoer struct {
	t         *testing.T
	responses []pagerDutyBusinessServicesResponse
	requests  []*http.Request
}

func (doer *pagerDutyBusinessServicesDoer) Do(request *http.Request) (*http.Response, error) {
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
		StatusCode: status, Header: response.headers,
		Body: io.NopCloser(strings.NewReader(response.body)), Request: request,
	}, nil
}

func TestPagerDutyBusinessServicesRouteUsesOffsetPaginationAndCanonicalRow(t *testing.T) {
	t.Parallel()
	doer := &pagerDutyBusinessServicesDoer{t: t, responses: []pagerDutyBusinessServicesResponse{
		{body: `{"business_services":[
			{"id":"PBS1","type":"business_service","name":"Payments","description":"Checkout path","updated_at":"2026-08-01T10:00:00.123456Z","html_url":"https://acme.pagerduty.com/business_services/PBS1"},
			{"id":"PBS2","type":"business_service","summary":"Support","created_at":"2026-07-31T09:00:00Z"}],"more":true}`},
		{body: `{"business_services":[{"id":"PBS3","type":"business_service","name":"Operations"}],"more":false}`},
	}}
	client := pagerDutyBusinessServicesTestClient(t, doer, providerfoundation.RetryPolicy{
		MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	})
	claim := nativeTestClaim("pagerduty", "business-services")
	credential := providerfoundation.Credential{
		Provider: "pagerduty", Config: map[string]string{"subdomain": " Acme "},
	}
	normalizedAt := time.Date(2026, 8, 9, 12, 0, 0, 987654321, time.FixedZone("PDT", -7*60*60))
	batch, err := (PagerDutyBusinessServicesRouteHandler{Entitlement: allowIncidentEntitlement, MaxPages: 10}).Collect(
		context.Background(), claim, credential, client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "operational_services" ||
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
	if got := doer.requests[0].URL.Path; got != "/business_services" {
		t.Fatalf("first path=%q", got)
	}
	if got := doer.requests[1].URL.RawQuery; got != "limit=100&offset=2" {
		t.Fatalf("second query=%q", got)
	}
	row := mustPagerDutyBusinessServiceRow(t, batch.Effects[0].Rows[0])
	if row.Provider != "pagerduty" || row.ProviderInstanceID != "acme" ||
		row.SourceEntityType != "business_service" || row.ExternalID != "PBS1" ||
		row.Name != "Payments" || row.Description == nil || *row.Description != "Checkout path" ||
		row.ServiceType == nil || *row.ServiceType != "business" || row.SourceURL == nil ||
		*row.SourceURL != "https://acme.pagerduty.com/business_services/PBS1" ||
		!row.SourceVersionAt.Equal(time.Date(2026, 8, 1, 10, 0, 0, 123456000, time.UTC)) ||
		!row.ObservedAt.Equal(time.Date(2026, 8, 9, 19, 0, 0, 987654000, time.UTC)) ||
		row.SourceRevision == nil || row.IngestRevision == nil || row.OrderingContract != 2 {
		t.Fatalf("row=%+v", row)
	}
	if secondary := mustPagerDutyBusinessServiceRow(t, batch.Effects[0].Rows[1]); secondary.Name != "Support" {
		t.Fatalf("secondary=%+v", secondary)
	}
	if tertiary := mustPagerDutyBusinessServiceRow(t, batch.Effects[0].Rows[2]); tertiary.Name != "Operations" ||
		!tertiary.SourceVersionAt.Equal(row.ObservedAt) {
		t.Fatalf("tertiary=%+v", tertiary)
	}
}

func TestPagerDutyBusinessServicesRoutePreservesRetryAndPermanentErrorSemantics(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("pagerduty", "business-services")
	credential := providerfoundation.Credential{
		Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"},
	}
	clientRetryDoer := &pagerDutyBusinessServicesDoer{t: t, responses: []pagerDutyBusinessServicesResponse{
		{status: http.StatusTooManyRequests, headers: http.Header{"Retry-After": {"0"}}, body: `{"message":"slow down"}`},
		{body: `{"business_services":[],"more":false}`},
	}}
	retryClient := pagerDutyBusinessServicesTestClient(t, clientRetryDoer, providerfoundation.RetryPolicy{
		MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	})
	batch, err := (PagerDutyBusinessServicesRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		context.Background(), claim, credential, retryClient, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if err != nil || len(clientRetryDoer.requests) != 2 || len(batch.Effects) != 1 {
		t.Fatalf("retry batch=%+v error=%v requests=%d", batch, err, len(clientRetryDoer.requests))
	}

	authDoer := &pagerDutyBusinessServicesDoer{t: t, responses: []pagerDutyBusinessServicesResponse{
		{status: http.StatusUnauthorized, body: `{"message":"bad token"}`},
	}}
	authClient := pagerDutyBusinessServicesTestClient(t, authDoer, providerfoundation.RetryPolicy{
		MaxAttempts: 3, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	})
	_, err = (PagerDutyBusinessServicesRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		context.Background(), claim, credential, authClient, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorAuthentication ||
		len(authDoer.requests) != 1 {
		t.Fatalf("auth error=%v requests=%d", err, len(authDoer.requests))
	}
}

func TestPagerDutyBusinessServicesRouteFailsClosedOnPaginationCapAndMissingInstance(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("pagerduty", "business-services")
	client := pagerDutyBusinessServicesTestClient(t, &pagerDutyBusinessServicesDoer{
		t: t, responses: []pagerDutyBusinessServicesResponse{{
			body: `{"business_services":[{"id":"one"}],"more":true}`,
		}}}, providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond})
	credential := providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}}
	_, err := (PagerDutyBusinessServicesRouteHandler{Entitlement: allowIncidentEntitlement, MaxPages: 1}).Collect(
		context.Background(), claim, credential, client, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("cap error=%v", err)
	}
	_, err = (PagerDutyBusinessServicesRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		context.Background(), claim, providerfoundation.Credential{Provider: "pagerduty"}, client,
		time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
		t.Fatalf("missing instance error=%v", err)
	}
}

func TestPagerDutyBusinessServicesRouteStopsWhenLeaseExpiresBetweenPages(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("pagerduty", "business-services")
	doer := &pagerDutyBusinessServicesDoer{t: t, responses: []pagerDutyBusinessServicesResponse{
		{body: `{"business_services":[{"id":"one"}],"more":true}`},
		{body: `{"business_services":[],"more":false}`},
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
	_, err = (PagerDutyBusinessServicesRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		client, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, providerfoundation.ErrLeaseLost) || len(doer.requests) != 1 {
		t.Fatalf("lease error=%v requests=%d asserts=%d", err, len(doer.requests), asserts)
	}
}

func pagerDutyBusinessServicesTestClient(
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

func mustPagerDutyBusinessServiceRow(t *testing.T, raw []byte) pagerDutyBusinessServiceRow {
	t.Helper()
	var row pagerDutyBusinessServiceRow
	if err := json.Unmarshal(raw, &row); err != nil {
		t.Fatal(err)
	}
	return row
}
