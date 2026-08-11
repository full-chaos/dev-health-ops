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

type pagerDutyServicesResponse struct {
	status  int
	body    string
	headers http.Header
}

type pagerDutyServicesDoer struct {
	t         *testing.T
	responses []pagerDutyServicesResponse
	requests  []*http.Request
}

func (doer *pagerDutyServicesDoer) Do(request *http.Request) (*http.Response, error) {
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

func TestPagerDutyServicesRouteBuildsTypedServicesAndSeparateMappingEffects(t *testing.T) {
	t.Parallel()
	doer := &pagerDutyServicesDoer{t: t, responses: []pagerDutyServicesResponse{
		{body: `{"services":[
			{"id":"PS1","type":"service","name":"Payments","summary":"Ignored","updated_at":"2026-08-01T10:00:00.123456Z","html_url":"https://acme.pagerduty.com/services/PS1","escalation_policy":{"id":"PE1"},"metadata":{"repository":"https://github.com/full-chaos/payments"}},
			{"id":"PS2","type":"service","summary":"Support","created_at":"2026-07-31T09:00:00Z","self":"/services/PS2"}],"more":true}`},
		{body: `{"services":[{"id":"PS3","type":"service","name":"Operations","repo":"full-chaos/operations"}],"more":false}`},
	}}
	client := pagerDutyServicesTestClient(t, doer, providerfoundation.RetryPolicy{
		MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	})
	claim := nativeTestClaim("pagerduty", "services")
	claim.DatasetOptions = map[string]any{
		"service_repository_mappings": map[string]any{
			"admin": map[string]any{
				"PS1": []any{map[string]any{"provider": "github", "full_name": "full-chaos/payments"}},
			},
			"compass": map[string]any{
				"PS2": []any{map[string]any{"provider": "gitlab", "full_name": "full-chaos/support"}},
			},
		},
	}
	credential := providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": " Acme "}}
	normalizedAt := time.Date(2026, 8, 9, 12, 0, 0, 987654321, time.FixedZone("PDT", -7*60*60))
	batch, err := (PagerDutyServicesRouteHandler{MaxPages: 10}).Collect(
		context.Background(), claim, credential, client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 2 || batch.Effects[0].Destination != "operational_services" ||
		batch.Effects[1].Destination != "operational_service_repository_mappings" ||
		batch.Effects[0].Recovery != EffectReadbackRequired || batch.Effects[1].Recovery != EffectReadbackRequired ||
		len(batch.Effects[0].Rows) != 3 || len(batch.Effects[1].Rows) != 3 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	if batch.Evidence.Requests != 2 || batch.Evidence.Pages != 2 || batch.Evidence.Records != 3 || batch.Evidence.CapReached {
		t.Fatalf("evidence=%+v", batch.Evidence)
	}
	if doer.requests[0].URL.Path != "/services" || doer.requests[0].URL.RawQuery != "limit=100&offset=0" ||
		doer.requests[1].URL.RawQuery != "limit=100&offset=2" {
		t.Fatalf("requests=%v", doer.requests)
	}
	var service pagerDutyServiceRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &service); err != nil {
		t.Fatal(err)
	}
	if service.Provider != "pagerduty" || service.ProviderInstanceID != "acme" ||
		service.SourceEntityType != "service" || service.ExternalID != "PS1" || service.Name != "Payments" ||
		service.ServiceType == nil || *service.ServiceType != "technical" || service.EscalationPolicyID == nil ||
		*service.EscalationPolicyID != pagerDutyCanonicalOperationalID(claim.OrgID, "pagerduty", "acme", "operational_escalation_policy", "PE1") ||
		service.SourceURL == nil || *service.SourceURL != "https://acme.pagerduty.com/services/PS1" ||
		!service.SourceVersionAt.Equal(time.Date(2026, 8, 1, 10, 0, 0, 123456000, time.UTC)) ||
		!service.ObservedAt.Equal(time.Date(2026, 8, 9, 19, 0, 0, 987654000, time.UTC)) {
		t.Fatalf("service=%+v", service)
	}
	var mapping pagerDutyServiceRepositoryMappingRow
	if err := json.Unmarshal(batch.Effects[1].Rows[0], &mapping); err != nil {
		t.Fatal(err)
	}
	if mapping.SourceEntityType != string(pagerDutyMappingAdmin) || mapping.RelationshipConfidence == nil ||
		*mapping.RelationshipConfidence != 1 || mapping.RepoProvider == nil || *mapping.RepoProvider != "github" ||
		mapping.RepoFullName == nil || *mapping.RepoFullName != "full-chaos/payments" ||
		mapping.ServiceID != service.ID || mapping.ValidFrom == nil || !mapping.IsActive ||
		mapping.SourceEventID == nil || *mapping.SourceEventID != "pagerduty_sync" {
		t.Fatalf("mapping=%+v", mapping)
	}
}

func TestPagerDutyServicesRouteRejectsAnotherPagerDutyDataset(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "teams")
	client := pagerDutyServicesTestClient(t, &pagerDutyServicesDoer{t: t}, providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond})
	_, err := (PagerDutyServicesRouteHandler{}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		client, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong dataset error=%v", err)
	}
}

func TestPagerDutyServicesRouteUsesProviderIDWhenNamesAreAbsent(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "services")
	row, err := normalizePagerDutyService(
		claim, "acme", pagerDutyServicePayload{ID: "PS-fallback"},
		time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if row.Name != "PS-fallback" {
		t.Fatalf("name=%q", row.Name)
	}
}

func TestPagerDutyServicesRoutePreservesRetryAuthCapAndLeaseSemantics(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("pagerduty", "services")
	credential := providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}}
	clientRetryDoer := &pagerDutyServicesDoer{t: t, responses: []pagerDutyServicesResponse{
		{status: http.StatusTooManyRequests, headers: http.Header{"Retry-After": {"0"}}, body: `{"message":"slow down"}`},
		{body: `{"services":[],"more":false}`},
	}}
	retryClient := pagerDutyServicesTestClient(t, clientRetryDoer, providerfoundation.RetryPolicy{
		MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	})
	batch, err := (PagerDutyServicesRouteHandler{}).Collect(context.Background(), claim, credential, retryClient, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC))
	if err != nil || len(clientRetryDoer.requests) != 2 || len(batch.Effects) != 2 {
		t.Fatalf("retry batch=%+v error=%v requests=%d", batch, err, len(clientRetryDoer.requests))
	}
	authDoer := &pagerDutyServicesDoer{t: t, responses: []pagerDutyServicesResponse{{status: http.StatusUnauthorized, body: `{"message":"bad token"}`}}}
	authClient := pagerDutyServicesTestClient(t, authDoer, providerfoundation.RetryPolicy{MaxAttempts: 3, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond})
	_, err = (PagerDutyServicesRouteHandler{}).Collect(context.Background(), claim, credential, authClient, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC))
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorAuthentication || len(authDoer.requests) != 1 {
		t.Fatalf("auth error=%v requests=%d", err, len(authDoer.requests))
	}
	capDoer := &pagerDutyServicesDoer{t: t, responses: []pagerDutyServicesResponse{{body: `{"services":[{"id":"one"}],"more":true}`}}}
	capClient := pagerDutyServicesTestClient(t, capDoer, providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond})
	_, err = (PagerDutyServicesRouteHandler{MaxPages: 1}).Collect(context.Background(), claim, credential, capClient, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC))
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("cap error=%v", err)
	}
	leaseDoer := &pagerDutyServicesDoer{t: t, responses: []pagerDutyServicesResponse{
		{body: `{"services":[{"id":"one"}],"more":true}`}, {body: `{"services":[],"more":false}`},
	}}
	asserts := 0
	leaseClient, err := providerfoundation.NewHTTPClient(
		"pagerduty", "https://api.pagerduty.com", leaseDoer,
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
	_, err = (PagerDutyServicesRouteHandler{}).Collect(context.Background(), claim, credential, leaseClient, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC))
	if !errors.Is(err, providerfoundation.ErrLeaseLost) || len(leaseDoer.requests) != 1 {
		t.Fatalf("lease error=%v requests=%d asserts=%d", err, len(leaseDoer.requests), asserts)
	}
}

func pagerDutyServicesTestClient(t *testing.T, doer providerfoundation.HTTPDoer, retry providerfoundation.RetryPolicy) *providerfoundation.HTTPClient {
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
