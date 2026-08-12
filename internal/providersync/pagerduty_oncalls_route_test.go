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

type pagerDutyOnCallsResponse struct {
	status int
	body   string
}

type pagerDutyOnCallsDoer struct {
	t         *testing.T
	responses []pagerDutyOnCallsResponse
	requests  []*http.Request
}

func (doer *pagerDutyOnCallsDoer) Do(request *http.Request) (*http.Response, error) {
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

func TestPagerDutyOnCallsRouteUsesOffsetPaginationAndCanonicalAssignment(t *testing.T) {
	t.Parallel()
	doer := &pagerDutyOnCallsDoer{t: t, responses: []pagerDutyOnCallsResponse{
		{body: `{"oncalls":[
			{"id":"OC1","type":"oncall","start":"2026-08-01T10:00:00.123456Z","end":"2026-08-01T18:00:00.123456Z","escalation_level":1,"user":{"id":"PU1"},"schedule":{"id":"PS1"},"escalation_policy":{"id":"PE1"},"updated_at":"2026-08-01T10:00:00.123456Z","html_url":"https://acme.pagerduty.com/oncalls/OC1","self":"/oncalls/OC1"},
			{"type":"oncall","start":"2026-08-02T10:00:00Z","end":"2026-08-02T18:00:00Z","escalation_level":2,"user":{"id":"PU2"},"schedule":{"id":"PS2"},"escalation_policy":{"id":"PE2"},"created_at":"2026-07-31T09:00:00Z","self":"/oncalls/composite"}],"more":true}`},
		{body: `{"oncalls":[],"more":false}`},
	}}
	client := pagerDutyOnCallsTestClient(t, doer, providerfoundation.RetryPolicy{
		MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	})
	claim := nativeTestClaim("pagerduty", "on-calls")
	credential := providerfoundation.Credential{
		Provider: "pagerduty", Config: map[string]string{"subdomain": " Acme "},
	}
	normalizedAt := time.Date(2026, 8, 9, 12, 0, 0, 987654321, time.FixedZone("PDT", -7*60*60))
	batch, err := (PagerDutyOnCallsRouteHandler{MaxPages: 10}).Collect(
		context.Background(), claim, credential, client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, batch,
	)
	if err != nil {
		t.Fatalf("production comparator rejected collected batch: %v", err)
	}
	if !comparison.Match || comparison.NativeRecords != 2 || comparison.PythonRecords != 2 {
		t.Fatalf("comparison=%+v", comparison)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "operational_on_call_assignments" ||
		batch.Effects[0].Recovery != EffectReadbackRequired || len(batch.Effects[0].Rows) != 2 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	if batch.Evidence.Requests != 2 || batch.Evidence.Pages != 2 || batch.Evidence.Records != 2 ||
		batch.Evidence.CapReached || batch.Watermark != nil || batch.Result["on_calls_synced"] != 2 {
		t.Fatalf("batch=%+v", batch)
	}
	if got := doer.requests[0].URL.RawQuery; got != "limit=100&offset=0" {
		t.Fatalf("first query=%q", got)
	}
	if got := doer.requests[0].URL.Path; got != "/oncalls" {
		t.Fatalf("first path=%q", got)
	}
	if got := doer.requests[1].URL.RawQuery; got != "limit=100&offset=2" {
		t.Fatalf("second query=%q", got)
	}
	row := mustPagerDutyOnCallRow(t, batch.Effects[0].Rows[0])
	if row.Provider != "pagerduty" || row.ProviderInstanceID != "acme" ||
		row.SourceEntityType != "oncall" || row.ExternalID != "OC1" ||
		row.SourceURL == nil || *row.SourceURL != "https://acme.pagerduty.com/oncalls/OC1" ||
		row.ScheduleID == nil || row.UserID == nil || row.EscalationPolicyID == nil ||
		row.EscalationLevel == nil || *row.EscalationLevel != 1 ||
		row.StartsAt == nil || row.EndsAt == nil || row.OrderingContract != 2 ||
		!row.SourceVersionAt.Equal(time.Date(2026, 8, 1, 10, 0, 0, 123456000, time.UTC)) ||
		!row.ObservedAt.Equal(time.Date(2026, 8, 9, 19, 0, 0, 987654000, time.UTC)) {
		t.Fatalf("row=%+v", row)
	}
	second := mustPagerDutyOnCallRow(t, batch.Effects[0].Rows[1])
	if !strings.Contains(second.ExternalID, "PE2|PS2|PU2|2|2026-08-02T10:00:00+00:00|2026-08-02T18:00:00+00:00") ||
		second.SourceURL == nil || *second.SourceURL != "/oncalls/composite" ||
		second.ScheduleID == nil || second.UserID == nil || second.EscalationPolicyID == nil {
		t.Fatalf("composite row=%+v", second)
	}
}

func TestPagerDutyOnCallsRouteRejectsAnotherPagerDutyDataset(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("pagerduty", "schedules")
	credential := providerfoundation.Credential{
		Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"},
	}
	client := pagerDutyOnCallsTestClient(t, &pagerDutyOnCallsDoer{
		t: t, responses: []pagerDutyOnCallsResponse{{body: `{"oncalls":[],"more":false}`}},
	}, providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond})
	if _, err := (PagerDutyOnCallsRouteHandler{}).Collect(
		context.Background(), claim, credential, client,
		time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong dataset error=%v", err)
	}
}

func TestPagerDutyOnCallsRoutePreservesRetryAndPermanentErrorSemantics(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("pagerduty", "on-calls")
	credential := providerfoundation.Credential{
		Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"},
	}
	retryDoer := &pagerDutyOnCallsDoer{t: t, responses: []pagerDutyOnCallsResponse{
		{status: http.StatusTooManyRequests, body: `{"message":"slow down"}`},
		{body: `{"oncalls":[],"more":false}`},
	}}
	retryClient := pagerDutyOnCallsTestClient(t, retryDoer, providerfoundation.RetryPolicy{
		MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	})
	batch, err := (PagerDutyOnCallsRouteHandler{}).Collect(
		context.Background(), claim, credential, retryClient,
		time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if err != nil || len(retryDoer.requests) != 2 || len(batch.Effects) != 1 {
		t.Fatalf("retry batch=%+v error=%v requests=%d", batch, err, len(retryDoer.requests))
	}

	authDoer := &pagerDutyOnCallsDoer{t: t, responses: []pagerDutyOnCallsResponse{
		{status: http.StatusUnauthorized, body: `{"message":"bad token"}`},
	}}
	authClient := pagerDutyOnCallsTestClient(t, authDoer, providerfoundation.RetryPolicy{
		MaxAttempts: 3, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	})
	_, err = (PagerDutyOnCallsRouteHandler{}).Collect(
		context.Background(), claim, credential, authClient,
		time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorAuthentication ||
		len(authDoer.requests) != 1 {
		t.Fatalf("auth error=%v requests=%d", err, len(authDoer.requests))
	}
}

func TestPagerDutyOnCallsRouteFailsClosedOnPaginationCapAndInvalidAssignment(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("pagerduty", "on-calls")
	credential := providerfoundation.Credential{
		Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"},
	}
	client := pagerDutyOnCallsTestClient(t, &pagerDutyOnCallsDoer{
		t: t, responses: []pagerDutyOnCallsResponse{{body: `{"oncalls":[{"type":"oncall","user":{"id":"PU1"}}],"more":true}`}},
	}, providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond})
	_, err := (PagerDutyOnCallsRouteHandler{MaxPages: 1}).Collect(
		context.Background(), claim, credential, client,
		time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("cap error=%v", err)
	}

	invalidClient := pagerDutyOnCallsTestClient(t, &pagerDutyOnCallsDoer{
		t: t, responses: []pagerDutyOnCallsResponse{{body: `{"oncalls":[{"type":"oncall","user":{"id":"PU1"}}],"more":false}`}},
	}, providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond})
	_, err = (PagerDutyOnCallsRouteHandler{}).Collect(
		context.Background(), claim, credential, invalidClient,
		time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
		t.Fatalf("invalid assignment error=%v", err)
	}
}

func TestPagerDutyOnCallsRouteStopsWhenLeaseExpiresBetweenPages(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("pagerduty", "on-calls")
	doer := &pagerDutyOnCallsDoer{t: t, responses: []pagerDutyOnCallsResponse{
		{body: `{"oncalls":[{"id":"OC1","type":"oncall"}],"more":true}`},
		{body: `{"oncalls":[],"more":false}`},
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
	_, err = (PagerDutyOnCallsRouteHandler{}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		client, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, providerfoundation.ErrLeaseLost) || len(doer.requests) != 1 {
		t.Fatalf("lease error=%v requests=%d asserts=%d", err, len(doer.requests), asserts)
	}
}

func pagerDutyOnCallsTestClient(
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

func mustPagerDutyOnCallRow(t *testing.T, raw []byte) pagerDutyOnCallRow {
	t.Helper()
	var row pagerDutyOnCallRow
	if err := json.Unmarshal(raw, &row); err != nil {
		t.Fatal(err)
	}
	return row
}
