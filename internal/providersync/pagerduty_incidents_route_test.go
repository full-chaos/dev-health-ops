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

type pagerDutyIncidentFamilyResponse struct {
	status  int
	body    string
	headers http.Header
}

type pagerDutyIncidentFamilyDoer struct {
	t         *testing.T
	responses []pagerDutyIncidentFamilyResponse
	requests  []*http.Request
}

func (doer *pagerDutyIncidentFamilyDoer) Do(request *http.Request) (*http.Response, error) {
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

func TestPagerDutyIncidentFamilyRouteNormalizesIncidentAndPreservesCreatedCursor(t *testing.T) {
	t.Parallel()
	doer := &pagerDutyIncidentFamilyDoer{t: t, responses: []pagerDutyIncidentFamilyResponse{{
		body: `{"incidents":[{"id":"PI1","type":"incident","incident_number":42,"title":"Database outage","status":"resolved","urgency":"high","created_at":"2026-07-17T12:00:00.123456Z","updated_at":"2026-07-18T10:00:00.654321Z","resolved_at":"2026-07-18T09:00:00Z","service":{"id":"PSVC1"},"priority":{"id":"P1","summary":"P1"},"html_url":"https://acme.pagerduty.com/incidents/PI1"}],"more":false}`,
	}}}
	client := pagerDutyIncidentFamilyTestClient(t, doer)
	claim := nativeTestClaim("pagerduty", "incidents")
	credential := providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": " Acme "}}
	normalizedAt := time.Date(2026, 7, 19, 12, 0, 0, 987654321, time.FixedZone("PDT", -7*60*60))
	batch, err := (PagerDutyIncidentFamilyRouteHandler{}).Collect(
		context.Background(), claim, credential, client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "operational_incidents" ||
		batch.Effects[0].Recovery != EffectReadbackRequired || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	if batch.Evidence.Requests != 1 || batch.Evidence.Pages != 1 || batch.Evidence.Records != 1 || batch.Watermark == nil ||
		!batch.Watermark.Equal(time.Date(2026, 7, 17, 12, 0, 0, 123456000, time.UTC)) {
		t.Fatalf("batch=%+v", batch)
	}
	query := doer.requests[0].URL.Query()
	if query.Get("since") != claim.SinceAt.Format(time.RFC3339Nano) || query.Get("until") != claim.BeforeAt.Format(time.RFC3339Nano) ||
		query.Get("limit") != "100" || query.Get("offset") != "0" {
		t.Fatalf("query=%v", query)
	}
	if doer.requests[0].URL.Path != "/incidents" {
		t.Fatalf("parent path=%q", doer.requests[0].URL.Path)
	}
	row := mustPagerDutyIncidentRow(t, batch.Effects[0].Rows[0])
	if row.ProviderInstanceID != "acme" || row.ExternalID != "PI1" || row.Title != "Database outage" ||
		row.SourceEventID == nil || *row.SourceEventID != "42" || row.RawStatus == nil || *row.RawStatus != "resolved" ||
		row.NormalizedStatus == nil || *row.NormalizedStatus != "resolved" || row.NormalizedSeverity == nil || *row.NormalizedSeverity != "high" ||
		row.NormalizedPriority == nil || *row.NormalizedPriority != "high" || row.ServiceID == nil || row.ServiceExternalID == nil ||
		*row.ServiceExternalID != "PSVC1" || row.SourceURL == nil || *row.SourceURL != "https://acme.pagerduty.com/incidents/PI1" ||
		row.SourceEventAt == nil || !row.SourceEventAt.Equal(time.Date(2026, 7, 17, 12, 0, 0, 123456000, time.UTC)) ||
		!row.SourceVersionAt.Equal(time.Date(2026, 7, 18, 10, 0, 0, 654321000, time.UTC)) || row.SourceRevision == nil || row.IngestRevision == nil {
		t.Fatalf("row=%+v", row)
	}
	if expected := pagerDutyCanonicalReferenceID(claim.OrgID, "acme", "operational_service", "PSVC1"); row.ServiceID == nil || *row.ServiceID != *expected {
		t.Fatalf("service id=%v expected=%v", row.ServiceID, expected)
	}
}

func TestPagerDutyIncidentFamilyRouteCapsChildrenAndClampsToEarliestUndrainedIncident(t *testing.T) {
	doer := &pagerDutyIncidentFamilyDoer{t: t, responses: []pagerDutyIncidentFamilyResponse{
		{body: `{"incidents":[
			{"id":"PI-OLD","created_at":"2026-07-10T12:00:00Z","updated_at":"2026-07-10T12:00:00Z"},
			{"id":"PI-NEW","created_at":"2026-07-11T12:00:00Z","updated_at":"2026-07-11T12:00:00Z"}],"more":false}`},
		{body: `{"alerts":[{"id":"A-OLD-1","status":"triggered","created_at":"2026-07-10T12:00:00Z"},{"id":"A-OLD-2","status":"triggered","created_at":"2026-07-10T12:01:00Z"}],"more":true}`},
		{body: `{"alerts":[],"more":false}`},
	}}
	client := pagerDutyIncidentFamilyTestClient(t, doer)
	claim := nativeTestClaim("pagerduty", "incident-alerts")
	claim.DatasetOptions = map[string]any{"enrichment_cap": 1}
	batch, err := (PagerDutyIncidentFamilyRouteHandler{}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		client, time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Evidence.Requests != 3 || batch.Evidence.Pages != 3 || batch.Evidence.Records != 1 || len(batch.Effects[0].Rows) != 1 || batch.Watermark == nil ||
		!batch.Watermark.Equal(time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("batch=%+v", batch)
	}
	if got := doer.requests[1].URL.Path; got != "/incidents/PI-OLD/alerts" {
		t.Fatalf("first child path=%q", got)
	}
	if len(doer.requests) != 3 {
		t.Fatalf("requests=%d", len(doer.requests))
	}
	row := mustPagerDutyAlertRow(t, batch.Effects[0].Rows[0])
	if row.IncidentID == nil || row.Title != "A-OLD-1" || row.NormalizedStatus == nil || *row.NormalizedStatus != "open" {
		t.Fatalf("alert=%+v", row)
	}
}

func TestPagerDutyIncidentFamilyRouteKeepsSinceBoundaryInclusive(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "incidents")
	boundary := claim.SinceAt.UTC().Format(time.RFC3339)
	doer := &pagerDutyIncidentFamilyDoer{t: t, responses: []pagerDutyIncidentFamilyResponse{{
		body: `{"incidents":[
			{"id":"PI-BEFORE","created_at":"2026-06-30T23:59:59Z","updated_at":"2026-06-30T23:59:59Z"},
			{"id":"PI-BOUNDARY","created_at":"` + boundary + `","updated_at":"` + boundary + `"}],"more":false}`,
	}}}
	batch, err := (PagerDutyIncidentFamilyRouteHandler{}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		pagerDutyIncidentFamilyTestClient(t, doer), time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	row := mustPagerDutyIncidentRow(t, batch.Effects[0].Rows[0])
	if row.ExternalID != "PI-BOUNDARY" || batch.Watermark == nil || !batch.Watermark.Equal(claim.SinceAt.UTC()) {
		t.Fatalf("row=%+v watermark=%v", row, batch.Watermark)
	}
}

func TestPagerDutyIncidentFamilyRouteDoesNotPaginateNotes(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "incident-notes")
	doer := &pagerDutyIncidentFamilyDoer{t: t, responses: []pagerDutyIncidentFamilyResponse{
		{body: `{"incidents":[{"id":"PI1","created_at":"2026-07-10T12:00:00Z","updated_at":"2026-07-10T12:00:00Z"}],"more":false}`},
		{body: `{"notes":[{"id":"N1","content":"first note","created_at":"2026-07-10T12:01:00Z"}]}`},
	}}
	batch, err := (PagerDutyIncidentFamilyRouteHandler{}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		pagerDutyIncidentFamilyTestClient(t, doer), time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 2 || doer.requests[1].URL.Path != "/incidents/PI1/notes" || doer.requests[1].URL.RawQuery != "" {
		t.Fatalf("requests=%d child=%s", len(doer.requests), doer.requests[1].URL.RequestURI())
	}
	if batch.Evidence.Requests != 2 || batch.Evidence.Pages != 2 || batch.Evidence.Records != 1 || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("batch=%+v", batch)
	}
}

func TestPagerDutyIncidentFamilyRouteDisabledEnrichmentMakesNoProviderCall(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "incident-notes")
	claim.DatasetOptions = map[string]any{"enabled": false}
	doer := &pagerDutyIncidentFamilyDoer{t: t}
	batch, err := (PagerDutyIncidentFamilyRouteHandler{}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		pagerDutyIncidentFamilyTestClient(t, doer), time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 0 || batch.Evidence.Requests != 0 || batch.Evidence.Pages != 0 || len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 0 {
		t.Fatalf("requests=%d batch=%+v", len(doer.requests), batch)
	}
}

func TestPagerDutyIncidentFamilyRouteZeroCapReadsParentsWithoutChildRequests(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "incident-notes")
	claim.DatasetOptions = map[string]any{"enrichment_cap": 0}
	doer := &pagerDutyIncidentFamilyDoer{t: t, responses: []pagerDutyIncidentFamilyResponse{{
		body: `{"incidents":[{"id":"PI1","created_at":"2026-07-10T12:00:00Z","updated_at":"2026-07-10T12:00:00Z"}],"more":false}`,
	}}}
	batch, err := (PagerDutyIncidentFamilyRouteHandler{}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		pagerDutyIncidentFamilyTestClient(t, doer), time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 1 || batch.Evidence.Requests != 1 || batch.Evidence.Records != 0 || batch.Watermark == nil ||
		!batch.Watermark.Equal(time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("requests=%d batch=%+v", len(doer.requests), batch)
	}
}

func TestPagerDutyIncidentFamilyRouteRejectsPaginationCap(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "incidents")
	doer := &pagerDutyIncidentFamilyDoer{t: t, responses: []pagerDutyIncidentFamilyResponse{{
		body: `{"incidents":[{"id":"PI1","created_at":"2026-07-10T12:00:00Z"}],"more":true}`,
	}}}
	_, err := (PagerDutyIncidentFamilyRouteHandler{MaxPages: 1}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		pagerDutyIncidentFamilyTestClient(t, doer), time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("error=%v", err)
	}
}

func pagerDutyIncidentFamilyTestClient(t *testing.T, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"pagerduty", "https://api.pagerduty.com", doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func mustPagerDutyIncidentRow(t *testing.T, raw []byte) pagerDutyIncidentRow {
	t.Helper()
	var row pagerDutyIncidentRow
	if err := json.Unmarshal(raw, &row); err != nil {
		t.Fatal(err)
	}
	return row
}

func mustPagerDutyAlertRow(t *testing.T, raw []byte) pagerDutyAlertRow {
	t.Helper()
	var row pagerDutyAlertRow
	if err := json.Unmarshal(raw, &row); err != nil {
		t.Fatal(err)
	}
	return row
}

// TestPagerDutyIncidentsRouteAdvancesWatermarkOnAnEmptyWindow is CHAOS-3870
// evidence. A quiet PagerDuty account -- no incidents in the window, the
// common case -- produced no watermark, so Complete skipped the watermark
// write entirely: the incremental window [W, now] grew without bound, every
// run re-listed the whole span, and watermark-lag monitoring fired forever.
// Python's shared worker falls back to the window end for exactly this case.
func TestPagerDutyIncidentsRouteAdvancesWatermarkOnAnEmptyWindow(t *testing.T) {
	t.Parallel()
	doer := &pagerDutyIncidentFamilyDoer{t: t, responses: []pagerDutyIncidentFamilyResponse{{
		body: `{"incidents":[],"more":false}`,
	}}}
	client := pagerDutyIncidentFamilyTestClient(t, doer)
	claim := nativeTestClaim("pagerduty", "incidents")
	credential := providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}}
	batch, err := (PagerDutyIncidentFamilyRouteHandler{}).Collect(
		context.Background(), claim, credential, client,
		time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil {
		t.Fatal("empty window left the watermark unset, so it can never advance")
	}
	if !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark = %s, want window end %s", batch.Watermark, claim.BeforeAt)
	}
	if batch.Evidence.Records != 0 {
		t.Fatalf("empty window reported %d records", batch.Evidence.Records)
	}
}

// A window the route could not read in full must NOT advance: the fail-closed
// refusal Go added over Python (which silently truncated and advanced anyway)
// is preserved.
func TestPagerDutyIncidentsRouteWithholdsWatermarkWhenTheWindowWasCapped(t *testing.T) {
	t.Parallel()
	batch, err := (PagerDutyIncidentFamilyRouteHandler{}).collectPagerDutyIncidents(
		nativeTestClaim("pagerduty", "incidents"), "acme",
		time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC),
		pagerDutyIncidentPageCollection{Items: nil, Pages: 1, CapReached: true},
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark != nil {
		t.Fatalf("capped window advanced the watermark to %s", batch.Watermark)
	}
}
