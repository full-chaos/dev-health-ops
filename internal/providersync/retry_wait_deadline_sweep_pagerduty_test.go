package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// sweepG5Doer answers its bodies in order with 200; a request past the list
// gets 404 (no test failure, so it is safe inside the sweep goroutines).
type sweepG5Doer struct {
	mu     sync.Mutex
	bodies []string
}

func (d *sweepG5Doer) Do(request *http.Request) (*http.Response, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	status, body := http.StatusNotFound, `{}`
	if len(d.bodies) > 0 {
		status, body = http.StatusOK, d.bodies[0]
		d.bodies = d.bodies[1:]
	}
	return &http.Response{
		StatusCode: status, Request: request, Header: http.Header{},
		Body: io.NopCloser(strings.NewReader(body)),
	}, nil
}

func sweepG5Collect(
	t *testing.T, ctx context.Context, dataset string, options map[string]any,
	doer HTTPDoerForSweep, wait time.Duration,
	collect func(context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time) (CompleteRouteBatch, error),
) (CompleteRouteBatch, error) {
	claim := nativeTestClaim("pagerduty", dataset)
	if options != nil {
		claim.DatasetOptions = options
	}
	credential := providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": " Acme "}}
	return collect(ctx, claim, credential,
		sweepClient(t, "pagerduty", "https://api.pagerduty.com", doer, wait),
		time.Date(2026, 8, 9, 12, 0, 0, 987654321, time.UTC))
}

func sweepG5Register(name, dataset string, options map[string]any, bodies []string,
	collect func(context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time) (CompleteRouteBatch, error)) {
	registerRetrySweepRoute(retrySweepRoute{
		name: name,
		newDoer: func() HTTPDoerForSweep {
			return &sweepG5Doer{bodies: append([]string(nil), bodies...)}
		},
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			return sweepG5Collect(t, ctx, dataset, options, doer, wait, collect)
		},
	})
}

func sweepG5Incidents() []string {
	return []string{
		`{"incidents":[{"id":"PI1","created_at":"2026-07-10T12:00:00Z","updated_at":"2026-07-10T12:00:00Z"}],"more":true}`,
		`{"incidents":[{"id":"PI2","created_at":"2026-07-11T12:00:00Z","updated_at":"2026-07-11T12:00:00Z"}],"more":false}`,
	}
}

func init() {
	pageTwo := func(key, one, two, three string) []string {
		return []string{
			`{"` + key + `":[` + one + `,` + two + `],"more":true}`,
			`{"` + key + `":[` + three + `],"more":false}`,
		}
	}
	oncall := func(id string) string {
		return `{"id":"` + id + `","type":"oncall","start":"2026-08-01T10:00:00Z","end":"2026-08-01T18:00:00Z","escalation_level":1,"user":{"id":"PU1"},"schedule":{"id":"PS1"},"escalation_policy":{"id":"PE1"},"updated_at":"2026-08-01T10:00:00Z"}`
	}
	simple := func(typ, id string) string {
		return `{"id":"` + id + `","type":"` + typ + `","name":"N` + id + `","updated_at":"2026-08-01T10:00:00Z"}`
	}
	sweepG5Register("pagerduty/on-calls", "on-calls", nil,
		pageTwo("oncalls", oncall("OC1"), oncall("OC2"), oncall("OC3")),
		PagerDutyOnCallsRouteHandler{Entitlement: allowIncidentEntitlement, MaxPages: 10}.Collect)
	sweepG5Register("pagerduty/escalation-policies", "escalation-policies", nil,
		pageTwo("escalation_policies", simple("escalation_policy", "PE1"), simple("escalation_policy", "PE2"), simple("escalation_policy", "PE3")),
		PagerDutyEscalationPoliciesRouteHandler{Entitlement: allowIncidentEntitlement, MaxPages: 10}.Collect)
	sweepG5Register("pagerduty/users", "users", nil,
		pageTwo("users", simple("user", "PU1"), simple("user", "PU2"), simple("user", "PU3")),
		PagerDutyUsersRouteHandler{Entitlement: allowIncidentEntitlement, MaxPages: 10}.Collect)
	sweepG5Register("pagerduty/schedules", "schedules", nil,
		pageTwo("schedules", simple("schedule", "PS1"), simple("schedule", "PS2"), simple("schedule", "PS3")),
		PagerDutySchedulesRouteHandler{Entitlement: allowIncidentEntitlement, MaxPages: 10}.Collect)
	sweepG5Register("pagerduty/business-services", "business-services", nil,
		pageTwo("business_services", simple("business_service", "PBS1"), simple("business_service", "PBS2"), simple("business_service", "PBS3")),
		PagerDutyBusinessServicesRouteHandler{Entitlement: allowIncidentEntitlement, MaxPages: 10}.Collect)
	sweepG5Register("pagerduty/teams", "teams", nil,
		pageTwo("teams", simple("team", "PT1"), simple("team", "PT2"), simple("team", "PT3")),
		PagerDutyTeamsRouteHandler{Entitlement: allowIncidentEntitlement, MaxPages: 10}.Collect)
	sweepG5Register("pagerduty/services", "services", nil,
		pageTwo("services", simple("service", "PS1"), simple("service", "PS2"), simple("service", "PS3")),
		PagerDutyServicesRouteHandler{Entitlement: allowIncidentEntitlement, MaxPages: 10}.Collect)

	family := PagerDutyIncidentFamilyRouteHandler{Entitlement: allowIncidentEntitlement}
	sweepG5Register("pagerduty/incidents", "incidents", nil, sweepG5Incidents(), family.Collect)
	sweepG5Register("pagerduty/incident-alerts", "incident-alerts", nil,
		append(sweepG5Incidents(),
			`{"alerts":[{"id":"A1","status":"triggered","created_at":"2026-07-10T12:00:00Z"}],"more":true}`,
			`{"alerts":[{"id":"A2","status":"resolved","created_at":"2026-07-10T12:01:00Z"}],"more":false}`,
			`{"alerts":[{"id":"A3","status":"triggered","created_at":"2026-07-11T12:00:00Z"}],"more":false}`),
		family.Collect)
	sweepG5Register("pagerduty/incident-log-entries", "incident-log-entries", nil,
		append(sweepG5Incidents(),
			`{"log_entries":[{"id":"L1","type":"status_change","summary":"Triggered","created_at":"2026-07-10T12:00:00Z"}],"more":true}`,
			`{"log_entries":[{"id":"L2","type":"status_change","summary":"Resolved","created_at":"2026-07-10T12:01:00Z"}],"more":false}`,
			`{"log_entries":[{"id":"L3","type":"status_change","summary":"Triggered","created_at":"2026-07-11T12:00:00Z"}],"more":false}`),
		family.Collect)
	sweepG5Register("pagerduty/incident-notes", "incident-notes", nil,
		append(sweepG5Incidents(),
			`{"notes":[{"id":"N1","content":"one","created_at":"2026-07-10T12:01:00Z"}]}`,
			`{"notes":[{"id":"N2","content":"two","created_at":"2026-07-11T12:01:00Z"}]}`),
		family.Collect)
}
