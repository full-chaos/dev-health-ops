package providersync

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// pagerDutyEntitlementDoer fails the test on ANY provider request. The
// entitlement re-check exists to run before provider fetch; a request here
// means the check was skipped or ran too late.
type pagerDutyEntitlementDoer struct {
	t        *testing.T
	requests int
}

func (doer *pagerDutyEntitlementDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests++
	doer.t.Fatalf("provider fetch %s must not run for a refused entitlement", request.URL.RequestURI())
	return nil, errors.New("unreachable")
}

func pagerDutyEntitlementTestClient(
	t *testing.T, doer providerfoundation.HTTPDoer, metrics *providerfoundation.Metrics,
) *providerfoundation.HTTPClient {
	t.Helper()
	client := pagerDutyIncidentFamilyTestClient(t, doer)
	client.Metrics = metrics
	return client
}

func refusingIncidentEntitlement(checks *int) incidentEntitlementFunc {
	return incidentEntitlementFunc(func(context.Context, string) error {
		*checks++
		return ErrIncidentEntitlementDisabled
	})
}

func assertIncidentEntitlementRefusalCounted(
	t *testing.T, metrics *providerfoundation.Metrics, provider, dataset, seam string, want int,
) {
	t.Helper()
	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	line := `dev_health_provider_incident_entitlement_refused_total{provider="` + provider +
		`",dataset="` + dataset + `",seam="` + seam + `"} `
	rendered := output.String()
	count := 0
	for _, candidate := range strings.Split(rendered, "\n") {
		if strings.HasPrefix(candidate, line) {
			count++
			if candidate != line+itoa(want) {
				t.Fatalf("counter=%q want=%q", candidate, line+itoa(want))
			}
		}
	}
	if (want == 0) != (count == 0) {
		t.Fatalf("counter presence=%d want=%d in:\n%s", count, want, rendered)
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := ""
	for value > 0 {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}
	return digits
}

// Seam 1 of 2 (CHAOS-4219): a PagerDuty unit for an organization whose
// canonical-incident feature is disabled is refused at execution BEFORE any
// provider request, with the same error Jira's route already returns, and the
// refusal is counted under the provider label on the ONE shared series.
func TestPagerDutyIncidentFamilyRouteRejectsDisabledEntitlementBeforeProviderFetch(t *testing.T) {
	t.Parallel()
	doer := &pagerDutyEntitlementDoer{t: t}
	metrics := providerfoundation.NewMetrics()
	client := pagerDutyEntitlementTestClient(t, doer, metrics)
	claim := nativeTestClaim("pagerduty", "incidents")
	checks := 0
	batch, err := (PagerDutyIncidentFamilyRouteHandler{
		Entitlement: refusingIncidentEntitlement(&checks),
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		client, time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrIncidentEntitlementDisabled) || checks != 1 || doer.requests != 0 ||
		batch.Watermark != nil || len(batch.Effects) != 0 {
		t.Fatalf("checks=%d requests=%d batch=%+v err=%v", checks, doer.requests, batch, err)
	}
	assertIncidentEntitlementRefusalCounted(t, metrics, "pagerduty", "incidents", "collect", 1)
	assertIncidentEntitlementRefusalCounted(t, metrics, "pagerduty", "incidents", "write", 0)
}

// Seam 2 of 2 (CHAOS-4219): a grant that was valid at collection and revoked
// before persistence is re-checked at the ClickHouse write boundary, before
// the sink touches the connection at all.
func TestPagerDutyIncidentFamilyEffectsRecheckRevokedEntitlementAtClickHouseWrite(t *testing.T) {
	t.Parallel()
	doer := &pagerDutyIncidentFamilyDoer{t: t, responses: []pagerDutyIncidentFamilyResponse{{
		body: `{"incidents":[{"id":"PI1","title":"Database outage","status":"resolved","created_at":"2026-07-17T12:00:00Z","updated_at":"2026-07-18T10:00:00Z"}],"more":false}`,
	}}}
	metrics := providerfoundation.NewMetrics()
	client := pagerDutyEntitlementTestClient(t, doer, metrics)
	claim := nativeTestClaim("pagerduty", "incidents")
	checks, revoked := 0, false
	entitlement := incidentEntitlementFunc(func(context.Context, string) error {
		checks++
		if revoked {
			return ErrIncidentEntitlementDisabled
		}
		return nil
	})
	batch, err := (PagerDutyIncidentFamilyRouteHandler{Entitlement: entitlement}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		client, time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC),
	)
	if err != nil || checks != 1 || len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("collect checks=%d batch=%+v err=%v", checks, batch, err)
	}
	revoked = true
	err = (PagerDutyIncidentFamilyClickHouseEffects{
		Conn:               unreachableConn{t: t},
		Lease:              providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		ProviderInstanceID: "acme",
		Entitlement:        entitlement,
		Metrics:            metrics,
	}).WriteEffect(context.Background(), claim, batch.Effects[0])
	if !errors.Is(err, ErrIncidentEntitlementDisabled) || checks != 2 {
		t.Fatalf("write checks=%d err=%v", checks, err)
	}
	assertIncidentEntitlementRefusalCounted(t, metrics, "pagerduty", "incidents", "write", 1)
	assertIncidentEntitlementRefusalCounted(t, metrics, "pagerduty", "incidents", "collect", 0)
}

type pagerDutyGatedRoute struct {
	dataset string
	build   func(IncidentEntitlement) CompleteRouteHandler
}

// pagerDutyGatedRoutes is the sweep: every PagerDuty dataset is
// canonical-incident gated (datasets.py _GATED_SYNC_TARGETS covers the
// "operational" legacy target every one of them maps to), so every route
// handler must carry the re-check. A handler missing from this table is
// caught by TestEveryNativeGoPagerDutyPairIsInTheEntitlementSweep below.
var pagerDutyGatedRoutes = []pagerDutyGatedRoute{
	{"services", func(e IncidentEntitlement) CompleteRouteHandler { return PagerDutyServicesRouteHandler{Entitlement: e} }},
	{"business-services", func(e IncidentEntitlement) CompleteRouteHandler {
		return PagerDutyBusinessServicesRouteHandler{Entitlement: e}
	}},
	{"escalation-policies", func(e IncidentEntitlement) CompleteRouteHandler {
		return PagerDutyEscalationPoliciesRouteHandler{Entitlement: e}
	}},
	{"schedules", func(e IncidentEntitlement) CompleteRouteHandler {
		return PagerDutySchedulesRouteHandler{Entitlement: e}
	}},
	{"on-calls", func(e IncidentEntitlement) CompleteRouteHandler { return PagerDutyOnCallsRouteHandler{Entitlement: e} }},
	{"users", func(e IncidentEntitlement) CompleteRouteHandler { return PagerDutyUsersRouteHandler{Entitlement: e} }},
	{"teams", func(e IncidentEntitlement) CompleteRouteHandler { return PagerDutyTeamsRouteHandler{Entitlement: e} }},
	{"incidents", func(e IncidentEntitlement) CompleteRouteHandler {
		return PagerDutyIncidentFamilyRouteHandler{Entitlement: e}
	}},
	{"incident-alerts", func(e IncidentEntitlement) CompleteRouteHandler {
		return PagerDutyIncidentFamilyRouteHandler{Entitlement: e}
	}},
	{"incident-log-entries", func(e IncidentEntitlement) CompleteRouteHandler {
		return PagerDutyIncidentFamilyRouteHandler{Entitlement: e}
	}},
	{"incident-notes", func(e IncidentEntitlement) CompleteRouteHandler {
		return PagerDutyIncidentFamilyRouteHandler{Entitlement: e}
	}},
}

func TestEveryPagerDutyRouteRechecksEntitlementBeforeProviderFetch(t *testing.T) {
	t.Parallel()
	for _, route := range pagerDutyGatedRoutes {
		route := route
		t.Run(route.dataset, func(t *testing.T) {
			t.Parallel()
			claim := nativeTestClaim("pagerduty", route.dataset)
			credential := providerfoundation.Credential{
				Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"},
			}
			at := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
			doer := &pagerDutyEntitlementDoer{t: t}
			metrics := providerfoundation.NewMetrics()
			client := pagerDutyEntitlementTestClient(t, doer, metrics)
			// An unwired entitlement is a construction defect, never a
			// pass-through.
			if _, err := route.build(nil).Collect(context.Background(), claim, credential, client, at); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("nil entitlement err=%v", err)
			}
			checks := 0
			batch, err := route.build(refusingIncidentEntitlement(&checks)).Collect(
				context.Background(), claim, credential, client, at,
			)
			if !errors.Is(err, ErrIncidentEntitlementDisabled) || checks != 1 || doer.requests != 0 ||
				len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("checks=%d requests=%d batch=%+v err=%v", checks, doer.requests, batch, err)
			}
			assertIncidentEntitlementRefusalCounted(t, metrics, "pagerduty", route.dataset, "collect", 1)
		})
	}
}

type pagerDutyGatedSink struct {
	dataset     string
	destination string
	build       func(unreachableConn, IncidentEntitlement, *providerfoundation.Metrics) EffectSink
}

var pagerDutyGatedSinks = []pagerDutyGatedSink{
	{"services", "operational_services", func(conn unreachableConn, e IncidentEntitlement, m *providerfoundation.Metrics) EffectSink {
		return PagerDutyServicesClickHouseEffects{Conn: conn, Lease: allowLease, ProviderInstanceID: "acme", Entitlement: e, Metrics: m}
	}},
	{"business-services", "operational_services", func(conn unreachableConn, e IncidentEntitlement, m *providerfoundation.Metrics) EffectSink {
		return PagerDutyBusinessServicesClickHouseEffects{Conn: conn, Lease: allowLease, ProviderInstanceID: "acme", Entitlement: e, Metrics: m}
	}},
	{"escalation-policies", "operational_escalation_policies", func(conn unreachableConn, e IncidentEntitlement, m *providerfoundation.Metrics) EffectSink {
		return PagerDutyEscalationPoliciesClickHouseEffects{Conn: conn, Lease: allowLease, Entitlement: e, Metrics: m}
	}},
	{"schedules", "operational_on_call_schedules", func(conn unreachableConn, e IncidentEntitlement, m *providerfoundation.Metrics) EffectSink {
		return PagerDutySchedulesClickHouseEffects{Conn: conn, Lease: allowLease, ProviderInstanceID: "acme", Entitlement: e, Metrics: m}
	}},
	{"on-calls", "operational_on_call_assignments", func(conn unreachableConn, e IncidentEntitlement, m *providerfoundation.Metrics) EffectSink {
		return PagerDutyOnCallsClickHouseEffects{Conn: conn, Lease: allowLease, ProviderInstanceID: "acme", Entitlement: e, Metrics: m}
	}},
	{"users", "operational_users", func(conn unreachableConn, e IncidentEntitlement, m *providerfoundation.Metrics) EffectSink {
		return PagerDutyUsersClickHouseEffects{Conn: conn, Lease: allowLease, ProviderInstanceID: "acme", Entitlement: e, Metrics: m}
	}},
	{"teams", "operational_teams", func(conn unreachableConn, e IncidentEntitlement, m *providerfoundation.Metrics) EffectSink {
		return PagerDutyTeamsClickHouseEffects{Conn: conn, Lease: allowLease, ProviderInstanceID: "acme", Entitlement: e, Metrics: m}
	}},
	{"incidents", "operational_incidents", func(conn unreachableConn, e IncidentEntitlement, m *providerfoundation.Metrics) EffectSink {
		return PagerDutyIncidentFamilyClickHouseEffects{Conn: conn, Lease: allowLease, ProviderInstanceID: "acme", Entitlement: e, Metrics: m}
	}},
	{"incident-alerts", "operational_alerts", func(conn unreachableConn, e IncidentEntitlement, m *providerfoundation.Metrics) EffectSink {
		return PagerDutyIncidentFamilyClickHouseEffects{Conn: conn, Lease: allowLease, ProviderInstanceID: "acme", Entitlement: e, Metrics: m}
	}},
	{"incident-log-entries", "operational_incident_timeline_events", func(conn unreachableConn, e IncidentEntitlement, m *providerfoundation.Metrics) EffectSink {
		return PagerDutyIncidentFamilyClickHouseEffects{Conn: conn, Lease: allowLease, ProviderInstanceID: "acme", Entitlement: e, Metrics: m}
	}},
	{"incident-notes", "operational_incident_notes", func(conn unreachableConn, e IncidentEntitlement, m *providerfoundation.Metrics) EffectSink {
		return PagerDutyIncidentFamilyClickHouseEffects{Conn: conn, Lease: allowLease, ProviderInstanceID: "acme", Entitlement: e, Metrics: m}
	}},
}

var allowLease = providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })

func TestEveryPagerDutySinkRechecksEntitlementAtClickHouseWrite(t *testing.T) {
	t.Parallel()
	for _, sink := range pagerDutyGatedSinks {
		sink := sink
		t.Run(sink.dataset, func(t *testing.T) {
			t.Parallel()
			claim := nativeTestClaim("pagerduty", sink.dataset)
			effect := EffectBatch{Destination: sink.destination}
			metrics := providerfoundation.NewMetrics()
			conn := unreachableConn{t: t}
			if err := sink.build(conn, nil, metrics).WriteEffect(context.Background(), claim, effect); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("nil entitlement err=%v", err)
			}
			checks := 0
			err := sink.build(conn, refusingIncidentEntitlement(&checks), metrics).WriteEffect(
				context.Background(), claim, effect,
			)
			if !errors.Is(err, ErrIncidentEntitlementDisabled) || checks != 1 {
				t.Fatalf("checks=%d err=%v", checks, err)
			}
			assertIncidentEntitlementRefusalCounted(t, metrics, "pagerduty", sink.dataset, "write", 1)
		})
	}
}

// The sweep tables above are only a proof if they are complete: every
// native_go pagerduty pair in the capability matrix must appear in BOTH.
func TestEveryNativeGoPagerDutyPairIsInTheEntitlementSweep(t *testing.T) {
	t.Parallel()
	routes, sinks := map[string]struct{}{}, map[string]struct{}{}
	for _, route := range pagerDutyGatedRoutes {
		routes[route.dataset] = struct{}{}
	}
	for _, sink := range pagerDutyGatedSinks {
		sinks[sink.dataset] = struct{}{}
	}
	seen := 0
	for _, pair := range BuildProviderMatrix().Pairs {
		if pair.Provider != "pagerduty" || pair.GoExecutor != ExecutorNativeGo {
			continue
		}
		seen++
		if _, ok := routes[pair.Dataset]; !ok {
			t.Errorf("pagerduty/%s route is not in the entitlement sweep", pair.Dataset)
		}
		if _, ok := sinks[pair.Dataset]; !ok {
			t.Errorf("pagerduty/%s sink is not in the entitlement sweep", pair.Dataset)
		}
	}
	if seen != len(routes) || seen != len(sinks) {
		t.Fatalf("native pagerduty pairs=%d routes=%d sinks=%d", seen, len(routes), len(sinks))
	}
}
