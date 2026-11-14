package authzenforce

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var catalogNamespaces = []string{
	"acr", "admin", "analytics", "authz", "billing", "credentials", "git", "integrations",
	"license", "members", "metrics", "org", "reports", "settings", "sso", "teams", "work_items",
}

func none(string) (string, bool) { return "", false }

func only(name, value string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		if key == name {
			return value, true
		}
		return "", false
	}
}

func TestNamespacesAreTheCatalogs(t *testing.T) {
	if got := Namespaces(); !slices.Equal(got, catalogNamespaces) {
		t.Fatalf("Namespaces() = %v, want %v", got, catalogNamespaces)
	}
}

func TestServicesAndEnvNames(t *testing.T) {
	if got := Services(); !slices.Equal(got, []Service{GoAPI, QueryAPI, QueryAPIHeaders}) {
		t.Fatalf("Services() = %v", got)
	}
	if GoAPI.Env() != "GO_API_AUTHZ_ENFORCE" || QueryAPI.Env() != "QUERY_API_AUTHZ_ENFORCE" || QueryAPIHeaders.Env() != "QUERY_API_HEADERS_AUTHZ_ENFORCE" {
		t.Fatalf("env names %q %q %q", GoAPI.Env(), QueryAPI.Env(), QueryAPIHeaders.Env())
	}
}

func TestEveryCellIsOffByDefault(t *testing.T) {
	cells := 0
	for _, service := range Services() {
		for _, sw := range []Switch{{}, Parse(service, ""), Load(service, none, nil), Load(service, only(service.Env(), "  "), nil)} {
			for _, namespace := range Namespaces() {
				cells++
				if sw.On(namespace) {
					t.Errorf("%s/%s is on by default", service, namespace)
				}
			}
			if sw.Ignored() != 0 {
				t.Errorf("%s: default ignored = %d", service, sw.Ignored())
			}
		}
	}
	if want := 4 * len(Services()) * len(catalogNamespaces); cells != want {
		t.Fatalf("checked %d cells, want %d", cells, want)
	}
}

func TestEachCellTurnsOnAlone(t *testing.T) {
	for _, service := range Services() {
		for _, on := range Namespaces() {
			sw := Load(service, only(service.Env(), on), nil)
			for _, namespace := range Namespaces() {
				if got, want := sw.On(namespace), namespace == on; got != want {
					t.Errorf("%s=%s: On(%s) = %v, want %v", service.Env(), on, namespace, got, want)
				}
			}
			if sw.Ignored() != 0 || sw.Service() != service {
				t.Errorf("%s=%s: ignored %d service %q", service.Env(), on, sw.Ignored(), sw.Service())
			}
		}
	}
}

func TestOneServicesVariableNeverTurnsAnotherOn(t *testing.T) {
	lookup := only(GoAPI.Env(), "billing,teams")
	if Load(QueryAPI, lookup, nil).On("billing") {
		t.Fatal("go-api's variable turned a query-api cell on")
	}
	sw := Load(GoAPI, lookup, nil)
	if !sw.On("billing") || !sw.On("teams") || sw.On("members") {
		t.Fatal("go-api's list was not read as billing and teams")
	}
}

func TestParse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     string
		on      []string
		ignored int
	}{
		{"empty", "", nil, 0},
		{"blank", " \t ", nil, 0},
		{"one", "billing", []string{"billing"}, 0},
		{"spaces", " billing , teams ", []string{"billing", "teams"}, 0},
		{"wrong case", "Billing", nil, 1},
		{"unknown", "nope", nil, 1},
		{"repeat", "billing,billing", []string{"billing"}, 1},
		{"empty item", "billing,,teams", []string{"billing", "teams"}, 1},
		{"trailing comma", "billing,", []string{"billing"}, 1},
		{"star", "*", nil, 1},
		{"star with a name", "billing,*", []string{"billing"}, 1},
		{"dotted action", "billing.read", nil, 1},
		{"semicolon", "billing;teams", nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sw := Parse(GoAPI, tc.raw)
			var got []string
			for _, namespace := range Namespaces() {
				if sw.On(namespace) {
					got = append(got, namespace)
				}
			}
			if !slices.Equal(got, tc.on) || sw.Ignored() != tc.ignored {
				t.Fatalf("Parse(%q) on %v ignored %d, want on %v ignored %d", tc.raw, got, sw.Ignored(), tc.on, tc.ignored)
			}
		})
	}
}

func TestAnUnknownNamespaceIsNeverOn(t *testing.T) {
	sw := Parse(GoAPI, "billing")
	for _, namespace := range []string{"", "*", "Billing", "billing.read", "nope"} {
		if sw.On(namespace) {
			t.Errorf("On(%q) = true", namespace)
		}
	}
}

func TestAnUnknownServiceIsOff(t *testing.T) {
	sw := Load(Service("auth-service"), only("AUTH_SERVICE_AUTHZ_ENFORCE", "billing"), nil)
	if sw.On("billing") {
		t.Fatal("an unknown service has a cell")
	}
	if Parse(Service("auth-service"), "billing").On("billing") || Parse("", "billing").On("billing") {
		t.Fatal("Parse gave an unknown service a cell")
	}
}

func TestABadValueLogsErrorAndNeverQuotesIt(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	sw := Load(GoAPI, only(GoAPI.Env(), "billing,hunter2-not-a-namespace,*"), logger)
	if !sw.On("billing") || sw.Ignored() != 2 {
		t.Fatalf("billing on %v ignored %d, want on and 2", sw.On("billing"), sw.Ignored())
	}
	line := out.String()
	if !strings.Contains(line, "level=ERROR") || !strings.Contains(line, GoAPI.Env()) {
		t.Fatalf("log %q has no ERROR line naming %s", line, GoAPI.Env())
	}
	if strings.Contains(line, "hunter2") || strings.Contains(line, "*") {
		t.Fatalf("log %q quotes an ignored value", line)
	}
	out.Reset()
	Load(GoAPI, only(GoAPI.Env(), "billing"), logger)
	if out.Len() != 0 {
		t.Fatalf("a clean value logged %q", out.String())
	}
}

func collect(t *testing.T, sw Switch) metricdata.ResourceMetrics {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	if err := Register(provider.Meter("test"), sw); err != nil {
		t.Fatal(err)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	return rm
}

func points(rm metricdata.ResourceMetrics, name string) []metricdata.DataPoint[int64] {
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == name {
				if g, ok := m.Data.(metricdata.Gauge[int64]); ok {
					return g.DataPoints
				}
			}
		}
	}
	return nil
}

func TestTheEnabledGaugeHasOneSeriesPerCellOfTheService(t *testing.T) {
	rm := collect(t, Parse(GoAPI, "billing,teams,nope"))
	pts := points(rm, "dev_health_authz_enforcement_enabled")
	if len(pts) != len(catalogNamespaces) {
		t.Fatalf("%d series, want %d (one per namespace of go-api)", len(pts), len(catalogNamespaces))
	}
	seen := map[string]int64{}
	for _, p := range pts {
		if p.Attributes.Len() != 2 {
			t.Fatalf("series carries %d labels, want namespace and service only", p.Attributes.Len())
		}
		service, _ := p.Attributes.Value(attribute.Key("service"))
		namespace, _ := p.Attributes.Value(attribute.Key("namespace"))
		if service.AsString() != "go-api" {
			t.Fatalf("service label %q", service.AsString())
		}
		seen[namespace.AsString()] = p.Value
	}
	for _, namespace := range Namespaces() {
		want := int64(0)
		if namespace == "billing" || namespace == "teams" {
			want = 1
		}
		if got, ok := seen[namespace]; !ok || got != want {
			t.Errorf("namespace %s: value %d present %v, want %d", namespace, got, ok, want)
		}
	}
}

func TestTheIgnoredItemsGaugeIsTheBadValueSignal(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int64
	}{{"billing", 0}, {"billing,nope,*", 2}} {
		pts := points(collect(t, Parse(QueryAPI, tc.raw)), "dev_health_authz_enforcement_ignored_items")
		if len(pts) != 1 || pts[0].Value != tc.want || pts[0].Attributes.Len() != 1 {
			t.Fatalf("%q: points %+v, want one series of %d with the service label", tc.raw, pts, tc.want)
		}
		if v, _ := pts[0].Attributes.Value(attribute.Key("service")); v.AsString() != "query-api" {
			t.Fatalf("service label %q", v.AsString())
		}
	}
}

func TestAZeroSwitchRegistersNothing(t *testing.T) {
	rm := collect(t, Switch{})
	if len(points(rm, "dev_health_authz_enforcement_enabled")) != 0 {
		t.Fatal("a switch with no service reported cells")
	}
}
