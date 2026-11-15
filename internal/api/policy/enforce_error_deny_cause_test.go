package policy

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const causeTeamsRead = "GET /api/v1/admin/teams"

type errorDenyPath struct {
	name  string
	cause string
	drive func(t *testing.T)
}

func causeDecide(t *testing.T, enf *Enforcer, o enforceObservation) {
	t.Helper()
	if v := enf.decide(context.Background(), o); v == enforceAllow {
		t.Fatalf("verdict %v, want a refusal", v)
	}
}

func causeTeamsObservation() enforceObservation {
	return enforceObservation{pattern: causeTeamsRead, bearer: shadowBearer, userAccessToken: true, orgID: "org-1", read: true}
}

func errorDenyPaths() []errorDenyPath {
	return []errorDenyPath{
		{"no user token", "no_user_token", func(t *testing.T) {
			enf := newEnforcerFor(t, newTestPDP(t, allowAll).client(t), onFor("teams"))
			causeDecide(t, enf, enforceObservation{pattern: causeTeamsRead, read: true})
		}},
		{"unconfigured", "unconfigured", func(t *testing.T) {
			enf, err := NewEnforcer(EnforceConfig{Unconfigured: true, Rows: shadowTestRows, Switch: onFor("teams"), Logger: quiet()})
			if err != nil {
				t.Fatal(err)
			}
			causeDecide(t, enf, causeTeamsObservation())
		}},
		{"no verdict", "no_verdict", func(t *testing.T) {
			causeDecide(t, newEnforcerFor(t, deadPDP(t).client(t), onFor("teams")), causeTeamsObservation())
		}},
		{"obligation", "obligation", func(t *testing.T) {
			pdp := newTestPDP(t, allowAll)
			pdp.obligations = map[string]any{"step_up": true}
			causeDecide(t, newEnforcerFor(t, pdp.client(t), onFor("teams")), causeTeamsObservation())
		}},
		{"no organization token", "no_org_token", func(t *testing.T) {
			o := causeTeamsObservation()
			o.orgID, o.orgless = "", orglessInputs{action: "admin.orgs"}
			causeDecide(t, newEnforcerFor(t, deadPDP(t).client(t), onFor("teams")), o)
		}},
		{"admin-or no verdict", "admin_or_no_verdict", func(t *testing.T) {
			o := enforceObservation{pattern: "GET /api/v1/admin/orgs", bearer: shadowBearer, userAccessToken: true, orgID: "org-1", read: true, adminOr: true}
			causeDecide(t, newEnforcerFor(t, deadPDP(t).client(t), onFor("admin")), o)
		}},
		{"impersonation lookup", "impersonation_lookup", func(t *testing.T) {
			cell := revokedRefusedCells[1]
			store := revokedImpersonatorStore(true, errors.New("impersonation row read failed"))
			got := enforceRequest(enforceHandler(t, store, userClaims(nil), newEnforcerFor(t, newTestPDP(t, allowAll).client(t), onFor(cell.namespace))), cell.method, cell.path)
			if got.status != http.StatusServiceUnavailable {
				t.Fatalf("status %d, want 503", got.status)
			}
		}},
	}
}

// driveErrorDenyPath runs one path over the real adder and returns the attributes of the one error_deny series it makes.
func driveErrorDenyPath(t *testing.T, p errorDenyPath) map[string]string {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	previous := enforceDecisionAdd
	enforceDecisionAdd = newEnforceDecisionAdder(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("cause-test"))
	t.Cleanup(func() { enforceDecisionAdd = previous })
	p.drive(t)
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	var series []map[string]string
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, _ := m.Data.(metricdata.Sum[int64])
			for _, point := range sum.DataPoints {
				attrs := map[string]string{}
				for _, kv := range point.Attributes.ToSlice() {
					attrs[string(kv.Key)] = kv.Value.Emit()
				}
				series = append(series, attrs)
			}
		}
	}
	if len(series) != 1 || series[0]["outcome"] != enforceOutcomeErrorDeny {
		t.Fatalf("%s: series %v, want exactly one error_deny series", p.name, series)
	}
	return series[0]
}

func attributeSetKey(attrs map[string]string) string {
	var parts []string
	for k, v := range attrs {
		if k == "namespace" {
			continue
		}
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// The seven error_deny paths must be told apart by the counter alone: seven distinct attribute sets once the namespace
// is set aside.
func TestTheSevenErrorDenyPathsMakeSevenDistinctSeries(t *testing.T) {
	distinct := map[string][]string{}
	for _, p := range errorDenyPaths() {
		key := attributeSetKey(driveErrorDenyPath(t, p))
		distinct[key] = append(distinct[key], p.name)
	}
	if len(distinct) != 7 {
		t.Fatalf("%d distinct error_deny attribute sets, want 7: %v", len(distinct), distinct)
	}
}

func TestEachErrorDenyPathSetsItsOwnCause(t *testing.T) {
	for _, p := range errorDenyPaths() {
		t.Run(p.cause, func(t *testing.T) {
			if got := driveErrorDenyPath(t, p)["cause"]; got != p.cause {
				t.Errorf("%s: cause %q, want %q", p.name, got, p.cause)
			}
		})
	}
}

func TestAllowDenyAndImpersonationRefusedCarryNoCause(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	add := newEnforceDecisionAdder(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("cause-test"))
	previous := enforceDecisionAdd
	enforceDecisionAdd = add
	t.Cleanup(func() { enforceDecisionAdd = previous })
	for _, outcome := range []string{enforceOutcomeAllow, enforceOutcomeDeny, enforceOutcomeImpersonationRefused} {
		countEnforce(context.Background(), "teams", outcome, "")
	}
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, _ := m.Data.(metricdata.Sum[int64])
			for _, point := range sum.DataPoints {
				seen++
				if _, ok := point.Attributes.Value("cause"); ok {
					t.Errorf("series %v carries a cause", point.Attributes.ToSlice())
				}
			}
		}
	}
	if seen != 3 {
		t.Fatalf("%d series, want 3", seen)
	}
}

func TestTheErrorDenyCauseSetIsClosed(t *testing.T) {
	want := []string{"no_user_token", "unconfigured", "no_verdict", "obligation", "no_org_token", "admin_or_no_verdict", "impersonation_lookup"}
	if strings.Join(enforceErrorDenyCauses, ",") != strings.Join(want, ",") {
		t.Fatalf("causes %v, want %v", enforceErrorDenyCauses, want)
	}
}
