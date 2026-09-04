package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
)

// TestInvestmentExplainSwitchFromEnvDefaultsDisabled proves the
// reachability contract's default half: with no env var set,
// investmentExplainOperation is NOT enabled -- matching DynamicSwitch's
// own "every operation starts disabled" default and team-lead's "default
// OFF" ruling.
func TestInvestmentExplainSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := investmentExplainSwitchFromEnv()
	if sw.Enabled(investmentExplainOperation) {
		t.Fatal("expected investmentExplainOperation to be disabled with no env var set")
	}
}

// TestInvestmentExplainSwitchFromEnvEnabledViaEnvVar proves the switch
// actually turns on when the operator sets the env var -- the other half
// of the same contract.
func TestInvestmentExplainSwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(investmentExplainEnabledEnvVar, "true")
	sw := investmentExplainSwitchFromEnv()
	if !sw.Enabled(investmentExplainOperation) {
		t.Fatal("expected investmentExplainOperation to be enabled with GO_API_INVESTMENT_EXPLAIN_ENABLED=true")
	}
}

// TestInvestmentExplainRouteUnreachableWhenSwitchDisabled is the
// reachability proof routeswitch's own package doc comment calls for
// (plan §6: "prove a route becomes reachable when, and only when, its
// individual switch is enabled... a registered operation handler
// existing in the Mux is exactly the 'cited constructor' -- it is NOT
// proof the operation is reachable"): a REGISTERED handler, dispatched
// through a Mux whose Switch reports the operation disabled, must never
// run and must respond 404 -- identical to an unregistered operation,
// which is the whole point (an outside observer cannot tell "wired but
// off" from "not wired at all").
func TestInvestmentExplainRouteUnreachableWhenSwitchDisabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch() // every operation starts disabled
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(investmentExplainOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/explain", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(investmentExplainOperation, rec, req)

	if reached {
		t.Fatal("registered handler ran despite the switch being disabled")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// TestInvestmentExplainRouteReachableWhenSwitchEnabled is the other half:
// the SAME registration, with the switch flipped on, must actually reach
// the handler.
func TestInvestmentExplainRouteReachableWhenSwitchEnabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	sw.Set(investmentExplainOperation, true)
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(investmentExplainOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/explain", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(investmentExplainOperation, rec, req)

	if !reached {
		t.Fatal("registered handler did not run despite the switch being enabled")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// TestBuildInvestmentExplainRouteStaysUnmountedWithoutConfig proves the
// "no dependencies configured, stay unmounted, don't fail to build"
// contract loadQueryRouteConfig's own doc comment establishes for
// /query -- this route reuses that exact config gate
// (loadInvestmentExplainRouteConfig), so it must behave the same way
// when CLICKHOUSE_URI/GO_API_ENVELOPE_* are unset: ok=false, err=nil,
// no live ClickHouse/JWKS dependency required for this test to run.
func TestBuildInvestmentExplainRouteStaysUnmountedWithoutConfig(t *testing.T) {
	for _, name := range []string{
		"CLICKHOUSE_URI", "GO_API_REGISTRY_POSTGRES_URI",
		"GO_API_ENVELOPE_JWKS_PATH", "GO_API_ENVELOPE_ISSUER",
		"GO_API_ENVELOPE_AUDIENCE",
	} {
		t.Setenv(name, "")
	}

	handler, cleanup, ok, err := buildInvestmentExplainRoute()
	if err != nil {
		t.Fatalf("buildInvestmentExplainRoute: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false with no dependencies configured")
	}
	if handler != nil {
		t.Fatal("expected a nil handler when ok=false")
	}
	if cleanup != nil {
		t.Fatal("expected a nil cleanup when ok=false")
	}
}
