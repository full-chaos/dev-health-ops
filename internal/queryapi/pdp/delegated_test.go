package pdp

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

// delegatedObligations are the obligations auth-service sets on a delegated allow of an organization read.
func delegatedObligations(assurance string) map[string]any {
	return map[string]any{"tenant_scope": strings.ToLower(testOrg), "repository_scope": "organization", "minimum_assurance": assurance}
}

// A delegated allow carries minimum_assurance. The gate carries out aal1 (every accepted session bearer is aal1);
// any higher or unknown level stays no verdict: a local deny, counted unavailable.
func TestPDPEnforceCarriesOutMinimumAssuranceAAL1Only(t *testing.T) {
	for _, tc := range []struct {
		assurance string
		pass      bool
	}{
		{"aal1", true},
		{"aal2", false},
		{"aal3", false},
		{"aal9", false},
		{"", false},
	} {
		t.Run("minimum_assurance="+tc.assurance, func(t *testing.T) {
			srv := pdptest.New(t)
			srv.Obligations(delegatedObligations(tc.assurance))
			m, cfg := newMeter(t)
			cfg.Mode, cfg.Decider = ModeEnforce, pepClient(t, srv.URL)
			err := New(cfg).Authorize(context.Background(), testProfile, testBearer, testOrg)
			got := m.points(t, resultMetric)
			if tc.pass {
				if err != nil || got["action="+testAction+",mode=enforce,result=allow"] != 1 {
					t.Fatalf("answered %v with results %v, want a pass counted allow", err, got)
				}
				return
			}
			denied, ok := err.(*DenyError)
			if !ok || !denied.Local || (denied.Reason != pep.ReasonUnknownObligation && denied.Reason != pep.ReasonMalformed) {
				t.Fatalf("answered %#v, want a local unknown_obligation or malformed deny", err)
			}
		})
	}
}

// delegatedRows is an endpoint-profile document with one read row and one write row of query-api.
const delegatedRows = `{"rows":[
{"id":"GET /r [dev-health-query-api @query]","service":"dev-health-query-api","method":"GET","route":"/r","action":"analytics.read","classification":"protected"},
{"id":"POST /w [dev-health-query-api @query]","service":"dev-health-query-api","method":"POST","route":"/w","action":"teams.write","classification":"protected"}]}`

// A delegated bearer only reads: in enforce mode a row whose action is not a read is refused before any PDP call; a
// read row is asked as usual and served on an aal1 allow.
func TestPDPAuthorizeDelegatedRefusesANonReadRowBeforeThePDP(t *testing.T) {
	srv := pdptest.New(t)
	srv.Obligations(delegatedObligations("aal1"))
	m, cfg := newMeter(t)
	cfg.Mode, cfg.Decider, cfg.Rows = ModeEnforce, pepClient(t, srv.URL), []byte(delegatedRows)
	gate := New(cfg)
	err := gate.AuthorizeDelegated(context.Background(), "POST /w", testBearer, testOrg)
	denied, ok := err.(*DenyError)
	if !ok || denied.Local || denied.Reason != pep.ReasonImpersonationDenied || len(srv.Requests()) != 0 {
		t.Fatalf("delegated write: %#v with %d PDP calls, want an impersonation_denied deny and no call", err, len(srv.Requests()))
	}
	if err := gate.AuthorizeDelegated(context.Background(), "GET /r", testBearer, testOrg); err != nil || len(srv.Requests()) != 1 {
		t.Fatalf("delegated read: %v with %d PDP calls, want a pass after one call", err, len(srv.Requests()))
	}
	if got := m.points(t, resultMetric); got["action=teams.write,mode=enforce,result=deny"] != 1 || got["action=analytics.read,mode=enforce,result=allow"] != 1 {
		t.Fatalf("results %v, want one teams.write deny and one analytics.read allow", got)
	}
}

// Shadow and off never deny a delegated bearer, on any row.
func TestPDPAuthorizeDelegatedShadowAndOffNeverDeny(t *testing.T) {
	for _, mode := range []string{ModeShadow, ModeOff} {
		srv := pdptest.New(t)
		_, cfg := newMeter(t)
		cfg.Mode, cfg.Rows = mode, []byte(delegatedRows)
		if mode == ModeShadow {
			cfg.Decider = pepClient(t, srv.URL)
		}
		gate := New(cfg)
		if err := gate.AuthorizeDelegated(context.Background(), "POST /w", testBearer, testOrg); err != nil {
			t.Fatalf("%s: %v, want no deny", mode, err)
		}
		gate.Wait()
	}
}

// Every query-api row the PDP gates names a read action, so a delegated bearer is refused by the read check on none.
func TestEveryQueryAPIGatedRowIsARead(t *testing.T) {
	gate := New(Config{Mode: ModeOff})
	if len(gate.actions) == 0 {
		t.Fatal("no gated rows")
	}
	for profile, action := range gate.actions {
		if a, err := policycatalog.Default().Action(action); err != nil || !a.ReadOnly() {
			t.Errorf("%s: action %q is not a read (%v)", profile, action, err)
		}
	}
}
