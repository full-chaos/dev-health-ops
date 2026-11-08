package pdp

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// byActionDecider denies the actions in denied through a denying PDP and allows the rest.
type byActionDecider struct {
	allow, deny pep.Decider
	denied      map[string]bool
}

func (d byActionDecider) Decide(ctx context.Context, req pep.Request) pep.Decision {
	if d.denied[req.Action] {
		return d.deny.Decide(ctx, req)
	}
	return d.allow.Decide(ctx, req)
}

func (d byActionDecider) DecideBatch(context.Context, []pep.Request) []pep.Decision {
	panic("DecideBatch is not used by the gate")
}

// CHAOS-13334: a PDP deny of an action every one of whose roots has a legacy check that refuses the caller does not
// refuse the operation: those roots get the refusal scope (KindNone) and are left to their own legacy refusal, the deny
// still counted; the other roots keep the scope decided for them. A deny of an action with any root its legacy check
// admits refuses the whole operation, as before.
func TestAuthorizeOperationLeavesALegacyRefusedDenyToItsRoot(t *testing.T) {
	const orgID = "22222222-2222-2222-2222-222222222222"
	allow, deny := pdptest.New(t), pdptest.New(t)
	deny.Deny("role_lacks_action")
	gate := func(t *testing.T) (*Gate, *meterReader) {
		meter, cfg := newMeter(t)
		cfg.Mode = ModeEnforce
		cfg.Decider = byActionDecider{allow: pepClient(t, allow.URL), deny: pepClient(t, deny.URL), denied: map[string]bool{"integrations.read": true}}
		cfg.Switch = authzenforce.Parse(authzenforce.QueryAPI, "analytics,integrations")
		return New(cfg), meter
	}
	caller := Caller{Class: CallerUser, Bearer: testBearer}
	dataHealth := Root{Key: "Query.dataHealth", Action: "integrations.read", LegacyRefuses: true}
	analytics := Root{Key: "Query.securityOverview", Action: "analytics.read"}

	t.Run("legacy refuses every denied root", func(t *testing.T) {
		g, meter := gate(t)
		scopes, err := g.AuthorizeOperation(context.Background(), caller, orgID, []Root{analytics, dataHealth, dataHealth})
		if err != nil {
			t.Fatalf("refused %v, want the operation admitted", err)
		}
		if got := scopes[dataHealth.Key]; got.Kind() != pepgate.KindNone {
			t.Fatalf("Query.dataHealth scope kind %v, want KindNone (the refusal)", got.Kind())
		}
		if got := scopes[analytics.Key]; got.Kind() != pepgate.KindOrganization {
			t.Fatalf("Query.securityOverview scope kind %v, want the decided organization scope", got.Kind())
		}
		points := meter.points(t, "dev_health_query_api_pdp_results_total")
		if points["action=integrations.read,mode=enforce,result=deny"] != 1 || points["action=analytics.read,mode=enforce,result=allow"] != 1 {
			t.Fatalf("counted %v, want one integrations.read deny and one analytics.read allow", points)
		}
	})
	t.Run("legacy admits a denied root", func(t *testing.T) {
		g, _ := gate(t)
		admitted := dataHealth
		admitted.LegacyRefuses = false
		for _, roots := range [][]Root{{admitted}, {dataHealth, admitted}, {admitted, analytics}} {
			if _, err := g.AuthorizeOperation(context.Background(), caller, orgID, roots); !isPDPDeny(err) {
				t.Fatalf("roots %+v: got %v, want the PDP deny", roots, err)
			}
		}
	})
	t.Run("legacy refuses but the PDP allows", func(t *testing.T) {
		g, _ := gate(t)
		refused := analytics
		refused.LegacyRefuses = true
		scopes, err := g.AuthorizeOperation(context.Background(), caller, orgID, []Root{refused})
		if err != nil || scopes[refused.Key].Kind() != pepgate.KindOrganization {
			t.Fatalf("got %v %v, want the decided organization scope (an allow changes nothing)", scopes, err)
		}
	})
}

func isPDPDeny(err error) bool {
	denied, ok := err.(*DenyError)
	return ok && !denied.Local
}

// acp (a): the legacy-refusal path applies only to an action in the registry, and only while every root of the
// operation on that action is in its registry entry and claims a refusing guard. A root on a registry action that is
// not in the entry (a new root with no guard), and a root of an action not in the registry, keep the PDP deny
// (FORBIDDEN), whatever LegacyRefuses says.
func TestAuthorizeOperationLegacyRefusalOnlyForRegisteredRoots(t *testing.T) {
	const orgID = "22222222-2222-2222-2222-222222222222"
	allow, deny := pdptest.New(t), pdptest.New(t)
	deny.Deny("role_lacks_action")
	gate := func(t *testing.T) *Gate {
		_, cfg := newMeter(t)
		cfg.Mode = ModeEnforce
		cfg.Decider = byActionDecider{allow: pepClient(t, allow.URL), deny: pepClient(t, deny.URL),
			denied: map[string]bool{"integrations.read": true, "analytics.read": true}}
		cfg.Switch = authzenforce.Parse(authzenforce.QueryAPI, "analytics,integrations")
		return New(cfg)
	}
	caller := Caller{Class: CallerUser, Bearer: testBearer}
	dataHealth := Root{Key: "Query.dataHealth", Action: "integrations.read", LegacyRefuses: true}
	newRoot := Root{Key: "Query.newIntegrationsRoot", Action: "integrations.read", LegacyRefuses: true}
	unregistered := Root{Key: "Query.securityOverview", Action: "analytics.read", LegacyRefuses: true}
	for name, roots := range map[string][]Root{
		"new root on a registry action":              {newRoot},
		"new root beside a registered root":          {dataHealth, newRoot},
		"root of an action not in the registry":      {unregistered},
		"registered root beside an unregistered one": {dataHealth, unregistered},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := gate(t).AuthorizeOperation(context.Background(), caller, orgID, roots); !isPDPDeny(err) {
				t.Fatalf("roots %+v: got %v, want the PDP deny (FORBIDDEN)", roots, err)
			}
		})
	}
}

// acp (e): on the legacy-refusal path the deny is counted exactly once (mode enforce, result deny, the action), and
// nothing else is counted, however many roots of the action the operation names.
func TestAuthorizeOperationLegacyRefusalCountsTheDenyOnce(t *testing.T) {
	const orgID = "22222222-2222-2222-2222-222222222222"
	deny := pdptest.New(t)
	deny.Deny("role_lacks_action")
	meter, cfg := newMeter(t)
	cfg.Mode = ModeEnforce
	cfg.Decider = pepClient(t, deny.URL)
	cfg.Switch = authzenforce.Parse(authzenforce.QueryAPI, "integrations")
	dataHealth := Root{Key: "Query.dataHealth", Action: "integrations.read", LegacyRefuses: true}
	scopes, err := New(cfg).AuthorizeOperation(context.Background(), Caller{Class: CallerUser, Bearer: testBearer}, orgID, []Root{dataHealth, dataHealth})
	if err != nil || scopes[dataHealth.Key].Kind() != pepgate.KindNone {
		t.Fatalf("got %v %v, want the root left to its legacy refusal", scopes, err)
	}
	points := meter.points(t, "dev_health_query_api_pdp_results_total")
	want := map[string]int64{"action=integrations.read,mode=enforce,result=deny": 1}
	if len(points) != len(want) || points["action=integrations.read,mode=enforce,result=deny"] != 1 {
		t.Fatalf("counted %v, want exactly %v", points, want)
	}
}
