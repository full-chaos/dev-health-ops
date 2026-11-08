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
