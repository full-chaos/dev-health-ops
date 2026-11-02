package authapi

import (
	"net/http"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// delegatedCatalogActions is every action of the embedded catalog with the
// resource kind a request for it names.
var delegatedCatalogActions = []struct{ action, kind string }{
	{"analytics.read", "organization"}, {"analytics.export", "organization"},
	{"metrics.read", "organization"}, {"metrics.compute", "organization"},
	{"work_items.read", "organization"}, {"work_items.sync", "organization"},
	{"git.read", "organization"}, {"git.sync", "organization"},
	{"teams.read", "organization"}, {"teams.write", "organization"},
	{"settings.read", "organization"}, {"settings.write", "organization"},
	{"integrations.read", "organization"}, {"integrations.write", "organization"},
	{"members.read", "organization"}, {"members.invite", "organization"},
	{"members.manage", "organization"}, {"members.update", "organization"},
	{"org.read", "organization"}, {"org.write", "organization"}, {"org.delete", "organization"},
	{"billing.read", "organization"}, {"billing.manage", "organization"}, {"license.read", "organization"},
	{"acr.context.read", "repository"},
	{"admin.users", "platform"}, {"admin.orgs", "platform"}, {"admin.impersonate", "platform"},
	{"authz.decide", "platform"}, {"admin.billing", "platform"}, {"admin.flags", "platform"},
	{"admin.audit", "platform"}, {"admin.telemetry", "platform"}, {"admin.stats", "platform"},
}

func TestDelegatedActionTableIsTheWholeCatalog(t *testing.T) {
	cat := policycatalog.Default()
	var want, got []string
	for _, a := range cat.Actions() {
		want = append(want, a.ID)
	}
	for _, c := range delegatedCatalogActions {
		got = append(got, c.action)
	}
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(want, got) {
		t.Fatalf("table %v, catalog %v", got, want)
	}
}

func (h *authorizeHandlerFixture) delegatedQuestion(c struct{ action, kind string }) map[string]any {
	key, org := "acme", h.org
	switch c.kind {
	case "repository":
		key = "full-chaos/dev-health-ops"
	case "platform":
		key, org = "platform", ""
	}
	return h.question(c.action, c.kind, key, org)
}

func (h *authorizeHandlerFixture) setSession(s DecisionSession) {
	h.state.sessions[h.sid] = s
}

// A session row that names a real actor or a delegating session is an
// impersonation session. Until delegated evaluation ships, the decision is
// delegation_invalid for every action, never a 503 and never the plain user.
func TestAuthorizeDelegatedSessionDeniesEveryCatalogAction(t *testing.T) {
	for name, s := range map[string]DecisionSession{
		"both columns":                   {Found: true, Live: true, HasActor: true, HasDelegatedFrom: true},
		"only actor_principal_id":        {Found: true, Live: true, HasActor: true},
		"only delegated_from_session_id": {Found: true, Live: true, HasDelegatedFrom: true},
	} {
		t.Run(name, func(t *testing.T) {
			for _, c := range delegatedCatalogActions {
				h := newAuthorizeHandlerFixture(t)
				s.PrincipalID = h.principal
				h.setSession(s)
				rec := h.one(h.delegatedQuestion(c))
				if rec.Code != http.StatusOK {
					t.Fatalf("%s: status %d body %s", c.action, rec.Code, rec.Body)
				}
				validateSurface(t, "authorization_decision.v1", rec.Body.Bytes())
				d := decodeWire[wireDecision](t, rec)
				if d.Allowed || d.ReasonCode != "delegation_invalid" || d.PolicyRevision < 1 {
					t.Errorf("%s: %+v, want a delegation_invalid deny under the catalog revision", c.action, d)
				}
			}
		})
	}
}

// A plain session of the same principal still gets the plain decision, and the
// same action a delegated session was denied is allowed.
func TestAuthorizePlainSessionIsUnchanged(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	d := decodeWire[wireDecision](t, h.one(h.question("analytics.read", "organization", "acme", h.org)))
	if !d.Allowed || d.ReasonCode != "allowed_by_role" {
		t.Fatalf("plain session: %+v", d)
	}
	h.setSession(DecisionSession{Found: true, Live: true, PrincipalID: h.principal, HasActor: true, HasDelegatedFrom: true})
	d = decodeWire[wireDecision](t, h.one(h.question("analytics.read", "organization", "acme", h.org)))
	if d.Allowed || d.ReasonCode != "delegation_invalid" {
		t.Fatalf("delegated session: %+v", d)
	}
}

// A dead session answers as today, before the delegation check.
func TestAuthorizeDeadDelegatedSessionIsRefusedAsBefore(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.setSession(DecisionSession{Found: true, Live: false, PrincipalID: h.principal, HasActor: true, HasDelegatedFrom: true})
	assertError(t, h.one(h.question("analytics.read", "organization", "acme", h.org)), http.StatusUnauthorized, reasonPrincipalReferenceInvalid)
	h.setSession(DecisionSession{Found: true, Live: true, PrincipalID: uuid.New(), HasActor: true, HasDelegatedFrom: true})
	assertError(t, h.one(h.question("analytics.read", "organization", "acme", h.org)), http.StatusUnauthorized, reasonPrincipalReferenceInvalid)
}

// One batch holds a plain session and a delegated session of the same
// principal: the snapshot cache never carries one's verdict to the other.
func TestAuthorizeBatchMixesPlainAndDelegatedSessionsOfOnePrincipal(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	delegated := uuid.New()
	h.state.sessions[delegated] = DecisionSession{Found: true, Live: true, PrincipalID: h.principal, HasActor: true, HasDelegatedFrom: true}
	q := h.question("analytics.read", "organization", "acme", h.org)
	dq := h.question("analytics.read", "organization", "acme", h.org)
	dq["principal_reference"] = h.userToken(h.principal, func(c *contracts.AccessTokenClaims) { c.Sid = session.WireSessionID(delegated) })
	for _, qs := range [][]map[string]any{{q, dq, q}, {dq, q, dq}} {
		rec := h.batch(qs...)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d body %s", rec.Code, rec.Body)
		}
		doc := decodeWire[struct {
			Decisions []struct {
				Index    int          `json:"index"`
				Decision wireDecision `json:"decision"`
			} `json:"decisions"`
		}](t, rec)
		if len(doc.Decisions) != len(qs) {
			t.Fatalf("%d decisions for %d items", len(doc.Decisions), len(qs))
		}
		for i, item := range doc.Decisions {
			isDelegated := qs[i]["principal_reference"] == dq["principal_reference"]
			if isDelegated && (item.Decision.Allowed || item.Decision.ReasonCode != "delegation_invalid") {
				t.Errorf("item %d delegated: %+v", i, item.Decision)
			}
			if !isDelegated && (!item.Decision.Allowed || item.Decision.ReasonCode != "allowed_by_role") {
				t.Errorf("item %d plain: %+v", i, item.Decision)
			}
		}
	}
}
