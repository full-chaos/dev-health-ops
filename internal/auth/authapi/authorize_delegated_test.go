package authapi

import (
	"errors"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
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
	{"billing.read", "organization"}, {"billing.manage", "organization"}, {"license.read", "organization"}, {"reports.write", "organization"},
	{"acr.context.read", "repository"}, {"acr.evidence.read", "repository"}, {"acr.credential.issue", "organization"}, {"acr.credential.revoke", "organization"}, {"sso.write", "organization"},
	{"admin.users", "platform"}, {"admin.orgs", "platform"}, {"admin.impersonate", "platform"},
	{"authz.decide", "platform"}, {"admin.billing", "platform"}, {"admin.flags", "platform"},
	{"admin.audit", "platform"}, {"admin.telemetry", "platform"}, {"admin.stats", "platform"},
	{"credentials.project", "platform"},
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

var delegatedActor = uuid.MustParse("11111111-1111-1111-1111-1111111111aa")

// delegate makes h's session a complete impersonation session of the target
// user in h's organization, whose actor is an active admin.
func (h *authorizeHandlerFixture) delegate(permitted ...string) {
	org := uuid.MustParse(h.org)
	h.setSession(DecisionSession{
		Found: true, Live: true, PrincipalID: h.principal, HasActor: true, HasDelegatedFrom: true,
		ActorPrincipalID: delegatedActor, OrganizationID: &org, AMR: []string{"pwd"}, PermittedActions: permitted, BaseLive: true,
	})
	h.state.actors = map[uuid.UUID]ActorState{delegatedActor: {Active: true, CanImpersonate: true}}
}

func (h *authorizeHandlerFixture) decideOne(q map[string]any) wireDecision {
	h.t.Helper()
	rec := h.one(q)
	if rec.Code != http.StatusOK {
		h.t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	validateSurface(h.t, "authorization_decision.v1", rec.Body.Bytes())
	return decodeWire[wireDecision](h.t, rec)
}

func (h *authorizeHandlerFixture) org1(action string) map[string]any {
	return h.question(action, "organization", "acme", h.org)
}

// A row that names an actor or a delegating session but cannot be decided
// (no organization, no actor, bad amr) is delegation_invalid for every action,
// never a 503 and never the plain user.
func TestAuthorizeIncompleteDelegatedRowDeniesEveryCatalogAction(t *testing.T) {
	t.Parallel()
	org := uuid.New()
	for name, s := range map[string]DecisionSession{
		"only delegated_from_session_id": {HasDelegatedFrom: true, OrganizationID: &org, AMR: []string{"pwd"}},
		"actor without organization":     {HasActor: true, ActorPrincipalID: delegatedActor, AMR: []string{"pwd"}},
		"actor with bad amr":             {HasActor: true, ActorPrincipalID: delegatedActor, OrganizationID: &org, AMR: []string{"nope"}},
		"actor with no amr":              {HasActor: true, ActorPrincipalID: delegatedActor, OrganizationID: &org},
	} {
		t.Run(name, func(t *testing.T) {
			for _, c := range delegatedCatalogActions {
				h := newAuthorizeHandlerFixture(t)
				s.Found, s.Live, s.BaseLive, s.PrincipalID = true, true, true, h.principal
				s.PermittedActions = []string{c.action}
				h.setSession(s)
				h.state.actors = map[uuid.UUID]ActorState{delegatedActor: {Active: true, CanImpersonate: true}}
				d := h.decideOne(h.delegatedQuestion(c))
				if d.Allowed || d.ReasonCode != "delegation_invalid" || d.PolicyRevision < 1 {
					t.Errorf("%s: %+v, want a delegation_invalid deny under the catalog revision", c.action, d)
				}
			}
		})
	}
}

func TestAuthorizeDelegatedAllowCarriesObligations(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.delegate("analytics.read")
	d := h.decideOne(h.org1("analytics.read"))
	if !d.Allowed || d.ReasonCode != "allowed_by_role" {
		t.Fatalf("%+v", d)
	}
	want := map[string]any{"tenant_scope": h.org, "minimum_assurance": "aal1"}
	if len(d.Obligations) != len(want) || d.Obligations["tenant_scope"] != want["tenant_scope"] || d.Obligations["minimum_assurance"] != want["minimum_assurance"] {
		t.Fatalf("obligations %v, want %v", d.Obligations, want)
	}
}

func TestAuthorizeDelegatedRepositoryAllowCarriesTheAllowlist(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.delegate("acr.context.read")
	d := h.decideOne(h.question("acr.context.read", "repository", "full-chaos/dev-health-ops", h.org))
	if !d.Allowed {
		t.Fatalf("%+v", d)
	}
	list, _ := d.Obligations["repository_allowlist"].([]any)
	if len(list) != 1 || list[0] != "full-chaos/dev-health-ops" || d.Obligations["tenant_scope"] != h.org {
		t.Fatalf("obligations %v", d.Obligations)
	}
}

func TestAuthorizeDelegatedAllowIsNeverCacheable(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.delegate("analytics.read", "acr.context.read")
	for _, q := range []map[string]any{
		h.org1("analytics.read"),
		h.question("acr.context.read", "repository", "full-chaos/dev-health-ops", h.org),
	} {
		d := h.decideOne(q)
		if !d.Allowed {
			t.Fatalf("%+v", d)
		}
		at, err := time.Parse(time.RFC3339Nano, d.ExpiresAt)
		if err != nil || !at.Equal(time.Unix(0, authorizeNow)) {
			t.Fatalf("expires_at %q (%v), want the decision time", d.ExpiresAt, err)
		}
	}
}

func TestAuthorizeDelegatedActionOutsidePermittedIsImpersonationDenied(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.delegate("git.read")
	d := h.decideOne(h.org1("analytics.read"))
	if d.Allowed || d.ReasonCode != "impersonation_denied" || d.Obligations != nil {
		t.Fatalf("%+v", d)
	}
}

func TestAuthorizeDelegatedOtherOrganizationIsDelegationInvalid(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.delegate("analytics.read")
	other := uuid.NewString()
	h.state.snapshots[snapKey{h.principal, other}] = &fakeSnapshot{
		principal: authorization.StatusActive, org: other, orgStatus: authorization.StatusActive,
		membership: &authorization.Membership{OrgID: other, Role: "admin", Status: authorization.StatusActive}, now: authorizeNow,
	}
	d := h.decideOne(h.question("analytics.read", "organization", "acme", other))
	if d.Allowed || d.ReasonCode != "delegation_invalid" {
		t.Fatalf("%+v", d)
	}
}

func TestAuthorizeDelegatedWildcardAndPlatformAreImpersonationDenied(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.delegate("acr.context.read", "admin.users")
	h.state.snapshots[snapKey{h.principal, ""}].platformRole = "platform_admin"
	for name, q := range map[string]map[string]any{
		"wildcard repository": h.question("acr.context.read", "repository", "*", h.org),
		"platform action":     h.question("admin.users", "platform", "platform", ""),
	} {
		if d := h.decideOne(q); d.Allowed || d.ReasonCode != "impersonation_denied" {
			t.Errorf("%s: %+v", name, d)
		}
	}
}

func TestAuthorizeDelegatedActorState(t *testing.T) {
	for name, tc := range map[string]struct {
		actor  ActorState
		action string
		want   string
		allow  bool
	}{
		"deactivated actor":         {ActorState{Active: false, CanImpersonate: true, Owner: true}, "analytics.read", "delegation_invalid", false},
		"actor lost impersonate":    {ActorState{Active: true, CanImpersonate: false}, "analytics.read", "delegation_invalid", false},
		"unknown actor":             {ActorState{}, "analytics.read", "delegation_invalid", false},
		"admin on a denied action":  {ActorState{Active: true, CanImpersonate: true}, "settings.write", "impersonation_denied", false},
		"owner on a denied action":  {ActorState{Active: true, CanImpersonate: true, Owner: true}, "settings.write", "allowed_by_role", true},
		"owner who lost the power":  {ActorState{Active: true, CanImpersonate: false, Owner: true}, "settings.write", "delegation_invalid", false},
		"deactivated owner, denied": {ActorState{Active: false, CanImpersonate: true, Owner: true}, "settings.write", "delegation_invalid", false},
	} {
		t.Run(name, func(t *testing.T) {
			h := newAuthorizeHandlerFixture(t)
			h.delegate(tc.action)
			h.state.actors[delegatedActor] = tc.actor
			d := h.decideOne(h.org1(tc.action))
			if d.Allowed != tc.allow || d.ReasonCode != tc.want {
				t.Fatalf("%+v", d)
			}
			if d.Allowed && d.Obligations["tenant_scope"] != h.org {
				t.Fatalf("an owner-exempt allow carries obligations: %v", d.Obligations)
			}
			if !d.Allowed && d.Obligations != nil {
				t.Fatalf("a deny carries obligations: %v", d.Obligations)
			}
		})
	}
}

func TestAuthorizeDelegatedFloorActionNeedsTheOwner(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.delegate("members.invite")
	if d := h.decideOne(h.org1("members.invite")); d.Allowed || d.ReasonCode != "impersonation_denied" {
		t.Fatalf("admin actor: %+v", d)
	}
}

func TestAuthorizeDelegatedBaseNotLiveIsRefused(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.delegate("analytics.read")
	s := h.state.sessions[h.sid]
	s.BaseLive = false
	h.setSession(s)
	assertError(t, h.one(h.org1("analytics.read")), http.StatusUnauthorized, reasonPrincipalReferenceInvalid)
}

func TestAuthorizeDelegatedReadFailureIsA503(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.delegate("analytics.read")
	h.state.actorErr = errors.New("db down")
	assertError(t, h.one(h.org1("analytics.read")), http.StatusServiceUnavailable, reasonStoreUnavailable)
}

func TestAuthorizeDelegatedActorIsReadOncePerReferenceAndNeverForPlain(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.decideOne(h.org1("analytics.read"))
	if h.state.actorReads != 0 {
		t.Fatalf("%d actor reads for a plain session", h.state.actorReads)
	}
	h.delegate("analytics.read")
	h.batch(h.org1("analytics.read"), h.org1("analytics.read"), h.org1("analytics.read"))
	if h.state.actorReads != 1 {
		t.Fatalf("%d actor reads for one delegated reference, want 1", h.state.actorReads)
	}
}

func TestAuthorizeDenyCarriesNoObligationsAndAPlainAllowItsTenantScope(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	if d := h.decideOne(h.org1("analytics.read")); !d.Allowed || d.Obligations == nil || d.Obligations["minimum_assurance"] != nil {
		t.Fatalf("plain allow: %+v", d)
	}
	if d := h.decideOne(h.question("admin.users", "platform", "platform", "")); d.Allowed || d.Obligations != nil {
		t.Fatalf("deny: %+v", d)
	}
}

// A plain session of the same principal still gets the plain decision.
func TestAuthorizePlainSessionIsUnchanged(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	d := h.decideOne(h.org1("analytics.read"))
	if !d.Allowed || d.ReasonCode != "allowed_by_role" {
		t.Fatalf("plain session: %+v", d)
	}
	h.setSession(DecisionSession{Found: true, Live: true, BaseLive: true, PrincipalID: h.principal, HasActor: true, HasDelegatedFrom: true})
	d = h.decideOne(h.org1("analytics.read"))
	if d.Allowed || d.ReasonCode != "delegation_invalid" {
		t.Fatalf("delegated session: %+v", d)
	}
}

// A dead session answers as before, before the delegation check.
func TestAuthorizeDeadDelegatedSessionIsRefusedAsBefore(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.setSession(DecisionSession{Found: true, Live: false, BaseLive: true, PrincipalID: h.principal, HasActor: true, HasDelegatedFrom: true})
	assertError(t, h.one(h.org1("analytics.read")), http.StatusUnauthorized, reasonPrincipalReferenceInvalid)
	h.setSession(DecisionSession{Found: true, Live: true, BaseLive: true, PrincipalID: uuid.New(), HasActor: true, HasDelegatedFrom: true})
	assertError(t, h.one(h.org1("analytics.read")), http.StatusUnauthorized, reasonPrincipalReferenceInvalid)
}

// One batch holds a plain session and a delegated session of the same
// principal: the snapshot cache never carries one's verdict to the other.
func TestAuthorizeBatchMixesPlainAndDelegatedSessionsOfOnePrincipal(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	delegated := uuid.New()
	org := uuid.MustParse(h.org)
	h.state.sessions[delegated] = DecisionSession{
		Found: true, Live: true, BaseLive: true, PrincipalID: h.principal, HasActor: true, HasDelegatedFrom: true,
		ActorPrincipalID: delegatedActor, OrganizationID: &org, AMR: []string{"pwd"}, PermittedActions: []string{"git.read"},
	}
	h.state.actors = map[uuid.UUID]ActorState{delegatedActor: {Active: true, CanImpersonate: true}}
	q := h.org1("analytics.read")
	dq := h.org1("analytics.read")
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
			if isDelegated && (item.Decision.Allowed || item.Decision.ReasonCode != "impersonation_denied") {
				t.Errorf("item %d delegated (action not permitted): %+v", i, item.Decision)
			}
			if !isDelegated && (!item.Decision.Allowed || item.Decision.ReasonCode != "allowed_by_role" || item.Decision.Obligations == nil ||
				item.Decision.Obligations["minimum_assurance"] != nil) {
				t.Errorf("item %d plain: %+v", i, item.Decision)
			}
		}
	}
}

// The wire rule of obligations is fail closed: a mismatch is a state failure,
// not a decision.
func TestObligationsFitRule(t *testing.T) {
	plain := &authorization.Obligations{TenantScope: "t"}
	delegatedWith := &authorization.Obligations{TenantScope: "t", MinimumAssurance: "aal1"}
	other := &authorization.Obligations{TenantScope: "u"}
	for name, tc := range map[string]struct {
		d         authorization.Decision
		delegated bool
		org       string
		want      bool
	}{
		"delegated allow with":              {authorization.Decision{Allowed: true, Obligations: delegatedWith}, true, "t", true},
		"delegated allow without assurance": {authorization.Decision{Allowed: true, Obligations: plain}, true, "t", false},
		"delegated allow without":           {authorization.Decision{Allowed: true}, true, "t", false},
		"delegated deny without":            {authorization.Decision{}, true, "t", true},
		"delegated deny with":               {authorization.Decision{Obligations: delegatedWith}, true, "t", false},
		"plain allow with":                  {authorization.Decision{Allowed: true, Obligations: plain}, false, "t", true},
		"plain allow without":               {authorization.Decision{Allowed: true}, false, "t", false},
		"plain allow of another tenant":     {authorization.Decision{Allowed: true, Obligations: other}, false, "t", false},
		"plain deny with":                   {authorization.Decision{Obligations: plain}, false, "t", false},
		"platform allow without":            {authorization.Decision{Allowed: true}, false, "", true},
		"platform allow with":               {authorization.Decision{Allowed: true, Obligations: plain}, false, "", false},
	} {
		if got := obligationsFit(tc.d, tc.delegated, tc.org); got != tc.want {
			t.Errorf("%s: %v, want %v", name, got, tc.want)
		}
	}
}
