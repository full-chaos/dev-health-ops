package authapi

import (
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// CHAOS-13945: POST /v1/authorize resolves a named principal reference (the principal's wire id, prn_...) only for the
// one bound caller (query-api), with no session read and without the principal's platform role.

type namedFixture struct {
	*authorizeHandlerFixture
	bound string // query-api's workload principal
	other string // another admitted caller
}

func newNamedFixture(t *testing.T, opts ...func(*AuthorizeDeps)) *namedFixture {
	t.Helper()
	h := newAuthorizeHandlerFixture(t)
	n := &namedFixture{authorizeHandlerFixture: h, bound: h.subject, other: session.WirePrincipalID(uuid.New())}
	n.mount(t, opts...)
	return n
}

func (n *namedFixture) mount(t *testing.T, opts ...func(*AuthorizeDeps)) {
	t.Helper()
	named, err := NewNamedPrincipalReferences(n.bound)
	if err != nil {
		t.Fatal(err)
	}
	references, err := NewPrincipalReferences(PrincipalReferencesConfig{IssuerURL: goldenIssuer, Keys: n.keys, Now: func() time.Time { return n.now }, NamedPrincipal: named})
	if err != nil {
		t.Fatal(err)
	}
	deps := AuthorizeDeps{
		Callers: n.callers(n.bound, n.other), References: references, State: n.state, Evaluator: authorization.NewEvaluator(), Audit: n.audit.write,
		ProfileRows: authorizeProfileDoc(t, PatternAuthorize, PatternAuthorizeBatch),
	}
	for _, opt := range opts {
		opt(&deps)
	}
	routes, err := AuthorizeRoutes(deps)
	if err != nil {
		t.Fatal(err)
	}
	server, err := httpapi.NewServer(httpapi.ServerOptions{
		Address: "127.0.0.1:0", Logger: slog.New(slog.DiscardHandler), Routes: routes,
		RequestTimeout: 5 * time.Second, MaxBodyBytes: 1 << 20, RateLimit: 1000, RateLimitBurst: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	n.routes = map[string]http.Handler{PatternAuthorize: server.Handler(), PatternAuthorizeBatch: server.Handler()}
}

func (n *namedFixture) named() string { return session.WirePrincipalID(n.principal) }

func (n *namedFixture) readQuestion(reference, org string) map[string]any {
	q := n.question("analytics.read", "organization", org, org)
	q["principal_reference"] = reference
	return q
}

func (n *namedFixture) ask(caller string, q map[string]any) (int, wireDecision) {
	n.t.Helper()
	rec := n.post(PatternAuthorize, n.workloadToken(func(c *contracts.WorkloadTokenClaims) { c.Sub = caller }), mustJSON(n.t, q))
	if rec.Code != http.StatusOK {
		return rec.Code, wireDecision{}
	}
	validateSurface(n.t, "authorization_decision.v1", rec.Body.Bytes())
	return rec.Code, decodeWire[wireDecision](n.t, rec)
}

// RC8: the bound caller's named reference is decided, with no session read.
func TestNamedPrincipal_boundCallerIsDecided(t *testing.T) {
	n := newNamedFixture(t)

	status, d := n.ask(n.bound, n.readQuestion(n.named(), n.org))

	if status != http.StatusOK || !d.Allowed || d.PrincipalID != n.named() {
		t.Fatalf("status %d decision %+v; want 200, an allow for %s", status, d, n.named())
	}
	if n.state.sessionReads != 0 {
		t.Fatalf("session reads %d; want 0 (the kind has no session)", n.state.sessionReads)
	}
}

// RC8: any other admitted caller presenting a named reference is refused, as is a malformed one from the bound caller.
func TestNamedPrincipal_refusedForAnyOtherCallerOrForm(t *testing.T) {
	n := newNamedFixture(t)
	for _, c := range []struct {
		name, caller, reference string
	}{
		{"another admitted caller", n.other, n.named()},
		{"bound caller, not a principal id", n.bound, "prn_notaprincipalidatall"},
		{"bound caller, a principal id one character too long", n.bound, n.named() + "A"},
		{"bound caller, a session id", n.bound, session.WireSessionID(n.sid)},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := n.post(PatternAuthorize, n.workloadToken(func(w *contracts.WorkloadTokenClaims) { w.Sub = c.caller }), mustJSON(t, n.readQuestion(c.reference, n.org)))
			if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), reasonPrincipalReferenceInvalid) {
				t.Fatalf("status %d body %s; want 401 %s", rec.Code, rec.Body, reasonPrincipalReferenceInvalid)
			}
		})
	}
}

// RC8: without the kind configured a named reference is refused, from any caller.
func TestNamedPrincipal_offRefusesTheKind(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	if _, why, err := h.references().ResolveFor(session.WirePrincipalID(h.principal), h.subject); why == "" || err != nil {
		t.Fatalf("off: why %q err %v; want a refusal", why, err)
	}
	if _, err := NewNamedPrincipalReferences("svc-query-api"); err == nil {
		t.Fatal("a caller that is not a principal id must not turn the kind on")
	}
}

// Q2: the bound caller still presents user access tokens (the REST path), decided as before.
func TestNamedPrincipal_boundCallerKeepsUserTokens(t *testing.T) {
	n := newNamedFixture(t)

	status, d := n.ask(n.bound, n.readQuestion(n.authorizeHandlerFixture.reference, n.org))

	if status != http.StatusOK || !d.Allowed || n.state.sessionReads != 1 {
		t.Fatalf("status %d decision %+v session reads %d; want the user-token allow with one session read", status, d, n.state.sessionReads)
	}
}

// RC9 (acp Q5): the named kind carries no platform role. A platform admin with no membership is denied; a platform
// admin who is a member gets the member decision. The user token of the same principal keeps the platform answer.
func TestNamedPrincipal_noPlatformAdminBypass(t *testing.T) {
	t.Run("platform admin, no membership", func(t *testing.T) {
		n := newNamedFixture(t)
		s := n.state.snapshots[snapKey{n.principal, n.org}]
		s.platformRole, s.membership = "platform_admin", nil

		_, user := n.ask(n.other, n.readQuestion(n.authorizeHandlerFixture.reference, n.org))
		status, named := n.ask(n.bound, n.readQuestion(n.named(), n.org))

		if !user.Allowed || user.ReasonCode != string(authorization.ReasonAllowedByPlatformAdmin) {
			t.Fatalf("user token %+v; want the platform admin allow (the fixture's premise)", user)
		}
		if status != http.StatusOK || named.Allowed {
			t.Fatalf("named status %d decision %+v; want a deny", status, named)
		}
	})
	t.Run("platform admin with a membership", func(t *testing.T) {
		n := newNamedFixture(t)
		n.makePlatformAdminMember()
		member := newNamedFixture(t)
		member.state.snapshots[snapKey{member.principal, member.org}].membership.Role = "member"

		_, named := n.ask(n.bound, n.readQuestion(n.named(), n.org))
		_, plain := member.ask(member.other, member.readQuestion(member.authorizeHandlerFixture.reference, member.org))

		named.DecisionID, plain.DecisionID, named.PrincipalID, plain.PrincipalID = "", "", "", ""
		named.OrganizationID, plain.OrganizationID = nil, nil
		named.Obligations["tenant_scope"], plain.Obligations["tenant_scope"] = nil, nil
		if !named.Allowed || named.ReasonCode == string(authorization.ReasonAllowedByPlatformAdmin) || !approverJSONEqual(t, named, plain) {
			t.Fatalf("named %+v\nplain member user token %+v; want the member decision", named, plain)
		}
	})
}

// Q4: no session check, but the principal and the membership are read live.
func TestNamedPrincipal_readsPrincipalAndMembershipLive(t *testing.T) {
	for _, c := range []struct {
		name  string
		setup func(s *fakeSnapshot)
	}{
		{"deactivated principal", func(s *fakeSnapshot) { s.principal = authorization.StatusSuspended }},
		{"removed membership", func(s *fakeSnapshot) { s.membership = nil }},
		{"ended membership", func(s *fakeSnapshot) { s.membership.Status = authorization.StatusRemoved }},
		{"another organization", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			n := newNamedFixture(t)
			org := n.org
			if c.setup != nil {
				c.setup(n.state.snapshots[snapKey{n.principal, n.org}])
			} else {
				org = uuid.NewString()
				n.state.snapshots[snapKey{n.principal, org}] = &fakeSnapshot{principal: authorization.StatusActive, org: org,
					orgStatus: authorization.StatusActive, now: authorizeNow}
			}

			status, d := n.ask(n.bound, n.readQuestion(n.named(), org))

			if status != http.StatusOK || d.Allowed {
				t.Fatalf("status %d decision %+v; want a deny", status, d)
			}
		})
	}
}

// A named decision never reads or fills the decision cache: its snapshot differs from the user token's (no platform
// role), so a cached user decision must never answer it.
func TestNamedPrincipal_neverUsesTheDecisionCache(t *testing.T) {
	cache := &countingCache{}
	n := newNamedFixture(t, func(d *AuthorizeDeps) { d.Cache, d.Ready = cache, func() bool { return true } })

	if status, _ := n.ask(n.bound, n.readQuestion(n.named(), n.org)); status != http.StatusOK {
		t.Fatalf("status %d; want 200", status)
	}
	if cache.gets != 0 || cache.puts != 0 {
		t.Fatalf("cache gets %d puts %d; want 0", cache.gets, cache.puts)
	}
}

// One batch naming the same principal by its user token and by the named kind decides each with its own snapshot.
func TestNamedPrincipal_batchKeepsTheSnapshotsApart(t *testing.T) {
	n := newNamedFixture(t)
	s := n.state.snapshots[snapKey{n.principal, n.org}]
	s.platformRole, s.membership = "platform_admin", nil
	body := mustJSON(t, map[string]any{"schema_version": "authorization_batch_request.v1", "requests": []any{
		n.readQuestion(n.authorizeHandlerFixture.reference, n.org), n.readQuestion(n.named(), n.org)}})

	rec := n.post(PatternAuthorizeBatch, n.workloadToken(func(c *contracts.WorkloadTokenClaims) { c.Sub = n.bound }), body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s; want 200", rec.Code, rec.Body)
	}
	got := decodeWire[struct {
		Decisions []struct {
			Decision wireDecision `json:"decision"`
		} `json:"decisions"`
	}](t, rec)
	if len(got.Decisions) != 2 || !got.Decisions[0].Decision.Allowed || got.Decisions[1].Decision.Allowed {
		t.Fatalf("decisions %+v; want the user token allowed (platform admin) and the named kind denied", got.Decisions)
	}
}
