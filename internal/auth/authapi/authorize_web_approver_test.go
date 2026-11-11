package authapi

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

// CHAOS-11207: POST /v1/authorize verifies a web approver reference, presented
// only by the acr-api caller, as a third principal-reference kind.

const webApproverTestIssuer = "https://www.example.test"

type approverFixture struct {
	*authorizeHandlerFixture
	webKey   ed25519.PrivateKey
	jwksFile string
	issuer   string
	approver string // the acr-api workload principal
	other    string // another admitted caller
}

func writeApproverJWKS(t *testing.T, path, kid string, public ed25519.PublicKey, retireAfter *time.Time) {
	t.Helper()
	key := map[string]any{"kty": "OKP", "crv": "Ed25519", "kid": kid, "alg": "EdDSA", "use": "sig", "x": base64.RawURLEncoding.EncodeToString(public)}
	if retireAfter != nil {
		key["retire_after"] = retireAfter.UTC().Format(time.RFC3339)
	}
	raw, err := json.Marshal(map[string]any{"keys": []any{key}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// newApproverFixture is the handler fixture with the kind on: the caller
// h.subject is acr-api; other is a second admitted caller.
func newApproverFixture(t *testing.T, opts ...func(*AuthorizeDeps)) *approverFixture {
	t.Helper()
	h := newAuthorizeHandlerFixture(t)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	a := &approverFixture{authorizeHandlerFixture: h, webKey: private, jwksFile: filepath.Join(t.TempDir(), "jwks.json"), issuer: webApproverTestIssuer,
		approver: h.subject, other: session.WirePrincipalID(uuid.New())}
	writeApproverJWKS(t, a.jwksFile, "web-test-1", public, nil)
	a.mount(t, opts...)
	return a
}

func (a *approverFixture) mount(t *testing.T, opts ...func(*AuthorizeDeps)) {
	t.Helper()
	approver, err := NewWebApproverReferences(WebApproverReferencesConfig{Issuer: a.issuer, JWKSFile: a.jwksFile, Caller: a.approver,
		Now: func() time.Time { return a.now }})
	if err != nil {
		t.Fatal(err)
	}
	references, err := NewPrincipalReferences(PrincipalReferencesConfig{IssuerURL: goldenIssuer, Keys: a.keys, Now: func() time.Time { return a.now }, WebApprover: approver})
	if err != nil {
		t.Fatal(err)
	}
	deps := AuthorizeDeps{
		Callers: a.callers(a.approver, a.other), References: references, State: a.state, Evaluator: authorization.NewEvaluator(), Audit: a.audit.write,
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
	a.routes = map[string]http.Handler{PatternAuthorize: server.Handler(), PatternAuthorizeBatch: server.Handler()}
}

func (a *approverFixture) claims() map[string]any {
	now := a.now.Unix()
	return map[string]any{
		"action": "acr.credential.issue", "aud": "auth-service", "iss": webApproverTestIssuer,
		"sub": a.principal.String(), "org_id": a.org, "sid": session.WireSessionID(a.sid), "impersonating": false,
		"iat": now, "nbf": now, "exp": now + 30, "jti": uuid.NewString(), "req": strings.Repeat("A", 43),
	}
}

func signApprover(t *testing.T, key ed25519.PrivateKey, header, claims map[string]any) string {
	t.Helper()
	h, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	c, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	input := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(c)
	return input + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(input)))
}

func (a *approverFixture) reference(claims map[string]any) string {
	return signApprover(a.t, a.webKey, map[string]any{"alg": "EdDSA", "typ": "web-approver+jwt", "kid": "web-test-1"}, claims)
}

func (a *approverFixture) issueQuestion(reference, org string) map[string]any {
	q := a.question("acr.credential.issue", "organization", org, org)
	q["principal_reference"] = reference
	return q
}

// ask posts one question with the workload token of caller.
func (a *approverFixture) ask(caller string, q map[string]any) (int, wireDecision) {
	a.t.Helper()
	rec := a.post(PatternAuthorize, a.workloadToken(func(c *contracts.WorkloadTokenClaims) { c.Sub = caller }), mustJSON(a.t, q))
	if rec.Code != http.StatusOK {
		return rec.Code, wireDecision{}
	}
	validateSurface(a.t, "authorization_decision.v1", rec.Body.Bytes())
	return rec.Code, decodeWire[wireDecision](a.t, rec)
}

func TestWebApprover_K1AllowsFromTheApproverCaller(t *testing.T) {
	a := newApproverFixture(t)

	status, d := a.ask(a.approver, a.issueQuestion(a.reference(a.claims()), a.org))

	if status != http.StatusOK || !d.Allowed {
		t.Fatalf("status %d decision %+v; want 200 allow", status, d)
	}
	if a.state.sessionReads != 1 {
		t.Fatalf("session reads %d; want 1 (the live session)", a.state.sessionReads)
	}
}

// Rule 1 parity (C5): one session, one organization, one action, asked once
// with the user's access token (what web's own check sends) and once with the
// approver reference: the same decision but its id.
func TestWebApprover_rule1ParityWithTheUserToken(t *testing.T) {
	other := uuid.NewString()
	for _, c := range []struct {
		name  string
		org   func(a *approverFixture) string
		setup func(a *approverFixture, org string)
	}{
		{"R1a platform admin, no membership", func(a *approverFixture) string { return a.org }, func(a *approverFixture, org string) {
			s := a.state.snapshots[snapKey{a.principal, org}]
			s.platformRole, s.membership = "platform_admin", nil
		}},
		{"R1b platform admin with a membership", func(a *approverFixture) string { return a.org }, func(a *approverFixture, _ string) { a.makePlatformAdminMember() }},
		{"R1c member", func(a *approverFixture) string { return a.org }, func(a *approverFixture, org string) {
			a.state.snapshots[snapKey{a.principal, org}].membership.Role = "member"
		}},
		{"R1d no membership, not an admin", func(a *approverFixture) string { return a.org }, func(a *approverFixture, org string) {
			a.state.snapshots[snapKey{a.principal, org}].membership = nil
		}},
		{"R1e platform admin, another organization than the token's", func(*approverFixture) string { return other }, func(a *approverFixture, org string) {
			a.state.snapshots[snapKey{a.principal, org}] = &fakeSnapshot{principal: authorization.StatusActive, platformRole: "platform_admin",
				org: org, orgStatus: authorization.StatusActive, now: authorizeNow}
		}},
		{"R1f delegated session", func(a *approverFixture) string { return a.org }, func(a *approverFixture, _ string) { a.delegate("acr.context.read") }},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := newApproverFixture(t)
			org := c.org(a)
			c.setup(a, org)
			claims := a.claims()
			claims["org_id"] = org

			userStatus, user := a.ask(a.other, a.issueQuestion(a.authorizeHandlerFixture.reference, org))
			approverStatus, approver := a.ask(a.approver, a.issueQuestion(a.reference(claims), org))

			if userStatus != http.StatusOK || approverStatus != http.StatusOK {
				t.Fatalf("status user %d approver %d; want 200 both", userStatus, approverStatus)
			}
			user.DecisionID, approver.DecisionID = "", ""
			if !approverJSONEqual(t, user, approver) {
				t.Fatalf("user-token decision %+v\napprover decision   %+v", user, approver)
			}
			if wantAllow := !strings.HasPrefix(c.name, "R1d") && !strings.HasPrefix(c.name, "R1f"); approver.Allowed != wantAllow {
				t.Fatalf("decision %+v; want allowed=%v", approver, wantAllow)
			}
			if strings.HasPrefix(c.name, "R1a") && approver.Obligations["repository_scope"] != "organization" {
				t.Fatalf("R1a decision %+v; want an organization-wide allow", approver)
			}
		})
	}
}

func approverJSONEqual(t *testing.T, x, y any) bool {
	t.Helper()
	return string(mustJSON(t, x)) == string(mustJSON(t, y))
}

func TestWebApprover_failsClosed(t *testing.T) {
	for _, c := range []struct {
		name   string
		caller func(a *approverFixture) string
		ref    func(a *approverFixture) string
		org    func(a *approverFixture) string
		setup  func(a *approverFixture)
		want   int
	}{
		{name: "another key", ref: func(a *approverFixture) string {
			_, other, _ := ed25519.GenerateKey(rand.Reader)
			return signApprover(a.t, other, map[string]any{"alg": "EdDSA", "typ": "web-approver+jwt", "kid": "web-test-1"}, a.claims())
		}},
		{name: "unknown kid", ref: func(a *approverFixture) string {
			return signApprover(a.t, a.webKey, map[string]any{"alg": "EdDSA", "typ": "web-approver+jwt", "kid": "other"}, a.claims())
		}},
		{name: "kid past retire_after", setup: func(a *approverFixture) {
			at := a.now.Add(-time.Second)
			writeApproverJWKS(a.t, a.jwksFile, "web-test-1", a.webKey.Public().(ed25519.PublicKey), &at)
		}},
		{name: "retire_after past the ceiling", setup: func(a *approverFixture) {
			at := a.now.Add(workload.AssertionOverlapCeiling + time.Hour)
			writeApproverJWKS(a.t, a.jwksFile, "web-test-1", a.webKey.Public().(ed25519.PublicKey), &at)
		}},
		{name: "another issuer", ref: claim("iss", "https://other.example.test")},
		{name: "audience acr-api", ref: claim("aud", "acr-api")},
		{name: "another action claim", ref: claim("action", "acr.context.read")},
		{name: "expired", ref: func(a *approverFixture) string {
			c := a.claims()
			c["iat"], c["nbf"], c["exp"] = a.now.Unix()-40, a.now.Unix()-40, a.now.Unix()-10
			return a.reference(c)
		}},
		{name: "not yet valid", ref: func(a *approverFixture) string {
			c := a.claims()
			c["iat"], c["nbf"], c["exp"] = a.now.Unix()+10, a.now.Unix()+10, a.now.Unix()+40
			return a.reference(c)
		}},
		{name: "life over 30 s", ref: claim("exp", "LIFE+31")},
		{name: "no sid", ref: dropClaim("sid")},
		{name: "malformed sid", ref: claim("sid", "sess_1")},
		{name: "session ended", setup: func(a *approverFixture) {
			a.setSession(DecisionSession{Found: true, PrincipalID: a.principal, Live: false})
		}},
		{name: "sid of another principal", setup: func(a *approverFixture) {
			a.setSession(DecisionSession{Found: true, PrincipalID: uuid.New(), Live: true})
		}},
		{name: "impersonating", ref: claim("impersonating", true)},
		{name: "impersonating missing", ref: dropClaim("impersonating")},
		{name: "sub in prn_ form", ref: func(a *approverFixture) string {
			c := a.claims()
			c["sub"] = session.WirePrincipalID(a.principal)
			return a.reference(c)
		}},
		{name: "sub not canonical", ref: func(a *approverFixture) string {
			c := a.claims()
			c["sub"] = strings.ToUpper(a.principal.String())
			return a.reference(c)
		}},
		{name: "a claim the kind lacks", ref: claim("repository_scopes", []string{"*"})},
		{name: "question organization differs", org: func(*approverFixture) string { return uuid.NewString() }},
		{name: "another caller presents it", caller: func(a *approverFixture) string { return a.other }},
		{name: "C-1 v: a typ JWT reference from the approver caller", ref: func(a *approverFixture) string {
			return signApprover(a.t, a.webKey, map[string]any{"alg": "EdDSA", "typ": "JWT", "kid": "web-test-1"}, a.claims())
		}},
		{name: "M14: the user's access token from the approver caller", ref: func(a *approverFixture) string { return a.authorizeHandlerFixture.reference }},
		{name: "C-1 ii: a workload token as the reference", ref: func(a *approverFixture) string { return a.workloadToken(nil) }},
		{name: "C-1 vi: the approver typ signed with the auth-service key", ref: func(a *approverFixture) string {
			return a.sign(webApproverReferenceType, a.claims())
		}},
		{name: "JWKS unreadable", setup: func(a *approverFixture) { _ = os.Remove(a.jwksFile) }, want: http.StatusServiceUnavailable},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := newApproverFixture(t)
			if c.setup != nil {
				c.setup(a)
			}
			ref := a.reference(a.claims())
			if c.ref != nil {
				ref = c.ref(a)
			}
			org := a.org
			if c.org != nil {
				org = c.org(a)
			}
			caller := a.approver
			if c.caller != nil {
				caller = c.caller(a)
			}
			want := c.want
			if want == 0 {
				want = http.StatusUnauthorized
			}

			status, d := a.ask(caller, a.issueQuestion(ref, org))

			if status != want {
				t.Fatalf("status %d decision %+v; want %d", status, d, want)
			}
		})
	}
}

// claim sets one claim; "LIFE+31" means iat + 31 s.
func claim(name string, value any) func(a *approverFixture) string {
	return func(a *approverFixture) string {
		c := a.claims()
		if value == "LIFE+31" {
			value = a.now.Unix() + 31
		}
		c[name] = value
		return a.reference(c)
	}
}

func dropClaim(name string) func(a *approverFixture) string {
	return func(a *approverFixture) string {
		c := a.claims()
		delete(c, name)
		return a.reference(c)
	}
}

func TestWebApprover_bindsToTheIssueActionOnItsOrganization(t *testing.T) {
	for name, mutate := range map[string]func(a *approverFixture, q map[string]any){
		"another action": func(_ *approverFixture, q map[string]any) { q["action"] = "acr.context.read" },
		"a repository": func(_ *approverFixture, q map[string]any) {
			q["resource"] = map[string]any{"type": "repository", "key": "acme/api", "organization_id": q["resource"].(map[string]any)["organization_id"]}
		},
		"a key not the org": func(a *approverFixture, q map[string]any) {
			q["resource"] = map[string]any{"type": "organization", "key": "acme", "organization_id": a.org}
		},
	} {
		t.Run(name, func(t *testing.T) {
			a := newApproverFixture(t)
			q := a.issueQuestion(a.reference(a.claims()), a.org)
			mutate(a, q)

			if status, _ := a.ask(a.approver, q); status != http.StatusUnauthorized {
				t.Fatalf("status %d; want 401", status)
			}
		})
	}
}

func TestWebApprover_refusedOnTheBatchRoute(t *testing.T) {
	a := newApproverFixture(t)
	body := mustJSON(t, map[string]any{"schema_version": "authorization_batch_request.v1", "requests": []any{a.issueQuestion(a.reference(a.claims()), a.org)}})

	rec := a.post(PatternAuthorizeBatch, a.workloadToken(nil), body)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("batch status %d; want 401", rec.Code)
	}
}

// C-1 (i): the approver reference is no caller credential.
func TestWebApprover_isRefusedAsTheCallerBearer(t *testing.T) {
	a := newApproverFixture(t)
	ref := a.reference(a.claims())

	rec := a.post(PatternAuthorize, ref, mustJSON(t, a.issueQuestion(ref, a.org)))

	if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), "invalid_credentials") {
		t.Fatalf("status %d body %s; want 401 invalid_credentials", rec.Code, rec.Body)
	}
}

// C-6: the same reference asked twice reads the session each time.
func TestWebApprover_readsTheSessionLiveEveryCall(t *testing.T) {
	a := newApproverFixture(t)
	ref := a.reference(a.claims())
	if status, d := a.ask(a.approver, a.issueQuestion(ref, a.org)); status != http.StatusOK || !d.Allowed {
		t.Fatalf("first ask %d %+v; want an allow", status, d)
	}
	a.setSession(DecisionSession{Found: true, PrincipalID: a.principal, Live: false})

	if status, _ := a.ask(a.approver, a.issueQuestion(ref, a.org)); status != http.StatusUnauthorized {
		t.Fatalf("second ask after the session ended %d; want 401", status)
	}
}

type countingCache struct{ gets, puts int }

func (c *countingCache) Begin() decisioncache.Stamp { return decisioncache.Stamp{} }
func (c *countingCache) Get(decisioncache.Key) (authorization.Decision, bool) {
	c.gets++
	return authorization.Decision{}, false
}
func (c *countingCache) Put(decisioncache.Key, authorization.Decision, decisioncache.Read) decisioncache.PutResult {
	c.puts++
	return decisioncache.Stored
}
func (c *countingCache) Len() int { return 0 }

// C-6: with the decision cache in use the kind never asks it.
func TestWebApprover_neverUsesTheDecisionCache(t *testing.T) {
	cache := &countingCache{}
	a := newApproverFixture(t, func(d *AuthorizeDeps) { d.Cache, d.Ready = cache, func() bool { return true } })

	if status, _ := a.ask(a.approver, a.issueQuestion(a.reference(a.claims()), a.org)); status != http.StatusOK {
		t.Fatalf("status %d; want 200", status)
	}
	if cache.gets != 0 || cache.puts != 0 {
		t.Fatalf("cache gets %d puts %d; want 0", cache.gets, cache.puts)
	}
}

// The life and skew equal acr's web assertion bounds (acr
// internal/auth/web_assertion.go: maxWebAssertionLifetime 30 s,
// webAssertionClockSkew 5 s).
func TestWebApprover_boundsEqualACRs(t *testing.T) {
	if webApproverLifetime != 30*time.Second || webApproverClockSkew != 5*time.Second {
		t.Fatalf("life %v skew %v; want 30s and 5s (acr's)", webApproverLifetime, webApproverClockSkew)
	}
}

// Without the kind (all settings unset) a web approver reference is refused as
// before: the kind adds nothing to a deployment that does not turn it on.
func TestWebApprover_offRefusesTheKind(t *testing.T) {
	a := newApproverFixture(t)
	ref := a.reference(a.claims())
	h := newAuthorizeHandlerFixture(t)

	if _, why, err := h.references().ResolveFor(ref, h.subject); why == "" || err != nil {
		t.Fatalf("off: why %q err %v; want a refusal", why, err)
	}
}

// The resolver itself refuses a missing or malformed sid (the decision
// transaction refuses it again): the reason names the session.
func TestWebApprover_resolverRefusesAMissingOrMalformedSid(t *testing.T) {
	a := newApproverFixture(t)
	approver, err := NewWebApproverReferences(WebApproverReferencesConfig{Issuer: a.issuer, JWKSFile: a.jwksFile, Caller: a.approver, Now: func() time.Time { return a.now }})
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range []string{dropClaim("sid")(a), claim("sid", "sess_1")(a)} {
		if _, why, err := approver.resolve(ref); why != "session" || err != nil {
			t.Fatalf("why %q err %v; want session", why, err)
		}
	}
}
