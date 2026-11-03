package authapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// saReference is the opaque principal_reference the service-account cells resolve through the handlers' resolver.
const saReference = "sa-reference-EXAMPLE-0000000001"

const saAction = "acr.context.read"

// saFixture is a service account with a live platform key and a live viewer grant on acme/api in its organization.
type saFixture struct {
	*authorizeHandlerFixture
	account    uuid.UUID
	accountOrg string
	credential uuid.UUID
	snap       *fakeSnapshot
	audits     []audit.AuditEvent
}

func newSAFixture(t *testing.T) *saFixture {
	t.Helper()
	h := newAuthorizeHandlerFixture(t)
	s := &saFixture{authorizeHandlerFixture: h, account: uuid.New(), accountOrg: uuid.NewString(), credential: uuid.New()}
	s.state.keys = map[uuid.UUID]DecisionKey{s.credential: s.liveKey()}
	s.snap = &fakeSnapshot{
		principal: authorization.StatusActive, org: s.accountOrg, orgStatus: authorization.StatusActive,
		account: &authorization.ServiceAccount{OrgID: s.accountOrg, Status: authorization.StatusActive},
		grants:  []authorization.Grant{{OrgID: s.accountOrg, Role: "viewer", ResourceKind: "repository", ResourceKey: "acme/api"}},
		now:     authorizeNow,
	}
	s.state.snapshots[snapKey{s.account, s.accountOrg}] = s.snap
	s.state.snapshots[snapKey{s.account, ""}] = &fakeSnapshot{principal: authorization.StatusActive, now: authorizeNow}
	s.serve(t, h.state, nil, nil)
	return s
}

func (s *saFixture) liveKey() DecisionKey {
	return DecisionKey{Found: true, PrincipalID: s.account, AccountOrgID: s.accountOrg, Class: serviceAccountKeyClass,
		DomainOwner: "platform", ExpiresAtUnixNano: authorizeNow + int64(time.Hour), NowUnixNano: authorizeNow}
}

func (s *saFixture) principalRef() ReferencedPrincipal {
	return ReferencedPrincipal{ID: s.account, Kind: ReferenceServiceAccount, CredentialID: s.credential, OrgID: s.accountOrg,
		TokenID: "jti-EXAMPLE-0000000000000001"}
}

// serve mounts the authorize handlers over state with a resolver that answers saReference with the service account
// (the sa+jwt form is verified by Resolve once the service-account verifier is in the pinned dev-health-go) and every
// other reference through the real resolver.
func (s *saFixture) serve(t *testing.T, state DecisionState, cache DecisionCache, ready func() bool) {
	t.Helper()
	s.routes = serveWithReferences(t, AuthorizeDeps{
		Callers: s.callers(), References: s.references(), State: state, Evaluator: authorization.NewEvaluator(),
		Cache: cache, Ready: ready,
		Audit: func(_ context.Context, events []audit.AuditEvent) error {
			s.audits = append(s.audits, events...)
			return nil
		},
	}, map[string]ReferencedPrincipal{saReference: s.principalRef()})
}

func serveWithReferences(t *testing.T, deps AuthorizeDeps, refs map[string]ReferencedPrincipal) map[string]http.Handler {
	t.Helper()
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	if deps.Meter == nil {
		deps.Meter = noop.NewMeterProvider().Meter("test")
	}
	if deps.Audit == nil {
		deps.Audit = func(context.Context, []audit.AuditEvent) error { return nil }
	}
	rec, err := newRecorder(deps.Logger, deps.Meter)
	if err != nil {
		t.Fatal(err)
	}
	cm, err := newCacheMetrics(deps.Meter, deps.Cache)
	if err != nil {
		t.Fatal(err)
	}
	h := authorizeHandlers{AuthorizeDeps: deps, rec: rec, cache: cm, decisionID: newDecisionID,
		resolve: func(reference string) (ReferencedPrincipal, string, error) {
			if p, ok := refs[reference]; ok {
				return p, "", nil
			}
			return deps.References.Resolve(reference)
		}}
	admitted := func(next http.HandlerFunc) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next(w, r.WithContext(context.WithValue(r.Context(), authorizeCallerKey{}, AuthorizeCaller{Subject: "test"})))
		})
	}
	return map[string]http.Handler{PatternAuthorize: admitted(h.authorize), PatternAuthorizeBatch: admitted(h.authorizeBatch)}
}

func (s *saFixture) saQuestion(action, kind, key, org string) map[string]any {
	q := s.question(action, kind, key, org)
	q["principal_reference"] = saReference
	return q
}

func (s *saFixture) repoQuestion() map[string]any {
	return s.saQuestion(saAction, "repository", "acme/api", s.accountOrg)
}

func (s *saFixture) decide(t *testing.T, q map[string]any) wireDecision {
	t.Helper()
	rec := s.one(q)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s, want 200", rec.Code, rec.Body)
	}
	return decodeWire[wireDecision](t, rec)
}

func TestAuthorizeServiceAccountWithALiveKeyAndGrantIsAllowed(t *testing.T) {
	s := newSAFixture(t)
	d := s.decide(t, s.repoQuestion())
	if !d.Allowed || d.ReasonCode != string(authorization.ReasonAllowedByRepositoryGrant) || d.PrincipalID != session.WirePrincipalID(s.account) {
		t.Fatalf("decision %+v, want allowed_by_repository_grant for %s", d, session.WirePrincipalID(s.account))
	}
	if s.state.keyReads != 1 || s.state.sessionReads != 0 {
		t.Fatalf("%d key reads, %d session reads; want 1 and 0", s.state.keyReads, s.state.sessionReads)
	}
}

func TestAuthorizeServiceAccountReferenceNeedsALiveKey(t *testing.T) {
	for name, mutate := range map[string]func(s *saFixture, k *DecisionKey){
		"unknown key":                 func(_ *saFixture, k *DecisionKey) { *k = DecisionKey{} },
		"revoked":                     func(_ *saFixture, k *DecisionKey) { k.Revoked = true },
		"expired at the clock":        func(_ *saFixture, k *DecisionKey) { k.ExpiresAtUnixNano = authorizeNow },
		"expired before the clock":    func(_ *saFixture, k *DecisionKey) { k.ExpiresAtUnixNano = authorizeNow - 1 },
		"no expiry":                   func(_ *saFixture, k *DecisionKey) { k.ExpiresAtUnixNano = 0 },
		"key of another principal":    func(_ *saFixture, k *DecisionKey) { k.PrincipalID = uuid.New() },
		"another credential class":    func(_ *saFixture, k *DecisionKey) { k.Class = "workload_bootstrap_credential" },
		"not platform owned":          func(_ *saFixture, k *DecisionKey) { k.DomainOwner = "acr" },
		"no service account row":      func(_ *saFixture, k *DecisionKey) { k.AccountOrgID = "" },
		"token names another org":     func(_ *saFixture, k *DecisionKey) { k.AccountOrgID = uuid.NewString() },
		"live key, another key asked": func(s *saFixture, _ *DecisionKey) { s.credential = uuid.New() },
	} {
		t.Run(name, func(t *testing.T) {
			s := newSAFixture(t)
			asked := s.credential
			k := s.state.keys[asked]
			mutate(s, &k)
			s.state.keys[asked] = k
			s.serve(t, s.state, nil, nil)
			assertError(t, s.one(s.repoQuestion()), http.StatusUnauthorized, reasonPrincipalReferenceInvalid)
			assertError(t, s.batch(s.question(saAction, "repository", "acme/api", s.org), s.repoQuestion()),
				http.StatusUnauthorized, reasonPrincipalReferenceInvalid)
			if s.state.snapReads != 0 {
				t.Fatalf("%d snapshot reads after a refused key, want 0", s.state.snapReads)
			}
		})
	}
}

func TestAuthorizeServiceAccountKeyReadFailureIsA503(t *testing.T) {
	s := newSAFixture(t)
	s.state.keyErr = errors.New("store down")
	assertError(t, s.one(s.repoQuestion()), http.StatusServiceUnavailable, reasonStoreUnavailable)
}

// A service-account reference is decided by its key row, never by a session: a live session of the same principal
// does not stand in for a dead key.
func TestAuthorizeServiceAccountReferenceReadsNoSession(t *testing.T) {
	s := newSAFixture(t)
	k := s.state.keys[s.credential]
	k.Revoked = true
	s.state.keys[s.credential] = k
	sid := uuid.New()
	s.state.sessions[sid] = DecisionSession{Found: true, PrincipalID: s.account, Live: true}
	ref := s.principalRef()
	ref.SessionID = session.WireSessionID(sid)
	s.routes = serveWithReferences(t, AuthorizeDeps{References: s.references(), State: s.state, Evaluator: authorization.NewEvaluator()},
		map[string]ReferencedPrincipal{saReference: ref})
	assertError(t, s.one(s.repoQuestion()), http.StatusUnauthorized, reasonPrincipalReferenceInvalid)
	if s.state.sessionReads != 0 {
		t.Fatalf("%d session reads for a service-account reference, want 0", s.state.sessionReads)
	}
}

// A user reference keeps the session check: a live key row of the same principal does not stand in for a dead session.
func TestAuthorizeUserReferenceStillNeedsALiveSession(t *testing.T) {
	s := newSAFixture(t)
	s.state.sessions[s.sid] = DecisionSession{Found: true, PrincipalID: s.principal}
	s.state.keys[s.credential] = DecisionKey{Found: true, PrincipalID: s.principal, AccountOrgID: s.org, Class: serviceAccountKeyClass,
		DomainOwner: "platform", ExpiresAtUnixNano: authorizeNow + int64(time.Hour)}
	user := ReferencedPrincipal{ID: s.principal, SessionID: session.WireSessionID(s.sid), CredentialID: s.credential, OrgID: s.org}
	s.routes = serveWithReferences(t, AuthorizeDeps{References: s.references(), State: s.state, Evaluator: authorization.NewEvaluator()},
		map[string]ReferencedPrincipal{saReference: user})
	assertError(t, s.one(s.saQuestion("settings.write", "organization", "acme", s.org)), http.StatusUnauthorized, reasonPrincipalReferenceInvalid)
	if s.state.keyReads != 0 || s.state.sessionReads != 1 {
		t.Fatalf("%d key reads, %d session reads for a user reference; want 0 and 1", s.state.keyReads, s.state.sessionReads)
	}
}

// pm Q2: a disabled account with a live key is a decision (service_account_inactive), not a refused reference.
func TestAuthorizeDisabledServiceAccountWithALiveKeyIsADecisionNotA401(t *testing.T) {
	s := newSAFixture(t)
	s.snap.account.Status = authorization.StatusSuspended
	d := s.decide(t, s.repoQuestion())
	if d.Allowed || d.ReasonCode != string(authorization.ReasonServiceAccountInactive) {
		t.Fatalf("decision %+v, want service_account_inactive", d)
	}
}

func TestAuthorizeServiceAccountOrgAndPlatformActionsAreDenied(t *testing.T) {
	s := newSAFixture(t)
	if d := s.decide(t, s.saQuestion("settings.write", "organization", "acme", s.accountOrg)); d.Allowed ||
		d.ReasonCode != string(authorization.ReasonNoMembership) {
		t.Fatalf("organization action: %+v, want no_membership", d)
	}
	if d := s.decide(t, s.saQuestion("admin.users", "platform", "platform", "")); d.Allowed ||
		d.ReasonCode != string(authorization.ReasonPlatformRoleAbsent) {
		t.Fatalf("platform action: %+v, want platform_role_absent", d)
	}
	if len(s.audits) != 2 {
		t.Fatalf("%d high-risk audit rows, want 2 (settings.write, admin.users)", len(s.audits))
	}
	for _, e := range s.audits {
		if e.ActorPrincipalID == nil || *e.ActorPrincipalID != s.account.String() || e.Outcome != audit.OutcomeDenied {
			t.Fatalf("high-risk audit row %+v, want a denied row whose actor is the service account", e)
		}
	}
}

func TestDecisionKeyLiveForEveryClause(t *testing.T) {
	s := newSAFixture(t)
	p := s.principalRef()
	if !s.liveKey().liveFor(p) {
		t.Fatal("the live key is refused")
	}
	for name, mutate := range map[string]func(k *DecisionKey){
		"not found":     func(k *DecisionKey) { k.Found = false },
		"principal":     func(k *DecisionKey) { k.PrincipalID = uuid.New() },
		"class":         func(k *DecisionKey) { k.Class = "user_access_token" },
		"domain owner":  func(k *DecisionKey) { k.DomainOwner = "external_push" },
		"account org":   func(k *DecisionKey) { k.AccountOrgID = uuid.NewString() },
		"no account":    func(k *DecisionKey) { k.AccountOrgID = "" },
		"revoked":       func(k *DecisionKey) { k.Revoked = true },
		"expired":       func(k *DecisionKey) { k.ExpiresAtUnixNano = k.NowUnixNano },
		"no expiry":     func(k *DecisionKey) { k.ExpiresAtUnixNano = 0 },
		"no clock":      func(k *DecisionKey) { k.NowUnixNano = 0 },
		"clock is past": func(k *DecisionKey) { k.NowUnixNano = k.ExpiresAtUnixNano + 1 },
	} {
		k := s.liveKey()
		mutate(&k)
		if k.liveFor(p) {
			t.Errorf("%s: the key is live, want refused", name)
		}
	}
	k := s.liveKey()
	p.OrgID = uuid.NewString()
	if k.liveFor(p) {
		t.Error("token org_id of another organization: the key is live, want refused")
	}
}

// pm Q4: a service-account decision never reads or writes the decision cache.
func TestAServiceAccountDecisionBypassesTheCache(t *testing.T) {
	c := newCacheFixture(t)
	s := saOnCache(t, c)
	for range 2 {
		if d := s.decide(t, s.repoQuestion()); !d.Allowed {
			t.Fatalf("decision %+v, want allowed", d)
		}
	}
	if c.cs.unfedReads != 0 || c.cs.versionReads != 0 || c.cache.Len() != 0 || c.state.snapReads != 2 {
		t.Fatalf("unfed %d, state version %d, entries %d, snapshots %d; want 0, 0, 0, 2",
			c.cs.unfedReads, c.cs.versionReads, c.cache.Len(), c.state.snapReads)
	}
	if hit, bypass := c.counter(MetricDecisionCacheLookups, cacheHit), c.counter(MetricDecisionCacheLookups, cacheBypass); hit != 0 || bypass != 2 {
		t.Fatalf("lookups hit %d bypass %d, want 0 and 2", hit, bypass)
	}
}

// pm Q4: a revoke or a disable bites on the very next call, with the cache in use and ready.
func TestARevokeOrDisableBitesOnTheNextServiceAccountCall(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(s *saFixture)
		check  func(t *testing.T, s *saFixture, rec *httptest.ResponseRecorder)
	}{
		"account disabled": {
			change: func(s *saFixture) { s.snap.account.Status = authorization.StatusSuspended },
			check: func(t *testing.T, _ *saFixture, rec *httptest.ResponseRecorder) {
				if d := decodeWire[wireDecision](t, rec); rec.Code != http.StatusOK || d.Allowed ||
					d.ReasonCode != string(authorization.ReasonServiceAccountInactive) {
					t.Fatalf("status %d decision %+v, want service_account_inactive", rec.Code, d)
				}
			},
		},
		"key revoked": {
			change: func(s *saFixture) {
				k := s.state.keys[s.credential]
				k.Revoked = true
				s.state.keys[s.credential] = k
			},
			check: func(t *testing.T, _ *saFixture, rec *httptest.ResponseRecorder) {
				assertError(t, rec, http.StatusUnauthorized, reasonPrincipalReferenceInvalid)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := newCacheFixture(t)
			s := saOnCache(t, c)
			if d := s.decide(t, s.repoQuestion()); !d.Allowed {
				t.Fatalf("first decision %+v, want allowed", d)
			}
			tc.change(s)
			tc.check(t, s, s.one(s.repoQuestion()))
		})
	}
}

// saOnCache is the service account on the cache fixture's state, cache and clock, the cache ready.
func saOnCache(t *testing.T, c *cacheFixture) *saFixture {
	t.Helper()
	s := &saFixture{authorizeHandlerFixture: c.authorizeHandlerFixture, account: uuid.New(), accountOrg: uuid.NewString(), credential: uuid.New()}
	c.reader = sdkmetric.NewManualReader()
	c.state.keys = map[uuid.UUID]DecisionKey{s.credential: s.liveKey()}
	s.snap = &fakeSnapshot{
		principal: authorization.StatusActive, org: s.accountOrg, orgStatus: authorization.StatusActive,
		account: &authorization.ServiceAccount{OrgID: s.accountOrg, Status: authorization.StatusActive},
		grants:  []authorization.Grant{{OrgID: s.accountOrg, Role: "viewer", ResourceKind: "repository", ResourceKey: "acme/api"}},
		now:     authorizeNow,
	}
	c.state.snapshots[snapKey{s.account, s.accountOrg}] = s.snap
	s.routes = serveWithReferences(t, AuthorizeDeps{
		References: c.references(), State: c.cs, Evaluator: authorization.NewEvaluator(),
		Cache: c.cache, Ready: func() bool { return c.ready }, Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(c.reader)).Meter("test"),
	}, map[string]ReferencedPrincipal{saReference: s.principalRef()})
	return s
}
