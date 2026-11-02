package authapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// fakeSnapshot is one (principal, organization) state for the handlers.
type fakeSnapshot struct {
	principal    authorization.Status
	platformRole string
	org          string
	orgStatus    authorization.Status
	membership   *authorization.Membership
	grants       []authorization.Grant
	grantsErr    error
	now          int64
}

func (s *fakeSnapshot) PrincipalStatus() authorization.Status { return s.principal }
func (s *fakeSnapshot) PlatformRole() string                  { return s.platformRole }
func (s *fakeSnapshot) OrganizationStatus(org string) authorization.Status {
	if org != s.org {
		return authorization.StatusUnknown
	}
	return s.orgStatus
}
func (s *fakeSnapshot) Membership(org string) (authorization.Membership, bool) {
	if org != s.org || s.membership == nil {
		return authorization.Membership{}, false
	}
	return *s.membership, true
}
func (s *fakeSnapshot) Grants(string) ([]authorization.Grant, error) { return s.grants, s.grantsErr }
func (s *fakeSnapshot) Workload() (authorization.WorkloadLimits, bool) {
	return authorization.WorkloadLimits{}, false
}
func (s *fakeSnapshot) ServiceAccount(string) (authorization.ServiceAccount, bool) {
	return authorization.ServiceAccount{}, false
}
func (s *fakeSnapshot) NowUnixNano() int64 { return s.now }

type snapKey struct {
	principal uuid.UUID
	org       string
}

// fakeDecisionState counts transactions and reads, and can fail each step.
type fakeDecisionState struct {
	sessions   map[uuid.UUID]DecisionSession
	snapshots  map[snapKey]*fakeSnapshot
	beginErr   error
	sessionErr error
	snapErr    error
	reads      int
	snapReads  int
}

func (f *fakeDecisionState) Read(ctx context.Context, fn func(DecisionTx) error) error {
	f.reads++
	if f.beginErr != nil {
		return f.beginErr
	}
	return fn(fakeDecisionTx{f})
}

type fakeDecisionTx struct{ f *fakeDecisionState }

func (t fakeDecisionTx) Session(_ context.Context, id uuid.UUID) (DecisionSession, error) {
	if t.f.sessionErr != nil {
		return DecisionSession{}, t.f.sessionErr
	}
	return t.f.sessions[id], nil
}

func (t fakeDecisionTx) Snapshot(_ context.Context, principal uuid.UUID, org string) (authorization.GrantSnapshot, error) {
	t.f.snapReads++
	if t.f.snapErr != nil {
		return nil, t.f.snapErr
	}
	if s, ok := t.f.snapshots[snapKey{principal, org}]; ok {
		return s, nil
	}
	return &fakeSnapshot{now: authorizeNow}, nil
}

var authorizeNow = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixNano()

type authorizeHandlerFixture struct {
	*authorizeFixture
	state     *fakeDecisionState
	principal uuid.UUID
	org       string
	reference string
	sid       uuid.UUID
	routes    map[string]http.Handler
}

// authorizeProfileDoc is an endpoint-profile document with an auth-service row
// for each pattern that admits the workload token.
func authorizeProfileDoc(t *testing.T, patterns ...string) []byte {
	t.Helper()
	var rows []map[string]any
	for _, p := range patterns {
		rows = append(rows, map[string]any{"service": AccessService, "method": "POST", "route": p, "accepted_credential_classes": []string{workloadTokenClass}})
	}
	raw, err := json.Marshal(map[string]any{"rows": rows})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func newAuthorizeHandlerFixture(t *testing.T) *authorizeHandlerFixture {
	t.Helper()
	f := newAuthorizeFixture(t)
	f.now = time.Unix(0, authorizeNow)
	principal := uuid.New()
	var sid string
	reference := f.userToken(principal, func(c *contracts.AccessTokenClaims) { sid = c.Sid })
	sessionID, err := session.ParseWireSessionID(sid)
	if err != nil {
		t.Fatal(err)
	}
	org := uuid.NewString()
	state := &fakeDecisionState{
		sessions: map[uuid.UUID]DecisionSession{sessionID: {Found: true, PrincipalID: principal, Live: true}},
		snapshots: map[snapKey]*fakeSnapshot{
			{principal, org}: {
				principal: authorization.StatusActive, org: org, orgStatus: authorization.StatusActive,
				membership: &authorization.Membership{OrgID: org, Role: "admin", Status: authorization.StatusActive}, now: authorizeNow,
			},
			{principal, ""}: {principal: authorization.StatusActive, now: authorizeNow},
		},
	}
	routes, err := AuthorizeRoutes(AuthorizeDeps{
		Callers: f.callers(), References: f.references(), State: state, Evaluator: authorization.NewEvaluator(),
		ProfileRows: authorizeProfileDoc(t, PatternAuthorize, PatternAuthorizeBatch),
	})
	if err != nil {
		t.Fatal(err)
	}
	byPattern := map[string]http.Handler{}
	for _, r := range routes {
		byPattern[r.Pattern] = r.Handler
	}
	return &authorizeHandlerFixture{authorizeFixture: f, state: state, principal: principal, org: org, reference: reference, sid: sessionID, routes: byPattern}
}

func (h *authorizeHandlerFixture) question(action, kind, key, org string) map[string]any {
	resource := map[string]any{"type": kind, "key": key}
	if org != "" {
		resource["organization_id"] = org
	}
	return map[string]any{
		"schema_version": "authorization_request.v1", "principal_reference": h.reference, "action": action, "resource": resource,
		"context": map[string]any{"request_id": "req_EXAMPLE0000000000000000001", "client_service": "dev-health-ops-api"},
	}
}

func (h *authorizeHandlerFixture) post(pattern string, bearer string, body []byte) *httptest.ResponseRecorder {
	h.t.Helper()
	req := httptest.NewRequest(http.MethodPost, pattern, bytes.NewReader(body))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	requestid.Middleware(h.routes[pattern]).ServeHTTP(rec, req)
	return rec
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func (h *authorizeHandlerFixture) one(q map[string]any) *httptest.ResponseRecorder {
	return h.post(PatternAuthorize, h.workloadToken(nil), mustJSON(h.t, q))
}

func (h *authorizeHandlerFixture) batch(qs ...map[string]any) *httptest.ResponseRecorder {
	return h.post(PatternAuthorizeBatch, h.workloadToken(nil), mustJSON(h.t, map[string]any{"schema_version": "authorization_batch_request.v1", "requests": qs}))
}

type wireDecision struct {
	DecisionID     string  `json:"decision_id"`
	Allowed        bool    `json:"allowed"`
	ReasonCode     string  `json:"reason_code"`
	PrincipalID    string  `json:"principal_id"`
	OrganizationID *string `json:"organization_id"`
	PolicyRevision int     `json:"policy_revision"`
	ExpiresAt      string  `json:"expires_at"`
}

func decodeWire[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("body %s: %v", rec.Body, err)
	}
	return v
}

func validateSurface(t *testing.T, surface string, body []byte) {
	t.Helper()
	if err := contracts.Validate(repoRoot(t), surface, decode(t, body)); err != nil {
		t.Fatalf("%s: %v\nbody %s", surface, err, body)
	}
}

var decisionIDShape = regexp.MustCompile(`^dec_[A-Za-z0-9_-]{26}$`)

func TestAuthorizeRoutesStayDarkWithoutTheirRows(t *testing.T) {
	f := newAuthorizeFixture(t)
	deps := AuthorizeDeps{Callers: f.callers(), References: f.references(), State: &fakeDecisionState{}, Evaluator: authorization.NewEvaluator()}
	routes, err := AuthorizeRoutes(deps)
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 0 {
		t.Fatalf("the embedded document has no authorize row, yet %d route(s) mounted", len(routes))
	}
	deps.ProfileRows = authorizeProfileDoc(t, PatternAuthorizeBatch)
	if routes, _ = AuthorizeRoutes(deps); len(routes) != 1 || routes[0].Pattern != PatternAuthorizeBatch {
		t.Fatalf("a document with only the batch row mounted %v", routes)
	}
	deps.ProfileRows = authorizeProfileDoc(t, PatternAuthorize, PatternAuthorizeBatch)
	if routes, _ = AuthorizeRoutes(deps); len(routes) != 2 {
		t.Fatalf("both rows: %d routes, want 2", len(routes))
	}
	if _, err := AuthorizeRoutes(AuthorizeDeps{}); err == nil {
		t.Fatal("AuthorizeRoutes accepted no state and no evaluator")
	}
}

func TestAuthorizeAnswersAValidDecisionDocument(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	rec := h.one(h.question("settings.write", "organization", "acme", h.org))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	validateSurface(t, "authorization_decision.v1", rec.Body.Bytes())
	d := decodeWire[wireDecision](t, rec)
	if !d.Allowed || d.ReasonCode != "allowed_by_role" {
		t.Fatalf("decision %+v, want an allow by role", d)
	}
	if !decisionIDShape.MatchString(d.DecisionID) || d.PrincipalID != session.WirePrincipalID(h.principal) ||
		d.OrganizationID == nil || *d.OrganizationID != h.org || d.PolicyRevision < 1 {
		t.Fatalf("decision %+v", d)
	}
	if want := time.Unix(0, authorizeNow).UTC().Format(expiresAtLayout); d.ExpiresAt != want {
		t.Fatalf("expires_at %s, want the decision time %s for a class none action", d.ExpiresAt, want)
	}
	if h.state.reads != 1 {
		t.Fatalf("%d state transactions, want 1", h.state.reads)
	}
}

func TestAuthorizeDeniesExpireAtTheDecisionTime(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	rec := h.one(h.question("org.delete", "organization", "acme", h.org))
	validateSurface(t, "authorization_decision.v1", rec.Body.Bytes())
	d := decodeWire[wireDecision](t, rec)
	if d.Allowed || d.ReasonCode != "role_lacks_action" {
		t.Fatalf("decision %+v, want role_lacks_action", d)
	}
	if want := time.Unix(0, authorizeNow).UTC().Format(expiresAtLayout); d.ExpiresAt != want {
		t.Fatalf("deny expires_at %s, want %s", d.ExpiresAt, want)
	}
}

func TestAuthorizeDecisionBoundAllowExpiresAtTheDecisionTime(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	d := decodeWire[wireDecision](t, h.one(h.question("analytics.read", "organization", "acme", h.org)))
	if !d.Allowed || d.ExpiresAt != time.Unix(0, authorizeNow).UTC().Format(expiresAtLayout) {
		t.Fatalf("decision_bound allow %+v, want expiry at the decision time", d)
	}
}

// No catalog role holds a repository action yet, so the handler cannot reach
// a repository allow; the expiry rule is held here on the decision itself.
func TestDecisionExpiry(t *testing.T) {
	now := authorizeNow
	bound := now + authorization.RepositoryAllowBoundNanos
	cases := map[string]struct {
		d    authorization.Decision
		want int64
	}{
		"repository allow keeps the core expiry": {authorization.Decision{Allowed: true, ExpiresAtUnixNano: bound}, bound},
		"allow without expiry":                   {authorization.Decision{Allowed: true}, now},
		"allow with a past expiry":               {authorization.Decision{Allowed: true, ExpiresAtUnixNano: now - 1}, now},
		"deny with an expiry":                    {authorization.Decision{ExpiresAtUnixNano: bound}, now},
		"deny":                                   {authorization.Decision{}, now},
	}
	for name, c := range cases {
		if got := decisionExpiry(c.d, now); !got.Equal(time.Unix(0, c.want)) {
			t.Errorf("%s: %v, want %v", name, got, time.Unix(0, c.want).UTC())
		}
	}
	if got := decisionExpiry(authorization.Decision{Allowed: true, ExpiresAtUnixNano: bound}, now).Format(expiresAtLayout); got != "2026-11-01T12:00:30.000000Z" {
		t.Errorf("wire form %s", got)
	}
}

func TestAuthorizePlatformDecisionHasANullOrganization(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	rec := h.one(h.question("admin.users", "platform", "platform", ""))
	validateSurface(t, "authorization_decision.v1", rec.Body.Bytes())
	d := decodeWire[wireDecision](t, rec)
	if d.Allowed || d.ReasonCode != "platform_role_absent" || d.OrganizationID != nil {
		t.Fatalf("decision %+v, want a platform_role_absent deny with a null organization", d)
	}
	if !strings.Contains(rec.Body.String(), `"organization_id":null`) {
		t.Fatalf("body %s has no explicit null organization_id", rec.Body)
	}
}

func TestAuthorizeBatchDecidesEachItemApart(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	qs := []map[string]any{
		h.question("settings.write", "organization", "acme", h.org),
		h.question("org.delete", "organization", "acme", h.org),
		h.question("analytics.read", "organization", "acme", h.org),
	}
	rec := h.batch(qs...)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	validateSurface(t, "authorization_batch_response.v1", rec.Body.Bytes())
	got := decodeWire[struct {
		Decisions []struct {
			Index    int          `json:"index"`
			Decision wireDecision `json:"decision"`
		} `json:"decisions"`
	}](t, rec)
	if len(got.Decisions) != len(qs) {
		t.Fatalf("%d decisions for %d requests", len(got.Decisions), len(qs))
	}
	ids := map[string]bool{}
	for i, item := range got.Decisions {
		alone := decodeWire[wireDecision](t, h.one(qs[i]))
		if item.Index != i || item.Decision.Allowed != alone.Allowed || item.Decision.ReasonCode != alone.ReasonCode {
			t.Errorf("item %d: batch %+v, alone %+v", i, item, alone)
		}
		ids[item.Decision.DecisionID] = true
	}
	if got.Decisions[1].Decision.Allowed || !got.Decisions[0].Decision.Allowed || !got.Decisions[2].Decision.Allowed {
		t.Fatalf("decisions %+v, want allow, deny, allow", got.Decisions)
	}
	if len(ids) != len(qs) {
		t.Fatalf("decision ids repeat: %v", ids)
	}
}

func TestAuthorizeBatchReadsEachPairOnceInOneTransaction(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	q := h.question("settings.write", "organization", "acme", h.org)
	if rec := h.batch(q, q, q, h.question("admin.users", "platform", "platform", "")); rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	if h.state.reads != 1 || h.state.snapReads != 2 {
		t.Fatalf("%d transactions and %d snapshot reads, want 1 and 2", h.state.reads, h.state.snapReads)
	}
}

func assertError(t *testing.T, rec *httptest.ResponseRecorder, status int, reason string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status %d, want %d; body %s", rec.Code, status, rec.Body)
	}
	if status != http.StatusBadRequest {
		validateSurface(t, contracts.ErrorSurface, rec.Body.Bytes())
	}
	var doc struct {
		ReasonCode string `json:"reason_code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil || doc.ReasonCode != reason {
		t.Fatalf("reason %q (%v), want %s", doc.ReasonCode, err, reason)
	}
	if strings.Contains(rec.Body.String(), `"allowed"`) {
		t.Fatalf("an error body carries a decision member: %s", rec.Body)
	}
}

func TestAuthorizeStateFailuresAreA503NeverADecision(t *testing.T) {
	q := func(h *authorizeHandlerFixture) map[string]any {
		return h.question("acr.context.read", "repository", "full-chaos/dev-health-ops", h.org)
	}
	for name, plant := range map[string]func(*authorizeHandlerFixture){
		"begin":        func(h *authorizeHandlerFixture) { h.state.beginErr = errors.New("pool exhausted") },
		"session read": func(h *authorizeHandlerFixture) { h.state.sessionErr = errors.New("read") },
		"snapshot":     func(h *authorizeHandlerFixture) { h.state.snapErr = errors.New("read") },
		"grant read": func(h *authorizeHandlerFixture) {
			h.state.snapshots[snapKey{h.principal, h.org}].grantsErr = errors.New("read")
		},
		"no clock": func(h *authorizeHandlerFixture) { h.state.snapshots[snapKey{h.principal, h.org}].now = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			h := newAuthorizeHandlerFixture(t)
			plant(h)
			assertError(t, h.one(q(h)), http.StatusServiceUnavailable, reasonStoreUnavailable)
			if name != "grant read" {
				// An organization action reads no grant, so only the other
				// failures reach it.
				assertError(t, h.one(h.question("settings.write", "organization", "acme", h.org)), http.StatusServiceUnavailable, reasonStoreUnavailable)
			}
			assertError(t, h.batch(h.question("settings.write", "organization", "acme", h.org), q(h)), http.StatusServiceUnavailable, reasonStoreUnavailable)
		})
	}
}

func TestAuthorizeKeySetOutageIsA503(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	token := h.workloadToken(nil)
	h.keys.err = errors.New("keystore down")
	assertError(t, h.post(PatternAuthorize, token, mustJSON(t, h.question("settings.write", "organization", "acme", h.org))),
		http.StatusServiceUnavailable, reasonDependency)
	if h.state.reads != 0 {
		t.Fatalf("%d state transactions after a key outage, want 0", h.state.reads)
	}
}

func TestAuthorizeRefusesTheCallerBeforeAnyStateRead(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	body := mustJSON(t, h.question("settings.write", "organization", "acme", h.org))
	for name, bearer := range map[string]string{
		"no bearer":      "",
		"wrong audience": h.workloadToken(func(c *contracts.WorkloadTokenClaims) { c.Aud = "dev-health-api" }),
		"unlisted sub":   h.workloadToken(func(c *contracts.WorkloadTokenClaims) { c.Sub = session.WirePrincipalID(uuid.New()) }),
		"user token":     h.reference,
		"service cred":   "svc_authsess_" + strings.Repeat("a", 43),
	} {
		h.keys.reads = 0
		assertError(t, h.post(PatternAuthorize, bearer, body), http.StatusUnauthorized, reasonInvalidCredentials)
		if h.state.reads != 0 {
			t.Fatalf("%s: %d state transactions, want 0", name, h.state.reads)
		}
		if name == "wrong audience" && h.keys.reads != 0 {
			t.Fatalf("wrong audience: %d key-set reads, want 0", h.keys.reads)
		}
	}
}

func TestAuthorizeRefusedReferenceIsADistinct401(t *testing.T) {
	cases := map[string]func(h *authorizeHandlerFixture) map[string]any{
		"session-route audience": func(h *authorizeHandlerFixture) map[string]any {
			q := h.question("settings.write", "organization", "acme", h.org)
			q["principal_reference"] = h.userToken(h.principal, func(c *contracts.AccessTokenClaims) { c.Aud, c.Scope = contracts.SessionRouteAudience, "auth:session" })
			return q
		},
		"expired token": func(h *authorizeHandlerFixture) map[string]any {
			q := h.question("settings.write", "organization", "acme", h.org)
			q["principal_reference"] = h.userToken(h.principal, func(c *contracts.AccessTokenClaims) { c.Iat -= 3600; c.Nbf -= 3600; c.Exp -= 3600 })
			return q
		},
		"revoked session": func(h *authorizeHandlerFixture) map[string]any {
			h.state.sessions[h.sid] = DecisionSession{Found: true, PrincipalID: h.principal}
			return h.question("settings.write", "organization", "acme", h.org)
		},
		"unknown session": func(h *authorizeHandlerFixture) map[string]any {
			delete(h.state.sessions, h.sid)
			return h.question("settings.write", "organization", "acme", h.org)
		},
		"session of another principal": func(h *authorizeHandlerFixture) map[string]any {
			h.state.sessions[h.sid] = DecisionSession{Found: true, PrincipalID: uuid.New(), Live: true}
			return h.question("settings.write", "organization", "acme", h.org)
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			h := newAuthorizeHandlerFixture(t)
			q := build(h)
			assertError(t, h.one(q), http.StatusUnauthorized, reasonPrincipalReferenceInvalid)
			assertError(t, h.batch(h.question("settings.write", "organization", "acme", h.org), q), http.StatusUnauthorized, reasonPrincipalReferenceInvalid)
		})
	}
}

func TestAuthorizeRefusesABodyThatAssertsIdentity(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	for name, mutate := range map[string]func(map[string]any){
		"principal_id":    func(q map[string]any) { q["principal_id"] = session.WirePrincipalID(uuid.New()) },
		"organization_id": func(q map[string]any) { q["organization_id"] = uuid.NewString() },
		"roles":           func(q map[string]any) { q["roles"] = []string{"owner"} },
	} {
		q := h.question("settings.write", "organization", "acme", h.org)
		mutate(q)
		assertError(t, h.one(q), http.StatusBadRequest, reasonMalformed)
		_ = name
	}
	if h.state.reads != 0 {
		t.Fatalf("%d state transactions for refused bodies, want 0", h.state.reads)
	}
}

func TestAuthorizeBatchBounds(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	q := h.question("settings.write", "organization", "acme", h.org)
	qs := make([]map[string]any, maxAuthorizationBatchItems+1)
	for i := range qs {
		qs[i] = q
	}
	assertError(t, h.batch(qs...), http.StatusBadRequest, reasonMalformed)
	assertError(t, h.batch(), http.StatusBadRequest, reasonMalformed)

	req := httptest.NewRequest(http.MethodPost, PatternAuthorizeBatch, io.NopCloser(strings.NewReader(strings.Repeat(" ", maxAuthorizeBatchBody+1))))
	req.ContentLength = -1
	req.Header.Set("Authorization", "Bearer "+h.workloadToken(nil))
	rec := httptest.NewRecorder()
	requestid.Middleware(h.routes[PatternAuthorizeBatch]).ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge || !strings.Contains(rec.Body.String(), `"payload_too_large"`) {
		t.Fatalf("over-cap body: %d %s, want the transport's 413 payload_too_large", rec.Code, rec.Body)
	}
	if h.state.reads != 0 {
		t.Fatalf("%d state transactions for refused batches, want 0", h.state.reads)
	}
}

func TestAuthorizeDecisionIDFailureIsA503(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rec, err := newRecorder(logger, noop.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	handlers := authorizeHandlers{AuthorizeDeps: AuthorizeDeps{Callers: h.callers(), References: h.references(), State: h.state,
		Evaluator: authorization.NewEvaluator(), Logger: logger}, rec: rec, decisionID: func() (string, error) { return "", errors.New("entropy") }}
	req := httptest.NewRequest(http.MethodPost, PatternAuthorize, bytes.NewReader(mustJSON(t, h.question("settings.write", "organization", "acme", h.org))))
	req.Header.Set("Authorization", "Bearer "+h.workloadToken(nil))
	w := httptest.NewRecorder()
	requestid.Middleware(http.HandlerFunc(handlers.authorize)).ServeHTTP(w, req)
	assertError(t, w, http.StatusServiceUnavailable, reasonDependency)
}

func TestNewDecisionIDHasTheContractShape(t *testing.T) {
	seen := map[string]bool{}
	for range 200 {
		id, err := newDecisionID()
		if err != nil || !decisionIDShape.MatchString(id) || seen[id] {
			t.Fatalf("id %q (%v) repeated or off shape", id, err)
		}
		seen[id] = true
	}
}

// Every Reason is on exactly one side: it reaches the wire as a decision
// reason_code, or it is a state failure answered 503.
func TestEveryReasonIsAWireCodeOrAStateFailure(t *testing.T) {
	for _, r := range authorization.AllReasons() {
		_, onWire := wireReasonCode(r)
		failure := slices.Contains(authorizeStateFailures, r)
		if onWire == failure {
			t.Errorf("reason %s: on the wire %v, state failure %v; want exactly one", r, onWire, failure)
		}
	}
	for _, r := range authorizeStateFailures {
		if !slices.Contains(authorization.AllReasons(), r) {
			t.Errorf("state failure %s is not a Reason", r)
		}
	}
	if _, ok := wireReasonCode("no_such_reason"); ok {
		t.Error("a value outside AllReasons maps to the wire")
	}
}

func TestTheWireReasonCodesAreTheSchemaAndVocabularySets(t *testing.T) {
	root := repoRoot(t)
	var schema struct {
		Properties struct {
			ReasonCode struct {
				Enum []string `json:"enum"`
			} `json:"reason_code"`
		} `json:"properties"`
	}
	readJSON(t, filepath.Join(contracts.ContractsDir(root), "jsonschema", "authorization_decision.v1.schema.json"), &schema)
	var vocab struct {
		DecisionReasonCode struct {
			Values []string `json:"values"`
		} `json:"decision_reason_code"`
	}
	readJSON(t, filepath.Join(contracts.ContractsDir(root), "vocabulary", "reason-codes.json"), &vocab)
	want := AuthorizeWireReasonCodes()
	for name, got := range map[string][]string{"schema enum": schema.Properties.ReasonCode.Enum, "vocabulary": vocab.DecisionReasonCode.Values} {
		got = slices.Clone(got)
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Errorf("%s %v, want %v", name, got, want)
		}
	}
	if _, err := os.Stat(filepath.Join(contracts.ContractsDir(root), "jsonschema", "authorization_decision.v1.schema.json")); err != nil {
		t.Fatal(err)
	}
}
