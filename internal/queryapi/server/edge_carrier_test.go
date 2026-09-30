package server

// Unit tests for /query's edge-access-token carrier (CHAOS-6263 PR (a),
// D2898/D2905): each is an EXECUTED, planted-defect test -- the property it
// pins is proven by first watching it fail without the fix (documented in
// each test's own comment where that matters) rather than trusted from
// reading the code. A fake policy.Store lets each test control is_active/
// is_superuser/token_version/membership/impersonation independently, without
// a real Postgres.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

const (
	ecSecret   = "edge-carrier-test-secret-32-bytes-min!!"
	ecIssuer   = "dev-health-ops"
	ecAudience = "dev-health-api"
)

// fakeEdgeStore is policy.Store with every answer set directly by the test,
// no Postgres. errUserState/errIsMember/errActiveImpersonation let a test
// force the "store failure" branches (ErrUnavailable vs a plain error).
type fakeEdgeStore struct {
	states       map[uuid.UUID]policy.UserState
	found        map[uuid.UUID]bool
	members      map[[2]uuid.UUID]bool
	sessions     map[uuid.UUID]*policy.Impersonation
	errUserState error
	errIsMember  error
	errSession   error
}

func (f *fakeEdgeStore) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	if f.errUserState != nil {
		return policy.UserState{}, false, f.errUserState
	}
	return f.states[id], f.found[id], nil
}

func (f *fakeEdgeStore) IsMember(_ context.Context, userID, orgID uuid.UUID) (bool, error) {
	if f.errIsMember != nil {
		return false, f.errIsMember
	}
	return f.members[[2]uuid.UUID{userID, orgID}], nil
}

func (f *fakeEdgeStore) ActiveImpersonation(_ context.Context, adminID uuid.UUID) (*policy.Impersonation, error) {
	if f.errSession != nil {
		return nil, f.errSession
	}
	return f.sessions[adminID], nil
}

// ecEdgeAuth builds a real edgetoken.Verifier (so ecMintEdgeToken's real
// signatures verify) wired to store via policy.NewAuthenticator -- the same
// construction buildQueryEdgeAuthenticatorFromEnv does.
func ecEdgeAuth(t *testing.T, store policy.Store) *policy.Authenticator {
	t.Helper()
	verifier, err := edgetoken.New(ecSecret, ecIssuer, ecAudience)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

// ecEdgeClaims is ecMintEdgeToken's input -- every field Python's
// create_access_token puts in a real edge access token that this path
// reads (services/auth.py).
type ecEdgeClaims struct {
	sub                 string
	orgID               string
	role                string
	isSuperuser         bool
	tokenVersion        int
	impersonatingUserID string
}

func ecMintEdgeToken(t *testing.T, c ecEdgeClaims) string {
	t.Helper()
	claims := jwt.MapClaims{
		"sub": c.sub, "org_id": c.orgID, "role": c.role,
		"is_superuser": c.isSuperuser, "tv": c.tokenVersion,
		"type": "access", "iss": ecIssuer, "aud": ecAudience,
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	if c.impersonatingUserID != "" {
		claims["impersonating_user_id"] = c.impersonatingUserID
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(ecSecret))
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func ecPost(handler http.HandlerFunc, bearer string) *httptest.ResponseRecorder {
	body := `{"query":"` + iaDocument + `"}`
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

var (
	ecUser   = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	ecOrg    = uuid.MustParse("22222222-2222-2222-2222-222222222222")
	ecTarget = uuid.MustParse("33333333-3333-3333-3333-333333333333")
)

// TestEdgeCarrier_AcceptedMemberIsServedAndScoped is the org-scoped, valid
// member case: a live active/member/non-superuser row -> reachable, and
// authctx.Claims carries exactly the token's org_id/role, no elevation.
func TestEdgeCarrier_AcceptedMemberIsServedAndScoped(t *testing.T) {
	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: false, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
	}
	auth := ecEdgeAuth(t, store)
	handler, seen := iaDispatchWithEdge(t, nil, auth, store)
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5})

	rec := ecPost(handler, token)
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	if len(*seen) != 1 {
		t.Fatalf("resolver ran %d times, want 1", len(*seen))
	}
	want := authctx.Claims{OrgID: ecOrg.String(), Role: "admin", IsSuperuser: false, ImpersonationActive: false}
	if (*seen)[0] != want {
		t.Fatalf("claims = %+v, want %+v", (*seen)[0], want)
	}
}

// TestEdgeCarrier_RevokedTokenVersionIsRefused plants the exact revocation
// defect: token claims tv=5, the live row now has token_version=6 (a
// logout-everywhere bump) -- the OLD unauthenticated dispatch (no edge
// carrier at all) would have no way to observe this; this test fails on a
// build that skips the live token_version compare and passes only once it
// runs.
func TestEdgeCarrier_RevokedTokenVersionIsRefused(t *testing.T) {
	store := &fakeEdgeStore{
		states: map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: false, TokenVersion: 6}},
		found:  map[uuid.UUID]bool{ecUser: true},
	}
	auth := ecEdgeAuth(t, store)
	handler, seen := iaDispatchWithEdge(t, nil, auth, store)
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "member", tokenVersion: 5})

	rec := ecPost(handler, token)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", rec.Code)
	}
	if len(*seen) != 0 {
		t.Fatalf("resolver ran %d times for a revoked token, want 0", len(*seen))
	}
}

// TestEdgeCarrier_InactiveUserIsRefused: is_active=false live, even with a
// current token_version and a real row.
func TestEdgeCarrier_InactiveUserIsRefused(t *testing.T) {
	store := &fakeEdgeStore{
		states: map[uuid.UUID]policy.UserState{ecUser: {IsActive: false, IsSuperuser: false, TokenVersion: 5}},
		found:  map[uuid.UUID]bool{ecUser: true},
	}
	auth := ecEdgeAuth(t, store)
	handler, seen := iaDispatchWithEdge(t, nil, auth, store)
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "member", tokenVersion: 5})

	rec := ecPost(handler, token)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", rec.Code)
	}
	if len(*seen) != 0 {
		t.Fatalf("resolver ran %d times for a deactivated user, want 0", len(*seen))
	}
}

// TestEdgeCarrier_MembershipRemovedIsRefused: active, current token_version,
// NOT a superuser, and the token's own claimed org has no membership row
// (removed after the token was issued). This is the check with no REST
// precedent -- REST never re-verifies the token's own org_id claim against
// memberships at all (only a foreign X-Org-Id header goes through
// mayUseOrg); /query's edge path is stricter here by design (D2905).
func TestEdgeCarrier_MembershipRemovedIsRefused(t *testing.T) {
	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: false, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{}, // no membership row for (ecUser, ecOrg)
	}
	auth := ecEdgeAuth(t, store)
	handler, seen := iaDispatchWithEdge(t, nil, auth, store)
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "member", tokenVersion: 5})

	rec := ecPost(handler, token)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", rec.Code)
	}
	if len(*seen) != 0 {
		t.Fatalf("resolver ran %d times for a removed membership, want 0", len(*seen))
	}
}

// TestEdgeCarrier_SuperuserFlagClearedInDatabaseOverridesAStaleToken plants
// the is_superuser-elevation defect directly: the TOKEN claims
// is_superuser=true (as it would if minted before a demotion), the live row
// says false. If the code trusted the token's own claim (the contradiction
// D2898 caught), authctx.Claims.IsSuperuser would read true here; it must
// read false, and RequirePlatformAdmin (already tested in
// platform.go/schema.resolvers.go) denies false the same as no principal.
func TestEdgeCarrier_SuperuserFlagClearedInDatabaseOverridesAStaleToken(t *testing.T) {
	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: false, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
	}
	auth := ecEdgeAuth(t, store)
	handler, seen := iaDispatchWithEdge(t, nil, auth, store)
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", isSuperuser: true, tokenVersion: 5})

	rec := ecPost(handler, token)
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200 (a demoted-but-active-and-a-member user is still served, just not as superuser), body=%s", rec.Code, rec.Body.String())
	}
	if len(*seen) != 1 || (*seen)[0].IsSuperuser {
		t.Fatalf("claims = %+v, want IsSuperuser=false (the live row, never the stale token claim)", *seen)
	}
}

// TestEdgeCarrier_ImpersonatingSuperuserSetsImpersonationActive: a live
// active impersonation session for this (live-superuser) admin sets
// ImpersonationActive and switches the effective org to the session's
// target org -- platform.go's RequirePlatformAdmin (already tested)
// then denies it, matching D2905's explicit "impersonating superuser ->
// platform dashboard 403" case one layer down (the claim this test pins).
func TestEdgeCarrier_ImpersonatingSuperuserSetsImpersonationActive(t *testing.T) {
	store := &fakeEdgeStore{
		states:   map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: true, TokenVersion: 5}},
		found:    map[uuid.UUID]bool{ecUser: true},
		sessions: map[uuid.UUID]*policy.Impersonation{ecUser: {TargetUserID: ecTarget, TargetOrgID: ecOrg, TargetRole: "viewer"}},
	}
	auth := ecEdgeAuth(t, store)
	handler, seen := iaDispatchWithEdge(t, nil, auth, store)
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: "99999999-9999-9999-9999-999999999999", role: "owner", isSuperuser: true, tokenVersion: 5})

	rec := ecPost(handler, token)
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	if len(*seen) != 1 {
		t.Fatalf("resolver ran %d times, want 1", len(*seen))
	}
	got := (*seen)[0]
	if !got.ImpersonationActive {
		t.Fatalf("claims = %+v, want ImpersonationActive=true", got)
	}
	if got.OrgID != ecOrg.String() {
		t.Fatalf("OrgID = %q, want the impersonation session's target org %q (never the token's own org_id claim while impersonating)", got.OrgID, ecOrg.String())
	}
	if got.Role != "viewer" {
		t.Fatalf("Role = %q, want the impersonation session's target role %q (the effective principal is the target's, as the Python edge stated it)", got.Role, "viewer")
	}
}

// TestEdgeCarrier_TwoAuthorizationHeadersAreAmbiguous is D2891's
// refuseAmbiguousCarrier extension: two bearer tokens can name two
// identities the same way one bearer plus the internal headers can: taking
// net/http's Header.Get "first value" would silently ignore the second.
func TestEdgeCarrier_TwoAuthorizationHeadersAreAmbiguous(t *testing.T) {
	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
	}
	auth := ecEdgeAuth(t, store)
	handler, seen := iaDispatchWithEdge(t, nil, auth, store)
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "member", tokenVersion: 5})

	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"query":"`+iaDocument+`"}`))
	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Add("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", rec.Code)
	}
	if len(*seen) != 0 {
		t.Fatalf("resolver ran %d times for two Authorization headers, want 0", len(*seen))
	}
}

// TestEdgeCarrier_StoreUnavailableIsRefusedNotServed: a store failure
// (ErrUnavailable) must never read as "no claim, proceed" -- it is refused,
// same posture as every other store failure on this path.
func TestEdgeCarrier_StoreUnavailableIsRefused(t *testing.T) {
	store := &fakeEdgeStore{errUserState: errors.Join(policy.ErrUnavailable, errors.New("connection reset"))}
	auth := ecEdgeAuth(t, store)
	handler, seen := iaDispatchWithEdge(t, nil, auth, store)
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "member", tokenVersion: 5})

	rec := ecPost(handler, token)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", rec.Code)
	}
	if len(*seen) != 0 {
		t.Fatalf("resolver ran %d times for a store failure, want 0", len(*seen))
	}
}

// TestEdgeCarrier_ImpersonationLookupFailureFailsClosed (D2919 condition 2,
// reversing this test's own earlier "fails open" version): a session
// LOOKUP failure for a superuser caller must refuse the request outright,
// never serve it as the plain non-impersonated principal -- a superuser
// who genuinely IS impersonating would otherwise pass a "not
// impersonating" gate (RequirePlatformAdmin) during exactly the database
// fault that hid it. go-api's own Scope.Impersonation (scope.go) fails
// open on this same lookup today; that is a documented, reported
// difference (D2919), not copied here.
func TestEdgeCarrier_ImpersonationLookupFailureFailsClosed(t *testing.T) {
	store := &fakeEdgeStore{
		states:     map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: true, TokenVersion: 5}},
		found:      map[uuid.UUID]bool{ecUser: true},
		members:    map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
		errSession: errors.New("session store timeout"),
	}
	auth := ecEdgeAuth(t, store)
	handler, seen := iaDispatchWithEdge(t, nil, auth, store)
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "owner", isSuperuser: true, tokenVersion: 5})

	rec := ecPost(handler, token)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401 (an impersonation-lookup failure must refuse, never serve as the non-impersonated principal), body=%s", rec.Code, rec.Body.String())
	}
	if len(*seen) != 0 {
		t.Fatalf("resolver ran %d times for a failed impersonation lookup, want 0", len(*seen))
	}
}

// TestEdgeCarrier_PlatformWideOrgSkipsMembershipCheck: orgID == "" (Python's
// own productTelemetryPlatformDashboard call shape, schema.py:222) must
// reach the resolver without an IsMember call at all -- there is no org to
// check membership against.
func TestEdgeCarrier_PlatformWideOrgSkipsMembershipCheck(t *testing.T) {
	store := &fakeEdgeStore{
		states: map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: true, TokenVersion: 5}},
		found:  map[uuid.UUID]bool{ecUser: true},
		// members intentionally nil/empty: IsMember must never be asked.
	}
	auth := ecEdgeAuth(t, store)
	handler, seen := iaDispatchWithEdge(t, nil, auth, store)
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: "", role: "owner", isSuperuser: true, tokenVersion: 5})

	rec := ecPost(handler, token)
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	if len(*seen) != 1 || (*seen)[0].OrgID != "" {
		t.Fatalf("claims = %+v, want OrgID=\"\"", *seen)
	}
}

// TestEdgeCarrier_ClaimsMatchTheEnvelopeCarrierForTheSameIdentity is D2905's
// differential requirement: for the SAME logical identity, the envelope
// path (principal.Verifier) and the edge path (policy.Authenticator) must
// produce byte-identical authctx.Claims.
func TestEdgeCarrier_ClaimsMatchTheEnvelopeCarrierForTheSameIdentity(t *testing.T) {
	verifier, priv := iaVerifier(t)
	envelopeHandler, envelopeSeen := iaDispatch(t, verifier)
	envelopeToken := iaEnvelope(t, priv, principal.Claims{OrgID: ecOrg.String(), Role: "admin", IsSuperuser: true, ImpersonationActive: false})
	if code := iaPost(t, envelopeHandler, func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+envelopeToken) }); code != http.StatusOK {
		t.Fatalf("envelope carrier: %d", code)
	}

	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
	}
	auth := ecEdgeAuth(t, store)
	edgeHandler, edgeSeen := iaDispatchWithEdge(t, nil, auth, store)
	edgeToken := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", isSuperuser: true, tokenVersion: 5})
	if rec := ecPost(edgeHandler, edgeToken); rec.Code != http.StatusOK {
		t.Fatalf("edge carrier: %d, body=%s", rec.Code, rec.Body.String())
	}

	if len(*envelopeSeen) != 1 || len(*edgeSeen) != 1 {
		t.Fatalf("expected exactly one resolver run per carrier: envelope=%+v edge=%+v", *envelopeSeen, *edgeSeen)
	}
	if (*envelopeSeen)[0] != (*edgeSeen)[0] {
		t.Fatalf("claims disagree between carriers: envelope=%+v edge=%+v", (*envelopeSeen)[0], (*edgeSeen)[0])
	}
}
