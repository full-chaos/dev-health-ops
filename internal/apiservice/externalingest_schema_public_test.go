package apiservice

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/externalingest"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

// orgScopeStore is the policy store behind the real scope middleware: every user is active at the token version the HS256 access token carries, and is a member
// of exactly one organisation. Every call is counted.
type orgScopeStore struct {
	member uuid.UUID
	calls  atomic.Int64
}

func (s *orgScopeStore) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	s.calls.Add(1)
	return policy.UserState{IsActive: true, TokenVersion: 1}, true, nil
}
func (s *orgScopeStore) Membership(_ context.Context, _, org uuid.UUID) (string, bool, error) {
	s.calls.Add(1)
	return "member", org == s.member, nil
}

func (s *orgScopeStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := s.Membership(ctx, user, org)
	return role, member, true, err
}
func (s *orgScopeStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	s.calls.Add(1)
	return nil, nil
}

// The two External Push schema routes are public: the handler's answer is the same for every caller. On the
// REAL listener chain (NewServer with the org-scope and impersonation middlewares, as configureWith builds it)
// an anonymous request, every kind of user token with no organisation header, and the token's own
// organisation give the same status, body bytes and validator, with no database attempt in the handler. A
// public row ignores the bearer: a valid user token with an organisation it is not a member of gets the
// anonymous answer, with no policy-store call.
func TestExternalPushSchemaRoutesAreGlobalOnTheRealListenerChain(t *testing.T) {
	tokens := usertokens.All(t)
	pool, connects := usertokens.CountingPool(t)
	// The middleware verifies a routed user access token with a fresh organisation. The shared tokens are
	// sent too; the middleware treats a token it does not accept as no credential.
	ed := edtokens.New(t)
	orgClaim := uuid.NewString()
	claims := struct{ Org string }{Org: orgClaim}
	store := &orgScopeStore{member: uuid.MustParse(claims.Org)}
	auth := ed.AuthenticatorOver(t, store, quietLogger())
	access := ed.MustMint(t, edtokens.Claims{UserID: uuid.New(), OrgID: orgClaim})
	scope := policy.NewScope(auth, quietLogger())
	routes := markResponseModels(externalingest.Routes(externalingest.Deps{Pool: pool, Logger: quietLogger()}))
	server, err := NewServer(apiTestConfig(), quietLogger(), routes, scope.Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	serial := 0
	do := func(path, authorization, org string) (*httptest.ResponseRecorder, int64) {
		serial++
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.RemoteAddr = fmt.Sprintf("10.7.%d.%d:4000", serial/256%256, serial%256)
		if authorization != "" {
			request.Header.Set("Authorization", authorization)
		}
		if org != "" {
			request.Header.Set("X-Org-Id", org)
		}
		before := store.calls.Load()
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		return recorder, store.calls.Load() - before
	}
	foreign := uuid.NewString()
	for _, path := range []string{"/api/v1/external-ingest/schemas", "/api/v1/external-ingest/schemas/external-ingest.v1"} {
		anonymous, calls := do(path, "", "")
		if anonymous.Code != http.StatusOK || anonymous.Body.Len() == 0 || calls != 0 {
			t.Fatalf("%s anonymous: %d, %d bytes, %d policy-store calls; want 200 with a body and 0 calls", path, anonymous.Code, anonymous.Body.Len(), calls)
		}
		same := func(name string, got *httptest.ResponseRecorder) {
			t.Helper()
			if got.Code != anonymous.Code || got.Body.String() != anonymous.Body.String() || got.Header().Get("ETag") != anonymous.Header().Get("ETag") {
				t.Errorf("%s %s: %d differs from the anonymous %d (bytes equal %v, validator equal %v)", path, name, got.Code, anonymous.Code,
					got.Body.String() == anonymous.Body.String(), got.Header().Get("ETag") == anonymous.Header().Get("ETag"))
			}
			usertokens.NoLeak(t, path+" "+name+" body", got.Body.String(), tokens)
		}
		for _, tok := range tokens {
			got, _ := do(path, "Bearer "+tok.Value, "")
			same(tok.Kind+" with no organisation header", got)
			got, _ = do(path, "Bearer "+tok.Value, claims.Org)
			same(tok.Kind+" with the token's own organisation", got)
		}
		// No credential and a foreign organisation header: the middleware has nothing to scope.
		got, _ := do(path, "", foreign)
		same("anonymous with a foreign organisation header", got)

		// A valid user token and an organisation it is not a member of: the public row does not judge the
		// bearer, so the answer is the anonymous one, with no policy-store call and no organisation data.
		foreignOrg, calls := do(path, "Bearer "+access, foreign)
		same("a valid user token with a foreign organisation", foreignOrg)
		if calls != 0 {
			t.Errorf("%s: the foreign-organisation request made %d policy-store calls, want 0", path, calls)
		}
		if strings.Contains(foreignOrg.Body.String(), foreign) || strings.Contains(foreignOrg.Body.String(), claims.Org) {
			t.Errorf("%s: the foreign-organisation answer carries organisation data", path)
		}
		member, calls := do(path, "Bearer "+access, claims.Org)
		same("the routed access token with its own organisation (recheck)", member)
		if calls != 0 {
			t.Errorf("%s: the member-organisation request made %d policy-store calls, want 0", path, calls)
		}
	}
	if got := connects.Count(); got != 0 {
		t.Errorf("the schema handlers reached the database: %d connection attempt(s), want 0", got)
	}
}

func (*orgScopeStore) ReadsPlatformRoles() bool { return false }
