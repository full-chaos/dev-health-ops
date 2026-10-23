package apiservice

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/externalingest"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
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
func (s *orgScopeStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	s.calls.Add(1)
	return nil, nil
}

// The two External Push schema routes are public: the handler's answer is the same for every caller. On the
// REAL listener chain (NewServer with the org-scope and impersonation middlewares, as configureWith builds it)
// an anonymous request, every kind of user token with no organisation header, and the token's own
// organisation give the same status, body bytes and validator, with no database attempt in the handler. The
// request-wide org-scope middleware still applies when a bearer and an X-Org-Id are sent: a user token with
// an organisation it is not a member of is refused 403 by the middleware, before the handler, with a pinned
// number of policy-store calls (as on every route).
func TestExternalPushSchemaRoutesAreGlobalOnTheRealListenerChain(t *testing.T) {
	tokens := usertokens.All(t)
	pool, connects := usertokens.CountingPool(t)
	verifier, err := edgetoken.New(usertokens.Secret, usertokens.LegacyIssuer, usertokens.Audience)
	if err != nil {
		t.Fatal(err)
	}
	// The middleware verifies go-api's own HS256 access token (no session claim): mint one the way go-api
	// does, with a fresh organisation. The shared tokens (with a session claim, as auth-service issues them)
	// are sent too; the middleware treats a token it does not accept as no credential.
	signer, err := edgetoken.NewSigner(usertokens.Secret, usertokens.LegacyIssuer, usertokens.Audience)
	if err != nil {
		t.Fatal(err)
	}
	orgClaim := uuid.NewString()
	access, err := signer.Access(edgetoken.AccessClaims{UserID: uuid.NewString(), Email: "ada@example.com", OrgID: orgClaim, Role: "member", TokenVersion: 1}, time.Now().UTC(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	claims := struct{ Org string }{Org: orgClaim}
	store := &orgScopeStore{member: uuid.MustParse(claims.Org)}
	auth, err := policy.NewAuthenticator(verifier, store, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, quietLogger())
	routes := markResponseModels(externalingest.Routes(externalingest.Deps{Pool: pool, Logger: quietLogger()}))
	server, err := NewServer(apiTestConfig(), quietLogger(), routes, scope.OrgScope, scope.Impersonation)
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

		// A valid user token and an organisation it is not a member of: the middleware refuses (403),
		// before the handler, after exactly the pinned policy-store calls (user row, then membership).
		refused, calls := do(path, "Bearer "+access, foreign)
		if refused.Code != http.StatusForbidden || !strings.Contains(refused.Body.String(), "X-Org-Id not permitted") {
			t.Errorf("%s: a foreign organisation was answered %d %s, want the middleware's 403", path, refused.Code, refused.Body.String())
		}
		if calls != 2 {
			t.Errorf("%s: the foreign-organisation refusal made %d policy-store calls, want 2 (user row, membership)", path, calls)
		}
		usertokens.NoLeak(t, path+" refusal body", refused.Body.String(), tokens)
		// The member organisation is a single policy-store call (the user row) and the handler's answer.
		member, calls := do(path, "Bearer "+access, claims.Org)
		same("the HS256 access token with its own organisation (recheck)", member)
		if calls != 1 {
			t.Errorf("%s: the member-organisation request made %d policy-store calls, want 1", path, calls)
		}
	}
	if got := connects.Count(); got != 0 {
		t.Errorf("the schema handlers reached the database: %d connection attempt(s), want 0", got)
	}
}
