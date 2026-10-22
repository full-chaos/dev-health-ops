package apiservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const exemptPath = "/api/v1/billing/webhooks/stripe"

type countingStore struct {
	scopeStore
	calls atomic.Int64
}

func (s *countingStore) UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	s.calls.Add(1)
	return s.scopeStore.UserState(ctx, id)
}

func (s *countingStore) Membership(ctx context.Context, user, org uuid.UUID) (string, bool, error) {
	s.calls.Add(1)
	return s.scopeStore.Membership(ctx, user, org)
}

func (s *countingStore) ActiveImpersonation(ctx context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	s.calls.Add(1)
	return s.scopeStore.ActiveImpersonation(ctx, admin)
}

func TestSignatureOnlyRouteSkipsScopeOnlyForExactRoute(t *testing.T) {
	key := strings.Repeat("fixture-key-", 3)
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	store := &countingStore{scopeStore: scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}}
	auth, err := policy.NewAuthenticator(verifier, store, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, quietLogger())
	reached := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }
	routes := []httpapi.Route{
		{Method: http.MethodPost, Pattern: exemptPath, Handler: http.HandlerFunc(reached)},
		{Method: http.MethodPost, Pattern: "/api/v1/billing/other", Handler: http.HandlerFunc(reached)},
	}
	server, err := NewServer(apiTestConfig(), quietLogger(), routes, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	sign := func(sub uuid.UUID, superuser bool) string {
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub": sub.String(), "type": "access", "org_id": uuid.NewString(), "is_superuser": superuser,
			"exp": time.Now().Add(time.Hour).Unix(),
		}).SignedString([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	send := func(method, target string, superuser bool) (int, int64) {
		before := store.calls.Load()
		sub := uuid.New()
		if superuser {
			sub = store.admin
		}
		request := httptest.NewRequest(method, target, nil)
		request.Header.Set("Authorization", "Bearer "+sign(sub, superuser))
		request.Header.Set("X-Org-Id", uuid.NewString())
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		return recorder.Code, store.calls.Load() - before
	}

	for _, superuser := range []bool{false, true} {
		code, calls := send(http.MethodPost, exemptPath, superuser)
		if code != http.StatusNoContent || calls != 0 {
			t.Errorf("exact route (superuser=%v): status %d, %d auth calls; want 204 and 0", superuser, code, calls)
		}
	}
	for name, target := range map[string][2]string{
		"other method":   {http.MethodGet, exemptPath},
		"trailing slash": {http.MethodPost, exemptPath + "/"},
		"encoded slash":  {http.MethodPost, "/api/v1/billing/webhooks%2Fstripe"},
		"other route":    {http.MethodPost, "/api/v1/billing/other"},
	} {
		if _, calls := send(target[0], target[1], false); calls == 0 {
			t.Errorf("%s: scope middlewares made no auth call", name)
		}
	}
}

func TestNewServerRefusesANilScopeLayerWithoutPanicking(t *testing.T) {
	if _, err := NewServer(apiTestConfig(), quietLogger(), nil, nil); err == nil {
		t.Fatal("a nil scope layer was accepted")
	}
}
