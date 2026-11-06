package apiservice

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
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
	tokens := edtokens.New(t)
	refusals := &refusalLog{}
	store := &countingStore{scopeStore: scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}}
	auth := tokens.AuthenticatorOver(t, store, slog.New(refusals))
	scope := policy.NewScope(auth, quietLogger())
	reached := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }
	routes := []httpapi.Route{
		{Method: http.MethodPost, Pattern: exemptPath, Handler: http.HandlerFunc(reached)},
		{Method: http.MethodPost, Pattern: "/api/v1/billing/other", Handler: http.HandlerFunc(reached)},
	}
	server, err := NewServer(apiTestConfig(), quietLogger(), routes, scope.Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	// send returns the status, the policy-store calls and the bearers the access check refused. The superuser's users row says superuser (scopeStore.admin).
	send := func(method, target string, superuser bool) (int, int64, int) {
		before, judged := store.calls.Load(), refusals.count("")
		sub := uuid.New()
		if superuser {
			sub = store.admin
		}
		request := httptest.NewRequest(method, target, nil)
		request.Header.Set("Authorization", "Bearer "+tokens.MustMint(t, edtokens.Claims{UserID: sub}))
		request.Header.Set("X-Org-Id", uuid.NewString())
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		return recorder.Code, store.calls.Load() - before, refusals.count("") - judged
	}

	for _, superuser := range []bool{false, true} {
		code, calls, _ := send(http.MethodPost, exemptPath, superuser)
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
		// A variant is no exempt route: the scope judges its bearer once (refused there: no route row, or a
		// row that takes no user token), where the exact route skips the scope.
		if code, _, judged := send(target[0], target[1], false); judged != 1 {
			t.Errorf("%s: the scope judged the bearer %d times (status %d), want 1", name, judged, code)
		}
	}
}

func TestNewServerRefusesANilScopeLayerWithoutPanicking(t *testing.T) {
	if _, err := NewServer(apiTestConfig(), quietLogger(), nil, nil); err == nil {
		t.Fatal("a nil scope layer was accepted")
	}
}

// refusalLog is a slog handler that keeps the reason of every "access token refused" record.
type refusalLog struct {
	mu      sync.Mutex
	reasons []string
}

func (l *refusalLog) Enabled(context.Context, slog.Level) bool { return true }
func (l *refusalLog) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *refusalLog) WithGroup(string) slog.Handler            { return l }
func (l *refusalLog) Handle(_ context.Context, r slog.Record) error {
	if r.Message != "access token refused" {
		return nil
	}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "reason" {
			l.mu.Lock()
			l.reasons = append(l.reasons, a.Value.String())
			l.mu.Unlock()
		}
		return true
	})
	return nil
}

// count is the refusals with reason ("" counts every reason).
func (l *refusalLog) count(reason string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, got := range l.reasons {
		if reason == "" || got == reason {
			n++
		}
	}
	return n
}
