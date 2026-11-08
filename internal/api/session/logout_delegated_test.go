package session

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

// activeUsers is a policy.Store whose every user is active at token version 0.
type activeUsers struct{}

func (activeUsers) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{IsActive: true}, true, nil
}
func (activeUsers) Membership(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	return "", false, nil
}
func (activeUsers) OrgMembership(context.Context, uuid.UUID, uuid.UUID) (string, bool, bool, error) {
	return "", false, false, nil
}
func (activeUsers) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}
func (activeUsers) ReadsPlatformRoles() bool { return false }

// logoutRoute mounts the session routes over a routed authenticator that admits every token of tokens
// and returns POST /api/v1/auth/logout and the Guard's log.
func logoutRoute(t *testing.T, tokens *edtokens.Issuer) (http.Handler, *bytes.Buffer) {
	t.Helper()
	deps := completeDeps(t)
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	auth := tokens.AuthenticatorOver(t, activeUsers{}, logger)
	deps.Auth, deps.Guard, deps.Logger = auth, policy.NewGuard(auth, logger), logger
	for _, route := range Routes(deps) {
		if route.Method == http.MethodPost && route.Pattern == "/api/v1/auth/logout" {
			// The route profile NewServer's RouteProfiles sets before the handler reads its optional bearer.
			profile := func(*http.Request) (string, bool) { return "POST /api/v1/auth/logout", true }
			return policy.RouteProfiles(profile)(route.Handler), logs
		}
	}
	t.Fatal("POST /api/v1/auth/logout is not mounted")
	return nil, nil
}

// orglessBearer is a routed user access token with no org_id, so logout writes no audit row; a non-nil
// actor makes it a delegated (impersonation) bearer.
func orglessBearer(t *testing.T, tokens *edtokens.Issuer, actor *uuid.UUID) string {
	t.Helper()
	return tokens.MustMint(t, edtokens.Claims{UserID: uuid.New(), Actor: actor})
}

func serveLogout(handler http.Handler, bearer string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", strings.NewReader(`{"refresh_token":"not-a-refresh-token"}`))
	request.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	request.Pattern = "POST /api/v1/auth/logout"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

const logoutDone = `{"message":"Logout successful"}`

// TestLogoutRefusesADelegatedBearer: logout reads its optional bearer outside
// the Guard, so it applies the Guard's delegated refusal itself: a delegated
// session's bearer is the Guard's 403, body and Warn line, before any revoke
// or audit write.
func TestLogoutRefusesADelegatedBearer(t *testing.T) {
	tokens := edtokens.New(t)
	handler, logs := logoutRoute(t, tokens)
	actor := uuid.New()
	got := serveLogout(handler, orglessBearer(t, tokens, &actor))
	if got.Code != http.StatusForbidden || strings.TrimSpace(got.Body.String()) != `{"detail":"Not allowed in an impersonation session"}` {
		t.Fatalf("delegated bearer: %d %s, want 403 the Guard's refusal", got.Code, got.Body.String())
	}
	if !strings.Contains(logs.String(), "api guard: delegated bearer refused by its row") || !strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("delegated bearer: no Guard refusal Warn line in %q", logs.String())
	}
}

// TestLogoutAnswersAnonymousAndPlainBearersAsBefore: logout with no bearer,
// a refused bearer or a plain (non-delegated) bearer answers as before.
func TestLogoutAnswersAnonymousAndPlainBearersAsBefore(t *testing.T) {
	tokens := edtokens.New(t)
	handler, _ := logoutRoute(t, tokens)
	for name, bearer := range map[string]string{"anonymous": "", "refused bearer": "not.a.token", "plain bearer": orglessBearer(t, tokens, nil)} {
		if got := serveLogout(handler, bearer); got.Code != http.StatusOK || strings.TrimSpace(got.Body.String()) != logoutDone {
			t.Errorf("%s: %d %s, want 200 %s", name, got.Code, got.Body.String(), logoutDone)
		}
	}
}
