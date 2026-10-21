//go:build integration

package session_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

const (
	ownedOrgB = "b0000000-0000-4000-8000-00000000000b"
	ownedOrgD = "d0000000-0000-4000-8000-00000000000d"
)

// pendingInviteFor seeds a pending invite of email to org and returns its token.
func pendingInviteFor(t *testing.T, ctx context.Context, st stack, org, email string) string {
	t.Helper()
	token, tokenHash := signedtoken.Build(uuid.New(), authmail.Config{}.Secret())
	if _, err := st.pool.Exec(ctx, `INSERT INTO org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
		VALUES (gen_random_uuid(), $1::uuid, $2, 'member', $3, NULL, 'pending', now() + interval '1 day', now(), now())`, org, email, tokenHash); err != nil {
		t.Fatalf("seed invite: %v", err)
	}
	return token
}

// newcomer seeds an active local user with no membership and returns an
// access token for it.
func newcomer(t *testing.T, ctx context.Context, st stack) string {
	t.Helper()
	id := uuid.New()
	email := "newcomer-" + id.String()[:8] + "@example.com"
	if _, err := st.pool.Exec(ctx, `INSERT INTO users (id, email, username, full_name, password_hash, auth_provider, is_active, is_verified,
		is_superuser, token_version, created_at, updated_at) VALUES ($1, $2, NULL, 'Newcomer', NULL, 'local', true, true, false, 0, now(), now())`,
		id, email); err != nil {
		t.Fatalf("seed newcomer: %v", err)
	}
	return changeToken(t, st, id.String(), email, "", "member", 0)
}

func issueCredential(t *testing.T) string {
	t.Helper()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	return "svc_authsess_" + base64.RawURLEncoding.EncodeToString(random)
}

func issueClient(t *testing.T, base string) *sessionissue.Client {
	t.Helper()
	parsed, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	client, err := sessionissue.New(parsed, issueCredential(t), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// fakeIssueRoute is auth-service's issue route: it answers a fixed pair and
// records each request body.
type fakeIssueRoute struct {
	mu     sync.Mutex
	bodies []map[string]any
	// onCall, when set, runs on each request before the answer.
	onCall func(body map[string]any)
}

func (f *fakeIssueRoute) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	raw, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(raw, &body)
	f.mu.Lock()
	f.bodies = append(f.bodies, body)
	onCall := f.onCall
	f.mu.Unlock()
	if onCall != nil {
		onCall(body)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"access_token":"issued.by.auth","refresh_token":"dhr1_issued_by_auth","token_type":"bearer","expires_in":3600}`)
}

func refreshTokenRows(t *testing.T, ctx context.Context, st stack) int {
	t.Helper()
	var n int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM refresh_tokens`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// With auth-service owning sessions every go-api sign-in route hands out the
// pair the issue route answered, with the session's principal, organization
// and amr, and go-api writes no refresh_tokens row.
func TestSessionsOwnedByAuthTakeEveryPairFromTheIssueRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	route := &fakeIssueRoute{}
	auth := httptest.NewServer(route)
	defer auth.Close()
	st := startStackWith(t, ctx, issueClient(t, auth.URL), nil)
	before := refreshTokenRows(t, ctx, st)
	orgA := st.specs["alice"]["org_id"].(string)
	alice := changeToken(t, st, changeAlice, "alice@example.com", orgA, "owner", 0)
	invite := pendingInviteFor(t, ctx, st, ownedOrgD, "alice@example.com")
	fresh := newcomer(t, ctx, st)
	// auth-service pins the session to the user's token_version as it reads
	// it: a password change must be committed before its session is issued.
	var versionAtIssue int64 = -1
	route.onCall = func(map[string]any) {
		if err := st.pool.QueryRow(context.Background(), `SELECT token_version FROM users WHERE id = $1::uuid`, changeAlice).Scan(&versionAtIssue); err != nil {
			t.Errorf("read token_version at issue: %v", err)
		}
	}

	for _, step := range []struct {
		name, path, token, body, org, amr string
	}{
		{"login", "/api/v1/auth/login", "", `{"email": "alice@example.com", "password": "` + sessionscenario.Password + `"}`, orgA, "pwd"},
		{"switch-org", "/api/v1/auth/switch-org", alice, `{"org_id": "` + ownedOrgB + `"}`, ownedOrgB, "pwd"},
		{"social", "/api/v1/auth/social-login", "", `{"provider": "github", "provider_access_token": "gh-frank"}`, "", "fed"},
		{"accept-invite", "/api/v1/auth/accept-invite", alice, `{"token": "` + invite + `"}`, ownedOrgD, "pwd"},
		{"onboard", "/api/v1/auth/onboard", fresh, `{"action": "create_org", "org_name": "Issued Onboarding Org"}`, "", "pwd"},
		{"change-password", "/api/v1/auth/change-password", alice, changeBody(sessionscenario.Password, newPassword), orgA, "pwd"},
	} {
		route.mu.Lock()
		route.bodies = nil
		route.mu.Unlock()
		status, body := changeRequest(t, ctx, st, http.MethodPost, step.path, step.token, step.body)
		if status != http.StatusOK {
			t.Fatalf("%s: %d %s", step.name, status, body)
		}
		if sessionscenario.Field(body, "access_token") != "issued.by.auth" || sessionscenario.Field(body, "refresh_token") != "dhr1_issued_by_auth" {
			t.Fatalf("%s: answered %s, want the issued pair", step.name, body)
		}
		route.mu.Lock()
		bodies := route.bodies
		route.mu.Unlock()
		if len(bodies) != 1 {
			t.Fatalf("%s: %d issue calls, want 1", step.name, len(bodies))
		}
		amr, _ := bodies[0]["amr"].([]any)
		if len(amr) != 1 || amr[0] != step.amr {
			t.Fatalf("%s: amr %v, want %s", step.name, bodies[0]["amr"], step.amr)
		}
		if step.org != "" && bodies[0]["organization_id"] != step.org {
			t.Fatalf("%s: organization %v, want %s", step.name, bodies[0]["organization_id"], step.org)
		}
	}
	if after := refreshTokenRows(t, ctx, st); after != before {
		t.Fatalf("refresh_tokens went from %d to %d rows; go-api must write none", before, after)
	}
	var committed int64
	if err := st.pool.QueryRow(ctx, `SELECT token_version FROM users WHERE id = $1::uuid`, changeAlice).Scan(&committed); err != nil {
		t.Fatal(err)
	}
	if versionAtIssue != committed || committed == 0 {
		t.Fatalf("the change-password session was issued at token_version %d, the committed version is %d", versionAtIssue, committed)
	}
}

// The executed 503 row: auth-service unreachable, every go-api sign-in route
// answers 503 with the proxy's unavailable text, mints no pair and writes no
// refresh_tokens row. A password change is committed before its session is
// issued, so it stands; the caller signs in again with the new password.
func TestSessionsOwnedByAuthAnswer503WhenTheIssueRouteIsDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	down := "http://" + closed.Addr().String()
	_ = closed.Close()
	st := startStackWith(t, ctx, issueClient(t, down), nil)
	before := refreshTokenRows(t, ctx, st)
	hashBefore := passwordHashOf(t, ctx, st, changeAlice)
	orgA := st.specs["alice"]["org_id"].(string)
	alice := changeToken(t, st, changeAlice, "alice@example.com", orgA, "owner", 0)
	invite := pendingInviteFor(t, ctx, st, ownedOrgD, "alice@example.com")
	fresh := newcomer(t, ctx, st)

	for _, step := range []struct{ name, path, token, body string }{
		{"login", "/api/v1/auth/login", "", `{"email": "alice@example.com", "password": "` + sessionscenario.Password + `"}`},
		{"switch-org", "/api/v1/auth/switch-org", alice, `{"org_id": "` + ownedOrgB + `"}`},
		{"social", "/api/v1/auth/social-login", "", `{"provider": "github", "provider_access_token": "gh-frank"}`},
		{"accept-invite", "/api/v1/auth/accept-invite", alice, `{"token": "` + invite + `"}`},
		{"onboard", "/api/v1/auth/onboard", fresh, `{"action": "create_org", "org_name": "Down Onboarding Org"}`},
		{"change-password", "/api/v1/auth/change-password", alice, changeBody(sessionscenario.Password, newPassword)},
	} {
		status, body := changeRequest(t, ctx, st, http.MethodPost, step.path, step.token, step.body)
		if status != http.StatusServiceUnavailable || !strings.Contains(body, sessionissue.UnavailableText) {
			t.Fatalf("%s: %d %s, want 503 %q", step.name, status, body, sessionissue.UnavailableText)
		}
		if strings.Contains(body, "access_token") || strings.Contains(body, "refresh_token") {
			t.Fatalf("%s: a 503 carries a token: %s", step.name, body)
		}
	}
	if after := refreshTokenRows(t, ctx, st); after != before {
		t.Fatalf("refresh_tokens went from %d to %d rows with the issue route down", before, after)
	}
	if passwordHashOf(t, ctx, st, changeAlice) == hashBefore {
		t.Fatal("the password change was not committed before its session was asked for")
	}
}
