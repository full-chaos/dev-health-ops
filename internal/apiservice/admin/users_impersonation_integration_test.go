//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// TestImpersonatingSuperuserListsOnlyTheTargetOrgUsers (CHAOS-9510): with a
// live impersonation session the users list is the target org's, whatever
// X-Org-Id the request carries and even when it carries none (which for a
// superuser would otherwise be the global list). Without a session the
// superuser's global list and X-Org-Id scoping are unchanged, and an ended
// session is not applied on the very next request.
func TestImpersonatingSuperuserListsOnlyTheTargetOrgUsers(t *testing.T) {
	ctx := context.Background()
	pool, _ := migratedPostgres(t, ctx)
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, query)
		}
	}
	ids := map[string]uuid.UUID{}
	for name, super := range map[string]bool{"root": true, "target": false, "adminuser": false, "outsider": false} {
		ids[name] = uuid.New()
		exec(`INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'x', true, true, $3, 0, now(), now())`, ids[name], "imp-"+name+"@example.com", super)
	}
	adminOrg, targetOrg, otherOrg := uuid.New(), uuid.New(), uuid.New()
	for slug, id := range map[string]uuid.UUID{"imp-admin-org": adminOrg, "imp-target-org": targetOrg, "imp-other-org": otherOrg} {
		exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, id, slug)
	}
	for _, m := range []struct {
		org        uuid.UUID
		user, role string
	}{{adminOrg, "adminuser", "owner"}, {targetOrg, "target", "owner"}, {otherOrg, "outsider", "owner"}} {
		exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, now(), now(), now())`, uuid.New(), m.org, ids[m.user], m.role)
	}

	const jwtKey = "impersonation-users-test-signing-key-32-bytes!!"
	const issuer, audience = "dev-health-ops", "dev-health-api"
	verifier, err := edgetoken.New(jwtKey, issuer, audience)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(jwtKey, issuer, audience)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range admin.Routes(admin.Deps{Pool: pool, Guard: policy.NewGuard(auth, logger), Logger: logger}) {
		if route.Pattern == "/api/v1/admin/users" && route.Method == http.MethodGet {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	scope := policy.NewScope(auth, logger)
	server := httptest.NewServer(scope.OrgScope(scope.Impersonation(mux)))
	t.Cleanup(server.Close)

	root, err := signer.Access(edgetoken.AccessClaims{UserID: ids["root"].String(), Email: "imp-root@example.com",
		OrgID: adminOrg.String(), Role: "owner", IsSuperuser: true}, time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	list := func(xOrg string) []string {
		t.Helper()
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/admin/users", nil)
		request.Header.Set("Authorization", "Bearer "+root)
		if xOrg != "" {
			request.Header.Set("X-Org-Id", xOrg)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		if response.StatusCode != 200 {
			t.Fatalf("status %d %s", response.StatusCode, body)
		}
		var rows []struct {
			Email string `json:"email"`
		}
		if err := json.Unmarshal(body, &rows); err != nil {
			t.Fatalf("%v: %s", err, body)
		}
		out := make([]string, 0, len(rows))
		for _, row := range rows {
			if !strings.HasPrefix(row.Email, "imp-") { // a user the migrations seed
				continue
			}
			out = append(out, strings.TrimSuffix(strings.TrimPrefix(row.Email, "imp-"), "@example.com"))
		}
		sort.Strings(out)
		return out
	}
	same := func(got []string, want ...string) bool {
		sort.Strings(want)
		return strings.Join(got, ",") == strings.Join(want, ",")
	}

	// No session: unchanged behaviour.
	if got := list(""); !same(got, "root", "target", "adminuser", "outsider") {
		t.Errorf("no session, no header: %v, want the global list", got)
	}
	if got := list(otherOrg.String()); !same(got, "outsider") {
		t.Errorf("no session, X-Org-Id other org: %v, want that org", got)
	}

	exec(`INSERT INTO impersonation_sessions (id, admin_user_id, target_user_id, target_org_id, target_role, created_at, expires_at)
VALUES ($1, $2, $3, $4, 'owner', now(), now() + interval '1 hour')`, uuid.New(), ids["root"], ids["target"], targetOrg)
	for _, header := range []string{"", adminOrg.String(), otherOrg.String(), targetOrg.String()} {
		if got := list(header); !same(got, "target") {
			t.Errorf("impersonating, X-Org-Id %q: %v, want only the target org's user", header, got)
		}
	}

	// The session ended: the next request is the superuser's own again.
	exec(`UPDATE impersonation_sessions SET ended_at = now() WHERE admin_user_id = $1`, ids["root"])
	if got := list(""); !same(got, "root", "target", "adminuser", "outsider") {
		t.Errorf("ended session: %v, want the global list again", got)
	}
}
