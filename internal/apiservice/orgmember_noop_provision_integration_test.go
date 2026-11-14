//go:build integration

package apiservice

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// An owner's same-role PATCH /api/v1/admin/orgs/{org_id}/members/{user_id} on a membership go-api wrote before the
// switch (a public.memberships row, no auth.memberships row) answers the same 200 and row with
// AUTH_PROXY_ORG_PROVISIONING off (the legacy local path) and on (auth-service's provisioning routes, in-process over
// the same database), and neither path writes anything.
func TestSameRoleMemberPatchOnAPublicOnlyMemberAnswersAsLegacy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("membernoop_runtime", pg)
	if err != nil {
		t.Fatal(err)
	}
	password := "membernoop-" + uuid.NewString()
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		t.Fatalf("authschema.Apply: %v", err)
	}
	for _, entry := range authschema.PublicReadColumns() {
		if _, err := admin.Exec(ctx, fmt.Sprintf(`GRANT SELECT (%s) ON public.%s TO %q`, strings.Join(entry.Columns, ", "), entry.Table, role)); err != nil {
			t.Fatalf("grant: %v", err)
		}
	}
	runtimeConfig := admin.Config().Copy()
	runtimeConfig.ConnConfig.User = role
	runtimeConfig.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, runtimeConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)

	authService, identities := orgDeleteAuthService(t, runtime, false)

	orgID, owner, member := uuid.New(), uuid.New(), uuid.New()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier, is_active, settings, managed_by, created_at, updated_at)
VALUES ($1, $2, 'Member Noop Org', 'community', true, '{}', 'self', now(), now())`, []any{orgID, "membernoop-" + orgID.String()[:8]}},
		{`INSERT INTO users (id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, 0, now(), now())`, []any{owner, "membernoop-owner-" + owner.String()[:8] + "@example.test"}},
		{`INSERT INTO users (id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, 0, now(), now())`, []any{member, "membernoop-member-" + member.String()[:8] + "@example.test"}},
		{`INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, 'owner', now(), now())`, []any{owner, orgID}},
		{`INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, 'member', now(), now())`, []any{member, orgID}},
	} {
		if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	for _, user := range []uuid.UUID{owner, member} {
		if err := identities.Project(ctx, user); err != nil {
			t.Fatalf("project principal: %v", err)
		}
	}
	if err := identities.ProjectOrganization(ctx, orgID); err != nil {
		t.Fatalf("project organization: %v", err)
	}
	joined := time.Now().UTC().Truncate(time.Second)
	if _, err := identities.BackfillMembership(ctx, identity.MembershipBackfillTarget{OrgID: orgID, UserID: owner, Role: "owner", JoinedAt: &joined}, false); err != nil {
		t.Fatalf("backfill membership: %v", err)
	}

	// written is the member's auth rows, the organization's audit rows, every outbox event and go-api's row.
	written := func() string {
		t.Helper()
		var authRows, auditRows, outboxRows int
		var publicRow string
		if err := admin.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2),
  (SELECT count(*) FROM auth.security_audit_events WHERE organization_id = $3),
  (SELECT count(*) FROM auth.auth_outbox_events),
  (SELECT role || '/' || COALESCE(updated_at::text, '-') FROM memberships WHERE org_id = $1 AND user_id = $2)`,
			orgID, member, orgID.String()).Scan(&authRows, &auditRows, &outboxRows, &publicRow); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("auth=%d audit=%d outbox=%d public=%s", authRows, auditRows, outboxRows, publicRow)
	}

	t.Setenv(EnvAuthProxyURL, authService.URL)
	t.Setenv(EnvAuthWorkloadSubjectFile, subjectFile(t))
	t.Setenv(EnvAuthWorkloadIssuer, testWorkloadIssuer)
	issuer := edtokens.New(t)
	serve := func(provisioning string) http.Handler {
		t.Helper()
		t.Setenv(EnvAuthProxyOrgProvisioning, provisioning)
		var deps Deps
		components, err := configureWith(ctx, apiTestConfig(), health.NewRegistry(time.Second), quietLogger(), func(built *Deps) { deps = *built })
		for index := len(components) - 1; index >= 0; index-- {
			_ = components[index].Shutdown(context.Background())
		}
		if err != nil {
			t.Fatalf("configureWith (provisioning %q): %v", provisioning, err)
		}
		auth := issuer.Authenticator(t, policy.PGStore{Pool: admin, AuthSchema: "auth"}, quietLogger())
		deps.Pool = admin
		deps.Auth, deps.Guard = auth, policy.NewGuard(auth, quietLogger())
		server, err := newServer(config.Config{APIAddress: "127.0.0.1:0"}, quietLogger(), Routes(deps, quietLogger()), nil,
			deps.AuthProxy.ScopeExemptRoutes(), policy.NewScope(auth, quietLogger()).Layers()...)
		if err != nil {
			t.Fatal(err)
		}
		return server.Handler()
	}
	patch := func(handler http.Handler) (int, string) {
		t.Helper()
		request := httptest.NewRequest(http.MethodPatch, "/api/v1/admin/orgs/"+orgID.String()+"/members/"+member.String(),
			strings.NewReader(`{"role":"member"}`))
		request.RemoteAddr = "192.0.2.10:4711"
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+issuer.MustMint(t, edtokens.Claims{UserID: owner, OrgID: orgID.String()}))
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Code, recorder.Body.String()
	}

	before := written()
	legacyCode, legacyBody := patch(serve(""))
	if legacyCode != http.StatusOK {
		t.Fatalf("switch off: %d %s, want 200", legacyCode, legacyBody)
	}
	if after := written(); after != before {
		t.Fatalf("switch off wrote: before %s, after %s", before, after)
	}
	code, body := patch(serve("true"))
	if code != legacyCode || body != legacyBody {
		t.Fatalf("switch on: %d %s; want the legacy %d %s", code, body, legacyCode, legacyBody)
	}
	if after := written(); after != before {
		t.Fatalf("switch on wrote: before %s, after %s", before, after)
	}
}
