//go:build integration

package apiservice

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

const (
	usersOracleSuperPassword = "Oracle-super-Passw0rd!"
	usersOracleNewPassword   = "Oracle-new-Passw0rd!x"
)

// usersOracleStack is one Postgres with both schemas and go-api's handler over it, built by configureWith: users owned
// (the prod path: AUTH_USERS_OWNED and AUTH_PLATFORM_ROLES_OWNED on, auth-service's user-admin routes in-process) or
// not (the legacy local writes). Both stacks hold the same seed.
type usersOracleStack struct {
	handler                                    http.Handler
	issuer                                     *edtokens.Issuer
	super, orgAdmin, inactive, orgA, suspended uuid.UUID
}

func (s usersOracleStack) token(t *testing.T, user uuid.UUID, org uuid.UUID) string {
	t.Helper()
	claims := edtokens.Claims{UserID: user}
	if org != uuid.Nil {
		claims.OrgID = org.String()
	}
	return s.issuer.MustMint(t, claims)
}

// usersOracleAuthService is auth-service's user-admin routes as authruntime builds them (usersAdminRoutes), over the
// auth schema as the runtime role with users and platform roles owned, plus stubWorkloadExchange on the exchange path.
func usersOracleAuthService(t *testing.T, runtime *pgxpool.Pool) *httptest.Server {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	identities, err := identity.New(identity.Config{Pool: runtime, Schema: schema, Logger: quietLogger(), UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	roles, err := platformrole.New(platformrole.Config{Pool: runtime, Schema: schema, Logger: quietLogger()})
	if err != nil {
		t.Fatal(err)
	}
	signer := token.NewFileSigner(token.FileConfig{KeyPath: orgDeleteSigningKey(t), KeyID: "k1"})
	subject := session.WirePrincipalID(uuid.New())
	workload, err := authapi.NewSessionIssueWorkload(authapi.SessionIssueWorkloadConfig{IssuerURL: testWorkloadIssuer, Keys: signer, Subject: subject})
	if err != nil {
		t.Fatal(err)
	}
	routes, err := authapi.UsersAdminRoutes(authapi.ProvisioningDeps{
		Users:  authapi.PGUsers{Pool: runtime, AuthSchema: schema, PlatformRolesOwned: true, UsersOwned: true},
		Reader: runtime, Organizations: identities, Workload: workload, Logger: quietLogger(),
		UsersAdminStores: authapi.UsersAdminStores{UsersAdmin: identities, PlatformRoles: authapi.PlatformRoleStore{Store: roles}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != authapi.UsersAdminRouteCount() {
		t.Fatalf("auth-service mounted %d of %d user-admin routes: %v", len(routes), authapi.UsersAdminRouteCount(),
			authapi.UsersAdminRowsError(nil))
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	mux.Handle("POST "+workloadtoken.ExchangePath, &stubWorkloadExchange{signer: signer, subject: subject})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func startUsersOracleStack(t *testing.T, ctx context.Context, owned bool) usersOracleStack {
	t.Helper()
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
	role, err := containers.RoleName("usersoracle_runtime", pg)
	if err != nil {
		t.Fatal(err)
	}
	password := "usersoracle-" + uuid.NewString()
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: owned,
		PlatformRolesOwned: owned}); err != nil {
		t.Fatalf("authschema.Apply (owned %v): %v", owned, err)
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

	s := usersOracleStack{issuer: edtokens.New(t)}
	id := func(name string) uuid.UUID { return uuid.NewSHA1(uuid.NameSpaceURL, []byte("usersoracle/"+name)) }
	s.super, s.orgAdmin, s.inactive, s.orgA, s.suspended = id("super"), id("org-admin"), id("inactive"), id("org-a"), id("org-suspended")
	superHash, err := passwordhash.Hash(usersOracleSuperPassword)
	if err != nil {
		t.Fatal(err)
	}
	type seedUser struct {
		id                uuid.UUID
		email             string
		hash              *string
		active, superuser bool
	}
	users := []seedUser{
		{s.super, "oracle-super@example.test", &superHash, true, true},
		{s.orgAdmin, "oracle-org-admin@example.test", nil, true, false},
		{s.inactive, "oracle-inactive@example.test", nil, false, false},
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, query)
		}
	}
	for _, o := range []struct {
		id     uuid.UUID
		slug   string
		active bool
	}{{s.orgA, "usersoracle-a", true}, {s.suspended, "usersoracle-suspended", false}} {
		exec(`INSERT INTO organizations (id, slug, name, tier, is_active, settings, managed_by, created_at, updated_at)
VALUES ($1, $2, $2, 'community', $3, '{}', 'self', now(), now())`, o.id, o.slug, o.active)
	}
	for _, u := range users {
		exec(`INSERT INTO users (id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, 'local', $4, true, $5, 0, now(), now())`, u.id, u.email, u.hash, u.active, u.superuser)
	}
	exec(`INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, 'admin', now(), now())`,
		s.orgAdmin, s.orgA)

	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	seeder, err := identity.New(identity.Config{Pool: admin, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range users {
		if _, err := seeder.BackfillUser(ctx, identity.BackfillTarget{ID: u.id, Email: u.email, Active: u.active, Verified: true,
			PasswordHash: u.hash, Superuser: u.superuser}, false); err != nil {
			t.Fatalf("project %s: %v", u.email, err)
		}
	}
	for _, org := range []uuid.UUID{s.orgA, s.suspended} {
		if err := seeder.ProjectOrganization(ctx, org); err != nil {
			t.Fatalf("project organization: %v", err)
		}
	}
	joined := time.Now().UTC().Truncate(time.Second)
	if _, err := seeder.BackfillMembership(ctx, identity.MembershipBackfillTarget{OrgID: s.orgA, UserID: s.orgAdmin, Role: "admin",
		JoinedAt: &joined}, false); err != nil {
		t.Fatalf("backfill membership: %v", err)
	}

	for _, name := range []string{usersowned.Env, rule.OwnedEnv, EnvAuthProxyURL, EnvAuthWorkloadSubjectFile, EnvAuthWorkloadIssuer,
		AuthProxyFlagEnv(AuthSessions), AuthProxyFlagEnv(AuthLocalLogin), AuthProxyFlagEnv(AuthCredentials), EnvAuthProxyOrgProvisioning} {
		t.Setenv(name, "")
	}
	if owned {
		authService := usersOracleAuthService(t, runtime)
		t.Setenv(usersowned.Env, "true")
		t.Setenv(rule.OwnedEnv, "true")
		for _, aggregate := range []AuthAggregate{AuthSessions, AuthLocalLogin, AuthCredentials} {
			t.Setenv(AuthProxyFlagEnv(aggregate), "true")
		}
		t.Setenv(EnvAuthProxyURL, authService.URL)
		t.Setenv(EnvAuthWorkloadSubjectFile, subjectFile(t))
		t.Setenv(EnvAuthWorkloadIssuer, testWorkloadIssuer)
	}
	var deps Deps
	components, err := configureWith(ctx, apiTestConfig(), health.NewRegistry(time.Second), quietLogger(), func(built *Deps) { deps = *built })
	for index := len(components) - 1; index >= 0; index-- {
		_ = components[index].Shutdown(context.Background())
	}
	if err != nil {
		t.Fatalf("configureWith (owned %v): %v", owned, err)
	}
	if deps.UsersOwned != owned || deps.PlatformRolesOwned != owned || (deps.UsersAdmin != nil) != owned {
		t.Fatalf("go-api built UsersOwned=%v PlatformRolesOwned=%v client=%v; want %v for all", deps.UsersOwned,
			deps.PlatformRolesOwned, deps.UsersAdmin != nil, owned)
	}
	auth := s.issuer.Authenticator(t, policy.PGStore{Pool: admin, AuthSchema: "auth", PlatformRolesOwned: owned}, quietLogger())
	deps.Pool = admin
	deps.Auth, deps.Guard = auth, policy.NewGuard(auth, quietLogger())
	server, err := newServer(config.Config{APIAddress: "127.0.0.1:0"}, quietLogger(), Routes(deps, quietLogger()), nil,
		deps.AuthProxy.ScopeExemptRoutes(), policy.NewScope(auth, quietLogger()).Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	s.handler = server.Handler()
	return s
}

// usersOracleVolatile matches the members whose values differ by run: they are compared by form only.
var usersOracleVolatile = regexp.MustCompile(`"(id|created_at|updated_at|last_login_at|joined_at)": ?"([^"]*)"`)

func usersOracleForm(body string) string {
	return usersOracleVolatile.ReplaceAllStringFunc(body, func(member string) string {
		parts := usersOracleVolatile.FindStringSubmatch(member)
		form := "<text>"
		if _, err := uuid.Parse(parts[2]); err == nil {
			form = "<uuid>"
		} else if _, err := time.Parse("2006-01-02T15:04:05.999999", parts[2]); err == nil {
			form = "<time>"
		} else if _, err := time.Parse(time.RFC3339Nano, parts[2]); err == nil {
			form = "<time>"
		}
		return `"` + parts[1] + `":"` + form + `"`
	})
}

// usersOracleCases is the length of usersOracleRun's case list: the cell compares exactly this many answers.
const usersOracleCases = 18

type usersOracleAnswer struct {
	name   string
	status int
	form   string
	raw    string
}

// usersOracleRun sends the case list to one stack, in order, and returns each answer.
func usersOracleRun(t *testing.T, s usersOracleStack) []usersOracleAnswer {
	t.Helper()
	superToken, adminToken := s.token(t, s.super, uuid.Nil), s.token(t, s.orgAdmin, s.orgA)
	var answers []usersOracleAnswer
	send := func(name, token, xOrg, method, path, body string) usersOracleAnswer {
		t.Helper()
		request := httptest.NewRequest(method, path, strings.NewReader(body))
		request.RemoteAddr = "192.0.2.10:4711"
		request.Header.Set("Authorization", "Bearer "+token)
		if body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		if xOrg != "" {
			request.Header.Set("X-Org-Id", xOrg)
		}
		recorder := httptest.NewRecorder()
		s.handler.ServeHTTP(recorder, request)
		raw := recorder.Body.String()
		answer := usersOracleAnswer{name: name, status: recorder.Code, form: usersOracleForm(raw), raw: raw}
		answers = append(answers, answer)
		return answer
	}
	idOf := func(a usersOracleAnswer) string {
		t.Helper()
		var created struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(a.raw), &created); err != nil || created.ID == "" {
			t.Fatalf("%s: no user id in %d %s", a.name, a.status, a.raw)
		}
		return created.ID
	}
	const users = "/api/v1/admin/users"
	platform := idOf(send("platform create", superToken, "", http.MethodPost, users,
		`{"email":"oracle-platform@example.test","password":"Oracle-user-Passw0rd!","username":"oracleplat","full_name":"Oracle Platform","is_verified":false}`))
	send("service account create", superToken, "", http.MethodPost, users,
		`{"email":"oracle-sa@example.test","auth_provider":"`+edgetokenmint.ServiceAuthProvider+`"}`)
	send("org-scoped create", adminToken, s.orgA.String(), http.MethodPost, users, `{"email":"oracle-org@example.test","role":"member"}`)
	send("create into a suspended org", superToken, s.suspended.String(), http.MethodPost, users,
		`{"email":"oracle-suspended@example.test","role":"viewer"}`)
	send("verify true", superToken, "", http.MethodPatch, users+"/"+platform, `{"is_verified":true}`)
	send("verify false", superToken, "", http.MethodPatch, users+"/"+platform, `{"is_verified":false}`)
	send("profile", superToken, "", http.MethodPatch, users+"/"+platform,
		`{"full_name":"Oracle Renamed","avatar_url":"https://example.test/a.png","username":"oracleplat2"}`)
	form := send("web form", superToken, "", http.MethodPatch, users+"/"+platform,
		`{"email":"oracle-platform@example.test","username":"oracleplat2","full_name":"Oracle Renamed","is_active":true,"is_verified":true,"is_superuser":false}`)
	noop := send("no-op patch", superToken, "", http.MethodPatch, users+"/"+platform, `{}`)
	if before, after := usersOracleUpdatedAt(t, form), usersOracleUpdatedAt(t, noop); !after.After(before) {
		t.Errorf("no-op PATCH updated_at %v, want after the web form's %v (the bump, D6915)", after, before)
	}
	send("duplicate email", superToken, "", http.MethodPost, users, `{"email":"oracle-platform@example.test"}`)
	send("duplicate username", superToken, "", http.MethodPost, users, `{"email":"oracle-other@example.test","username":"oracleplat2"}`)
	send("superuser required", adminToken, s.orgA.String(), http.MethodPost, users,
		`{"email":"oracle-promote@example.test","role":"member","is_superuser":true}`)
	send("set password", superToken, "", http.MethodPost, users+"/"+platform+"/password",
		`{"admin_password":"`+usersOracleSuperPassword+`","password":"`+usersOracleNewPassword+`"}`)
	send("superuser grant", superToken, "", http.MethodPatch, users+"/"+platform, `{"is_superuser":true}`)
	send("superuser revoke", superToken, "", http.MethodPatch, users+"/"+platform, `{"is_superuser":false}`)
	send("superuser on an inactive user", superToken, "", http.MethodPatch, users+"/"+s.inactive.String(), `{"is_superuser":true}`)
	send("soft delete", superToken, "", http.MethodDelete, users+"/"+platform, "")
	send("repeat delete", superToken, "", http.MethodDelete, users+"/"+platform, "")
	return answers
}

func usersOracleUpdatedAt(t *testing.T, a usersOracleAnswer) time.Time {
	t.Helper()
	var body struct {
		UpdatedAt string `json:"updated_at"`
	}
	if err := json.Unmarshal([]byte(a.raw), &body); err != nil {
		t.Fatalf("%s: %d %s", a.name, a.status, a.raw)
	}
	for _, layout := range []string{"2006-01-02T15:04:05.999999", time.RFC3339Nano} {
		if at, err := time.Parse(layout, body.UpdatedAt); err == nil {
			return at
		}
	}
	t.Fatalf("%s: updated_at %q is not a time", a.name, body.UpdatedAt)
	return time.Time{}
}

// While users are owned, every /api/v1/admin/users write answers byte-equal to the not-owned (legacy) go-api for the
// same request on the same seed, ids and timestamps compared by form only. The named parity answers are pinned: a
// create into a suspended org is 201 and is_superuser true on an inactive user is 200 (pm D6914), the repeat delete is
// 404, and no owned write answers 409 operation_moved or a 5xx.
func TestUsersOwnedForwardAnswerEqualsNotOwnedAnswer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	var legacy, owned []usersOracleAnswer
	t.Run("not owned", func(t *testing.T) { legacy = usersOracleRun(t, startUsersOracleStack(t, ctx, false)) })
	t.Run("owned", func(t *testing.T) { owned = usersOracleRun(t, startUsersOracleStack(t, ctx, true)) })
	if len(owned) != usersOracleCases || len(legacy) != usersOracleCases {
		t.Fatalf("owned ran %d cases, not owned %d; want %d each", len(owned), len(legacy), usersOracleCases)
	}
	pinned := map[string]int{"create into a suspended org": http.StatusCreated, "superuser on an inactive user": http.StatusOK,
		"repeat delete": http.StatusNotFound}
	compared := 0
	for i, o := range owned {
		l := legacy[i]
		t.Run(o.name, func(t *testing.T) {
			compared++
			if o.name != l.name {
				t.Fatalf("case %d is %q owned, %q not owned", i, o.name, l.name)
			}
			if o.status != l.status || o.form != l.form {
				t.Errorf("owned %d %s, not owned %d %s", o.status, o.form, l.status, l.form)
			}
			if want, ok := pinned[o.name]; ok && o.status != want {
				t.Errorf("owned %d, want %d", o.status, want)
			}
			if o.status >= 500 || strings.Contains(o.raw, "operation_moved") {
				t.Errorf("owned answered %d %s", o.status, o.raw)
			}
		})
	}
	if compared != usersOracleCases {
		t.Fatalf("compared %d cases, want %d", compared, usersOracleCases)
	}
}
