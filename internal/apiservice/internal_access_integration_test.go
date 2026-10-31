//go:build integration

package apiservice

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/acr"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// accessLogin creates a ClickHouse login granted exactly posture and opens a
// connection as it. The login name carries a per-run suffix and is dropped at
// cleanup: a remote test server (DEV_HEALTH_TEST_CLICKHOUSE_DSN) is shared.
func accessLogin(t *testing.T, ctx context.Context, admin driver.Conn, dsn, name string, posture chclickhouse.Posture) driver.Conn {
	t.Helper()
	conn, _ := accessLoginDSN(t, ctx, admin, dsn, name, posture)
	return conn
}

// accessLoginDSN is accessLogin, also returning the login's DSN.
func accessLoginDSN(t *testing.T, ctx context.Context, admin driver.Conn, dsn, name string, posture chclickhouse.Posture) (driver.Conn, string) {
	t.Helper()
	const password = "access-mount-test-words"
	login := name + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if err := admin.Exec(ctx, "CREATE USER "+login+" IDENTIFIED WITH plaintext_password BY '"+password+"'"); err != nil {
		t.Fatalf("create login %s: %v", login, err)
	}
	t.Cleanup(func() { _ = admin.Exec(context.Background(), "DROP USER IF EXISTS "+login) })
	for _, statement := range chclickhouse.GrantStatements(login, posture) {
		if err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(login, password)
	conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(parsed.String()))
	if err != nil {
		t.Fatalf("open login %s: %v", login, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, parsed.String()
}

// TestInternalRoutesServeThePrincipalAccessReadOnTheAPIPosture drives the
// mounted read the way go-api builds it (InternalRoutes over the pool and the
// ClickHouse connection) through the internal listener's server wrapper, with
// a svc_acr_ credential that holds principal_access:read and a ClickHouse
// login granted exactly chclickhouse.APIPosture. The same read on a login
// granted the posture without its repos row answers 503: the repos grant is
// what the inventory read needs, and a missing grant never answers a partial
// or empty scope.
func TestInternalRoutesServeThePrincipalAccessReadOnTheAPIPosture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	chAdmin, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(ch.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chAdmin.Close() })

	org, user := uuid.New(), uuid.New()
	pgseed.Org(ctx, t, pool, org.String(), "community")
	if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'access-mount@example.com', 'not-a-hash', true, true, false, 0, now(), now())`, user); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), user, org); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"ask_dev", "agent_context_runtime"} {
		var featureID string
		if err := pool.QueryRow(ctx, `SELECT id::text FROM feature_flags WHERE key = $1`, key).Scan(&featureID); err != nil {
			t.Fatalf("feature %s is not registered by the migrations: %v", key, err)
		}
		pgseed.OrgOverride(ctx, t, pool, org.String(), featureID, true)
	}
	const token = "svc_acr_access-mount-0123456789abcdef"
	if _, err := pool.Exec(ctx, `INSERT INTO internal_service_credentials
		(id, service_name, token_hash, token_prefix, scopes, created_at)
		VALUES (gen_random_uuid(), 'acr', $1, $2, '["principal_access:read"]'::json, now())`,
		admin.HashServiceToken(token), token[:16]); err != nil {
		t.Fatal(err)
	}
	for _, repo := range []string{"Acme/Web", "acme/api"} {
		if err := chAdmin.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, ?)`,
			uuid.New(), repo, org.String(), "github", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}

	var database string
	if err := chAdmin.QueryRow(ctx, "SELECT currentDatabase()").Scan(&database); err != nil {
		t.Fatal(err)
	}
	posture := chclickhouse.APIPosture(database)
	granted := accessLogin(t, ctx, chAdmin, ch.URI, "access_mount_api", posture)
	if err := chclickhouse.CheckAPIClickHouseAuthorization(ctx, granted); err != nil {
		t.Fatalf("the APIPosture login fails go-api's readiness check: %v", err)
	}
	var withoutRepos chclickhouse.Posture
	for _, table := range posture.RequiredTables {
		if table.Table != "repos" {
			withoutRepos.RequiredTables = append(withoutRepos.RequiredTables, table)
		}
	}
	if len(withoutRepos.RequiredTables) != len(posture.RequiredTables)-1 {
		t.Fatalf("APIPosture has no repos row: %+v", posture.RequiredTables)
	}
	ungranted := accessLogin(t, ctx, chAdmin, ch.URI, "access_mount_norepos", withoutRepos)

	readWith := func(t *testing.T, deps Deps, query string) *httptest.ResponseRecorder {
		t.Helper()
		server, err := NewInternalServer(config.Config{APIInternalAddress: "127.0.0.1:0"}, quietLog(),
			InternalRoutes(deps, quietLog()))
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodGet, "/api/v1/internal/acr/orgs/"+org.String()+"/principals/"+user.String()+"/access"+query, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		return recorder
	}
	read := func(t *testing.T, conn driver.Conn) *httptest.ResponseRecorder {
		t.Helper()
		return readWith(t, Deps{Pool: pool, ClickHouse: conn}, "")
	}

	t.Run("posture login", func(t *testing.T) {
		rec := read(t, granted)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d %s, want 200", rec.Code, rec.Body.String())
		}
		var body struct {
			SchemaVersion string `json:"schema_version"`
			Member        bool   `json:"member"`
			Entitlements  struct {
				AskDev              bool `json:"ask_dev"`
				AgentContextRuntime bool `json:"agent_context_runtime"`
			} `json:"entitlements"`
			RepositoryScope struct {
				Kind  string   `json:"kind"`
				Repos []string `json:"repos"`
			} `json:"repository_scope"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.SchemaVersion != "acr_principal_access.v1" || !body.Member || !body.Entitlements.AskDev || !body.Entitlements.AgentContextRuntime ||
			body.RepositoryScope.Kind != acr.RepositoryScopeRepos || !reflect.DeepEqual(body.RepositoryScope.Repos, []string{"acme/api", "acme/web"}) {
			t.Fatalf("answer %s", rec.Body.String())
		}
	})
	t.Run("posture without repos", func(t *testing.T) {
		if rec := read(t, ungranted); rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status %d %s, want 503", rec.Code, rec.Body.String())
		}
	})
	// InternalRoutes hands the access read go-api's session issue client
	// (Deps.SessionIssuer) as its session state reader (D6559): a sid read is
	// decided on that client's answer. Without the client, or with an issuer
	// that cannot read state, a sid read is a 503 and a row-only read is not.
	sid := uuid.New()
	pin := int64(0)
	t.Run("a sid read is decided on the session issue client's state answer", func(t *testing.T) {
		for name, tc := range map[string]struct {
			state sessionissue.State
			live  bool
		}{
			"bound to the org": {sessionissue.State{PrincipalID: user, OrganizationID: &org, LegacyTokenVersion: &pin}, true},
			"bound to no org":  {sessionissue.State{PrincipalID: user, LegacyTokenVersion: &pin}, false},
			"expired":          {sessionissue.State{PrincipalID: user, OrganizationID: &org, Expired: true, LegacyTokenVersion: &pin}, false},
		} {
			states := &stateIssuer{states: map[uuid.UUID]sessionissue.State{sid: tc.state}}
			rec := readWith(t, Deps{Pool: pool, ClickHouse: granted, SessionIssuer: states}, "?sid="+sessionid.Wire(sid))
			var body struct {
				Session *struct {
					Live bool `json:"live"`
				} `json:"session"`
			}
			if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &body) != nil || body.Session == nil || body.Session.Live != tc.live {
				t.Fatalf("%s: %d %s, want 200 with session.live %v", name, rec.Code, rec.Body.String(), tc.live)
			}
			if states.calls != 1 {
				t.Fatalf("%s: %d state reads, want 1", name, states.calls)
			}
		}
	})
	t.Run("without a session state reader a sid read is unavailable", func(t *testing.T) {
		for name, issuer := range map[string]sessionissue.Issuer{"no issuer": nil, "an issuer that reads no state": issueOnly{}} {
			deps := Deps{Pool: pool, ClickHouse: granted, SessionIssuer: issuer}
			if rec := readWith(t, deps, "?sid="+sessionid.Wire(sid)); rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("%s: a sid read answered %d %s, want 503", name, rec.Code, rec.Body.String())
			}
			if rec := readWith(t, deps, ""); rec.Code != http.StatusOK {
				t.Fatalf("%s: a row-only read answered %d %s, want 200", name, rec.Code, rec.Body.String())
			}
		}
	})
}

// stateIssuer is go-api's session issue client as the access read sees it:
// an Issuer that also reads session state.
type stateIssuer struct {
	issueOnly
	states map[uuid.UUID]sessionissue.State
	calls  int
}

func (s *stateIssuer) State(_ context.Context, id uuid.UUID) (sessionissue.State, error) {
	s.calls++
	state, ok := s.states[id]
	if !ok {
		return sessionissue.State{}, sessionissue.ErrSessionNotFound
	}
	return state, nil
}

// issueOnly is an Issuer that reads no session state.
type issueOnly struct{}

func (issueOnly) Issue(context.Context, *http.Request, sessionissue.Request) (sessionissue.Pair, error) {
	return sessionissue.Pair{}, sessionissue.ErrUnavailable
}

// TestInternalRoutesServeProjectScopeOnlyWithTheFlagAndItsGrant drives the
// mounted read for an org with projects and no repositories (CHAOS-12224):
//   - readiness is exact both ways: a login granted APIPostureFor(db, true)
//     passes only with the project scope on, the APIPosture login only with it
//     off (a grant that rolled before the flag is refused);
//   - with the flag and the grant, the answer carries project_scope with the
//     org's (provider, id) pairs;
//   - with the flag off, the answer is byte-identical to the read before
//     project_scope existed, and no project read happens on a login that may
//     not read projects;
//   - with the flag on and no grant, the read fails closed (503).
func TestInternalRoutesServeProjectScopeOnlyWithTheFlagAndItsGrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	chAdmin, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(ch.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chAdmin.Close() })

	org, user := uuid.New(), uuid.New()
	pgseed.Org(ctx, t, pool, org.String(), "community")
	if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'not-a-hash', true, true, false, 0, now(), now())`, user, "access-projects-"+user.String()+"@example.com"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), user, org); err != nil {
		t.Fatal(err)
	}
	const token = "svc_acr_access-projects-0123456789"
	if _, err := pool.Exec(ctx, `INSERT INTO internal_service_credentials
		(id, service_name, token_hash, token_prefix, scopes, created_at)
		VALUES (gen_random_uuid(), 'acr', $1, $2, '["principal_access:read"]'::json, now())`,
		admin.HashServiceToken(token), token[:16]); err != nil {
		t.Fatal(err)
	}
	for _, project := range [][2]string{{"linear", "P-1"}, {"jira", "10001"}} {
		if err := chAdmin.Exec(ctx, `INSERT INTO projects (id, org_id, provider, name, updated_at) VALUES (?, ?, ?, ?, ?)`,
			project[1], org.String(), project[0], "Project "+project[1], time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	var database string
	if err := chAdmin.QueryRow(ctx, "SELECT currentDatabase()").Scan(&database); err != nil {
		t.Fatal(err)
	}
	withProjects, withProjectsDSN := accessLoginDSN(t, ctx, chAdmin, ch.URI, "access_projscope_on", chclickhouse.APIPostureFor(database, true))
	plain, plainDSN := accessLoginDSN(t, ctx, chAdmin, ch.URI, "access_projscope_off", chclickhouse.APIPosture(database))

	// buildDeps hands the flag to the api_clickhouse readiness check it
	// registers: the check go-api's /ready runs is exact for the flag in force.
	t.Run("buildDeps registers the readiness check with the flag", func(t *testing.T) {
		for name, tc := range map[string]struct {
			dsn    string
			scope  bool
			failed bool
		}{
			"projects login, scope on":  {withProjectsDSN, true, false},
			"projects login, scope off": {withProjectsDSN, false, true},
			"plain login, scope on":     {plainDSN, true, true},
			"plain login, scope off":    {plainDSN, false, false},
		} {
			registry := health.NewRegistry(5 * time.Second)
			_, components, err := buildDeps(ctx, config.Config{APIAddress: "127.0.0.1:0", APIClickHouseURI: secrets.NewValue(tc.dsn)},
				registry, quietLog(), AuthProxyConfig{}, tc.scope)
			if err != nil {
				t.Fatalf("%s: buildDeps: %v", name, err)
			}
			readiness := registry.CheckRequired(ctx)
			closeComponents(components)
			found := false
			for _, check := range readiness.Checks {
				if check.Name == apiClickHouseCheck {
					found = true
					if check.Failed != tc.failed {
						t.Errorf("%s: %s failed=%v, want %v", name, apiClickHouseCheck, check.Failed, tc.failed)
					}
				}
			}
			if !found {
				t.Fatalf("%s: no %s check registered: %+v", name, apiClickHouseCheck, readiness)
			}
		}
	})

	t.Run("readiness is exact both ways", func(t *testing.T) {
		for name, tc := range map[string]struct {
			conn  driver.Conn
			scope bool
			ok    bool
		}{
			"projects login, scope on":  {withProjects, true, true},
			"projects login, scope off": {withProjects, false, false},
			"plain login, scope on":     {plain, true, false},
			"plain login, scope off":    {plain, false, true},
		} {
			err := chclickhouse.CheckAPIClickHouseAuthorizationFor(ctx, tc.conn, tc.scope)
			if (err == nil) != tc.ok {
				t.Errorf("%s: readiness err = %v, want ok=%v", name, err, tc.ok)
			}
		}
	})
	read := func(t *testing.T, deps Deps) *httptest.ResponseRecorder {
		t.Helper()
		server, err := NewInternalServer(config.Config{APIInternalAddress: "127.0.0.1:0"}, quietLog(), InternalRoutes(deps, quietLog()))
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodGet, "/api/v1/internal/acr/orgs/"+org.String()+"/principals/"+user.String()+"/access", nil)
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		return recorder
	}
	prefix := `{"schema_version":"acr_principal_access.v1","org_id":"` + org.String() + `","user_id":"` + user.String() +
		`","member":true,"impersonating":false,"entitlements":{"ask_dev":false,"agent_context_runtime":false},"repository_scope":{"kind":"none"}`
	t.Run("flag and grant: project_scope", func(t *testing.T) {
		rec := read(t, Deps{Pool: pool, ClickHouse: withProjects, ACRAccessProjectScope: true})
		want := prefix + `,"project_scope":{"kind":"projects","projects":[{"provider":"jira","id":"10001"},{"provider":"linear","id":"P-1"}]}}`
		if rec.Code != http.StatusOK || rec.Body.String() != want {
			t.Fatalf("status %d body\n%s\nwant\n%s", rec.Code, rec.Body.String(), want)
		}
	})
	t.Run("flag off: byte-identical, no project read", func(t *testing.T) {
		rec := read(t, Deps{Pool: pool, ClickHouse: plain})
		if rec.Code != http.StatusOK || rec.Body.String() != prefix+`}` {
			t.Fatalf("status %d body\n%s\nwant\n%s}", rec.Code, rec.Body.String(), prefix)
		}
	})
	t.Run("flag on, no grant: fails closed", func(t *testing.T) {
		if rec := read(t, Deps{Pool: pool, ClickHouse: plain, ACRAccessProjectScope: true}); rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status %d body %s, want 503", rec.Code, rec.Body.String())
		}
	})
}
