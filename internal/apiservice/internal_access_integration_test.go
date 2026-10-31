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

	"github.com/full-chaos/dev-health-ops/internal/apiservice/acr"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
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
	return conn
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

	read := func(t *testing.T, conn driver.Conn) *httptest.ResponseRecorder {
		t.Helper()
		server, err := NewInternalServer(config.Config{APIInternalAddress: "127.0.0.1:0"}, quietLog(),
			InternalRoutes(Deps{Pool: pool, ClickHouse: conn}, quietLog()))
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodGet, "/api/v1/internal/acr/orgs/"+org.String()+"/principals/"+user.String()+"/access", nil)
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		return recorder
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
}
