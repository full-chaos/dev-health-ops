//go:build integration

package session_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/session"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

var metricsDDL = []string{
	"CREATE TABLE repo_metrics_daily (org_id String, computed_at DateTime('UTC')) ENGINE = MergeTree ORDER BY org_id",
	"CREATE TABLE user_metrics_daily (org_id String, computed_at DateTime('UTC')) ENGINE = MergeTree ORDER BY org_id",
	"CREATE TABLE team_metrics_daily (org_id String, computed_at DateTime('UTC')) ENGINE = MergeTree ORDER BY org_id",
	"CREATE TABLE work_item_metrics_daily (org_id String, computed_at DateTime('UTC')) ENGINE = MergeTree ORDER BY org_id",
}

// TestSessionRoutesMatchGolden replays the session scenario against the Go
// routes alone and compares every answer and row set with the Python
// answers the venue oracle recorded.
func TestSessionRoutesMatchGolden(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	pool, pg, server, signer, specs := st.pool, st.pg, st.server, st.signer, st.specs

	tokens := map[string]string{}
	for name, spec := range specs {
		claims := edgetoken.AccessClaims{Role: "member"}
		claims.UserID, _ = spec["user_id"].(string)
		claims.Email, _ = spec["email"].(string)
		claims.OrgID, _ = spec["org_id"].(string)
		if role, ok := spec["role"].(string); ok {
			claims.Role = role
		}
		claims.IsSuperuser, _ = spec["is_superuser"].(bool)
		if value, ok := spec["username"].(string); ok {
			claims.Username = &value
		}
		if value, ok := spec["full_name"].(string); ok {
			claims.FullName = &value
		}
		if version, ok := spec["token_version"].(int); ok {
			claims.TokenVersion = int64(version)
		}
		token, err := signer.Access(claims, time.Now(), fmt.Sprintf("00000000-0000-4000-8000-%012d", len(tokens)))
		if err != nil {
			t.Fatal(err)
		}
		tokens[name] = token
	}

	receipt := sessionscenario.Run(&sessionscenario.Harness{
		T: t, GoBase: server.URL, Tokens: tokens,
		Exec: func(sql string) {
			if _, err := pool.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		},
		Reset: st.openWindows,
		Rows:  func(query string) (string, string) { return "", sessionscenario.TableRows(t, ctx, pg.URI, query) },
	})
	t.Log("\n" + receipt)
}

// stack is the Go route set on Postgres, ClickHouse and Valkey
// containers, seeded with the scenario.
type stack struct {
	pool *pgxpool.Pool
	pg   *containers.Instance
	// openWindows starts a fresh window on every rate limit.
	openWindows func()
	server      *httptest.Server
	signer      *edgetoken.Signer
	specs       map[string]map[string]any
}

func startStack(t *testing.T, ctx context.Context) stack {
	t.Helper()
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })

	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	// The migrated Postgres schema, not a hand-written slice: the slice had
	// impersonation_sessions.started_at, which the real table does not have
	// (CHAOS-6769). TestSessionVenueOracle runs the same scenario on the real
	// Alembic chain; this replay is the Python-free check of the Go routes.
	pgschema.Apply(ctx, t, pool)
	hash, err := bcrypt.GenerateFromPassword([]byte(sessionscenario.Password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	specs := sessionscenario.Seed(t, ctx, pool, string(hash))

	chConn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(ch.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chConn.Close() })
	for _, statement := range append(append([]string{}, metricsDDL...), sessionscenario.MetricsStatements...) {
		if err := chConn.Exec(ctx, statement); err != nil {
			t.Fatalf("metrics: %v", err)
		}
	}
	// The limits' clock: Reset moves it past every window.
	var limiterOffset atomic.Int64
	limiterNow := func() time.Time { return time.Now().Add(time.Duration(limiterOffset.Load())) }

	provider := &sessionscenario.FakeProvider{}
	fake := httptest.NewServer(provider)
	t.Cleanup(fake.Close)
	for _, entry := range sessionscenario.SocialEnv {
		key, value, _ := strings.Cut(entry, "=")
		t.Setenv(key, value)
	}
	t.Setenv("SOCIAL_GOOGLE_CLIENT_SECRET", "")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	verifier, err := edgetoken.New(sessionscenario.Key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(sessionscenario.Key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	oauth := oauthprovider.NewClient()
	oauth.Endpoints = sessionscenario.ProviderEndpoints(fake.URL)
	routes := session.Routes(session.Deps{Pool: pool, Guard: policy.NewGuard(auth, logger), Auth: auth, Verifier: verifier,
		Signer: signer, ClickHouse: chConn, Limits: httpapi.NewMemoryCounters(limiterNow), Write: apiservice.WriteError, OAuth: oauth, Logger: logger})
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	scope := policy.NewScope(auth, logger)
	server := httptest.NewServer(scope.OrgScope(scope.Impersonation(mux)))
	t.Cleanup(server.Close)
	return stack{pool: pool, pg: pg, server: server, signer: signer, specs: specs,
		openWindows: func() { limiterOffset.Add(int64(16 * time.Minute)) }}
}

// Concurrent refreshes of one token serialize on the token row (FOR
// UPDATE, as find_by_hash_for_update): one request rotates it, every other
// one waits, then finds it just rotated and answers the same successor.
// Without the row lock several requests rotate it and hand out different
// successors.
func TestConcurrentRefreshesShareOneSuccessor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	post := func(path, body string) (int, string) {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, st.server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Error(err)
			return 0, ""
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Forwarded-For", "203.0.113.9")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Error(err)
			return 0, ""
		}
		defer response.Body.Close()
		raw, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(raw)
	}
	status, body := post("/api/v1/auth/login", `{"email": "alice@example.com", "password": "`+sessionscenario.Password+`"}`)
	if status != http.StatusOK {
		t.Fatalf("login: %d %s", status, body)
	}
	refresh := sessionscenario.Field(body, "refresh_token")

	const racers = 8
	jtis := make(chan string, racers)
	start := make(chan struct{})
	done := make(chan struct{}, racers)
	for range racers {
		go func() {
			defer func() { done <- struct{}{} }()
			<-start
			status, body := post("/api/v1/auth/refresh", `{"refresh_token": "`+refresh+`"}`)
			if status != http.StatusOK {
				t.Errorf("refresh: %d %s", status, body)
				return
			}
			jtis <- sessionscenario.JTIOf(sessionscenario.Field(body, "refresh_token"))
		}()
	}
	close(start)
	for range racers {
		<-done
	}
	close(jtis)
	seen := map[string]int{}
	for jti := range jtis {
		seen[jti]++
	}
	if len(seen) != 1 {
		t.Fatalf("racing refreshes handed out %d different successors: %v", len(seen), seen)
	}
	var rows int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM refresh_tokens rt JOIN users u ON u.id = rt.user_id
WHERE u.email = 'alice@example.com'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("alice has %d refresh-token rows, want the original and one successor", rows)
	}
}
