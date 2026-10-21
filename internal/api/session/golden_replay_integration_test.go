//go:build integration

package session_test

import (
	"context"
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
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

var metricsDDL = []string{
	"CREATE TABLE repo_metrics_daily (org_id String, computed_at DateTime('UTC')) ENGINE = MergeTree ORDER BY org_id",
	// user_metrics_daily holds the retraction marker (migration 116, CHAOS-9135): the activity read
	// takes the newest row of each key (FINAL, which only a replacing engine allows) and tests the
	// marker, so the table has the engine and the column of the real one.
	"CREATE TABLE user_metrics_daily (org_id String, computed_at DateTime('UTC'), retracted UInt8 DEFAULT 0) ENGINE = ReplacingMergeTree(computed_at) ORDER BY org_id",
}

// teamKeyedMetricTables are the two activity tables that can hold retraction
// rows. The activity read takes their newest rows and tests their measure
// columns, so they are created from the schema baseline of the migration
// chain, not as a table of two columns.
var teamKeyedMetricTables = []string{"team_metrics_daily", "work_item_metrics_daily"}

func teamKeyedMetricsDDL(t *testing.T) []string {
	t.Helper()
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	var statements []string
	for _, table := range teamKeyedMetricTables {
		found := false
		for _, object := range baseline.Objects {
			if object.Name == table {
				statements, found = append(statements, object.Create), true
			}
		}
		if !found {
			t.Fatalf("the schema baseline has no table %s", table)
		}
	}
	return statements
}

// TestSessionRoutesMatchGolden replays the session scenario against the Go
// routes alone and compares every answer and row set with the Python
// answers the venue oracle recorded.
func TestSessionRoutesMatchGolden(t *testing.T) {
	// CHAOS-7204: Go records the client IP behind a trusted peer only; Python records
	// the header's first hop. Trust this test's loopback peer so both planes agree.
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	pg, server, specs := st.pg, st.server, st.specs

	target := sessionscenario.Target{BaseURL: server.URL, PostgresURI: pg.URI, ResetLimits: st.openWindows}
	t.Log("\n" + sessionscenario.RunTarget(t, ctx, target, sessionscenario.MintTokens(t, specs)))
}

// TestSessionCorpusExternalTarget replays the same corpus against the server at
// DEVHEALTH_AUTH_ORACLE_BASE_URL (go-api, later auth-service), through its URL and stores only.
func TestSessionCorpusExternalTarget(t *testing.T) {
	target, ok := sessionscenario.TargetFromEnv(t)
	if !ok {
		t.Skip(sessionscenario.EnvBaseURL + " is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Log("\n" + sessionscenario.RunTarget(t, ctx, target, sessionscenario.Prepare(t, ctx, target)))
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
	return startStackWith(t, ctx, nil, nil)
}

// startStackWith is startStack with issuer as session.Deps.Issuer (non-nil:
// auth-service owns sessions, AUTH_PROXY_SESSIONS) and, when routePool is
// set, the routes on the pool it returns for the seeded database instead of
// the admin pool.
func startStackWith(t *testing.T, ctx context.Context, issuer sessionissue.Issuer,
	routePool func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, uri string) *pgxpool.Pool) stack {
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
	for _, statement := range append(append(append([]string{}, metricsDDL...), teamKeyedMetricsDDL(t)...), sessionscenario.MetricsStatements...) {
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
	routesPool := pool
	if routePool != nil {
		routesPool = routePool(t, ctx, pool, pg.URI)
	}
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: routesPool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	oauth := oauthprovider.NewClient()
	oauth.Endpoints = sessionscenario.ProviderEndpoints(fake.URL)
	routes := session.Routes(session.Deps{Pool: routesPool, Guard: policy.NewGuard(auth, logger), Auth: auth, Verifier: verifier,
		Signer: signer, ClickHouse: chConn, Limits: httpapi.NewMemoryCounters(limiterNow), Write: apiservice.WriteError, OAuth: oauth, Logger: logger,
		Issuer: issuer})
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
