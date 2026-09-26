package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestQueryAPIPostureCheckIsOptIn(t *testing.T) {
	t.Parallel()
	for name, value := range map[string]string{"unset": "", "blank": "   \t"} {
		env := func(key string) string {
			if key == "QUERY_API_DATABASE_ROLE" {
				return value
			}
			return ""
		}
		if check := queryAPIPostureCheck(env, nil); check != nil {
			t.Errorf("%s: a deployment that names no role must not be checked", name)
		}
	}
	named := func(key string) string {
		if key == "QUERY_API_DATABASE_ROLE" {
			return " dev_health_query_api "
		}
		return ""
	}
	check := queryAPIPostureCheck(named, nil)
	if check == nil {
		t.Fatal("a deployment that names a role must be checked")
	}
	// With no pool the check cannot pass: naming a role and having no way to
	// verify it is not ready.
	if err := check(context.Background()); err == nil {
		t.Fatal("the check passed with no Postgres pool")
	}
}

func TestQueryAPIPostureReadinessClassesAFailureAndPassesOtherwise(t *testing.T) {
	t.Parallel()
	if err := queryAPIPostureReadiness(context.Background(), nil); err != nil {
		t.Fatalf("no check configured: %v", err)
	}
	if err := queryAPIPostureReadiness(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("a passing check: %v", err)
	}
	cause := errors.New("saved_reports: missing [INSERT] for role \"r\" at dial tcp 10.0.0.1:5432")
	err := queryAPIPostureReadiness(context.Background(), func(context.Context) error { return cause })
	var dependency *readyzDependencyError
	if !errors.As(err, &dependency) || dependency.Class != readyzClassPosture || !errors.Is(err, cause) {
		t.Fatalf("a failing check must be a %q dependency error wrapping its cause: %v", readyzClassPosture, err)
	}
	if got := readyzDependencyClass(err); got != readyzClassPosture {
		t.Fatalf("/readyz would report class %q, want %q", got, readyzClassPosture)
	}
}

// buildQueryRoute needs a live ClickHouse to run, so the wiring of the check
// into /readyz is pinned at the call site: the readiness func it hands back
// must be built WITH the posture check, or naming QUERY_API_DATABASE_ROLE
// would gate nothing.
func TestBuildQueryRouteWiresThePostureCheckIntoReadiness(t *testing.T) {
	t.Parallel()
	src, err := os.ReadFile("query_route.go")
	if err != nil {
		t.Fatal(err)
	}
	const call = "readinessCheck(chClient, pgPool, verifier, queryAPIPostureCheck(getenv, pgPool))"
	if strings.Count(string(src), call) != 1 {
		t.Fatalf("query_route.go must build its readiness check exactly once, with %q", call)
	}
}

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

type fakeJWKS struct{ err error }

func (f fakeJWKS) CheckJWKS() error { return f.err }

// readinessCheck's composition, by behaviour: the posture check is a real part
// of it (a failing posture makes /readyz fail with its own class, a passing or
// absent one does not), and an earlier dependency's failure keeps its own class.
func TestReadinessCheckComposesThePostureCheck(t *testing.T) {
	t.Parallel()
	posture := errors.New("saved_reports: missing [INSERT] for role")
	cases := []struct {
		name      string
		ch, pg    error
		jwks      error
		posture   func(context.Context) error
		wantClass string
	}{
		{name: "all healthy, no posture check", posture: nil},
		{name: "all healthy, posture passes", posture: func(context.Context) error { return nil }},
		{name: "posture fails", posture: func(context.Context) error { return posture }, wantClass: readyzClassPosture},
		{name: "clickhouse wins over posture", ch: errors.New("ch"), posture: func(context.Context) error { return posture }, wantClass: readyzClassClickHouse},
		{name: "postgres wins over posture", pg: errors.New("pg"), posture: func(context.Context) error { return posture }, wantClass: readyzClassPostgres},
		{name: "jwks wins over posture", jwks: errors.New("jwks"), posture: func(context.Context) error { return posture }, wantClass: readyzClassJWKS},
	}
	for _, test := range cases {
		check := readinessCheck(fakePinger{test.ch}, fakePinger{test.pg}, fakeJWKS{test.jwks}, test.posture)
		err := check(context.Background())
		if test.wantClass == "" {
			if err != nil {
				t.Errorf("%s: %v, want ready", test.name, err)
			}
			continue
		}
		if got := readyzDependencyClass(err); err == nil || got != test.wantClass {
			t.Errorf("%s: error %v (class %q), want class %q", test.name, err, got, test.wantClass)
		}
	}
}

func TestQueryAPIRiverSchemaDefaultsLikeEveryOtherService(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct{ env, want string }{
		"unset":    {"", "river"},
		"blank":    {"  \t", "river"},
		"explicit": {" custom_river ", "custom_river"},
	} {
		get := func(key string) string {
			if key == "RIVER_DATABASE_SCHEMA" {
				return test.env
			}
			return ""
		}
		if got := queryAPIRiverSchema(get); got != test.want {
			t.Errorf("%s: schema %q, want %q", name, got, test.want)
		}
	}
}

// silentPostgresPool is a pool whose server accepts every connection and never
// answers: the shape of a posture query that is slow (1.4-1.9 s on the
// production catalog) or wedged. A probe that waits on the query pays for it.
func silentPostgresPool(t *testing.T) (*pgxpool.Pool, *atomic.Int32) {
	t.Helper()
	accepted := &atomic.Int32{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			t.Cleanup(func() { _ = connection.Close() })
		}
	}()
	pool, err := pgxpool.New(context.Background(),
		"postgres://query_api:x@"+listener.Addr().String()+"/db?sslmode=disable&connect_timeout=30")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool, accepted
}

// The readiness contract (D2549/D2588): the probe answers within its budget and
// NEVER waits on a live expensive check. With a posture query that cannot
// answer, /readyz must fail closed at once ("postgres_posture"), not hold the
// probe until the handler's own 3 s bound (longer than the kubelet's 2 s probe
// timeout). It answers again, ready, only once the background run has proven
// the role -- which a silent server never does, so it must stay unready.
func TestReadyzAnswersWithinTheProbeBudgetWhileThePostureQueryIsSlow(t *testing.T) {
	pool, _ := silentPostgresPool(t)
	env := func(key string) string {
		if key == "QUERY_API_DATABASE_ROLE" {
			return "devhealth_query_api"
		}
		return ""
	}
	ready := readinessCheck(fakePinger{}, fakePinger{}, fakeJWKS{}, queryAPIPostureCheck(env, pool))
	handler := readyzHandler(ready)

	const budget = 500 * time.Millisecond
	for probe := 1; probe <= 3; probe++ {
		recorder := httptest.NewRecorder()
		started := time.Now()
		handler(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if elapsed := time.Since(started); elapsed > budget {
			t.Fatalf("probe %d took %v (> %v): /readyz waited on the posture query", probe, elapsed, budget)
		}
		if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), readyzClassPosture) {
			t.Fatalf("probe %d: status %d body %q, want 503 naming %q (an unproven role is not ready)",
				probe, recorder.Code, recorder.Body.String(), readyzClassPosture)
		}
	}
}

// The proof starts at construction (process start), not on the first probe: a
// deployment that names a role begins dialing Postgres for the posture query
// before any /readyz arrives.
func TestPostureProofStartsAtConstructionNotOnTheFirstProbe(t *testing.T) {
	pool, accepted := silentPostgresPool(t)
	env := func(key string) string {
		if key == "QUERY_API_DATABASE_ROLE" {
			return "devhealth_query_api"
		}
		return ""
	}
	if check := queryAPIPostureCheck(env, pool); check == nil {
		t.Fatal("a named role must be checked")
	}
	deadline := time.Now().Add(5 * time.Second)
	for accepted.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no connection reached Postgres with no probe made: the proof did not start at construction")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
