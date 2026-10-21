package apiservice

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// mountedRoute is one route a go-api listener serves.
type mountedRoute struct {
	Listener string // api, internal, billing-edge
	Method   string
	Pattern  string
	Handler  http.Handler
}

func (r mountedRoute) key() string { return r.Method + " " + r.Pattern + "@" + r.Listener }

// mountedRoutes builds the fully wired route set of every go-api listener the
// way a configured service does: Pool and ClickHouse handles that dial lazily
// make the pool- and ClickHouse-backed areas mount, and a Guard makes the
// guarded areas mount.
func mountedRoutes(t *testing.T) []mountedRoute {
	t.Helper()
	key := strings.Repeat("fixture-key-", 3)
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	clickHouse, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickHouse.Close() })
	deps := Deps{Pool: pool, ClickHouse: clickHouse, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()), Verifier: verifier, Signer: signer}
	var out []mountedRoute
	for _, route := range Routes(deps, quietLogger()) {
		out = append(out, mountedRoute{"api", route.Method, route.Pattern, route.Handler})
	}
	for _, route := range InternalRoutes(deps, quietLogger()) {
		out = append(out, mountedRoute{"internal", route.Method, route.Pattern, route.Handler})
	}
	for _, route := range billing.EdgeRoutes(billing.Deps{Pool: pool, Logger: quietLogger()}) {
		out = append(out, mountedRoute{"billing-edge", route.Method, route.Pattern, route.Handler})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}
