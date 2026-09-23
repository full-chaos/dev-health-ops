// Package health is plan area A: the api's probes (api/main.py): /health,
// /ready and /health/workers.
package health

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
)

// Deps is what the probes read.
type Deps struct {
	// Pool is the api pool; nil means Postgres is not configured.
	Pool *pgxpool.Pool
	// ClickHouseDSN and ValkeyURI are the dependencies /health pings; ""
	// means not configured.
	ClickHouseDSN string
	ValkeyURI     string
	// ExpectedWorkerGroups is EXPECTED_WORKER_GROUPS: nil when unset.
	ExpectedWorkerGroups *[]string
	Logger               *slog.Logger
}

// checkTimeout bounds each dependency check.
const checkTimeout = 5 * time.Second

// Routes returns the area's routes. Each probe is api_route(GET, HEAD);
// Python keeps those methods in a set with no fixed order, so the 405 Allow
// lists them sorted.
func Routes(deps Deps) []httpapi.Route {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	ready := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		write(w, http.StatusOK, object("status", "ready"))
	})
	health := http.HandlerFunc(deps.health)
	workers := http.HandlerFunc(deps.workers)
	var routes []httpapi.Route
	for _, probe := range []struct {
		pattern string
		handler http.Handler
	}{{"/health", health}, {"/ready", ready}, {"/health/workers", workers}} {
		routes = append(routes,
			httpapi.Route{Method: http.MethodGet, Pattern: probe.pattern, Handler: probe.handler, Allow: "GET, HEAD"},
			httpapi.Route{Method: http.MethodHead, Pattern: probe.pattern, Handler: probe.handler})
	}
	return routes
}

func object(pairs ...pyjson.Value) *pyjson.Object {
	out := pyjson.NewObject()
	for index := 0; index+1 < len(pairs); index += 2 {
		out.Set(pairs[index].(string), pairs[index+1])
	}
	return out
}

func write(w http.ResponseWriter, status int, body *pyjson.Object) {
	payload, _ := pyjson.Marshal(body) // string values only; cannot fail
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(status)
	_, _ = io.Copy(w, bytes.NewReader(payload))
}

// health is the deep check: Postgres (connectivity and the application
// schema revision), ClickHouse and Valkey, concurrently; the rate limiter's
// backend is informational. Any required check not "ok"/"not_configured" is
// a 503 with the same body.
func (d Deps) health(w http.ResponseWriter, r *http.Request) {
	checks := []func(context.Context) (string, string){d.checkPostgres, d.checkClickHouse, d.checkValkey}
	results := make([][2]string, len(checks))
	var wait sync.WaitGroup
	for index, check := range checks {
		wait.Add(1)
		go func() {
			defer wait.Done()
			ctx, cancel := context.WithTimeout(r.Context(), checkTimeout)
			defer cancel()
			key, status := check(ctx)
			results[index] = [2]string{key, status}
		}()
	}
	wait.Wait()
	services := pyjson.NewObject()
	overall := "ok"
	for _, result := range results {
		services.Set(result[0], result[1])
		if result[1] != "ok" && result[1] != "not_configured" {
			overall = "down"
		}
	}
	// The api has no client rate limiter (the Python api's backend is
	// redis, memory or noop): it reports the truth.
	services.Set("rate_limiter", "noop")
	status := http.StatusOK
	if overall != "ok" {
		status = http.StatusServiceUnavailable
		d.Logger.WarnContext(r.Context(), "api health down", slog.Any("services", servicesAttr(results)))
	}
	write(w, status, object("status", overall, "services", services))
}

func servicesAttr(results [][2]string) map[string]string {
	out := make(map[string]string, len(results))
	for _, result := range results {
		out[result[0]] = result[1]
	}
	return out
}

// checkPostgres is _check_postgres_health: reachable, and an
// alembic_version head at or after the minimum application revision.
func (d Deps) checkPostgres(ctx context.Context) (string, string) {
	if d.Pool == nil {
		return "postgres", "not_configured"
	}
	rows, err := d.Pool.Query(ctx, `SELECT version_num FROM alembic_version`)
	if err != nil {
		d.Logger.WarnContext(ctx, "api health: postgres check failed", slog.String("error", err.Error()))
		return "postgres", "down"
	}
	defer rows.Close()
	satisfied := false
	for rows.Next() {
		var revision string
		if err := rows.Scan(&revision); err != nil {
			return "postgres", "down"
		}
		satisfied = satisfied || satisfyingRevisions[revision]
	}
	if rows.Err() != nil || !satisfied {
		d.Logger.WarnContext(ctx, "api health: postgres schema below the required revision",
			slog.String("minimum", minimumSchemaRevision))
		return "postgres", "down"
	}
	return "postgres", "ok"
}

// checkClickHouse is _check_clickhouse_health: SELECT 1 on a fresh
// connection.
func (d Deps) checkClickHouse(ctx context.Context) (string, string) {
	if d.ClickHouseDSN == "" {
		// Python falls back to a localhost default, which a pod cannot reach.
		return "clickhouse", "down"
	}
	connection, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(d.ClickHouseDSN))
	if err != nil {
		d.Logger.WarnContext(ctx, "api health: clickhouse unavailable")
		return "clickhouse", "down"
	}
	defer connection.Close()
	var one uint8
	if err := connection.QueryRow(ctx, `SELECT 1 AS ok`).Scan(&one); err != nil {
		d.Logger.WarnContext(ctx, "api health: clickhouse query failed")
		return "clickhouse", "down"
	}
	return "clickhouse", "ok"
}

// checkValkey is _check_redis_health: PING on a fresh client.
func (d Deps) checkValkey(ctx context.Context) (string, string) {
	if d.ValkeyURI == "" {
		return "redis", "not_configured"
	}
	config := valkey.DefaultConfig(d.ValkeyURI)
	config.ClientName = "dev-health-api"
	config.DialTimeout = 2 * time.Second
	client, err := valkey.Open(ctx, config)
	if err != nil {
		d.Logger.WarnContext(ctx, "api health: valkey unavailable")
		return "redis", "down"
	}
	client.Close()
	return "redis", "ok"
}

// workers is /health/workers.
func (d Deps) workers(w http.ResponseWriter, r *http.Request) {
	if d.ExpectedWorkerGroups == nil {
		// Celery-authoritative mode: the Go api cannot inspect Celery.
		d.Logger.WarnContext(r.Context(), "api health/workers: EXPECTED_WORKER_GROUPS unset; Celery inspection is not available")
		write(w, http.StatusServiceUnavailable, object("status", "down", "services", object("celery", "down")))
		return
	}
	expected := *d.ExpectedWorkerGroups
	if len(expected) == 0 {
		write(w, http.StatusServiceUnavailable,
			object("status", "down", "services", object("expected_worker_groups", "misconfigured")))
		return
	}
	statuses := d.presence(r.Context(), expected)
	services := pyjson.NewObject()
	overall := "ok"
	for _, group := range expected {
		services.Set("go_worker:"+group, statuses[group])
		if statuses[group] != "ok" {
			overall = "down"
		}
	}
	services.Set("celery", "retired")
	status := http.StatusOK
	if overall != "ok" {
		status = http.StatusServiceUnavailable
	}
	write(w, status, object("status", overall, "services", services))
}

// presence is _check_go_worker_presence: a group is "ok" when it has a
// worker_instances row that has not expired; every group is "unknown" when
// Postgres cannot answer.
func (d Deps) presence(ctx context.Context, expected []string) map[string]string {
	out := make(map[string]string, len(expected))
	unknown := func() map[string]string {
		for _, group := range expected {
			out[group] = "unknown"
		}
		return out
	}
	if d.Pool == nil {
		return unknown()
	}
	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	rows, err := d.Pool.Query(ctx,
		`SELECT DISTINCT worker_group FROM public.worker_instances WHERE expires_at > statement_timestamp()`)
	if err != nil {
		d.Logger.WarnContext(ctx, "api health/workers: presence read failed", slog.String("error", err.Error()))
		return unknown()
	}
	defer rows.Close()
	live := map[string]bool{}
	for rows.Next() {
		var group string
		if err := rows.Scan(&group); err != nil {
			return unknown()
		}
		live[group] = true
	}
	if rows.Err() != nil {
		return unknown()
	}
	for _, group := range expected {
		out[group] = "absent"
		if live[group] {
			out[group] = "ok"
		}
	}
	return out
}
