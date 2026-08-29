// Command query-api is the Go read-only GraphQL analytics service (CHAOS-4366
// Wave 0 / CHAOS-4352 plan §3). This binary is deliberately EMPTY of
// resolver logic: every field resolver gqlgen generated in
// internal/graph/schema.resolvers.go panics with "not implemented" (see
// that file's header comment and the plan §6 Wave-0 scope: "deploy an
// empty Go query-api and prove a route becomes reachable when, and only
// when, its individual switch is enabled").
//
// The GraphQL executable schema is wired below but is NOT mounted on any
// route in this Wave -- there is nothing behind it that should serve
// traffic yet. What IS live: /healthz, /readyz, and the routeswitch
// mechanism (internal/routeswitch), proven reachable-only-when-enabled by
// its own table-driven test. A later wave mounts /query behind
// routeswitch.Mux, keyed by the operation registry
// (src/dev_health_ops/models/go_api_registry.py's ROUTING_STATE table, once
// a Go reader exists -- CHAOS-4377/dev-health-go).
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	gqlhandler "github.com/99designs/gqlgen/graphql/handler"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph"
)

const defaultAddr = ":8090"

// newExecutableSchemaHandler constructs the gqlgen HTTP handler over the
// canonical SDL's generated schema. Kept separate from main() so a future
// integration test can exercise it directly without starting a real
// listener. Unused for now (see package doc) -- built here, not mounted,
// so `go build`/`go vet` catch a schema/resolver mismatch immediately
// rather than only once a later wave tries to wire it in.
func newExecutableSchemaHandler() http.Handler {
	schema := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}})
	return gqlhandler.NewDefaultServer(schema)
}

func addr() string {
	if v := os.Getenv("QUERY_API_ADDR"); v != "" {
		return v
	}
	return defaultAddr
}

func healthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

// readyzHandler reports readiness. Wave 0 has no store dependency to check
// yet (no resolver reads anything), so readiness is process-liveness only
// -- deliberately NOT claiming more than is true. A later wave adds a real
// dependency check (ClickHouse/registry reachability) here, matching this
// codebase's "a measurement that did not happen must FAIL, loudly"
// discipline: this handler must not start asserting DB health before it
// actually checks it.
func readyzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	}
}

func main() {
	// Constructed to prove the schema/resolver pair builds and links
	// correctly (see newExecutableSchemaHandler's doc comment); not
	// mounted on any mux route in this Wave.
	_ = newExecutableSchemaHandler()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthzHandler())
	mux.HandleFunc("/readyz", readyzHandler())

	// CHAOS-4367 Wave 1 / CHAOS-4368 Wave 2: mount the real featureFlags
	// and reviewEdges routes when their (shared) dependencies are
	// configured. See query_route.go's doc comments for what "configured"
	// means and why an unconfigured environment falls back to Wave 0's
	// "nothing mounted" behavior instead of failing to start.
	if routeCfg, ok := loadQueryRouteConfig(); ok {
		queryHandler, cleanup, buildErr := buildQueryRoute(routeCfg)
		if buildErr != nil {
			log.Fatalf("query-api: build /query route: %v", buildErr)
		}
		defer cleanup()
		mux.HandleFunc("/query", queryHandler)
		log.Print("query-api: /query route mounted (featureFlags, reviewEdges)")
	} else {
		log.Print("query-api: /query route not configured (CLICKHOUSE_URI/GO_API_REGISTRY_POSTGRES_URI/GO_API_ENVELOPE_*/GO_API_SCHEMA_DIGEST unset) -- staying Wave-0 empty")
	}

	server := &http.Server{
		Addr:              addr(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("query-api listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("query-api: listen error: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("query-api: graceful shutdown error: %v", err)
	}
}
