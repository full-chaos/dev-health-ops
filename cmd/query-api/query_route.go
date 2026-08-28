// CHAOS-4367 Wave 1: wires the ONE live route this binary now serves --
// featureFlags -- behind routeswitch.Mux + the registry-backed
// PostgresSwitch, gated by a verified effective-principal envelope. See
// main.go's package doc for what Wave 0 left empty; this file is what
// Wave 1 adds on top of it.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"

	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/featureflags"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
)

// registeredFeatureFlagsDocument is the ONE query document query-api
// recognizes for the featureFlags operation this wave (plan §7 open
// decision 2: "GraphQL eligibility = registered documents only"). A
// request's exact query text must digest-match this canonical document
// before its operation name even reaches the reachability check -- this
// is what closes PostgresSwitch's documented gap #1 ("document identity
// is NOT verified against the live request ... wiring the exact
// registered-document-identity contract end to end is a later wave's
// job, when Mux is actually mounted on a live route") for the one route
// this wave mounts.
//
// Known, deliberate gap (same "name it, don't hide it" convention
// PostgresSwitch's own doc comment uses): this is a hand-registered
// single document, not Wave 0 deliverable 2's actual web-operations
// inventory. A later wave sources the registered-document set from that
// real inventory; Wave 1 does not build that general pipeline.
const registeredFeatureFlagsDocument = `query FeatureFlags($orgId: String!, $provider: String, $project: String, $includeArchived: Boolean, $limit: Int!) {
  featureFlags(orgId: $orgId, provider: $provider, project: $project, includeArchived: $includeArchived, limit: $limit) {
    flags {
      flagId
      flagKey
      provider
      projectKey
      flagType
      createdAt
      archivedAt
    }
    totalCount
    degradedReason
  }
}`

// digestHex is this wave's own document/schema digest convention: no
// canonical algorithm has landed in this repo yet (go_api_registry.py's
// schema_digest/document_digest are opaque caller-supplied strings; no
// compute_document_digest exists to match against). sha256 hex of the
// trimmed document text is documented here so a later wave that DOES
// land a canonical algorithm can see exactly what this wave used and
// migrate deliberately, rather than silently drifting.
func digestHex(document string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(document)))
	return hex.EncodeToString(sum[:])
}

// queryRouteConfig is env-sourced configuration for the live /query
// route. All fields are required together -- see loadQueryRouteConfig.
type queryRouteConfig struct {
	ClickHouseURI       string
	RegistryPostgresURI string
	EnvelopeJWKSPath    string
	EnvelopeIssuer      string
	EnvelopeAudience    string
	SchemaDigest        string
}

// loadQueryRouteConfig reads the /query route's configuration from the
// environment. ok is false when ANY required variable is unset -- Wave
// 0's default ("nothing mounted, only /healthz and /readyz live") is
// preserved for any caller that does not set these, rather than the
// process failing to start. This mirrors the deployment shape: a plain
// `go build`/`go vet`/CI run, or an operator who has not yet configured
// this service's dependencies, must not be forced to also configure
// ClickHouse/Postgres/JWKS just to build or run the binary.
func loadQueryRouteConfig() (queryRouteConfig, bool) {
	cfg := queryRouteConfig{
		ClickHouseURI:       os.Getenv("CLICKHOUSE_URI"),
		RegistryPostgresURI: os.Getenv("GO_API_REGISTRY_POSTGRES_URI"),
		EnvelopeJWKSPath:    os.Getenv("GO_API_ENVELOPE_JWKS_PATH"),
		EnvelopeIssuer:      os.Getenv("GO_API_ENVELOPE_ISSUER"),
		EnvelopeAudience:    os.Getenv("GO_API_ENVELOPE_AUDIENCE"),
		SchemaDigest:        os.Getenv("GO_API_SCHEMA_DIGEST"),
	}
	if cfg.ClickHouseURI == "" || cfg.RegistryPostgresURI == "" || cfg.EnvelopeJWKSPath == "" ||
		cfg.EnvelopeIssuer == "" || cfg.EnvelopeAudience == "" || cfg.SchemaDigest == "" {
		return cfg, false
	}
	return cfg, true
}

// buildQueryRoute wires the real featureFlags path from env-sourced
// config: the shared dev-health-go ClickHouse client, a real Postgres
// pool, and the effective-principal verifier, then hands them to
// newQueryHandler. The returned cleanup func closes the Postgres pool;
// call it on shutdown.
func buildQueryRoute(cfg queryRouteConfig) (http.HandlerFunc, func(), error) {
	chClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: cfg.ClickHouseURI})
	if err != nil {
		return nil, nil, err
	}

	pgPool, err := pgxpool.New(context.Background(), cfg.RegistryPostgresURI)
	if err != nil {
		return nil, nil, err
	}

	verifier, err := principal.NewVerifier(cfg.EnvelopeJWKSPath, cfg.EnvelopeIssuer, cfg.EnvelopeAudience)
	if err != nil {
		pgPool.Close()
		return nil, nil, err
	}

	handler := newQueryHandler(chClient, pgPool, verifier, cfg.SchemaDigest)
	cleanup := func() { pgPool.Close() }
	return handler, cleanup, nil
}

// newQueryHandler wires the routeswitch.Mux + registry-backed
// PostgresSwitch + gqlgen handler pipeline over ALREADY-CONSTRUCTED
// dependencies -- the plan §6 "deploy an empty Go query-api and prove a
// route becomes reachable when, and only when, its individual switch is
// enabled" contract, now with a real resolver behind it (CHAOS-4367 Wave
// 1). Split out from buildQueryRoute so a reachability test can wire this
// exact pipeline against a real Postgres testcontainer and a fake
// ClickHouse client, without needing a real ClickHouse or a real
// CLICKHOUSE_URI to prove the SWITCH half of the contract -- see
// query_route_integration_test.go.
func newQueryHandler(chClient featureflags.QueryClient, pgPool *pgxpool.Pool, verifier *principal.Verifier, schemaDigest string) http.HandlerFunc {
	documentDigest := digestHex(registeredFeatureFlagsDocument)
	sw := routeswitch.NewPostgresSwitch(pgPool, schemaDigest, map[string]string{
		"featureFlags": documentDigest,
	})
	routeMux := routeswitch.NewMux(sw)

	schema := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{ClickHouse: chClient}})
	routeMux.Register("featureFlags", gqlhandler.NewDefaultServer(schema))

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// Bounded read: this is a request body, not an internal reader --
		// an unbounded io.ReadAll on caller input is the same class of
		// risk WithRowLimit exists to prevent on the query side.
		bodyBytes, readErr := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		_ = r.Body.Close()
		if readErr != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		var parsed struct {
			Query string `json:"query"`
		}
		if err := json.Unmarshal(bodyBytes, &parsed); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		operation, ok := operationForDocument(parsed.Query, documentDigest)
		if !ok {
			// Unregistered document: plan §5's safe default ("unregistered
			// documents ... stay on Python") applied at this router --
			// indistinguishable from an unknown route, exactly like an
			// operation with no Mux registration at all.
			http.NotFound(w, r)
			return
		}

		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		claims, err := verifier.Verify(token)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		r = r.WithContext(authctx.WithClaims(r.Context(), authctx.Claims{OrgID: claims.OrgID}))
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

		routeMux.Dispatch(operation, w, r)
	}
}

// operationForDocument resolves a request's raw query text to a
// registered operation name -- "registered documents only" (plan §7 open
// decision 2), never an AST-shape match. Only featureFlags is registered
// this wave.
func operationForDocument(query, featureFlagsDigest string) (string, bool) {
	if digestHex(query) == featureFlagsDigest {
		return "featureFlags", true
	}
	return "", false
}

func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if token == "" {
		return "", false
	}
	return token, true
}
