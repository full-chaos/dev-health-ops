// CHAOS-4367 Wave 1: wires the ONE live route this binary now serves --
// featureFlags -- behind routeswitch.Mux + the registry-backed
// PostgresSwitch, gated by a verified effective-principal envelope. See
// main.go's package doc for what Wave 0 left empty; this file is what
// Wave 1 adds on top of it.
//
// CHAOS-4368 Wave 2 adds a SECOND operation, reviewEdges, on the same
// /query route and the same routeswitch.Mux + PostgresSwitch pipeline --
// each operation gets its own registered document + digest + Mux
// registration, gated independently (the go_api_routing_state row for one
// operation has no effect on the other's reachability).
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

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
//
// Document text is byte-for-byte the REAL production query (codex review,
// 2026-08-28, round 3 -- corrected after round 2's wrong conclusion):
// web/src/lib/feature-flags/queries.ts's FEATURE_FLAG_REGISTRY_QUERY names
// its operation "FeatureFlagRegistry", not "FeatureFlags". Round 2 of this
// review raised the same name mismatch citing only the operation inventory
// doc, and was dismissed as a documentation-only ambiguity because Wave 0's
// registry test fixtures use `selected_operation="featureFlags"`internally
// -- that dismissal was wrong for a different reason than the one being
// discussed: `selected_operation` is this code's OWN internal map key
// (Mux.Register / PostgresSwitch.documentDigests), never compared against
// the document text, so it can stay "featureFlags" (matching that existing
// test-fixture precedent) with NO effect on reachability. What actually
// gates reachability is operationForDocument's digest match against this
// constant's literal text -- and that text previously used the WRONG
// operation name, so a real web request's digest would never have matched
// it, 404-ing every real featureFlags query while local tests (which also
// used the wrong name on both sides) stayed green. Copied verbatim from
// the real client source so the digest this route checks is the digest a
// real request actually produces.
const registeredFeatureFlagsDocument = `query FeatureFlagRegistry($orgId: String!, $provider: String, $project: String, $includeArchived: Boolean, $limit: Int!) {
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

// registeredReviewEdgesDocument is CHAOS-4368 Wave 2's registered document
// for the reviewEdges operation -- same "registered documents only"
// contract registeredFeatureFlagsDocument's doc comment explains, applied
// to a second operation. Copied byte-for-byte from the REAL production
// query web actually sends (web/src/lib/graphql/queries.ts's
// REVIEW_EDGES_QUERY, operation name "ReviewEdges", input variable named
// `$input` of type `ReviewEdgesInput!` -- NOT individual scalar args, a
// different shape than featureFlags's query above) -- learning Wave 1's
// codex-round-3 lesson forward: this text is sourced from the client file
// itself, not reconstructed from the SDL or an operation-inventory doc,
// so the digest this route checks is the digest a real web request
// actually produces.
const registeredReviewEdgesDocument = `query ReviewEdges($input: ReviewEdgesInput!) {
  reviewEdges(input: $input) {
    edges {
      reviewer
      author
      reviewsCount
      day
      repoId
    }
    totalCount
  }
}`

// registeredCognitiveLoadDocument is CHAOS-4369 Wave 3's registered
// document for the cognitiveLoad operation -- same "registered documents
// only" contract, sourced byte-for-byte from the REAL production query
// (web/src/lib/graphql/queries.ts's COGNITIVE_LOAD_QUERY, operation name
// "CognitiveLoad", input variable named `$input` of type
// `CognitiveLoadInput!` -- same single-input-object shape as
// registeredReviewEdgesDocument above, not featureFlags's individual
// scalar args).
const registeredCognitiveLoadDocument = `query CognitiveLoad($input: CognitiveLoadInput!) {
  cognitiveLoad(input: $input) {
    orgId
    teamId
    totalDays
    signals {
      day
      prInterruptionLoad
      contextSpreadCount
      reviewRequestLoad
      afterHoursCommitRatio
      weekendCommitRatio
    }
  }
}`

// registeredComplexityTimeseriesDocument is CHAOS-4369 Wave 3's registered
// document for the complexityTimeseries operation -- same "registered
// documents only" contract, same "sourced from the real client file, not
// reconstructed" discipline as registeredReviewEdgesDocument above. Copied
// byte-for-byte from web/src/lib/graphql/queries.ts's
// COMPLEXITY_TIMESERIES_QUERY, operation name "ComplexityTimeseries",
// input variable named `$input` of type `ComplexityTimeseriesInput!`.
const registeredComplexityTimeseriesDocument = `query ComplexityTimeseries($input: ComplexityTimeseriesInput!) {
  complexityTimeseries(input: $input) {
    points {
      date
      scopeId
      scopeName
      locTotal
      cyclomaticPerKloc
      cyclomaticTotal
      cyclomaticAvg
      highComplexityFunctions
      veryHighComplexityFunctions
    }
    totalScope
  }
}`

// registeredHotspotsDocument is CHAOS-4369 Wave 3's registered document
// for the hotspots operation (the second of the two Wave 3 operations,
// after complexityTimeseries) -- same "registered documents only"
// contract, same "sourced from the real client file, not reconstructed"
// discipline as registeredReviewEdgesDocument above. Copied byte-for-byte
// from web/src/lib/graphql/queries.ts's HOTSPOTS_QUERY, operation name
// "Hotspots", input variable named `$input` of type `HotspotsInput!`.
const registeredHotspotsDocument = `query Hotspots($input: HotspotsInput!) {
  hotspots(input: $input) {
    rows {
      filePath
      repoId
      repoName
      churnLoc30d
      churnCommits30d
      cyclomaticTotal
      cyclomaticAvg
      blameConcentration
      riskScore
      evidenceUrl
    }
  }
}`

// registeredOperatingReviewDocument is CHAOS-4352 Wave 4 Lane B's
// (CHAOS-4505) registered document for the operatingReview operation --
// same "registered documents only" contract, same "sourced from the real
// client file, not reconstructed" discipline as every document above.
// Copied byte-for-byte from web/src/lib/graphql/queries.ts's
// OPERATING_REVIEW_QUERY (queries.ts:394-425), operation name
// "OperatingReview", TWO variables -- `$orgId` of type `String!` AND
// `$input` of type `OperatingReviewInput!` -- unlike every other operation
// registered in this file except featureFlags, matching the schema's
// `operatingReview(orgId: String!, input: OperatingReviewInput!)` (the
// $orgId variable is parsed by this route but never trusted for scoping;
// see operatingreview package's doc comment's Authorization section).
const registeredOperatingReviewDocument = `query OperatingReview($orgId: String!, $input: OperatingReviewInput!) {
  operatingReview(orgId: $orgId, input: $input) {
    orgId
    teamId
    weekStart
    priorWeekStart
    sections {
      key
      title
      changed
      improved
      worsened
      metrics {
        key
        label
        value
        unit
        delta {
          value
          priorValue
          absolute
          percent
          status
        }
      }
    }
    recommendations
    recommendationsEmptyState
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
	// Eager readiness check, matching cmd/dev-health-worker's own
	// documented contract for this exact env var (deploy/go-workers/
	// README.md, "ClickHouse: the Go worker needs the native port, not
	// the HTTP port"): CLICKHOUSE_URI resolves to a DIFFERENT port for a
	// Go process (native wire protocol, :9000 locally) than for a Python
	// process (HTTP, :8123 locally) despite sharing the same env var
	// name across this repo's deployments -- operator-configured per
	// process, not auto-translated here (codex review, 2026-08-28: this
	// route previously mounted successfully even when CLICKHOUSE_URI was
	// the repo-standard HTTP endpoint, then failed every request with a
	// handshake error instead of failing loudly at startup). Ping now so
	// a misconfigured endpoint refuses to start, the same "measurement
	// that did not happen must FAIL, loudly" discipline dev-health-worker
	// already applies to this identical class of mistake.
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := chClient.Ping(pingCtx); err != nil {
		return nil, nil, fmt.Errorf("query-api: ClickHouse readiness check failed (CLICKHOUSE_URI must be the NATIVE protocol port, not the HTTP port -- see deploy/go-workers/README.md): %w", err)
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
// enabled" contract, now with real resolvers behind it (CHAOS-4367 Wave 1
// featureFlags; CHAOS-4368 Wave 2 reviewEdges). Split out from
// buildQueryRoute so a reachability test can wire this exact pipeline
// against a real Postgres testcontainer and a fake ClickHouse client,
// without needing a real ClickHouse or a real CLICKHOUSE_URI to prove the
// SWITCH half of the contract -- see query_route_integration_test.go.
//
// Both operations share the SAME gqlgen handler instance (one schema, one
// executable server -- gqlgen's handler is safe for concurrent reuse
// across requests) but are registered under DISTINCT Mux operation keys,
// each gated by its own go_api_routing_state row: enabling featureFlags
// does not enable reviewEdges and vice versa.
//
// Inherited, pre-existing gap this wave does NOT close (codex review,
// 2026-08-28, re-raising it against this route -- it is PostgresSwitch's
// own documented gap #2, not something introduced here): eligible_orgs
// and rollout_percentage are not enforced. Mode=canary/primary is
// reachable for every authenticated org once dispatched here, because
// Switch.Enabled(operation string) takes no org argument at all -- see
// PostgresSwitch's doc comment in internal/routeswitch/postgres_switch.go
// for why (threading org through Enabled is a later wave's job, the same
// wave that would also verify request document identity, gap #1). Both
// waves are local dual-run proof only (plan §5 stage 2); org-scoped
// canary enforcement is a stage-5 concern this PR does not claim to
// satisfy.
func newQueryHandler(chClient featureflags.QueryClient, pgPool *pgxpool.Pool, verifier *principal.Verifier, schemaDigest string) http.HandlerFunc {
	// digestByOperation is this route's registered-document inventory:
	// operation name -> the sha256 digest of that operation's registered
	// document text. CHAOS-4369 Wave 3 generalizes what Wave 1/2 hardcoded
	// as two named locals (featureFlagsDigest, reviewEdgesDigest) into a
	// map keyed by operation name, so a later wave adding a further
	// operation extends this map rather than threading another positional
	// parameter through newQueryHandler/operationForDocument.
	digestByOperation := map[string]string{
		"featureFlags":         digestHex(registeredFeatureFlagsDocument),
		"reviewEdges":          digestHex(registeredReviewEdgesDocument),
		"cognitiveLoad":        digestHex(registeredCognitiveLoadDocument),
		"complexityTimeseries": digestHex(registeredComplexityTimeseriesDocument),
		"hotspots":             digestHex(registeredHotspotsDocument),
		"operatingReview":      digestHex(registeredOperatingReviewDocument),
	}
	sw := routeswitch.NewPostgresSwitch(pgPool, schemaDigest, digestByOperation)
	routeMux := routeswitch.NewMux(sw)

	// operationByDigest is digestByOperation's reverse index, built once
	// here rather than on every request -- operationForDocument does a
	// single map lookup per request, not a linear scan.
	operationByDigest := make(map[string]string, len(digestByOperation))
	for operation, digest := range digestByOperation {
		operationByDigest[digest] = operation
	}

	schema := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{ClickHouse: chClient}})
	gqlHandler := gqlhandler.NewDefaultServer(schema)
	for operation := range digestByOperation {
		routeMux.Register(operation, gqlHandler)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// Same body-size contract the Python edge's
		// GraphQLQuerySizeLimitMiddleware enforces for /graphql
		// (security.py's GRAPHQL_MAX_QUERY_BYTES, default 16 KiB) --
		// codex review, 2026-08-28: reading up to 1 MiB unconditionally
		// let a body between the configured limit and 1 MiB through
		// silently, bypassing that existing request-size contract for
		// this canaried operation. LimitReader+1 lets a body of EXACTLY
		// the limit succeed while still detecting one byte over it,
		// without buffering the oversized remainder.
		limit := graphQLMaxQueryBytes()
		bodyBytes, readErr := io.ReadAll(io.LimitReader(r.Body, int64(limit)+1))
		_ = r.Body.Close()
		if readErr != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if len(bodyBytes) > limit {
			http.Error(w, "GraphQL request body exceeds size limit", http.StatusRequestEntityTooLarge)
			return
		}

		var parsed struct {
			Query string `json:"query"`
		}
		if err := json.Unmarshal(bodyBytes, &parsed); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		operation, ok := operationForDocument(parsed.Query, operationByDigest)
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
// decision 2), never an AST-shape match. operationByDigest is the reverse
// (digest -> operation) index newQueryHandler builds once from
// digestByOperation, so this is a single O(1) map lookup per request
// regardless of how many operations this route registers, rather than a
// growing chain of per-operation switch cases.
func operationForDocument(query string, operationByDigest map[string]string) (string, bool) {
	operation, ok := operationByDigest[digestHex(query)]
	return operation, ok
}

// defaultGraphQLMaxQueryBytes mirrors security.py's
// DEFAULT_GRAPHQL_MAX_QUERY_BYTES (16 KiB) exactly.
const defaultGraphQLMaxQueryBytes = 16 * 1024

// graphQLMaxQueryBytes mirrors security.py's get_graphql_max_query_bytes:
// same env var name, same fall-back-to-default behavior for an unset or
// unparseable value, same floor of 1 (never zero or negative).
func graphQLMaxQueryBytes() int {
	raw := os.Getenv("GRAPHQL_MAX_QUERY_BYTES")
	if raw == "" {
		return defaultGraphQLMaxQueryBytes
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return defaultGraphQLMaxQueryBytes
	}
	if value < 1 {
		return 1
	}
	return value
}

// bearerToken mirrors services/auth.py's extract_token_from_header
// exactly: split on ANY whitespace (not just a literal "Bearer " prefix),
// require exactly two fields, and compare the scheme case-INSENSITIVELY
// -- codex review, 2026-08-28: the previous case-sensitive prefix check
// rejected the standards-valid `bearer <token>` scheme Python's edge
// already accepts, a real authentication-behavior divergence for the
// same canaried operation.
func bearerToken(header string) (string, bool) {
	parts := strings.Fields(header)
	if len(parts) != 2 {
		return "", false
	}
	scheme, token := parts[0], parts[1]
	if !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	return token, true
}
