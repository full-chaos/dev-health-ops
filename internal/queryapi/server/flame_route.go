// GET /api/v1/flame -- the Go REST handler mirroring
// people_route.go/quadrant_route.go's shape for a REST route mounted on
// query-api's mux, gated by its own routeswitch DynamicSwitch entry.
//
// Auth: the same bearer-envelope verifier every other REST route in this
// binary uses. Python's route additionally rate-limits ("20/minute",
// main.py:784), and its meta response lists /api/v1/flame among
// supported_endpoints (main.py:789) -- this port carries no rate limiter,
// the same documented gap every sibling REST route in this binary already
// carries (this service has no rate-limiting mechanism anywhere yet).
//
// Business logic (entity_id parsing, the three per-entity-type frame
// builders, the four ClickHouse readers) lives in internal/flame -- see
// that package's own doc comment for the full parity contract, including
// its two declared data-layer divergences and its one deliberate
// simplification.
//
// ERROR BODIES: every non-2xx response this route sends uses Python's own
// FastAPI/Starlette contract -- {"detail": ...} JSON, never a plain-text
// body -- via this binary's one shared REST error-response path
// (rest_error_response.go). This route keeps no local copy of that
// envelope.
package server

import (
	"log"
	"net/http"
	"strconv"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/flame"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// flameOperation is this route's routeswitch operation name -- a
// PATH+METHOD-keyed entry, same convention as peopleSearchOperation/
// quadrantOperation (a REST route has no GraphQL document to digest).
const flameOperation = "REST:GET:/api/v1/flame"

// flameEnabledEnvVar is the operator-facing toggle -- unset or anything
// other than a true-ish value leaves the route disabled, matching
// DynamicSwitch's own default.
const flameEnabledEnvVar = "GO_API_FLAME_ENABLED"

// flameForbiddenQueryParams ports _FORBIDDEN_QUERY_PARAMS (main.py:
// 167-175), checked by _reject_comparative_params (main.py:202-208, called
// at main.py:791) -- the same set peopleForbiddenQueryParams already
// declares for /api/v1/people; kept as this route's own copy rather than a
// shared package-level var, matching this binary's existing
// per-route-file convention (see people_route.go's own copy).
var flameForbiddenQueryParams = map[string]bool{
	"compare_to":  true,
	"rank":        true,
	"percentile":  true,
	"score":       true,
	"leaderboard": true,
	"top":         true,
	"bottom":      true,
}

func flameSwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(flameEnabledEnvVar)); enabled {
		sw.Set(flameOperation, true)
	}
	return sw
}

// loadFlameRouteConfig reads this route's own dependency set -- ClickHouse
// plus the envelope verifier trio. Deliberately NOT loadQueryRouteConfig,
// same reasoning as quadrant_route.go/people_route.go's own doc comments:
// this route never touches registry Postgres.
func loadFlameRouteConfig(getenv getenvFunc) (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = getenv("CLICKHOUSE_URI")
	jwksPath = getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = getenv("GO_API_ENVELOPE_ISSUER")
	audience = getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildFlameRoute constructs the handler, its own routeswitch Mux, and a
// cleanup function. ok is false when this route's dependencies are not
// configured -- main() only calls mux.HandleFunc when ok is true, same
// "stay unmounted, don't fail to build/start" contract every other
// optionally-configured route in this binary follows.
func buildFlameRoute(getenv getenvFunc) (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadFlameRouteConfig(getenv)
	if !cfgOK {
		return nil, nil, false, nil
	}

	verifier, err := principal.NewVerifier(jwksPath, issuer, audience)
	if err != nil {
		return nil, nil, false, err
	}

	// Optional -- nil whenever the pod has not been given the edge
	// credential's key material, in which case authenticateRESTRequest
	// falls back to its pre-existing envelope-only behaviour. See
	// buildEdgeVerifierFromEnv's own doc comment for the pod env
	// contract this reads.
	edgeVerifier, err := buildEdgeVerifierFromEnv(getenv)
	if err != nil {
		return nil, nil, false, err
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(clickHouseURI))
	if err != nil {
		return nil, nil, false, err
	}

	routeMux := routeswitch.NewMux(flameSwitchFromEnv(getenv))
	routeMux.Register(flameOperation, newFlameWorkHandler(readClient))

	// Auth runs BEFORE Dispatch, same order every other route in this
	// binary uses.
	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTMethodNotAllowed(w, r, "flame")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "flame")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(flameOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// newFlameWorkHandler is the routeswitch-registered handler for
// GET /api/v1/flame (main.py:783-802). Reached only after
// buildFlameRoute's entryHandler has already authenticated the request
// and attached authctx.Claims.
func newFlameWorkHandler(client flame.QueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only: buildFlameRoute's entryHandler always
			// authenticates and attaches claims before Dispatch reaches this
			// handler -- see authenticateRESTRequest's own doc comment for
			// the three real 401 shapes this route actually answers.
			writeRESTUnauthorized(w, r, "flame", "Not authenticated")
			return
		}

		query := r.URL.Query()

		// Order matters here, confirmed by this binary's own established
		// people_route.go precedent: FastAPI/Pydantic resolves entity_type/
		// entity_id (both required, no default) at the framework level
		// BEFORE the route function body ever runs, so a MISSING one
		// answers 422 even when a forbidden comparative param is ALSO
		// present on the same request -- _reject_comparative_params
		// (main.py:202-208, called at main.py:791) only runs once
		// execution actually reaches the function body. Both are checked,
		// aggregated into one 422, in the SAME order main.py's own flame()
		// signature declares them (entity_type, entity_id).
		var validationErrors []pydanticErrorDetail
		if !query.Has("entity_type") {
			validationErrors = append(validationErrors, missingFieldError([]any{"query", "entity_type"}, nil))
		}
		if !query.Has("entity_id") {
			validationErrors = append(validationErrors, missingFieldError([]any{"query", "entity_id"}, nil))
		}
		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		// _reject_comparative_params (main.py:202-208): ANY of these
		// query-param KEYS present -- regardless of value -- is a 400.
		for key := range query {
			if flameForbiddenQueryParams[key] {
				writeRESTError(w, r, "flame", claims.OrgID, http.StatusBadRequest, "Comparative parameters are not supported.")
				return
			}
		}

		resp, err := flame.BuildResponse(r.Context(), client, claims.OrgID, flame.Params{
			EntityType: lastQueryValue(query, "entity_type"),
			EntityID:   lastQueryValue(query, "entity_id"),
		})
		if err != nil {
			if reqErr, ok := flame.AsRequestError(err); ok {
				writeRESTError(w, r, "flame", claims.OrgID, reqErr.Status, reqErr.Message)
				return
			}
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:800-801) -- any error that is
			// not one of build_flame_response's own typed HTTPExceptions
			// degrades to a generic 503, never a raw ClickHouse error on
			// the wire.
			writeRESTDataUnavailable(w, r, "flame", claims.OrgID, err)
			return
		}

		// The success body is the route's response_model, written as
		// pydantic-core dump_json writes it (writeModelResponse).
		if encodeErr := writeModelResponse(w, resp); encodeErr != nil {
			log.Printf("query-api: flame: encode response failed: org_id=%s request_id=%s err=%v",
				claims.OrgID, envelopeRequestID(r), encodeErr)
			writeModelFailure(w)
		}
	}
}
