// GET /api/v1/filters/options -- the Go REST handler mirroring
// quadrant_route.go's shape for a REST route mounted on query-api's mux,
// gated by its own routeswitch DynamicSwitch entry (a path-keyed
// operation, same reasoning as quadrantOperation's doc comment: a REST
// route has no GraphQL document to digest, so it is not
// PostgresSwitch-backed).
//
// Auth: the same bearer-envelope verifier /query, /api/v1/investment/
// explain and /api/v1/quadrant use (principal.Verifier, authctx.Claims
// carrying OrgID). Python's route additionally rate-limits
// ("60/minute", main.py:1456); this port carries no rate limiter, the
// same documented gap every other ported route in this binary carries
// (query-api has no rate-limiting mechanism anywhere yet).
//
// The route takes no query parameters and returns no typed errors --
// Python's filter_options handler wraps its whole body in a single
// try/except that maps ANY exception to 503 "Data unavailable"
// (main.py:1455-1467), so this port's only two outcomes are 200 with the
// full FilterOptionsResponse shape or 503.
//
// Business logic (the five ClickHouse reads, the email-shape filter on
// developers, the static investment-taxonomy work_category list, and the
// declared ReplacingMergeTree dedup fixes) lives in internal/filteroptions
// -- see that package's doc comment for the full parity contract.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/filteroptions"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
)

// filterOptionsOperation is this route's routeswitch operation name --
// a PATH-keyed entry, same convention as quadrantOperation.
const filterOptionsOperation = "REST:GET:/api/v1/filters/options"

// filterOptionsEnabledEnvVar is the operator-facing toggle. Unset or
// anything other than a true-ish value leaves the route disabled,
// matching DynamicSwitch's own default.
const filterOptionsEnabledEnvVar = "GO_API_FILTER_OPTIONS_ENABLED"

func filterOptionsSwitchFromEnv() *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(os.Getenv(filterOptionsEnabledEnvVar)); enabled {
		sw.Set(filterOptionsOperation, true)
	}
	return sw
}

// loadFilterOptionsRouteConfig reads this route's own dependency set --
// ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig: like quadrant, this route never touches registry
// Postgres, so requiring GO_API_REGISTRY_POSTGRES_URI just to mount a pure
// ClickHouse read route would be an unrelated-dependency coupling.
func loadFilterOptionsRouteConfig() (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = os.Getenv("CLICKHOUSE_URI")
	jwksPath = os.Getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = os.Getenv("GO_API_ENVELOPE_ISSUER")
	audience = os.Getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildFilterOptionsRoute constructs the handler, its own routeswitch Mux,
// and a cleanup function. ok is false when this route's dependencies are
// not configured -- main() only calls mux.HandleFunc when ok is true, same
// "stay unmounted, don't fail to build/start" contract every other
// optionally-configured route in this binary follows.
func buildFilterOptionsRoute() (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadFilterOptionsRouteConfig()
	if !cfgOK {
		return nil, nil, false, nil
	}

	verifier, err := principal.NewVerifier(jwksPath, issuer, audience)
	if err != nil {
		return nil, nil, false, fmt.Errorf("filteroptions: build envelope verifier: %w", err)
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(clickHouseURI))
	if err != nil {
		return nil, nil, false, fmt.Errorf("filteroptions: build read client: %w", err)
	}

	routeMux := routeswitch.NewMux(filterOptionsSwitchFromEnv())
	routeMux.Register(filterOptionsOperation, newFilterOptionsWorkHandler(readClient))

	// Auth runs BEFORE Dispatch, same order every other route in this
	// binary uses: resolve operation, then auth, then Dispatch -- claims
	// travel to the registered handler via the request context.
	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTMethodNotAllowed(w, r, "filteroptions")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, "filteroptions")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(filterOptionsOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// newFilterOptionsWorkHandler is the routeswitch-registered handler --
// reached only after buildFilterOptionsRoute's entryHandler has already
// authenticated the request and attached authctx.Claims to its context.
func newFilterOptionsWorkHandler(client filteroptions.QueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only: buildFilterOptionsRoute's entryHandler always
			// authenticates and attaches claims before Dispatch reaches this
			// handler -- see authenticateRESTRequest's own doc comment for
			// the three real 401 shapes this route actually answers.
			writeRESTUnauthorized(w, r, "filteroptions", "Not authenticated")
			return
		}

		resp, err := filteroptions.BuildResponse(r.Context(), client, claims.OrgID)
		if err != nil {
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:1466-1467) -- any ClickHouse
			// failure degrades to a generic 503, never a raw error on the
			// wire.
			writeRESTDataUnavailable(w, r, "filteroptions", claims.OrgID)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// json.NewEncoder(w).Encode is this repo's JSON-response path
		// (internal/auth/httpapi/envelope.go's WriteError is the
		// precedent) -- never a raw w.Write of pre-marshalled bytes. The
		// 200 status is already on the wire by the time Encode runs, so a
		// failure here cannot change what the client sees -- but it is
		// NOT silently dropped: it is logged with the same request-scoped
		// fields query_route.go's digest-miss log line uses (org_id, the
		// caller-supplied X-Request-Id), so an operator can correlate a
		// truncated or aborted response back to the request that produced
		// it instead of it vanishing.
		if encodeErr := json.NewEncoder(w).Encode(resp); encodeErr != nil {
			log.Printf("query-api: filteroptions: encode response failed: org_id=%s request_id=%s err=%v",
				claims.OrgID, envelopeRequestID(r), encodeErr)
		}
	}
}
