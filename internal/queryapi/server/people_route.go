// GET /api/v1/people -- the Go REST handler mirroring
// quadrant_route.go/drilldown_prs_route.go's shape for a REST route
// mounted on query-api's mux, gated by its own routeswitch entry.
//
// Auth: the same bearer-envelope verifier every other REST route in this
// binary uses. Python's route additionally rate-limits ("60/minute",
// main.py:1049); this port carries no rate limiter, the same documented
// gap quadrant_route.go/drilldown_prs_route.go already carry (this
// service has no rate-limiting mechanism anywhere yet).
//
// Business logic (identity-alias resolution, the ClickHouse read, the
// ReplacingMergeTree dedup) lives in internal/people -- see that
// package's own doc comment for the full parity contract.
//
// ERROR BODIES: every non-2xx response this route sends uses Python's
// own FastAPI/Starlette contract -- {"detail": ...} JSON, never a plain-
// text body -- via this binary's one shared REST error-response path
// (rest_error_response.go: writeRESTError/writeRESTUnauthorized/
// writeRESTMethodNotAllowed/writeRESTDataUnavailable/
// authenticateRESTRequest), the same helper every sibling REST route in
// this binary uses. This route keeps no local copy of that envelope.
package server

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// peopleSearchOperation is this route's routeswitch operation name -- a
// PATH+METHOD-keyed entry, same convention as quadrantOperation/
// drilldownPRsGetOperation (a REST route has no GraphQL document to
// digest).
const peopleSearchOperation = "REST:GET:/api/v1/people"

// peopleSearchEnabledEnvVar is the operator-facing toggle -- unset or
// anything other than a true-ish value leaves the route disabled,
// matching DynamicSwitch's own default.
const peopleSearchEnabledEnvVar = "GO_API_PEOPLE_SEARCH_ENABLED"

// peopleForbiddenQueryParams ports _FORBIDDEN_QUERY_PARAMS
// (main.py:167-175), checked by _reject_comparative_params (main.py:
// 202-208) before the route's own body runs.
var peopleForbiddenQueryParams = map[string]bool{
	"compare_to":  true,
	"rank":        true,
	"percentile":  true,
	"score":       true,
	"leaderboard": true,
	"top":         true,
	"bottom":      true,
}

func peopleSearchSwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(peopleSearchEnabledEnvVar)); enabled {
		sw.Set(peopleSearchOperation, true)
	}
	return sw
}

// loadPeopleSearchRouteConfig reads this route's own dependency set --
// ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig, same reasoning as quadrant_route.go/
// drilldown_prs_route.go's own doc comments: this route never touches
// registry Postgres.
func loadPeopleSearchRouteConfig(getenv getenvFunc) (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = getenv("CLICKHOUSE_URI")
	jwksPath = getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = getenv("GO_API_ENVELOPE_ISSUER")
	audience = getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildPeopleSearchRoute constructs the handler, its own routeswitch Mux,
// and a cleanup function. ok is false when this route's dependencies are
// not configured -- main() only calls mux.HandleFunc when ok is true,
// same "stay unmounted, don't fail to build/start" contract every other
// optionally-configured route in this binary follows.
func buildPeopleSearchRoute(getenv getenvFunc) (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadPeopleSearchRouteConfig(getenv)
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

	reader, err := people.NewReader(readClient)
	if err != nil {
		_ = readClient.Close()
		return nil, nil, false, err
	}

	routeMux := routeswitch.NewMux(peopleSearchSwitchFromEnv(getenv))
	routeMux.Register(peopleSearchOperation, newPeopleSearchHandler(reader))

	// Auth runs BEFORE Dispatch, same order every other route in this
	// binary uses. authenticateRESTRequest (rest_error_response.go) is the
	// ONE place get_current_user's three 401 branches are classified --
	// this route keeps no local copy of that distinction.
	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTMethodNotAllowed(w, r, "people")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "people")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(peopleSearchOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// writePeopleSearchResponse writes resp as the final 200 JSON body via
// json.NewEncoder(w).Encode -- this repo's JSON-response path, never a
// raw w.Write of pre-marshalled bytes.
func writePeopleSearchResponse(w http.ResponseWriter, r *http.Request, orgID string, resp []people.SearchResult) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if encodeErr := json.NewEncoder(w).Encode(resp); encodeErr != nil {
		log.Printf("query-api: people: encode response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
	}
}

// newPeopleSearchHandler is the routeswitch-registered handler for
// GET /api/v1/people -- the Go port of people_search (api/main.py:
// 1048-1065). Reached only after buildPeopleSearchRoute's entryHandler
// has already authenticated the request and attached authctx.Claims.
func newPeopleSearchHandler(reader *people.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only: buildPeopleSearchRoute's entryHandler always
			// authenticates and attaches claims before Dispatch reaches this
			// handler -- see authenticateRESTRequest's own doc comment for
			// the three real 401 shapes this route actually answers.
			writeRESTUnauthorized(w, r, "people", "Not authenticated")
			return
		}

		query := r.URL.Query()

		// Order matters here, confirmed live: FastAPI/Pydantic resolves
		// query-PARAMETER dependencies (limit's `int = 20`) at the
		// framework level BEFORE the route function body ever runs, so a
		// malformed limit answers 422 even when a forbidden comparative
		// param is ALSO present on the same request -- _reject_comparative_params
		// (main.py:202-208, main.py:1056) only runs once execution
		// actually reaches the function body. This port must check limit
		// first for the same reason.
		q := query.Get("q")

		limit := 20
		if raw := query.Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				// Python's `limit: int = 20` is FastAPI/Pydantic
				// query-param validation, not a handler-level try/except:
				// a non-numeric value never reaches the handler at all,
				// it is rejected up front as 422 -- same shape
				// drilldown_prs_route.go's range_days already
				// establishes for this binary.
				writePydanticValidationError(w, r, claims.OrgID, intQueryParamError([]any{"query", "limit"}, raw))
				return
			}
			limit = parsed
		}

		// _reject_comparative_params (main.py:202-208): ANY of these
		// query-param KEYS present -- regardless of value, and regardless
		// of whether this endpoint's own signature declares that param --
		// is a 400.
		for key := range query {
			if peopleForbiddenQueryParams[key] {
				writeRESTError(w, r, "people", claims.OrgID, http.StatusBadRequest, "Comparative parameters are not supported.")
				return
			}
		}

		// Python bounds limit TWICE with the same max (main.py:1061's
		// `_bounded_limit_param(limit, 50)`, then services/people.py:411's
		// `_bounded_limit(limit, _MAX_SEARCH_LIMIT)` inside
		// search_people_response) -- see internal/people.SearchParams.Limit's
		// own doc comment for why this port applies the bound only once,
		// inside BuildSearchResponse, rather than reproducing the
		// redundant double-clamp here.
		resp, err := people.BuildSearchResponse(r.Context(), reader, claims.OrgID, people.SearchParams{
			Query: q,
			Limit: limit,
			Now:   time.Now().UTC(),
		})
		if err != nil {
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:1064-1065).
			writeRESTDataUnavailable(w, r, "people", claims.OrgID, err)
			return
		}
		writePeopleSearchResponse(w, r, claims.OrgID, resp)
	}
}
