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
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/people"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
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

func peopleSearchSwitchFromEnv() *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(os.Getenv(peopleSearchEnabledEnvVar)); enabled {
		sw.Set(peopleSearchOperation, true)
	}
	return sw
}

// loadPeopleSearchRouteConfig reads this route's own dependency set --
// ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig, same reasoning as quadrant_route.go/
// drilldown_prs_route.go's own doc comments: this route never touches
// registry Postgres.
func loadPeopleSearchRouteConfig() (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = os.Getenv("CLICKHOUSE_URI")
	jwksPath = os.Getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = os.Getenv("GO_API_ENVELOPE_ISSUER")
	audience = os.Getenv("GO_API_ENVELOPE_AUDIENCE")
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
func buildPeopleSearchRoute() (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadPeopleSearchRouteConfig()
	if !cfgOK {
		return nil, nil, false, nil
	}

	verifier, err := principal.NewVerifier(jwksPath, issuer, audience)
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

	routeMux := routeswitch.NewMux(peopleSearchSwitchFromEnv())
	routeMux.Register(peopleSearchOperation, newPeopleSearchHandler(reader))

	// Auth runs BEFORE Dispatch, same order every other route in this
	// binary uses.
	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		verifyCtx := principal.WithRequestMeta(r.Context(), r.RemoteAddr, envelopeRequestID(r))
		claims, err := verifier.Verify(verifyCtx, token)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), authctx.Claims{OrgID: claims.OrgID}))
		routeMux.Dispatch(peopleSearchOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// writePeopleDetailError writes a Starlette-shaped `{"detail": message}`
// JSON body -- fastapi.exception_handlers.http_exception_handler's exact
// envelope for an HTTPException with no custom handler registered
// (confirmed live: `JSONResponse({"detail": exc.detail}, status_code=
// exc.status_code)`), which is what BOTH of this route's own typed
// HTTPExceptions (_reject_comparative_params's 400, the outer 503
// fallback) produce on the Python side. This is a NEW, more literal port
// than quadrant_route.go/drilldown_prs_route.go's own plain-text
// http.Error for their equivalent 503 fallback -- not a fix applied to
// those already-shipped routes, just this route's own choice to reproduce
// the verified body shape now that it is known.
func writePeopleDetailError(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"detail": detail})
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
			http.Error(w, "unauthorized", http.StatusUnauthorized)
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
				writePeopleDetailError(w, http.StatusBadRequest, "Comparative parameters are not supported.")
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
			writePeopleDetailError(w, http.StatusServiceUnavailable, "Data unavailable")
			return
		}
		writePeopleSearchResponse(w, r, claims.OrgID, resp)
	}
}
