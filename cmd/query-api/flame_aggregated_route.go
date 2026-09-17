// GET /api/v1/flame/aggregated -- the Go REST handler mirroring
// quadrant_route.go/flame_route.go's shape for a REST route
// mounted on query-api's mux, gated by its own routeswitch DynamicSwitch
// entry (a path-keyed operation, same reasoning as quadrantOperation's
// doc comment: a REST route has no GraphQL document to digest).
//
// Auth: the same bearer-envelope verifier every other REST route in this
// binary uses. Python's route additionally rate-limits ("20/minute",
// main.py:806); this port carries no rate limiter, the same documented
// gap every sibling REST route in this binary already carries (this
// service has no rate-limiting mechanism anywhere yet).
//
// Business logic (the mode dispatch, the three tree builders, the six
// ClickHouse readers) lives in internal/aggflame -- see that package's
// own doc comment for the full parity contract, including its declared
// data-layer style choices and the confirmed Python dead-parameter
// quirks it reproduces verbatim.
//
// ERROR BODIES: every non-2xx response this route sends uses Python's own
// FastAPI/Starlette contract -- {"detail": ...} JSON, never a plain-text
// body -- via this binary's one shared REST error-response path
// (rest_error_response.go). This route keeps no local copy of that
// envelope.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/aggflame"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
)

// flameAggregatedOperation is this route's routeswitch operation name --
// a PATH+METHOD-keyed entry, same convention as flameOperation/
// quadrantOperation.
const flameAggregatedOperation = "REST:GET:/api/v1/flame/aggregated"

// flameAggregatedEnabledEnvVar is the operator-facing toggle -- unset or
// anything other than a true-ish value leaves the route disabled,
// matching DynamicSwitch's own default.
const flameAggregatedEnabledEnvVar = "GO_API_FLAME_AGGREGATED_ENABLED"

// flameAggregatedForbiddenQueryParams ports _FORBIDDEN_QUERY_PARAMS
// (main.py:167-175), checked by _reject_comparative_params (main.py:
// 202-208, called at main.py:836) -- this route's own copy, matching
// flameForbiddenQueryParams' own doc comment on why each REST route file
// keeps its own copy rather than a shared package-level var.
var flameAggregatedForbiddenQueryParams = map[string]bool{
	"compare_to":  true,
	"rank":        true,
	"percentile":  true,
	"score":       true,
	"leaderboard": true,
	"top":         true,
	"bottom":      true,
}

func flameAggregatedSwitchFromEnv() *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(os.Getenv(flameAggregatedEnabledEnvVar)); enabled {
		sw.Set(flameAggregatedOperation, true)
	}
	return sw
}

// loadFlameAggregatedRouteConfig reads this route's own dependency set --
// ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig, same reasoning as quadrant_route.go/
// flame_route.go's own doc comments: this route never touches registry
// Postgres.
func loadFlameAggregatedRouteConfig() (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = os.Getenv("CLICKHOUSE_URI")
	jwksPath = os.Getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = os.Getenv("GO_API_ENVELOPE_ISSUER")
	audience = os.Getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildFlameAggregatedRoute constructs the handler, its own routeswitch
// Mux, and a cleanup function. ok is false when this route's dependencies
// are not configured -- main() only calls mux.HandleFunc when ok is true,
// same "stay unmounted, don't fail to build/start" contract every other
// optionally-configured route in this binary follows.
func buildFlameAggregatedRoute() (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadFlameAggregatedRouteConfig()
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
	edgeVerifier, err := buildEdgeVerifierFromEnv()
	if err != nil {
		return nil, nil, false, err
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(clickHouseURI))
	if err != nil {
		return nil, nil, false, err
	}

	routeMux := routeswitch.NewMux(flameAggregatedSwitchFromEnv())
	routeMux.Register(flameAggregatedOperation, newFlameAggregatedWorkHandler(readClient))

	// Auth runs BEFORE Dispatch, same order every other route in this
	// binary uses.
	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTMethodNotAllowed(w, r, "flame_aggregated")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "flame_aggregated")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(flameAggregatedOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// newFlameAggregatedWorkHandler is the routeswitch-registered handler for
// GET /api/v1/flame/aggregated (main.py:805-873). Reached only after
// buildFlameAggregatedRoute's entryHandler has already authenticated the
// request and attached authctx.Claims.
//
// Validation error aggregation matches FastAPI's own solve_dependencies
// exactly (live-captured behaviour, same convention as quadrant_route.go's
// own doc comment): every one of mode/start_date/end_date/range_days/
// limit/min_value is checked, and every failing one is reported together
// in a single 422, in the SAME order main.py's own flame_aggregated()
// signature declares them (mode, start_date, end_date, range_days,
// team_id, repo_id, provider, work_scope_id, limit, min_value) -- team_id/
// repo_id/provider/work_scope_id are plain `str = ""` params that cannot
// fail Pydantic coercion, so only the other six ever contribute a 422
// detail entry. Only AFTER a fully successful framework-level parse does
// _reject_comparative_params (400) and the mode-enum check (400, owned by
// internal/aggflame.BuildResponse) ever run -- confirmed live for
// /api/v1/people and /api/v1/flame, the same ordering this port reuses.
func newFlameAggregatedWorkHandler(client aggflame.QueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only: buildFlameAggregatedRoute's entryHandler
			// always authenticates and attaches claims before Dispatch
			// reaches this handler -- see authenticateRESTRequest's own
			// doc comment for the three real 401 shapes this route
			// actually answers.
			writeRESTUnauthorized(w, r, "flame_aggregated", "Not authenticated")
			return
		}

		query := r.URL.Query()
		var validationErrors []pydanticErrorDetail

		if !query.Has("mode") {
			validationErrors = append(validationErrors, missingFieldError([]any{"query", "mode"}, nil))
		}
		mode := query.Get("mode")

		startDate, startPresent, startOK := parseISODateQueryParam(query.Get("start_date"))
		if startPresent && !startOK {
			validationErrors = append(validationErrors, dateQueryParamError([]any{"query", "start_date"}, query.Get("start_date")))
		}
		endDate, endPresent, endOK := parseISODateQueryParam(query.Get("end_date"))
		if endPresent && !endOK {
			validationErrors = append(validationErrors, dateQueryParamError([]any{"query", "end_date"}, query.Get("end_date")))
		}

		rangeDays := 30
		if query.Has("range_days") {
			raw := query.Get("range_days")
			if parsed, err := strconv.Atoi(raw); err == nil {
				rangeDays = parsed
			} else {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "range_days"}, raw))
			}
		}

		limit := 500
		if query.Has("limit") {
			raw := query.Get("limit")
			if parsed, err := strconv.Atoi(raw); err == nil {
				limit = parsed
			} else {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "limit"}, raw))
			}
		}

		minValue := 1
		if query.Has("min_value") {
			raw := query.Get("min_value")
			if parsed, err := strconv.Atoi(raw); err == nil {
				minValue = parsed
			} else {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "min_value"}, raw))
			}
		}

		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		// _reject_comparative_params (main.py:202-208, called at
		// main.py:836): ANY of these query-param KEYS present --
		// regardless of value -- is a 400.
		for key := range query {
			if flameAggregatedForbiddenQueryParams[key] {
				writeRESTError(w, r, "flame_aggregated", claims.OrgID, http.StatusBadRequest, "Comparative parameters are not supported.")
				return
			}
		}

		// Date-window resolution (main.py:844-854): end_day defaults to
		// utc_today(), start_day defaults to end_day - range_days.
		endDay := utcToday()
		if endPresent {
			endDay = endDate
		}
		startDay := endDay.AddDate(0, 0, -rangeDays)
		if startPresent {
			startDay = startDate
		}

		// main.py:867-868's own clamps, applied here (the route layer),
		// never inside internal/aggflame -- matching Params' own doc
		// comment.
		if limit < 1 {
			limit = 1
		} else if limit > 1000 {
			limit = 1000
		}
		if minValue < 0 {
			minValue = 0
		}

		resp, err := aggflame.BuildResponse(r.Context(), client, claims.OrgID, aggflame.Params{
			Mode:        mode,
			StartDay:    startDay,
			EndDay:      endDay,
			TeamID:      query.Get("team_id"),
			RepoID:      query.Get("repo_id"),
			Provider:    query.Get("provider"),
			WorkScopeID: query.Get("work_scope_id"),
			Limit:       limit,
			MinValue:    minValue,
		})
		if err != nil {
			if reqErr, ok := aggflame.AsRequestError(err); ok {
				writeRESTError(w, r, "flame_aggregated", claims.OrgID, reqErr.Status, reqErr.Message)
				return
			}
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:871-873) -- any error that is
			// not one of build_aggregated_flame_response's own typed
			// errors degrades to a generic 503, never a raw ClickHouse
			// error on the wire.
			writeRESTDataUnavailable(w, r, "flame_aggregated", claims.OrgID, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// json.NewEncoder(w).Encode is this repo's JSON-response path,
		// never a raw w.Write of pre-marshalled bytes.
		if encodeErr := json.NewEncoder(w).Encode(resp); encodeErr != nil {
			log.Printf("query-api: flame_aggregated: encode response failed: org_id=%s request_id=%s err=%v",
				claims.OrgID, envelopeRequestID(r), encodeErr)
		}
	}
}

// utcToday ports utc_today() (main.py's own helper backing
// flame_aggregated's default end_day) -- today's date at UTC midnight, no
// time-of-day component, matching a Python `date.today()`-shaped value
// used only for day-granularity arithmetic (AddDate below).
func utcToday() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}
