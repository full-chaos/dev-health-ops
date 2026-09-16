// GET /api/v1/quadrant, CHAOS-5550 -- the Go REST handler mirroring
// investment_explain_route.go's shape for a REST route mounted on
// query-api's mux, gated by its own routeswitch DynamicSwitch entry
// (a path-keyed operation, same reasoning as
// investmentExplainOperation's doc comment: a REST route has no GraphQL
// document to digest, so it is not PostgresSwitch-backed).
//
// Auth: the same bearer-envelope verifier /query and /api/v1/investment/
// explain use (principal.Verifier, authctx.Claims carrying OrgID) --
// never a hand-rolled claims check, per this repo's established
// chokepoint. Python's route additionally rate-limits ("60/minute", main.py
// :881); this port carries no rate limiter, the same documented gap
// investment_explain_route.go already carries for its own route (this
// service has no rate-limiting mechanism anywhere yet).
//
// Business logic (the resolver, the four quadrant definitions, the
// ClickHouse readers, the team/repo/person scope split, the
// cycle_throughput attribution quirk, and the person-scope identity
// resolution) lives in internal/quadrant -- see that package's doc
// comment for the full parity contract.
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
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/quadrant"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
)

// quadrantOperation is this route's routeswitch operation name -- a
// PATH-keyed entry, same convention as investmentExplainOperation.
const quadrantOperation = "REST:GET:/api/v1/quadrant"

// quadrantEnabledEnvVar is the operator-facing toggle. Unset or anything
// other than a true-ish value leaves the route disabled, matching
// DynamicSwitch's own default.
const quadrantEnabledEnvVar = "GO_API_QUADRANT_ENABLED"

func quadrantSwitchFromEnv() *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(os.Getenv(quadrantEnabledEnvVar)); enabled {
		sw.Set(quadrantOperation, true)
	}
	return sw
}

// loadQuadrantRouteConfig reads this route's own dependency set --
// ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig: this route never touches registry Postgres (no
// org-BYO settings, no registry lookup at all), so requiring
// GO_API_REGISTRY_POSTGRES_URI just to mount a pure ClickHouse read
// route would be an unrelated-dependency coupling, not a real
// requirement.
func loadQuadrantRouteConfig() (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = os.Getenv("CLICKHOUSE_URI")
	jwksPath = os.Getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = os.Getenv("GO_API_ENVELOPE_ISSUER")
	audience = os.Getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildQuadrantRoute constructs the handler, its own routeswitch Mux, and
// a cleanup function. ok is false when this route's dependencies are not
// configured -- main() only calls mux.HandleFunc when ok is true, same
// "stay unmounted, don't fail to build/start" contract every other
// optionally-configured route in this binary follows.
func buildQuadrantRoute() (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadQuadrantRouteConfig()
	if !cfgOK {
		return nil, nil, false, nil
	}

	verifier, err := principal.NewVerifier(jwksPath, issuer, audience)
	if err != nil {
		return nil, nil, false, fmt.Errorf("quadrant: build envelope verifier: %w", err)
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(clickHouseURI))
	if err != nil {
		return nil, nil, false, fmt.Errorf("quadrant: build read client: %w", err)
	}

	routeMux := routeswitch.NewMux(quadrantSwitchFromEnv())
	routeMux.Register(quadrantOperation, newQuadrantWorkHandler(readClient))

	// Auth runs BEFORE Dispatch, same order every other route in this
	// binary uses (query_route.go, investment_explain_route.go): resolve
	// operation, then auth, then Dispatch -- claims travel to the
	// registered handler via the request context.
	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTMethodNotAllowed(w, r, "quadrant")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, "quadrant")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(quadrantOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// newQuadrantWorkHandler is the routeswitch-registered handler -- reached
// only after buildQuadrantRoute's entryHandler has already authenticated
// the request and attached authctx.Claims to its context.
//
// Validation error aggregation matches FastAPI's own solve_dependencies
// exactly (live-captured, see pydantic_validation_error.go and this PR's
// TEST-EVIDENCE): every one of type/range_days/start_date/end_date is
// checked, and every failing one is reported together in a single 422,
// in the SAME order Python's endpoint signature declares them
// (type, scope_type, scope_id, range_days, start_date, end_date, bucket)
// -- never a fail-fast single-error 400 on the first bad field.
func newQuadrantWorkHandler(client quadrant.QueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only: buildQuadrantRoute's entryHandler always
			// authenticates and attaches claims before Dispatch reaches this
			// handler, so this branch has no live Python counterpart -- see
			// authenticateRESTRequest's own doc comment for the three real
			// 401 shapes this route actually answers.
			writeRESTUnauthorized(w, r, "quadrant", "Not authenticated")
			return
		}

		query := r.URL.Query()
		var validationErrors []pydanticErrorDetail

		quadrantType := query.Get("type")
		if !query.Has("type") {
			validationErrors = append(validationErrors, missingFieldError([]any{"query", "type"}, nil))
		}
		scopeType := query.Get("scope_type")
		if scopeType == "" {
			scopeType = "org"
		}
		bucket := query.Get("bucket")
		if bucket == "" {
			bucket = "week"
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

		startDate, startPresent, startOK := parseISODateQueryParam(query.Get("start_date"))
		if startPresent && !startOK {
			validationErrors = append(validationErrors, dateQueryParamError([]any{"query", "start_date"}, query.Get("start_date")))
		}
		endDate, endPresent, endOK := parseISODateQueryParam(query.Get("end_date"))
		if endPresent && !endOK {
			validationErrors = append(validationErrors, dateQueryParamError([]any{"query", "end_date"}, query.Get("end_date")))
		}

		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		params := quadrant.Params{
			Type:      quadrantType,
			ScopeType: scopeType,
			ScopeID:   query.Get("scope_id"),
			RangeDays: rangeDays,
			Bucket:    bucket,
		}
		if startPresent {
			params.StartDate = &startDate
		}
		if endPresent {
			params.EndDate = &endDate
		}

		resp, err := quadrant.BuildResponse(r.Context(), client, claims.OrgID, params)
		if err != nil {
			if reqErr, ok := quadrant.AsRequestError(err); ok {
				writeRESTError(w, r, "quadrant", claims.OrgID, reqErr.Status, reqErr.Message)
				return
			}
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:906-909) -- any error that is
			// not one of build_quadrant_response's own typed HTTPExceptions
			// (unknown type, invalid scope/bucket, unsupported metric)
			// degrades to a generic 503, never a raw ClickHouse error on
			// the wire.
			writeRESTDataUnavailable(w, r, "quadrant", claims.OrgID)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// json.NewEncoder(w).Encode is this repo's JSON-response path
		// (filter_options_route.go's newFilterOptionsWorkHandler is the
		// precedent this route now matches) -- never a raw w.Write of
		// pre-marshalled bytes. The 200 status is already on the wire by
		// the time Encode runs, so a failure here cannot change what the
		// client sees -- but it is NOT silently dropped: it is logged with
		// the same request-scoped fields (org_id, the caller-supplied
		// X-Request-Id) so an operator can correlate a truncated or
		// aborted response back to the request that produced it.
		if encodeErr := json.NewEncoder(w).Encode(resp); encodeErr != nil {
			log.Printf("query-api: quadrant: encode response failed: org_id=%s request_id=%s err=%v",
				claims.OrgID, envelopeRequestID(r), encodeErr)
		}
	}
}
