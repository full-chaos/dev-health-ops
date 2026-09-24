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
package server

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/quadrant"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/timewindow"
)

// quadrantOperation is this route's routeswitch operation name -- a
// PATH-keyed entry, same convention as investmentExplainOperation.
const quadrantOperation = "REST:GET:/api/v1/quadrant"

// quadrantEnabledEnvVar is the operator-facing toggle. Unset or anything
// other than a true-ish value leaves the route disabled, matching
// DynamicSwitch's own default.
const quadrantEnabledEnvVar = "GO_API_QUADRANT_ENABLED"

func quadrantSwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(quadrantEnabledEnvVar)); enabled {
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
func loadQuadrantRouteConfig(getenv getenvFunc) (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = getenv("CLICKHOUSE_URI")
	jwksPath = getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = getenv("GO_API_ENVELOPE_ISSUER")
	audience = getenv("GO_API_ENVELOPE_AUDIENCE")
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
func buildQuadrantRoute(getenv getenvFunc) (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadQuadrantRouteConfig(getenv)
	if !cfgOK {
		return nil, nil, false, nil
	}

	verifier, err := principal.NewVerifier(jwksPath, issuer, audience)
	if err != nil {
		return nil, nil, false, fmt.Errorf("quadrant: build envelope verifier: %w", err)
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
		return nil, nil, false, fmt.Errorf("quadrant: build read client: %w", err)
	}

	routeMux := routeswitch.NewMux(quadrantSwitchFromEnv(getenv))
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
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "quadrant")
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

		quadrantType := lastQueryValue(query, "type")
		if !query.Has("type") {
			validationErrors = append(validationErrors, missingFieldError([]any{"query", "type"}, nil))
		}
		scopeType := lastQueryValue(query, "scope_type")
		if scopeType == "" {
			scopeType = "org"
		}
		bucket := lastQueryValue(query, "bucket")
		if bucket == "" {
			bucket = "week"
		}
		rangeDays := 30
		if query.Has("range_days") {
			raw := lastQueryValue(query, "range_days")
			if parsed, parseErr := parseQueryInt([]any{"query", "range_days"}, raw); parseErr == nil {
				rangeDays = parsed
			} else {
				validationErrors = append(validationErrors, *parseErr)
			}
		}

		startDate, startPresent, startOK := parseISODateQueryParam(lastQueryValue(query, "start_date"))
		if startPresent && !startOK {
			validationErrors = append(validationErrors, dateQueryParamError([]any{"query", "start_date"}, lastQueryValue(query, "start_date")))
		}
		endDate, endPresent, endOK := parseISODateQueryParam(lastQueryValue(query, "end_date"))
		if endPresent && !endOK {
			validationErrors = append(validationErrors, dateQueryParamError([]any{"query", "end_date"}, lastQueryValue(query, "end_date")))
		}

		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		// _reject_comparative_params (main.py:893): runs AFTER every
		// FastAPI-signature validation above (framework-level, so it always
		// resolves first) and BEFORE build_quadrant_response -- reuses
		// people_route.go's own peopleForbiddenQueryParams, the same set
		// people_summary_route.go and heatmap_route.go already check.
		for key := range query {
			if peopleForbiddenQueryParams[key] {
				writeRESTError(w, r, "quadrant", claims.OrgID, http.StatusBadRequest, "Comparative parameters are not supported.")
				return
			}
		}

		params := quadrant.Params{
			Type:      quadrantType,
			ScopeType: scopeType,
			ScopeID:   lastQueryValue(query, "scope_id"),
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
			if errors.Is(err, timewindow.ErrOverflow) {
				writeTimeWindowOverflow(w, r, "quadrant", claims.OrgID)
				return
			}
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
			writeRESTDataUnavailable(w, r, "quadrant", claims.OrgID, err)
			return
		}

		// The success body is the route's response_model, written as
		// pydantic-core dump_json writes it (writeModelResponse).
		if encodeErr := writeModelResponse(w, resp); encodeErr != nil {
			log.Printf("query-api: quadrant: encode response failed: org_id=%s request_id=%s err=%v",
				claims.OrgID, envelopeRequestID(r), encodeErr)
			writeModelFailure(w)
		}
	}
}
