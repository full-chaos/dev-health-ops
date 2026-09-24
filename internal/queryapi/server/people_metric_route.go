// GET /api/v1/people/{person_id}/metric -- the Go REST
// handler mirroring people_summary_route.go's shape for a REST route
// mounted on query-api's mux, gated by its own routeswitch entry. See
// people_summary_route.go's own package doc comment for the path-
// parameter mechanism (r.PathValue) and the migration-matrix matcher
// note -- both apply identically here.
//
// Auth: the same bearer-envelope verifier every other REST route in this
// binary uses. Unlike EVERY sibling REST route in this file's family,
// Python's people_metric carries NO `@limiter.limit(...)` decorator at
// all (main.py:1092-1093, confirmed by its absence) -- so there is no
// rate-limiter gap to document for this one route (this binary has no
// rate-limiting mechanism regardless).
//
// Business logic lives in internal/people -- see metric.go's own doc
// comment for the full parity contract.
package server

import (
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/timewindow"
)

// peopleMetricPath is the literal path pattern this route mounts.
const peopleMetricPath = "/api/v1/people/{person_id}/metric"

// peopleMetricOperation is this route's routeswitch operation name.
const peopleMetricOperation = "REST:GET:" + peopleMetricPath

// peopleMetricEnabledEnvVar is the operator-facing toggle.
const peopleMetricEnabledEnvVar = "GO_API_PEOPLE_METRIC_ENABLED"

func peopleMetricSwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(peopleMetricEnabledEnvVar)); enabled {
		sw.Set(peopleMetricOperation, true)
	}
	return sw
}

// loadPeopleMetricRouteConfig reads this route's own dependency set,
// same reasoning as loadPeopleSummaryRouteConfig's own doc comment.
func loadPeopleMetricRouteConfig(getenv getenvFunc) (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = getenv("CLICKHOUSE_URI")
	jwksPath = getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = getenv("GO_API_ENVELOPE_ISSUER")
	audience = getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildPeopleMetricRoute constructs the handler, its own routeswitch
// Mux, and a cleanup function -- same "stay unmounted, don't fail to
// build/start" contract every other optionally-configured route in this
// binary follows.
func buildPeopleMetricRoute(getenv getenvFunc) (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadPeopleMetricRouteConfig(getenv)
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

	routeMux := routeswitch.NewMux(peopleMetricSwitchFromEnv(getenv))
	routeMux.Register(peopleMetricOperation, newPeopleMetricHandler(reader))

	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTMethodNotAllowed(w, r, "people_metric")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "people_metric")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(peopleMetricOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// writePeopleMetricResponse writes resp as the final 200 JSON body via
// json.NewEncoder(w).Encode.
func writePeopleMetricResponse(w http.ResponseWriter, r *http.Request, orgID string, resp people.MetricResponse) {
	if encodeErr := writeModelResponse(w, resp); encodeErr != nil {
		log.Printf("query-api: people_metric: encode response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
		writeModelFailure(w)
	}
}

// newPeopleMetricHandler is the routeswitch-registered handler for
// GET /api/v1/people/{person_id}/metric -- the Go port of people_metric
// (api/main.py:1092-1120). Reached only after buildPeopleMetricRoute's
// entryHandler has already authenticated the request and attached
// authctx.Claims.
//
// Validation-error precedence and field order matches Python's own
// signature declaration order (person_id is a path param, resolved
// before FastAPI even attempts query-param validation; then metric,
// range_days, compare_days, main.py:1093-1098): metric is a REQUIRED
// query param (`metric: str`, no default) -- its absence is a
// "missing"-type 422 detail, aggregated together with any malformed
// range_days/compare_days in the SAME response, all BEFORE
// _reject_comparative_params (main.py:1101) ever runs.
func newPeopleMetricHandler(reader *people.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "people_metric", "Not authenticated")
			return
		}

		personID := r.PathValue("person_id")
		query := r.URL.Query()
		var validationErrors []pydanticErrorDetail

		metric := lastQueryValue(query, "metric")
		if !query.Has("metric") {
			validationErrors = append(validationErrors, missingFieldError([]any{"query", "metric"}, nil))
		}

		rangeDays := 14
		// An explicit empty value is still parsed (pydantic: int_parsing).
		if query.Has("range_days") {
			raw := lastQueryValue(query, "range_days")
			parsed, parseErr := parseQueryInt([]any{"query", "range_days"}, raw)
			if parseErr != nil {
				validationErrors = append(validationErrors, *parseErr)
			} else {
				rangeDays = parsed
			}
		}
		compareDays := 14
		// An explicit empty value is still parsed (pydantic: int_parsing).
		if query.Has("compare_days") {
			raw := lastQueryValue(query, "compare_days")
			parsed, parseErr := parseQueryInt([]any{"query", "compare_days"}, raw)
			if parseErr != nil {
				validationErrors = append(validationErrors, *parseErr)
			} else {
				compareDays = parsed
			}
		}

		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		for key := range query {
			if peopleForbiddenQueryParams[key] {
				writeRESTError(w, r, "people_metric", claims.OrgID, http.StatusBadRequest, "Comparative parameters are not supported.")
				return
			}
		}

		resp, err := people.BuildMetricResponse(r.Context(), reader, claims.OrgID, people.MetricParams{
			PersonID:    personID,
			Metric:      metric,
			RangeDays:   rangeDays,
			CompareDays: compareDays,
			Now:         time.Now().UTC(),
		})
		if err != nil {
			if errors.Is(err, timewindow.ErrOverflow) {
				writeTimeWindowOverflow(w, r, "people_metric", claims.OrgID)
				return
			}
			if reqErr, ok := people.AsRequestError(err); ok {
				writeRESTError(w, r, "people_metric", claims.OrgID, reqErr.Status, reqErr.Message)
				return
			}
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:1119-1120).
			writeRESTDataUnavailable(w, r, "people_metric", claims.OrgID, err)
			return
		}
		writePeopleMetricResponse(w, r, claims.OrgID, resp)
	}
}
