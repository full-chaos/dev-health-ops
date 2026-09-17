// GET /api/v1/people/{person_id}/summary -- the Go REST
// handler mirroring quadrant_route.go/people_route.go's shape for a REST
// route mounted on query-api's mux, gated by its own routeswitch entry.
// This is the first PATH-PARAMETER route this binary mounts: Go 1.22+'s
// net/http.ServeMux natively supports a "{person_id}" wildcard segment in
// a plain (non-method-prefixed) pattern string, read back via
// r.PathValue("person_id") -- see internal/migrationmatrix/
// restendpoints_test.go's own pre-existing "/api/v1/work-units/
// {work_unit_id}/explain" fixture, which already proves
// LoadFastAPIRoutes/LoadQueryAPIMuxRoutes/LoadRESTEndpoints treat a
// "{...}" segment as an ordinary literal-string match (mux.HandleFunc's
// own registration string, read verbatim by muxHandleFuncRe) -- no
// matcher change was needed for this route to render "ported" on the
// migration matrix.
//
// Auth: the same bearer-envelope verifier every other REST route in this
// binary uses. Python's route additionally rate-limits ("60/minute",
// main.py:1069); this port carries no rate limiter, the same documented
// gap every sibling REST route in this binary already carries.
//
// Business logic (identity resolution, the freshness/coverage/per-metric
// ClickHouse reads, the ReplacingMergeTree dedup) lives in internal/people
// -- see summary.go's own doc comment for the full parity contract.
//
// ERROR BODIES: every non-2xx response uses this binary's one shared REST
// error-response path (rest_error_response.go), same as every sibling
// REST route.
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

// peopleSummaryPath is the literal path pattern this route mounts --
// used for BOTH mux.HandleFunc (main.go) and the routeswitch operation
// name, so the two can never drift apart.
const peopleSummaryPath = "/api/v1/people/{person_id}/summary"

// peopleSummaryOperation is this route's routeswitch operation name -- a
// PATH+METHOD-keyed entry, same convention as peopleSearchOperation.
const peopleSummaryOperation = "REST:GET:" + peopleSummaryPath

// peopleSummaryEnabledEnvVar is the operator-facing toggle -- unset or
// anything other than a true-ish value leaves the route disabled,
// matching DynamicSwitch's own default.
const peopleSummaryEnabledEnvVar = "GO_API_PEOPLE_SUMMARY_ENABLED"

func peopleSummarySwitchFromEnv() *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(os.Getenv(peopleSummaryEnabledEnvVar)); enabled {
		sw.Set(peopleSummaryOperation, true)
	}
	return sw
}

// loadPeopleSummaryRouteConfig reads this route's own dependency set --
// ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig: this route never touches registry Postgres,
// same reasoning as peopleSearchRoute's own doc comment.
func loadPeopleSummaryRouteConfig() (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = os.Getenv("CLICKHOUSE_URI")
	jwksPath = os.Getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = os.Getenv("GO_API_ENVELOPE_ISSUER")
	audience = os.Getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildPeopleSummaryRoute constructs the handler, its own routeswitch
// Mux, and a cleanup function. ok is false when this route's dependencies
// are not configured -- main() only calls mux.HandleFunc when ok is
// true, same "stay unmounted, don't fail to build/start" contract every
// other optionally-configured route in this binary follows.
func buildPeopleSummaryRoute() (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadPeopleSummaryRouteConfig()
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

	routeMux := routeswitch.NewMux(peopleSummarySwitchFromEnv())
	routeMux.Register(peopleSummaryOperation, newPeopleSummaryHandler(reader))

	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTMethodNotAllowed(w, r, "people_summary")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, "people_summary")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(peopleSummaryOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// writePeopleSummaryResponse writes resp as the final 200 JSON body via
// json.NewEncoder(w).Encode -- this repo's JSON-response path, never a
// raw w.Write of pre-marshalled bytes.
func writePeopleSummaryResponse(w http.ResponseWriter, r *http.Request, orgID string, resp people.SummaryResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if encodeErr := json.NewEncoder(w).Encode(resp); encodeErr != nil {
		log.Printf("query-api: people_summary: encode response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
	}
}

// newPeopleSummaryHandler is the routeswitch-registered handler for
// GET /api/v1/people/{person_id}/summary -- the Go port of people_summary
// (api/main.py:1068-1089). Reached only after buildPeopleSummaryRoute's
// entryHandler has already authenticated the request and attached
// authctx.Claims.
//
// Validation-error precedence matches peopleSearchHandler's own doc
// comment: FastAPI/Pydantic resolves range_days/compare_days at the
// framework level before _reject_comparative_params (main.py:1077) ever
// runs inside the route function body.
func newPeopleSummaryHandler(reader *people.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "people_summary", "Not authenticated")
			return
		}

		personID := r.PathValue("person_id")
		query := r.URL.Query()
		var validationErrors []pydanticErrorDetail

		rangeDays := 14
		if raw := query.Get("range_days"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "range_days"}, raw))
			} else {
				rangeDays = parsed
			}
		}
		compareDays := 14
		if raw := query.Get("compare_days"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "compare_days"}, raw))
			} else {
				compareDays = parsed
			}
		}

		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		// _reject_comparative_params (main.py:202-208): ANY of these
		// query-param KEYS present is a 400, checked only after every
		// query param above already parsed cleanly.
		for key := range query {
			if peopleForbiddenQueryParams[key] {
				writeRESTError(w, r, "people_summary", claims.OrgID, http.StatusBadRequest, "Comparative parameters are not supported.")
				return
			}
		}

		resp, err := people.BuildSummaryResponse(r.Context(), reader, claims.OrgID, people.SummaryParams{
			PersonID:    personID,
			RangeDays:   rangeDays,
			CompareDays: compareDays,
			Now:         time.Now().UTC(),
		})
		if err != nil {
			if reqErr, ok := people.AsRequestError(err); ok {
				writeRESTError(w, r, "people_summary", claims.OrgID, reqErr.Status, reqErr.Message)
				return
			}
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:1088-1089).
			writeRESTDataUnavailable(w, r, "people_summary", claims.OrgID, err)
			return
		}
		writePeopleSummaryResponse(w, r, claims.OrgID, resp)
	}
}
