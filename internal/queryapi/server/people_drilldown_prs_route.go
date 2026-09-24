// GET /api/v1/people/{person_id}/drilldown/prs -- the Go REST handler
// mirroring people_summary_route.go/people_metric_route.go's shape for a
// REST route mounted on query-api's mux, gated by its own routeswitch
// entry. See people_summary_route.go's own package doc comment for the
// path-parameter mechanism (r.PathValue) -- it applies identically here.
//
// Auth: the same bearer-envelope verifier every other REST route in this
// binary uses. Python's route additionally rate-limits ("60/minute",
// main.py:1127); this port carries no rate limiter, the same documented
// gap every sibling REST route in this binary already carries.
//
// Business logic (identity resolution, the ClickHouse read, the
// ReplacingMergeTree dedup) lives in internal/people -- see
// drilldownprs.go's own doc comment for the full parity contract.
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

// peopleDrilldownPRsPath is the literal path pattern this route mounts.
const peopleDrilldownPRsPath = "/api/v1/people/{person_id}/drilldown/prs"

// peopleDrilldownPRsOperation is this route's routeswitch operation name.
const peopleDrilldownPRsOperation = "REST:GET:" + peopleDrilldownPRsPath

// peopleDrilldownPRsEnabledEnvVar is the operator-facing toggle.
const peopleDrilldownPRsEnabledEnvVar = "GO_API_PEOPLE_DRILLDOWN_PRS_ENABLED"

func peopleDrilldownPRsSwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(peopleDrilldownPRsEnabledEnvVar)); enabled {
		sw.Set(peopleDrilldownPRsOperation, true)
	}
	return sw
}

// loadPeopleDrilldownPRsRouteConfig reads this route's own dependency set,
// same reasoning as loadPeopleSummaryRouteConfig's own doc comment.
func loadPeopleDrilldownPRsRouteConfig(getenv getenvFunc) (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = getenv("CLICKHOUSE_URI")
	jwksPath = getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = getenv("GO_API_ENVELOPE_ISSUER")
	audience = getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildPeopleDrilldownPRsRoute constructs the handler, its own routeswitch
// Mux, and a cleanup function -- same "stay unmounted, don't fail to
// build/start" contract every other optionally-configured route in this
// binary follows.
func buildPeopleDrilldownPRsRoute(getenv getenvFunc) (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadPeopleDrilldownPRsRouteConfig(getenv)
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

	routeMux := routeswitch.NewMux(peopleDrilldownPRsSwitchFromEnv(getenv))
	routeMux.Register(peopleDrilldownPRsOperation, newPeopleDrilldownPRsHandler(reader))

	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTMethodNotAllowed(w, r, "people_drilldown_prs")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "people_drilldown_prs")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(peopleDrilldownPRsOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// writePeopleDrilldownPRsResponse writes resp as the final 200 JSON body
// via json.NewEncoder(w).Encode -- this repo's JSON-response path, never a
// raw w.Write of pre-marshalled bytes.
func writePeopleDrilldownPRsResponse(w http.ResponseWriter, r *http.Request, orgID string, resp *people.DrilldownPRsResponse) {
	if encodeErr := writeModelResponse(w, resp); encodeErr != nil {
		log.Printf("query-api: people_drilldown_prs: encode response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
		writeModelFailure(w)
	}
}

// newPeopleDrilldownPRsHandler is the routeswitch-registered handler for
// GET /api/v1/people/{person_id}/drilldown/prs -- the Go port of
// people_drilldown_prs (api/main.py:1123-1149). Reached only after
// buildPeopleDrilldownPRsRoute's entryHandler has already authenticated
// the request and attached authctx.Claims.
//
// Validation-error precedence matches peopleSummaryHandler's own doc
// comment: FastAPI/Pydantic resolves range_days/limit/cursor at the
// framework level before _reject_comparative_params (main.py:1136) ever
// runs inside the route function body.
func newPeopleDrilldownPRsHandler(reader *people.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "people_drilldown_prs", "Not authenticated")
			return
		}

		personID := r.PathValue("person_id")
		query := r.URL.Query()
		var validationErrors []pydanticErrorDetail

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
		limit := 50
		if raw := lastQueryValue(query, "limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "limit"}, raw))
			} else {
				limit = parsed
			}
		}
		cursorTime, cursorPresent, cursorOK := parseISODateTimeQueryParam(lastQueryValue(query, "cursor"))
		if cursorPresent && !cursorOK {
			validationErrors = append(validationErrors, dateTimeQueryParamError([]any{"query", "cursor"}, lastQueryValue(query, "cursor")))
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
				writeRESTError(w, r, "people_drilldown_prs", claims.OrgID, http.StatusBadRequest, "Comparative parameters are not supported.")
				return
			}
		}

		var cursorPtr *time.Time
		if cursorPresent {
			cursorPtr = &cursorTime
		}

		resp, err := people.BuildDrilldownPRsResponse(r.Context(), reader, claims.OrgID, people.DrilldownPRsParams{
			PersonID:  personID,
			RangeDays: rangeDays,
			Limit:     limit,
			Cursor:    cursorPtr,
			Now:       time.Now().UTC(),
		})
		if err != nil {
			if errors.Is(err, timewindow.ErrOverflow) {
				writeTimeWindowOverflow(w, r, "people_drilldown_prs", claims.OrgID)
				return
			}
			if reqErr, ok := people.AsRequestError(err); ok {
				writeRESTError(w, r, "people_drilldown_prs", claims.OrgID, reqErr.Status, reqErr.Message)
				return
			}
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:1148-1149).
			writeRESTDataUnavailable(w, r, "people_drilldown_prs", claims.OrgID, err)
			return
		}
		writePeopleDrilldownPRsResponse(w, r, claims.OrgID, resp)
	}
}
