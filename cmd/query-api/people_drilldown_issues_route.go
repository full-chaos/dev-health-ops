// GET /api/v1/people/{person_id}/drilldown/issues -- the Go REST handler
// mirroring people_drilldown_prs_route.go's shape for a REST route mounted
// on query-api's mux, gated by its own routeswitch entry.
//
// Auth: the same bearer-envelope verifier every other REST route in this
// binary uses. Python's route additionally rate-limits ("60/minute",
// main.py:1156); this port carries no rate limiter, the same documented
// gap every sibling REST route in this binary already carries.
//
// Business logic (identity resolution, the ClickHouse read, the
// ReplacingMergeTree dedup) lives in internal/people -- see
// drilldownissues.go's own doc comment for the full parity contract.
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

// peopleDrilldownIssuesPath is the literal path pattern this route mounts.
const peopleDrilldownIssuesPath = "/api/v1/people/{person_id}/drilldown/issues"

// peopleDrilldownIssuesOperation is this route's routeswitch operation name.
const peopleDrilldownIssuesOperation = "REST:GET:" + peopleDrilldownIssuesPath

// peopleDrilldownIssuesEnabledEnvVar is the operator-facing toggle.
const peopleDrilldownIssuesEnabledEnvVar = "GO_API_PEOPLE_DRILLDOWN_ISSUES_ENABLED"

func peopleDrilldownIssuesSwitchFromEnv() *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(os.Getenv(peopleDrilldownIssuesEnabledEnvVar)); enabled {
		sw.Set(peopleDrilldownIssuesOperation, true)
	}
	return sw
}

// loadPeopleDrilldownIssuesRouteConfig reads this route's own dependency
// set, same reasoning as loadPeopleSummaryRouteConfig's own doc comment.
func loadPeopleDrilldownIssuesRouteConfig() (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = os.Getenv("CLICKHOUSE_URI")
	jwksPath = os.Getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = os.Getenv("GO_API_ENVELOPE_ISSUER")
	audience = os.Getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildPeopleDrilldownIssuesRoute constructs the handler, its own
// routeswitch Mux, and a cleanup function -- same "stay unmounted, don't
// fail to build/start" contract every other optionally-configured route in
// this binary follows.
func buildPeopleDrilldownIssuesRoute() (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadPeopleDrilldownIssuesRouteConfig()
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

	reader, err := people.NewReader(readClient)
	if err != nil {
		_ = readClient.Close()
		return nil, nil, false, err
	}

	routeMux := routeswitch.NewMux(peopleDrilldownIssuesSwitchFromEnv())
	routeMux.Register(peopleDrilldownIssuesOperation, newPeopleDrilldownIssuesHandler(reader))

	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTMethodNotAllowed(w, r, "people_drilldown_issues")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "people_drilldown_issues")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(peopleDrilldownIssuesOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// writePeopleDrilldownIssuesResponse writes resp as the final 200 JSON
// body via json.NewEncoder(w).Encode -- this repo's JSON-response path,
// never a raw w.Write of pre-marshalled bytes.
func writePeopleDrilldownIssuesResponse(w http.ResponseWriter, r *http.Request, orgID string, resp *people.DrilldownIssuesResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if encodeErr := json.NewEncoder(w).Encode(resp); encodeErr != nil {
		log.Printf("query-api: people_drilldown_issues: encode response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
	}
}

// newPeopleDrilldownIssuesHandler is the routeswitch-registered handler
// for GET /api/v1/people/{person_id}/drilldown/issues -- the Go port of
// people_drilldown_issues (api/main.py:1152-1178). Reached only after
// buildPeopleDrilldownIssuesRoute's entryHandler has already authenticated
// the request and attached authctx.Claims.
func newPeopleDrilldownIssuesHandler(reader *people.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "people_drilldown_issues", "Not authenticated")
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
		limit := 50
		if raw := query.Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "limit"}, raw))
			} else {
				limit = parsed
			}
		}
		cursorTime, cursorPresent, cursorOK := parseISODateTimeQueryParam(query.Get("cursor"))
		if cursorPresent && !cursorOK {
			validationErrors = append(validationErrors, dateTimeQueryParamError([]any{"query", "cursor"}, query.Get("cursor")))
		}

		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		for key := range query {
			if peopleForbiddenQueryParams[key] {
				writeRESTError(w, r, "people_drilldown_issues", claims.OrgID, http.StatusBadRequest, "Comparative parameters are not supported.")
				return
			}
		}

		var cursorPtr *time.Time
		if cursorPresent {
			cursorPtr = &cursorTime
		}

		resp, err := people.BuildDrilldownIssuesResponse(r.Context(), reader, claims.OrgID, people.DrilldownIssuesParams{
			PersonID:  personID,
			RangeDays: rangeDays,
			Limit:     limit,
			Cursor:    cursorPtr,
			Now:       time.Now().UTC(),
		})
		if err != nil {
			if reqErr, ok := people.AsRequestError(err); ok {
				writeRESTError(w, r, "people_drilldown_issues", claims.OrgID, reqErr.Status, reqErr.Message)
				return
			}
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:1177-1178).
			writeRESTDataUnavailable(w, r, "people_drilldown_issues", claims.OrgID, err)
			return
		}
		writePeopleDrilldownIssuesResponse(w, r, claims.OrgID, resp)
	}
}
