// GET+POST /api/v1/opportunities -- mirrors home_route.go's own shape
// (one mux path, one routeswitch operation PER METHOD, one handler
// registering both).
//
// Auth: the same bearer-envelope verifier every other REST route in
// this binary uses. Python's routes additionally rate-limit
// ("60/minute" on both, main.py:1182/1212), same documented gap every
// other ported REST route in this binary already carries.
//
// Business logic (build_opportunities_response's own card-building over
// an already-built home.Response) lives in internal/opportunities -- see
// that package's doc comment for the full parity contract. Unlike
// home_route.go, this route never builds or threads a Postgres pool:
// build_opportunities_response's own call to build_home_response never
// passes semantic_session (services/opportunities.py:64-69), so this
// port carries no PGQueryClient at all, matching that omission exactly
// rather than reproducing home's own Postgres dependency.
//
// No caching either: Python's build_opportunities_response shares its
// caller's HOME_CACHE instance with /api/v1/home itself -- see
// internal/opportunities.BuildResponse's own doc comment for the
// resulting cross-route, up-to-60s data-freshness gap this port leaves
// uncovered.
package server

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/opportunities"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

const (
	opportunitiesGetOperation  = "REST:GET:/api/v1/opportunities"
	opportunitiesPostOperation = "REST:POST:/api/v1/opportunities"
)

const opportunitiesEnabledEnvVar = "GO_API_OPPORTUNITIES_ENABLED"

// opportunitiesMaxBodyBytes is a Go-side-only safety cap on the POST
// body -- Python's real endpoint has no request-body size limit at all
// for this route, matching home_route.go's own homeMaxBodyBytes for the
// same reason (same HomeRequest body shape, api/models/filters.py:53-54).
const opportunitiesMaxBodyBytes = 1 << 20

func opportunitiesSwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(opportunitiesEnabledEnvVar)); enabled {
		sw.Set(opportunitiesGetOperation, true)
		sw.Set(opportunitiesPostOperation, true)
	}
	return sw
}

func buildOpportunitiesRoute(getenv getenvFunc) (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	cfg, cfgOK := loadQueryRouteConfig(getenv)
	if !cfgOK {
		return nil, nil, false, nil
	}

	verifier, err := principal.NewVerifier(cfg.EnvelopeJWKSPath, cfg.EnvelopeIssuer, cfg.EnvelopeAudience)
	if err != nil {
		return nil, nil, false, fmt.Errorf("opportunities: build envelope verifier: %w", err)
	}
	edgeVerifier, err := buildEdgeVerifierFromEnv(getenv)
	if err != nil {
		return nil, nil, false, err
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(cfg.ClickHouseURI))
	if err != nil {
		return nil, nil, false, fmt.Errorf("opportunities: build read client: %w", err)
	}

	routeMux := routeswitch.NewMux(opportunitiesSwitchFromEnv(getenv))
	routeMux.Register(opportunitiesGetOperation, newOpportunitiesGetHandler(readClient))
	routeMux.Register(opportunitiesPostOperation, newOpportunitiesPostHandler(readClient))

	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "opportunities")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		switch r.Method {
		case http.MethodGet:
			routeMux.Dispatch(opportunitiesGetOperation, w, r)
		case http.MethodPost:
			routeMux.Dispatch(opportunitiesPostOperation, w, r)
		default:
			writeRESTMethodNotAllowed(w, r, "opportunities")
		}
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// newOpportunitiesGetHandler ports opportunities() (main.py:1181-1208).
func newOpportunitiesGetHandler(client home.QueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "opportunities", "Not authenticated")
			return
		}

		query := r.URL.Query()
		scopeType := lastQueryValue(query, "scope_type")
		if scopeType == "" {
			scopeType = "org"
		}
		scopeID := lastQueryValue(query, "scope_id")

		var validationErrors []pydanticErrorDetail

		rangeDays := 14
		// An explicit empty value is still parsed (pydantic: int_parsing).
		if query.Has("range_days") {
			raw := lastQueryValue(query, "range_days")
			if parsed, parseErr := parseQueryInt([]any{"query", "range_days"}, raw); parseErr == nil {
				rangeDays = parsed
			} else {
				validationErrors = append(validationErrors, *parseErr)
			}
		}
		compareDays := 14
		// An explicit empty value is still parsed (pydantic: int_parsing).
		if query.Has("compare_days") {
			raw := lastQueryValue(query, "compare_days")
			if parsed, parseErr := parseQueryInt([]any{"query", "compare_days"}, raw); parseErr == nil {
				compareDays = parsed
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

		// _filters_from_query (main.py:178-199) constructs
		// ScopeFilter(level=scope_type, ...) inside opportunities()'s own
		// try block -- a genuine Pydantic model construction that raises
		// for any scope_type outside the Literal set, caught by the same
		// generic `except Exception: 503` every other failure here is,
		// matching home_route.go's own identical precedent for the same
		// _filters_from_query call. homeValidScopeLevels (home_route.go)
		// is the SAME Literal set both routes' _filters_from_query call
		// validates against -- reused, not re-declared.
		if !homeValidScopeLevels[scopeType] {
			writeRESTDataUnavailable(w, r, "opportunities", claims.OrgID,
				fmt.Errorf("opportunities: ScopeFilter construction: scope_type %q is not one of the valid levels", scopeType))
			return
		}

		var ids []string
		if scopeID != "" {
			ids = []string{scopeID}
		}
		f := home.Filters{
			Time:  home.TimeFilter{RangeDays: rangeDays, CompareDays: compareDays},
			Scope: home.ScopeFilter{Level: scopeType, IDs: ids},
		}
		if startPresent {
			f.Time.StartDate = &startDate
		}
		if endPresent {
			f.Time.EndDate = &endDate
		}

		resp, err := opportunities.BuildResponse(r.Context(), client, claims.OrgID, f, time.Now().UTC())
		if err != nil {
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:1194-1208).
			writeRESTDataUnavailable(w, r, "opportunities", claims.OrgID, err)
			return
		}

		// Python sets this header only on the GET route, and only once
		// build_opportunities_response has already succeeded
		// (main.py:1204-1206).
		w.Header().Set("X-DevHealth-Deprecated", "use POST with filters")
		writeOpportunitiesResponse(w, r, claims.OrgID, resp)
	}
}

// newOpportunitiesPostHandler ports opportunities_post (main.py:1211-1226).
func newOpportunitiesPostHandler(client home.QueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "opportunities", "Not authenticated")
			return
		}

		bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, opportunitiesMaxBodyBytes+1))
		if err != nil {
			writeRESTError(w, r, "opportunities", claims.OrgID, http.StatusBadRequest, "bad request")
			return
		}
		if len(bodyBytes) > opportunitiesMaxBodyBytes {
			writeRESTError(w, r, "opportunities", claims.OrgID, http.StatusRequestEntityTooLarge, "request body exceeds size limit")
			return
		}

		decoded, bodyIsEmptyOrNull, syntaxDetail := decodeRequestBody([]any{"body"}, bodyBytes)
		if syntaxDetail != nil {
			writePydanticValidationError(w, r, claims.OrgID, *syntaxDetail)
			return
		}
		if bodyIsEmptyOrNull {
			writePydanticValidationError(w, r, claims.OrgID, missingFieldError([]any{"body"}, nil))
			return
		}

		body, isObject := decoded.(*pyjson.Object)
		if !isObject {
			writePydanticValidationError(w, r, claims.OrgID, modelAttributesTypeError([]any{"body"}, decoded))
			return
		}

		var validationErrors []pydanticErrorDetail
		filtersValue, hasFilters := body.Get("filters")
		if !hasFilters {
			validationErrors = append(validationErrors, missingFieldError([]any{"body", "filters"}, body))
		} else {
			validationErrors = append(validationErrors, validateMetricFilter([]any{"body", "filters"}, filtersValue)...)
		}
		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		filtersMap, _ := legacyJSON(filtersValue).(map[string]any)
		// homeFiltersFromMap (home_route.go) reads the SAME validated
		// MetricFilter shape HomeRequest carries -- opportunities_post's
		// own payload (main.py:1211-1226) is the identical HomeRequest
		// Pydantic model, so this is reuse of an already-reviewed
		// function, not a parallel re-implementation.
		resp, err := opportunities.BuildResponse(r.Context(), client, claims.OrgID, homeFiltersFromMap(filtersMap), time.Now().UTC())
		if err != nil {
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:1218-1226).
			writeRESTDataUnavailable(w, r, "opportunities", claims.OrgID, err)
			return
		}
		writeOpportunitiesResponse(w, r, claims.OrgID, resp)
	}
}

// writeOpportunitiesResponse is this route's one JSON-response path --
// never a raw w.Write of pre-marshalled bytes, matching every other
// route in this binary.
func writeOpportunitiesResponse(w http.ResponseWriter, r *http.Request, orgID string, resp *opportunities.Response) {
	if encodeErr := writeModelResponse(w, resp); encodeErr != nil {
		log.Printf("query-api: opportunities: encode response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
		writeModelFailure(w)
	}
}
