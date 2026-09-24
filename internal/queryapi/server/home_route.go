// GET+POST /api/v1/home -- the Go REST handler mirroring
// quadrant_route.go/sankey_route.go's shape: one mux path, one
// routeswitch operation PER METHOD, one handler registering both.
//
// Auth: the same bearer-envelope verifier every other REST route in
// this binary uses. Python's routes additionally rate-limit
// ("60/minute" on both, main.py:470/490); this port carries no rate
// limiter, the documented gap every other ported REST route in this
// binary already carries.
//
// Business logic (the response builder, every ClickHouse/Postgres
// reader, and every dedup fix it applies) lives in
// internal/home -- see that package's doc comment for the full parity
// contract.
package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

const (
	homeGetOperation  = "REST:GET:/api/v1/home"
	homePostOperation = "REST:POST:/api/v1/home"
)

const homeEnabledEnvVar = "GO_API_HOME_ENABLED"

// homeMaxBodyBytes is a Go-side-only safety cap on the POST body --
// Python's real endpoint has no request-body size limit at all for this
// route, matching investment_explain_route.go's own documented gap for
// the same reason.
const homeMaxBodyBytes = 1 << 20

func homeSwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(homeEnabledEnvVar)); enabled {
		sw.Set(homeGetOperation, true)
		sw.Set(homePostOperation, true)
	}
	return sw
}

func buildHomeRoute(getenv getenvFunc) (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	cfg, cfgOK := loadQueryRouteConfig(getenv)
	if !cfgOK {
		return nil, nil, false, nil
	}

	verifier, err := principal.NewVerifier(cfg.EnvelopeJWKSPath, cfg.EnvelopeIssuer, cfg.EnvelopeAudience)
	if err != nil {
		return nil, nil, false, fmt.Errorf("home: build envelope verifier: %w", err)
	}
	edgeVerifier, err := buildEdgeVerifierFromEnv(getenv)
	if err != nil {
		return nil, nil, false, err
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(cfg.ClickHouseURI))
	if err != nil {
		return nil, nil, false, fmt.Errorf("home: build read client: %w", err)
	}

	// registryPool backs FetchLatestSuccessfulSyncAt -- its own pool, same
	// DSN as /query's and investment/explain's own registry-Postgres
	// pools, each independently-built route owning its own pool object
	// rather than threading a shared one across routes. Lazy (pgxpool.New
	// does not dial at construction).
	registryPool, err := pgxpool.New(context.Background(), cfg.RegistryPostgresURI)
	if err != nil {
		_ = readClient.Close()
		return nil, nil, false, fmt.Errorf("home: build registry postgres pool: %w", err)
	}

	routeMux := routeswitch.NewMux(homeSwitchFromEnv(getenv))
	routeMux.Register(homeGetOperation, newHomeGetHandler(readClient, registryPool))
	routeMux.Register(homePostOperation, newHomePostHandler(readClient, registryPool))

	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "home")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		switch r.Method {
		case http.MethodGet:
			routeMux.Dispatch(homeGetOperation, w, r)
		case http.MethodPost:
			routeMux.Dispatch(homePostOperation, w, r)
		default:
			writeRESTMethodNotAllowed(w, r, "home")
		}
	}

	cleanupFn := func() {
		_ = readClient.Close()
		registryPool.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// homeValidScopeLevels mirrors ScopeFilter.level's Literal set
// (api/models/filters.py:16-18), matching sankeyValidScopeLevels' own
// precedent.
var homeValidScopeLevels = map[string]bool{
	"org": true, "team": true, "repo": true, "service": true, "developer": true,
}

// newHomeGetHandler ports home() (main.py:489-518).
func newHomeGetHandler(client home.QueryClient, pgPool home.PGQueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "home", "Not authenticated")
			return
		}

		query := r.URL.Query()
		scopeType := query.Get("scope_type")
		if scopeType == "" {
			scopeType = "org"
		}
		scopeID := query.Get("scope_id")

		var validationErrors []pydanticErrorDetail

		rangeDays := 14
		if raw := query.Get("range_days"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil {
				rangeDays = parsed
			} else {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "range_days"}, raw))
			}
		}
		compareDays := 14
		if raw := query.Get("compare_days"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil {
				compareDays = parsed
			} else {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "compare_days"}, raw))
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

		// _filters_from_query (main.py:178-199) constructs
		// ScopeFilter(level=scope_type, ...) -- a genuine Pydantic model
		// construction that raises for any scope_type outside the Literal
		// set, INSIDE home()'s own try block, so it is caught by the same
		// generic `except Exception: 503` every other failure here is --
		// never a 400/404, matching sankey_route.go's own identical
		// precedent for the same _filters_from_query call.
		if !homeValidScopeLevels[scopeType] {
			writeRESTDataUnavailable(w, r, "home", claims.OrgID,
				fmt.Errorf("home: ScopeFilter construction: scope_type %q is not one of the valid levels", scopeType))
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

		resp, err := home.BuildResponse(r.Context(), client, pgPool, claims.OrgID, f, time.Now().UTC())
		if err != nil {
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:503-518).
			writeRESTDataUnavailable(w, r, "home", claims.OrgID, err)
			return
		}

		// Python sets this header only on the GET route, and only once
		// build_home_response has already succeeded (main.py:514-516).
		w.Header().Set("X-DevHealth-Deprecated", "use POST with filters")
		writeHomeResponse(w, r, claims.OrgID, resp)
	}
}

// homeRequestBody ports HomeRequest (api/models/filters.py:53-54).
type homeRequestBody struct {
	Filters map[string]any `json:"filters"`
}

// newHomePostHandler ports home_post (main.py:469-486).
func newHomePostHandler(client home.QueryClient, pgPool home.PGQueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "home", "Not authenticated")
			return
		}

		bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, homeMaxBodyBytes+1))
		if err != nil {
			writeRESTError(w, r, "home", claims.OrgID, http.StatusBadRequest, "bad request")
			return
		}
		if len(bodyBytes) > homeMaxBodyBytes {
			writeRESTError(w, r, "home", claims.OrgID, http.StatusRequestEntityTooLarge, "request body exceeds size limit")
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
		f := homeFiltersFromMap(filtersMap)

		resp, err := home.BuildResponse(r.Context(), client, pgPool, claims.OrgID, f, time.Now().UTC())
		if err != nil {
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:477-486).
			writeRESTDataUnavailable(w, r, "home", claims.OrgID, err)
			return
		}
		writeHomeResponse(w, r, claims.OrgID, resp)
	}
}

// homeFiltersFromMap reads a validated MetricFilter map (already passed
// validateMetricFilter) into home.Filters, applying TimeFilter/
// ScopeFilter's own Pydantic defaults for an absent field/key, matching
// investment_explain_route.go's timeWindow/scopeRepoFilter helpers'
// established reading convention for the same shared MetricFilter shape.
func homeFiltersFromMap(filters map[string]any) home.Filters {
	timeFilter, _ := filters["time"].(map[string]any)
	scope, _ := filters["scope"].(map[string]any)
	what, _ := filters["what"].(map[string]any)
	why, _ := filters["why"].(map[string]any)

	level, _ := scope["level"].(string)
	if level == "" {
		level = "org"
	}

	f := home.Filters{
		Time: home.TimeFilter{
			RangeDays:   intFromAny(timeFilter["range_days"], 14),
			CompareDays: intFromAny(timeFilter["compare_days"], 14),
		},
		Scope: home.ScopeFilter{Level: level, IDs: stringsFromAny(scope["ids"])},
		What:  home.WhatFilter{Repos: stringsFromAny(what["repos"])},
		Why:   home.WhyFilter{WorkCategory: stringsFromAny(why["work_category"])},
	}
	if startDate, ok := dateFromAny(timeFilter["start_date"]); ok {
		f.Time.StartDate = &startDate
	}
	if endDate, ok := dateFromAny(timeFilter["end_date"]); ok {
		f.Time.EndDate = &endDate
	}
	return f
}

// writeHomeResponse is this route's one JSON-response path -- never a
// raw w.Write of pre-marshalled bytes, matching every other route in
// this binary.
func writeHomeResponse(w http.ResponseWriter, r *http.Request, orgID string, resp *home.Response) {
	if encodeErr := writeModelResponse(w, resp); encodeErr != nil {
		log.Printf("query-api: home: encode response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
		writeModelFailure(w)
	}
}
