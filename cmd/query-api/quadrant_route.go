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
// ClickHouse readers, the team/repo scope split, the cycle_throughput
// attribution quirk) lives in internal/quadrant -- see that package's
// doc comment for the full parity contract and the documented
// developer/person scope gap.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

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

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: clickHouseURI})
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
		routeMux.Dispatch(quadrantOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// parseQuadrantDate parses a "YYYY-MM-DD" query param the way FastAPI's
// `date | None` parameter does on the happy path. ok is false for an
// absent value; a present-but-malformed value is a caller error --
// Python's Pydantic-driven param validation answers 422 for this, this
// port answers 400 (a documented, Go-side-only status-code divergence;
// the DATA contract -- what a well-formed request returns -- is
// unaffected).
func parseQuadrantDate(raw string) (t time.Time, present bool, ok bool) {
	if raw == "" {
		return time.Time{}, false, true
	}
	parsed, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, true, false
	}
	return parsed.UTC(), true, true
}

// newQuadrantWorkHandler is the routeswitch-registered handler -- reached
// only after buildQuadrantRoute's entryHandler has already authenticated
// the request and attached authctx.Claims to its context.
func newQuadrantWorkHandler(client quadrant.QueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		query := r.URL.Query()
		quadrantType := query.Get("type")
		if quadrantType == "" {
			http.Error(w, "type is required", http.StatusBadRequest)
			return
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
		if raw := query.Get("range_days"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil {
				rangeDays = parsed
			}
			// A non-numeric range_days falls back to the default, same as
			// _normalize_range_days' own except-branch fallback
			// (quadrant.py:322-327) would if it ever saw one -- FastAPI's
			// own int-typed param validation never lets Python's fallback
			// fire in practice, but the value is identical either way.
		}

		startDate, startPresent, startOK := parseQuadrantDate(query.Get("start_date"))
		if startPresent && !startOK {
			http.Error(w, "invalid start_date", http.StatusBadRequest)
			return
		}
		endDate, endPresent, endOK := parseQuadrantDate(query.Get("end_date"))
		if endPresent && !endOK {
			http.Error(w, "invalid end_date", http.StatusBadRequest)
			return
		}

		params := quadrant.Params{
			Type:      quadrantType,
			ScopeType: scopeType,
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
				http.Error(w, reqErr.Message, reqErr.Status)
				return
			}
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:906-909) -- any error that is
			// not one of build_quadrant_response's own typed HTTPExceptions
			// (unknown type, invalid scope/bucket, unsupported metric)
			// degrades to a generic 503, never a raw ClickHouse error on
			// the wire.
			http.Error(w, "Data unavailable", http.StatusServiceUnavailable)
			return
		}

		body, encodeErr := json.Marshal(resp)
		if encodeErr != nil {
			http.Error(w, "Data unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}
}
