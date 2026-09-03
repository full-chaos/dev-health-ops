// This file is CHAOS-4977 step 5a: the Go REST handler for
// POST /api/v1/investment/explain, mounted on query-api's mux alongside
// /query (query_route.go), gated by its own routeswitch entry --
// team-lead ruling (a): a genuine new REST route, not a 13th GraphQL
// operation, since this endpoint is a REST POST with an LLM call chain
// and a streaming keep-alive body, not resolver-shaped.
//
// SCOPE, deliberately narrower than the full Python endpoint in two
// documented ways (each independently reversible without an interface
// change elsewhere):
//
//  1. No rate limiting. Python's investment_explain carries
//     @limiter.limit("20/minute"); query-api has no rate-limiting
//     mechanism anywhere yet (grepped cmd/query-api -- none). Building a
//     generic limiter is a separate task from wiring this one handler;
//     the route stays unreachable by default via routeswitch regardless.
//  2. The written investment_explanations/llm_token_usage rows use a
//     SEPARATE, narrow write connection (internal/storage/clickhouse,
//     the same helper the worker binaries use) rather than the
//     read-only analytics.QueryClient this service otherwise uses
//     everywhere -- see cachewrite.go's own package doc comment for why
//     (dev-health-go's Client hard-rejects non-SELECT statements; this
//     repo doesn't control that module).
//
// resolve_repo_filter_ids (api/services/filtering.py:95-110), INCLUDING
// its team-scope branch, IS ported (investmentexplain/repofilter.go's
// (*Reader).ResolveRepoFilterIDs) -- team-lead ruling, CHAOS-4977: "team
// scope is the attribution use case, a 400 on scope.level='team' is a
// parity break." scopeRepoIDs below is now a thin adapter over it.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investmentexplain"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// investmentExplainOperation is this route's routeswitch operation name
// -- a PATH-keyed entry (team-lead ruling), not a GraphQL document
// digest: there is no GraphQL document behind a REST route. Backed by
// its own DynamicSwitch (below) rather than folded into /query's
// PostgresSwitch, because PostgresSwitch.Enabled requires a
// (schema_digest, document_digest) pair for every operation
// (go_api_routing_state's real primary key, routeswitch/postgres_switch.go)
// -- a REST path has no document to digest, and inventing a sentinel
// value to satisfy that shape would model this route as something it
// structurally is not. DynamicSwitch has no such requirement: it is
// exactly "operation name -> bool", which is what a path-keyed entry
// needs. It defaults every operation to disabled (default OFF, per the
// ruling) and is toggled by GO_API_INVESTMENT_EXPLAIN_ENABLED below --
// the same class of env-var gate this service already uses elsewhere
// (loadQueryRouteConfig's "6 vars together or stay unmounted"), not a
// new registry table. Upgradable to a registry-backed Switch later
// (Switch is an interface) without changing Mux.Dispatch's call site.
const investmentExplainOperation = "REST:POST:/api/v1/investment/explain"

// investmentExplainEnabledEnvVar is the operator-facing toggle -- unset
// or anything other than a true-ish value leaves the route disabled,
// matching DynamicSwitch's own default.
const investmentExplainEnabledEnvVar = "GO_API_INVESTMENT_EXPLAIN_ENABLED"

func investmentExplainSwitchFromEnv() *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(os.Getenv(investmentExplainEnabledEnvVar)); enabled {
		sw.Set(investmentExplainOperation, true)
	}
	return sw
}

// loadInvestmentExplainRouteConfig reuses loadQueryRouteConfig's env
// vars (CLICKHOUSE_URI + the GO_API_ENVELOPE_* trio) rather than
// inventing a second set: both routes need the same ClickHouse and
// envelope-verification dependencies in the same deployment
// (deploy/go-api/compose-query-api.yml declares them once for the whole
// service). Does NOT require GO_API_SCHEMA_DIGEST or
// GO_API_REGISTRY_POSTGRES_URI -- neither is meaningful for a REST route.
func loadInvestmentExplainRouteConfig() (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	cfg, queryOk := loadQueryRouteConfig()
	if !queryOk {
		return "", "", "", "", false
	}
	return cfg.ClickHouseURI, cfg.EnvelopeJWKSPath, cfg.EnvelopeIssuer, cfg.EnvelopeAudience, true
}

// buildInvestmentExplainRoute constructs the handler, its own
// routeswitch Mux, and a cleanup function. ok is false when this
// route's dependencies are not configured (same "stay unmounted, don't
// fail to build/start" contract loadQueryRouteConfig's own doc comment
// describes) -- main() only calls mux.HandleFunc when ok is true.
func buildInvestmentExplainRoute() (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadInvestmentExplainRouteConfig()
	if !cfgOK {
		return nil, nil, false, nil
	}

	verifier, err := principal.NewVerifier(jwksPath, issuer, audience)
	if err != nil {
		return nil, nil, false, fmt.Errorf("investment/explain: build envelope verifier: %w", err)
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: clickHouseURI})
	if err != nil {
		return nil, nil, false, fmt.Errorf("investment/explain: build read client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writeConn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(clickHouseURI))
	if err != nil {
		_ = readClient.Close()
		return nil, nil, false, fmt.Errorf("investment/explain: build write connection: %w", err)
	}

	reader, err := investmentexplain.NewReader(readClient)
	if err != nil {
		_ = readClient.Close()
		_ = writeConn.Close()
		return nil, nil, false, fmt.Errorf("investment/explain: build reader: %w", err)
	}
	cacheWriter, err := investmentexplain.NewCacheWriter(writeConn)
	if err != nil {
		_ = readClient.Close()
		_ = writeConn.Close()
		return nil, nil, false, fmt.Errorf("investment/explain: build cache writer: %w", err)
	}

	routeMux := routeswitch.NewMux(investmentExplainSwitchFromEnv())
	routeMux.Register(investmentExplainOperation, newInvestmentExplainWorkHandler(reader, cacheWriter))

	// Auth runs BEFORE Dispatch, same order /query's newQueryHandler uses
	// (query_route.go:1180-1194: resolve operation, THEN auth, THEN
	// Dispatch) -- claims travel to the registered handler via the
	// request context, matching that same call's
	// r.WithContext(authctx.WithClaims(...)) before Dispatch, not as a
	// separate parameter Mux.Dispatch has no room for.
	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		claims, err := verifier.Verify(token)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), authctx.Claims{OrgID: claims.OrgID}))
		routeMux.Dispatch(investmentExplainOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
		_ = writeConn.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// investmentExplainRequestBody ports api/models/filters.py's
// InvestmentExplainRequest -- the JSON body shape. Filters is kept as a
// raw map rather than a typed MetricFilter (this port has no Go
// MetricFilter type -- see reader.go's own established boundary of
// accepting already-resolved scope/filter values from callers): it is
// used two ways below, as the cache-key payload verbatim (matching
// filters.model_dump(mode="json") being dumped as-is) and via the
// narrow timeWindow/scopeRepoIDs/workCategoryFromFilters field reads.
type investmentExplainRequestBody struct {
	Theme       *string        `json:"theme"`
	Subcategory *string        `json:"subcategory"`
	Filters     map[string]any `json:"filters"`
	LLMModel    *string        `json:"llm_model"`
}

// keepAliveInterval matches Python's keep_alive_wrapper (main.py:
// 381-412) exactly: a " " chunk every 5 seconds while the real work is
// still in flight.
const keepAliveInterval = 5 * time.Second

// writeKeepAliveJSON ports keep_alive_wrapper's streaming contract: a
// single space byte, flushed, every keepAliveInterval, until work
// completes, then the final JSON body as the LAST chunk -- Content-Type
// stays application/json throughout, matching Python's
// StreamingResponse(..., media_type="application/json") despite the
// leading whitespace making this a multi-chunk, not single-JSON-value,
// wire body (a lenient parser accepts leading whitespace before a JSON
// document). If w is not an http.Flusher, this degrades to writing
// unflushed (still correct bytes on the wire eventually, just not
// guaranteed to arrive incrementally) rather than panicking.
func writeKeepAliveJSON(ctx context.Context, w http.ResponseWriter, work func(context.Context) ([]byte, error)) {
	w.Header().Set("Content-Type", "application/json")
	flusher, _ := w.(http.Flusher)

	type workResult struct {
		body []byte
		err  error
	}
	resultCh := make(chan workResult, 1)
	go func() {
		body, err := work(ctx)
		resultCh <- workResult{body: body, err: err}
	}()

	ticker := time.NewTicker(keepAliveInterval)
	defer ticker.Stop()
	for {
		select {
		case result := <-resultCh:
			if result.err != nil {
				// Python's except-branch: two JSON error objects, back to
				// back, as the final chunks (main.py:401-412) -- an
				// already-started 200 response cannot change status code
				// at this point, matching Python's own StreamingResponse
				// behavior (headers are already sent).
				//
				// These are literal byte-parity strings, not
				// json.Marshal(map[string]string{...}): encoding/json
				// sorts map keys alphabetically ("detail" before "error")
				// and uses compact separators, while Python's
				// json.dumps({"error": ..., "detail": ...}) preserves the
				// dict's own key order and the default ", "/": " spacing
				// -- confirmed against a live `python3 -c 'json.dumps(...)'`
				// run, not assumed. Caught by codex round 1 (P1); the two
				// error bodies never carry attacker-controlled data, so a
				// hand-written literal is safe and exact, not a shortcut.
				_, _ = w.Write([]byte(`{"error": "Streaming error", "detail": "An internal error has occurred."}`))
				if flusher != nil {
					flusher.Flush()
				}
				_, _ = w.Write([]byte(`{"error": "Streaming error", "detail": "An internal streaming error occurred."}`))
				if flusher != nil {
					flusher.Flush()
				}
				log.Printf("query-api: investment/explain streaming error: %v", result.err)
				return
			}
			_, _ = w.Write(result.body)
			if flusher != nil {
				flusher.Flush()
			}
			return
		case <-ticker.C:
			_, _ = w.Write([]byte(" "))
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

// newInvestmentExplainWorkHandler is the routeswitch-registered handler --
// reached only after buildInvestmentExplainRoute's entryHandler has
// already authenticated the request and attached authctx.Claims to its
// context (matching /query's newQueryHandler's own division: the outer
// function authenticates and calls Mux.Dispatch, the dispatched work
// reads claims back out of the context rather than re-verifying).
func newInvestmentExplainWorkHandler(
	reader *investmentexplain.Reader,
	writer *investmentexplain.CacheWriter,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		var reqBody investmentExplainRequestBody
		if len(bodyBytes) > 0 {
			if err := json.Unmarshal(bodyBytes, &reqBody); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
		}

		llmProvider := r.URL.Query().Get("llm_provider")
		if llmProvider == "" {
			llmProvider = "auto"
		}
		forceRefresh, _ := strconv.ParseBool(r.URL.Query().Get("force_refresh"))

		opts, buildErr := buildExplainOptions(r.Context(), reader, claims.OrgID, reqBody, llmProvider, forceRefresh)
		if buildErr != nil {
			http.Error(w, buildErr.Error(), http.StatusBadRequest)
			return
		}

		// A provider Python genuinely supports but this Go port cannot
		// construct a client for (investmentexplain.
		// ResolveUnsupportedProviderKind's own doc comment) must answer
		// BEFORE any streaming begins -- a plain 501, not the normal
		// streamed llm_unavailable body -- so the Python REST forwarder's
		// non-200 fallback routes the request to Python's real completion
		// instead of a wrong Go answer. Team-lead ruling, CHAOS-4977 codex
		// round 1's #5.
		if _, unsupported := investmentexplain.ResolveUnsupportedProviderKind(llmProvider); unsupported {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotImplemented)
			_, _ = w.Write([]byte(`{"error": "unsupported_provider"}`))
			return
		}

		writeKeepAliveJSON(r.Context(), w, func(ctx context.Context) ([]byte, error) {
			explanation, err := reader.ExplainInvestmentMix(ctx, writer, investmentexplain.CompleteInvestmentMixExplanation, opts)
			if err != nil {
				return nil, err
			}
			encoded, err := investmentexplain.EncodeInvestmentMixExplanation(explanation)
			if err != nil {
				return nil, err
			}
			return []byte(encoded), nil
		})
	}
}

// buildExplainOptions ports explain_investment_mix's request-to-options
// translation, including time_window's own math
// (api/services/filtering.py:78-92) verbatim.
func buildExplainOptions(ctx context.Context, reader *investmentexplain.Reader, orgID string, body investmentExplainRequestBody, llmProvider string, forceRefresh bool) (investmentexplain.ExplainInvestmentMixOptions, error) {
	startTS, endTS := timeWindow(body.Filters)
	repoIDs, err := scopeRepoIDs(ctx, reader, body.Filters, orgID)
	if err != nil {
		return investmentexplain.ExplainInvestmentMixOptions{}, err
	}
	scope, _ := body.Filters["scope"].(map[string]any)
	scopeLevel, _ := scope["level"].(string)
	if scopeLevel == "" {
		scopeLevel = "org"
	}

	var theme, subcategory, llmModel string
	if body.Theme != nil {
		theme = *body.Theme
	}
	if body.Subcategory != nil {
		subcategory = *body.Subcategory
	}
	if body.LLMModel != nil {
		llmModel = *body.LLMModel
	}

	// A request whose JSON body omits "filters" entirely leaves
	// body.Filters nil -- Pydantic materializes MetricFilter's own
	// default_factory in that case (a real, fully-populated object), not
	// null, so the cache key must be computed against THAT value, not a
	// bare `null`. A first draft passed body.Filters straight through,
	// silently hashing null for the omitted-filters request shape and
	// missing every cache entry Python would have hit -- caught by codex
	// round 1 (P1).
	filtersForCacheKey := any(body.Filters)
	if body.Filters == nil {
		filtersForCacheKey = investmentexplain.DefaultMetricFilterForCacheKey()
	}

	return investmentexplain.ExplainInvestmentMixOptions{
		OrgID:              orgID,
		StartTS:            startTS,
		EndTS:              endTS,
		RepoIDs:            repoIDs,
		ScopeLevel:         scopeLevel,
		WorkCategory:       workCategoryFromFilters(body.Filters),
		Theme:              theme,
		Subcategory:        subcategory,
		FiltersForCacheKey: filtersForCacheKey,
		LLMProvider:        llmProvider,
		LLMModel:           llmModel,
		ForceRefresh:       forceRefresh,
		Now:                time.Now().UTC(),
	}, nil
}

// timeWindow ports time_window (api/services/filtering.py:78-92)
// exactly, reading filters["time"]'s fields with the same defaults
// TimeFilter's Pydantic model declares (range_days/compare_days: 14,
// start_date/end_date: unset) when the field or the whole "time" key is
// absent -- matching MetricFilter's own default_factory=TimeFilter for a
// request that omits "filters" or "filters.time" entirely. Returns
// (start_ts, end_ts) as UTC midnight instants (datetime.combine(day,
// time.min, tzinfo=utc)), matching build_investment_response/
// build_work_unit_investments' own conversion from time_window's
// start_day/end_day.
func timeWindow(filters map[string]any) (startTS, endTS time.Time) {
	timeFilter, _ := filters["time"].(map[string]any)

	rangeDays := intFromAny(timeFilter["range_days"], 14)
	if rangeDays < 1 {
		rangeDays = 1
	}

	endDate, hasEndDate := dateFromAny(timeFilter["end_date"])
	if !hasEndDate {
		endDate = time.Now().UTC().Truncate(24 * time.Hour)
	}
	endDay := endDate.AddDate(0, 0, 1)

	startDate, hasStartDate := dateFromAny(timeFilter["start_date"])
	var startDay time.Time
	if hasStartDate {
		startDay = startDate
		if !startDay.Before(endDay) {
			startDay = endDay.AddDate(0, 0, -1)
		}
	} else {
		startDay = endDay.AddDate(0, 0, -rangeDays)
	}

	return startDay, endDay
}

// scopeRepoIDs reads filters.scope.level/ids and filters.what.repos out
// of the raw request body and hands them to
// (*investmentexplain.Reader).ResolveRepoFilterIDs, which ports
// resolve_repo_filter_ids (api/services/filtering.py:95-110) in full,
// team-scope branch included.
func scopeRepoIDs(ctx context.Context, reader *investmentexplain.Reader, filters map[string]any, orgID string) ([]string, error) {
	scope, _ := filters["scope"].(map[string]any)
	level, _ := scope["level"].(string)
	if level == "" {
		level = "org"
	}
	scopeIDs := stringsFromAny(scope["ids"])
	what, _ := filters["what"].(map[string]any)
	whatRepos := stringsFromAny(what["repos"])
	return reader.ResolveRepoFilterIDs(ctx, level, scopeIDs, whatRepos, orgID)
}

// workCategoryFromFilters reads filters.why.work_category verbatim --
// _split_category_filters (work_units.py:70-84, already ported as
// investmentexplain's splitCategoryFilters) does the theme/subcategory
// splitting; this just extracts the raw list the request body carries.
func workCategoryFromFilters(filters map[string]any) []string {
	why, _ := filters["why"].(map[string]any)
	return stringsFromAny(why["work_category"])
}

func stringsFromAny(value any) []string {
	list, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		if s, ok := item.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

// intFromAny reads a JSON-decoded value the way Pydantic's lenient int
// coercion reads a request field: a JSON number (encoding/json always
// decodes a bare "2.0"/"2.5" number to float64) or a numeric STRING both
// coerce -- e.g. `{"range_days": "2"}` is a normal Pydantic request body
// (form/query-param-style stringly-typed JSON is common from hand-built
// clients) and TimeFilter(range_days="2") genuinely resolves to 2, not a
// validation error (confirmed via a live `uv run python3` construction
// against the real TimeFilter model, not assumed). A first draft here
// only handled float64, so a string "2" silently fell through to the
// 14-day fallback instead of the request's own intent -- caught by codex
// round 1 (P1). Non-numeric or absent values still fall back, matching
// TimeFilter's own default_factory for an omitted field (an explicit
// JSON null is a DIFFERENT case Pydantic actually rejects outright --
// out of scope here, same as the rest of this route's documented
// narrower-than-full-Pydantic-validation boundary).
func intFromAny(value any, fallback int) int {
	switch v := value.(type) {
	case float64:
		return int(v)
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
		return fallback
	default:
		return fallback
	}
}

// dateFromAny parses a "YYYY-MM-DD" JSON string value (Pydantic's
// date's mode="json" wire form) into a UTC midnight time.Time. ok is
// false for a missing/null/malformed value, matching Python's
// filters.time.start_date/end_date being None.
func dateFromAny(value any) (t time.Time, ok bool) {
	s, isString := value.(string)
	if !isString || s == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}
