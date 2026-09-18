// POST /api/v1/work-units/{work_unit_id}/explain -- the Go REST handler for
// the last LLM-backed route on the FastAPI surface (api/main.py:688-780).
//
// NOT STREAMED, unlike its sibling POST /api/v1/investment/explain. The
// Python handler's return annotation names StreamingResponse and one of its
// comments mentions keep_alive_wrapper, but its only non-error return is a
// bare WorkUnitExplanation (main.py:764) which FastAPI serialises against
// `response_model=WorkUnitExplanation` as a single application/json body.
// This port writes that one body; there is no keep-alive chunking here.
//
// Business logic is split the same way every sibling route splits it: the
// ClickHouse reads and the per-unit assembly are investmentexplain's
// already-ported (*Reader).BuildWorkUnitInvestments, called here with
// limit=1 and a work_unit_id filter exactly as main.py:759-766 calls
// build_work_unit_investments; the prompt, the completion and the response
// parser are workunitexplain. This file is the HTTP layer -- parameters,
// auth, provider gating and status mapping.
//
// SCOPE, one documented narrowing: no rate limiting. Python carries
// @limiter.limit("20/minute") (main.py:692), IP-keyed through the shared
// slowapi singleton; query-api has no rate-limiting mechanism anywhere, and
// every ported route here declares the same gap. The route stays
// unreachable by default via routeswitch regardless.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5/pgxpool"

	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/analytics"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investmentexplain"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/teamscope"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/workunitexplain"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// workUnitExplainOperation is this route's routeswitch operation name --
// METHOD+PATH keyed, with the path spelled exactly as FastAPI declares it
// so the migration matrix's own path matching lines the two planes up.
const workUnitExplainOperation = "REST:POST:/api/v1/work-units/{work_unit_id}/explain"

// workUnitExplainEnabledEnvVar is the operator-facing toggle -- unset or
// anything other than a true-ish value leaves the route disabled, matching
// DynamicSwitch's own default.
const workUnitExplainEnabledEnvVar = "GO_API_WORK_UNIT_EXPLAIN_ENABLED"

func workUnitExplainSwitchFromEnv() *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(os.Getenv(workUnitExplainEnabledEnvVar)); enabled {
		sw.Set(workUnitExplainOperation, true)
	}
	return sw
}

// buildWorkUnitExplainRoute constructs the handler, its own routeswitch
// Mux, and a cleanup function. ok is false when this route's dependencies
// are not configured -- the same "stay unmounted, don't fail to start"
// contract every optionally-configured route in this binary follows.
//
// The dependency set matches investment/explain's: a read client for the
// investment reads, a narrow write connection for the llm_token_usage row
// (dev-health-go's query Client rejects non-SELECT statements, which is why
// the write goes through internal/storage/clickhouse instead), and a
// registry-Postgres pool for org-BYO LLM settings.
func buildWorkUnitExplainRoute() (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	cfg, cfgOK := loadQueryRouteConfig()
	if !cfgOK {
		return nil, nil, false, nil
	}

	verifier, err := principal.NewVerifier(cfg.EnvelopeJWKSPath, cfg.EnvelopeIssuer, cfg.EnvelopeAudience)
	if err != nil {
		return nil, nil, false, fmt.Errorf("work-unit explain: build envelope verifier: %w", err)
	}

	edgeVerifier, err := buildEdgeVerifierFromEnv()
	if err != nil {
		return nil, nil, false, err
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(cfg.ClickHouseURI))
	if err != nil {
		return nil, nil, false, fmt.Errorf("work-unit explain: build read client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writeConn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(cfg.ClickHouseURI))
	if err != nil {
		_ = readClient.Close()
		return nil, nil, false, fmt.Errorf("work-unit explain: build write connection: %w", err)
	}

	reader, err := investmentexplain.NewReader(analytics.PinInvestmentMembershipScope(readClient))
	if err != nil {
		_ = readClient.Close()
		_ = writeConn.Close()
		return nil, nil, false, fmt.Errorf("work-unit explain: build reader: %w", err)
	}
	cacheWriter, err := investmentexplain.NewCacheWriter(writeConn)
	if err != nil {
		_ = readClient.Close()
		_ = writeConn.Close()
		return nil, nil, false, fmt.Errorf("work-unit explain: build token usage writer: %w", err)
	}

	// pgxpool.New is lazy, so this does not dial Postgres at startup.
	orgSettingsPool, err := pgxpool.New(context.Background(), cfg.RegistryPostgresURI)
	if err != nil {
		_ = readClient.Close()
		_ = writeConn.Close()
		return nil, nil, false, fmt.Errorf("work-unit explain: build org-settings postgres pool: %w", err)
	}
	// A missing key is not fatal: an ENCRYPTED org settings row then behaves
	// exactly like a corrupt one (skipped, never fatal) and that org falls
	// back to the platform provider. Logged once so an operator has a signal
	// for the silently-degraded case -- see investment/explain's own call
	// site for the full reasoning, which applies unchanged here.
	decryptor, decryptorErr := providerfoundation.NewFernetDecryptor(
		secrets.NewValue(os.Getenv("SETTINGS_ENCRYPTION_KEY")), os.Getenv("SETTINGS_ENCRYPTION_SALT"))
	if decryptorErr != nil {
		log.Printf(
			"work-unit explain: SETTINGS_ENCRYPTION_KEY is not configured (%v) -- every ENCRYPTED org BYO LLM setting will be skipped and that org's requests will use the platform default provider instead",
			decryptorErr,
		)
	}
	orgSettings := llmorgsettings.Store{Pool: orgSettingsPool, Decryptor: decryptor}

	routeMux := routeswitch.NewMux(workUnitExplainSwitchFromEnv())
	routeMux.Register(workUnitExplainOperation, newWorkUnitExplainHandler(reader, cacheWriter, orgSettings))

	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTMethodNotAllowed(w, r, "work_unit_explain")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "work_unit_explain")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(workUnitExplainOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
		_ = writeConn.Close()
		orgSettingsPool.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// workUnitExplainQuery is this route's parsed query string. Python declares
// scope_type/scope_id/llm_provider/llm_model as plain `str` parameters,
// which Pydantic never rejects for an empty value, and
// range_days/start_date/end_date as typed ones, which it does -- so only
// the typed three can contribute a 422.
type workUnitExplainQuery struct {
	scopeType   string
	scopeID     string
	rangeDays   int
	startDate   *time.Time
	endDate     *time.Time
	llmProvider string
	llmModel    string
}

// parseWorkUnitExplainQuery ports the endpoint's own parameter defaults
// (main.py:695-703) and the 422s its typed parameters raise. The
// int/date validators are the shared ones every sibling REST route uses,
// so an invalid range_days answers the identical envelope here and there.
func parseWorkUnitExplainQuery(r *http.Request) (workUnitExplainQuery, []pydanticErrorDetail) {
	query := r.URL.Query()
	parsed := workUnitExplainQuery{
		scopeType:   query.Get("scope_type"),
		scopeID:     query.Get("scope_id"),
		rangeDays:   14,
		llmProvider: query.Get("llm_provider"),
		llmModel:    query.Get("llm_model"),
	}
	if parsed.scopeType == "" {
		parsed.scopeType = "org"
	}
	if parsed.llmProvider == "" {
		parsed.llmProvider = "auto"
	}

	var validationErrors []pydanticErrorDetail
	if query.Has("range_days") {
		raw := query.Get("range_days")
		value, err := strconv.Atoi(raw)
		if err != nil {
			validationErrors = append(validationErrors, intQueryParamError([]any{"query", "range_days"}, raw))
		} else {
			parsed.rangeDays = value
		}
	}
	startDate, startPresent, startOK := parseISODateQueryParam(query.Get("start_date"))
	if startPresent && !startOK {
		validationErrors = append(validationErrors, dateQueryParamError([]any{"query", "start_date"}, query.Get("start_date")))
	} else if startPresent {
		parsed.startDate = &startDate
	}
	endDate, endPresent, endOK := parseISODateQueryParam(query.Get("end_date"))
	if endPresent && !endOK {
		validationErrors = append(validationErrors, dateQueryParamError([]any{"query", "end_date"}, query.Get("end_date")))
	} else if endPresent {
		parsed.endDate = &endDate
	}
	return parsed, validationErrors
}

// newWorkUnitExplainHandler is the routeswitch-registered handler.
// Reached only after buildWorkUnitExplainRoute's entryHandler has
// authenticated the request and attached authctx.Claims.
func newWorkUnitExplainHandler(
	reader *investmentexplain.Reader,
	writer *investmentexplain.CacheWriter,
	orgSettings llmorgsettings.Resolver,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only -- the entry handler always authenticates first.
			writeRESTUnauthorized(w, r, "work_unit_explain", "Not authenticated")
			return
		}

		workUnitID := r.PathValue("work_unit_id")

		parsed, validationErrors := parseWorkUnitExplainQuery(r)
		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		// get_provider runs BEFORE any read (main.py:751-758) so that a
		// missing credential answers a 4xx rather than a 200 with a broken
		// body. Its three outcomes are reproduced in the same order Python
		// reaches them: a resolution failure and an unusable credential both
		// raise (-> 422 with the error's own text), while the resolved name
		// "none" constructs NoneProvider and never touches a credential at
		// all.
		kind, resolveErr := investmentexplain.ResolveProviderKindForOrg(
			r.Context(), parsed.llmProvider, claims.OrgID, orgSettings)
		if resolveErr != nil {
			// resolve_provider_name's only failure is an "auto" request
			// nothing in the environment answers, which raises
			// _missing_provider_error("auto") -- the resolver's own Go
			// message is not that text, so the ported literal is used
			// rather than the error's.
			writeRESTError(w, r, "work_unit_explain", claims.OrgID, http.StatusUnprocessableEntity,
				missingLLMProviderDetail(providerNameAuto))
			return
		}
		if kind != categorize.ProviderKindNone {
			// A provider Python genuinely serves but this port has no client
			// for must answer a distinct non-200 rather than a plausible
			// "credential missing" 422 -- the same guard, for the same
			// reason, as investment/explain's own.
			if _, unsupported := investmentexplain.ResolveUnsupportedProviderKindForOrg(
				r.Context(), parsed.llmProvider, claims.OrgID, orgSettings); unsupported {
				writeRESTError(w, r, "work_unit_explain", claims.OrgID, http.StatusNotImplemented, "unsupported_provider")
				return
			}
			if !investmentexplain.IsLLMAvailableForOrg(r.Context(), parsed.llmProvider, claims.OrgID, orgSettings) {
				// get_provider's own `if not _provider_has_required_config`
				// branch raises _missing_provider_error(provider_name) --
				// the RESOLVED name, not the requested one, which is what
				// an "auto" request that resolved to a configured-but-
				// unusable provider reports.
				writeRESTError(w, r, "work_unit_explain", claims.OrgID, http.StatusUnprocessableEntity,
					missingLLMProviderDetail(string(kind)))
				return
			}
		}

		// _filters_from_query (main.py:178-199) constructs
		// ScopeFilter(level=scope_type, ...), a genuine Pydantic model
		// construction that raises for any scope_type outside the Literal
		// set -- and it runs INSIDE this endpoint's own try block, after
		// get_provider, so an unknown scope_type is caught by the same
		// generic handler every other failure here is and answers 503, never
		// a 422. Confirmed against the live app, whose captured body is
		// testdata/work_unit_explain/unknown_scope_type_is_not_a_validation_error.json.
		// homeValidScopeLevels (home_route.go) is that Literal set, reused
		// rather than restated.
		if !homeValidScopeLevels[parsed.scopeType] {
			writeWorkUnitExplainUnavailable(w, r, claims.OrgID,
				fmt.Errorf("work_unit_explain: ScopeFilter construction: scope_type %q is not one of the valid levels", parsed.scopeType))
			return
		}

		startTS, endTS := timeWindow(drilldownTimeFilterMap(parsed.rangeDays, parsed.startDate, parsed.endDate))

		var scopeIDs []string
		if parsed.scopeID != "" {
			scopeIDs = []string{parsed.scopeID}
		}
		// _filters_from_query never populates what.repos for this route
		// (it has no query parameter for it), so only scope_id can carry a
		// repo reference here.
		repoIDs, err := reader.ResolveRepoFilterIDs(r.Context(), parsed.scopeType, scopeIDs, nil, claims.OrgID)
		if err != nil {
			writeWorkUnitExplainUnavailable(w, r, claims.OrgID, err)
			return
		}
		// ResolveRepoFilterIDs resolves only the explicit repo refs a request
		// names (its own doc comment): a team's repositories come from
		// team_repo_ownership, as a membership test pushed into the read
		// rather than a materialised id list. Resolving repo ids alone
		// applies no narrowing at all for scope_type=team, and a request for
		// a unit outside the team is answered instead of refused. Same shared
		// condition, same column, same shape as the work-units GET and the
		// investment-explain POST.
		var teamCondition string
		var teamBindings []dhclickhouse.Binding
		if parsed.scopeType == "team" && len(scopeIDs) > 0 {
			teamCondition, teamBindings = teamscope.RepoCondition(claims.OrgID, repoScopeColumn, scopeIDs, time.Now().UTC())
		}

		investments, err := reader.BuildWorkUnitInvestments(r.Context(), investmentexplain.BuildWorkUnitInvestmentsOptions{
			OrgID:              claims.OrgID,
			StartTS:            startTS,
			EndTS:              endTS,
			RepoIDs:            repoIDs,
			TeamScopeCondition: teamCondition,
			TeamScopeBindings:  teamBindings,
			Limit:              1,
			IncludeText:        true,
			WorkUnitID:         workUnitID,
		})
		if err != nil {
			writeWorkUnitExplainUnavailable(w, r, claims.OrgID, err)
			return
		}
		if len(investments) == 0 {
			writeWorkUnitExplainNotFound(w, r, claims.OrgID, workUnitID)
			return
		}

		complete := func(ctx context.Context, requestedProvider, requestedModel, fullPrompt string) (
			categorize.CompletionResult, string, string, error,
		) {
			return investmentexplain.CompleteWorkUnitExplanationForOrg(
				ctx, requestedProvider, requestedModel, claims.OrgID, orgSettings, fullPrompt)
		}

		explanation, err := workunitexplain.ExplainWorkUnit(
			r.Context(), writer, complete,
			workunitexplain.FromWorkUnitInvestment(investments[0]),
			workunitexplain.Options{
				OrgID:                claims.OrgID,
				LLMProvider:          parsed.llmProvider,
				LLMModel:             parsed.llmModel,
				ResolvedProviderKind: kind,
				Now:                  time.Now().UTC(),
			})
		if err != nil {
			// `except LLMError as exc: raise _http_exception_from_llm_error(exc)`
			// (main.py:772-773) for a provider failure, and the endpoint's own
			// outer `except Exception` for everything else. A non-LLM error
			// never reaches Python's LLMError branch, so it must not reach
			// this one either.
			if class, isLLM := categorize.ClassifyLLMError(err); isLLM {
				writeRESTError(w, r, "work_unit_explain", claims.OrgID,
					workUnitExplainLLMErrorStatus(class), err.Error())
				return
			}
			writeWorkUnitExplainUnavailable(w, r, claims.OrgID, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// The encoder writes straight to the ResponseWriter, so a failure
		// here is already a partially written 200 -- logged, never turned
		// into a status this response has passed the point of sending.
		if err := workunitexplain.WriteJSON(w, explanation); err != nil {
			log.Printf("query-api: work_unit_explain: write response failed: org_id=%s request_id=%s err=%v",
				claims.OrgID, envelopeRequestID(r), err)
		}
	}
}

// writeWorkUnitExplainNotFound answers `HTTPException(404, f"Work unit
// {work_unit_id} not found")` (main.py:771-774) with the caller's own id
// interpolated verbatim.
//
// It encodes through workunitexplain.WriteJSON rather than the shared
// writeRESTError, because this is the ONE error body on this route that
// carries caller-supplied text and the shared writer escapes `<`, `>` and
// `&`, which the reference does not. See WriteJSON's own doc comment for
// what that path does and does not reproduce. Captured fixtures:
// testdata/work_unit_explain/not_found*.json.
func writeWorkUnitExplainNotFound(w http.ResponseWriter, r *http.Request, orgID, workUnitID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	if err := workunitexplain.WriteJSON(w, restErrorBody{
		Detail: fmt.Sprintf("Work unit %s not found", workUnitID),
	}); err != nil {
		log.Printf("query-api: work_unit_explain: write 404 body failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), err)
	}
}

// providerNameAuto is the provider name _missing_provider_error reports
// for a resolution failure: resolve_provider_name only fails for an
// "auto" request, and the exception it raises carries provider="auto".
const providerNameAuto = "auto"

// missingProviderEnvHints ports _missing_provider_error's env-hint table
// (llm/providers/__init__.py:135-143). These are environment variable
// NAMES, which is all the reference's own message carries; no value of any
// of them is read, logged or emitted anywhere on this path.
var missingProviderEnvHints = map[string]string{
	"openai":    "OPENAI_API_KEY",
	"anthropic": "ANTHROPIC_API_KEY",
	"gemini":    "GEMINI_API_KEY",
	"qwen":      "QWEN_API_KEY or DASHSCOPE_API_KEY",
}

// missingLLMProviderDetail ports _missing_provider_error
// (llm/providers/__init__.py:115-150) together with the suffix
// LLMError.__str__ appends (llm/errors.py:162-170), since the endpoint
// answers 422 with str(exc) and that string carries the provider and model
// the exception was constructed with -- always provider=<name>,
// model="none" here.
//
// The "auto" branch has TWO forms in the reference, and only the second is
// reproduced: the first fires when a bare LLM_API_KEY/LLM_BASE_URL was
// supplied without a provider to use it with, which is a per-call
// credential path (get_provider's api_key/base_url arguments) no REST
// caller can reach -- this endpoint never passes either.
//
// Every string below is pinned against a body captured from the live
// reference app, in testdata/work_unit_explain/provider_unconfigured_*.json.
func missingLLMProviderDetail(provider string) string {
	if provider == providerNameAuto {
		return "No LLM provider is configured for auto. Set --llm-provider mock for " +
			"fixtures/testing, or configure LLM_PROVIDER plus provider credentials " +
			"such as OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY, " +
			"QWEN_API_KEY/DASHSCOPE_API_KEY, LOCAL_LLM_BASE_URL, OLLAMA_BASE_URL, " +
			"or OLLAMA_MODEL. | provider=auto | model=none"
	}
	envHint, named := missingProviderEnvHints[provider]
	if !named {
		envHint = "LLM_PROVIDER"
	}
	return fmt.Sprintf(
		"LLM provider '%s' is not configured. Set %s or choose --llm-provider mock "+
			"for fixtures/testing. | provider=%s | model=none",
		provider, envHint, provider)
}

// workUnitExplainLLMErrorStatus ports _http_exception_from_llm_error
// (main.py:414-421): auth and every unclassified LLMError answer 422, a
// rate limit answers 429, a server error answers 503.
func workUnitExplainLLMErrorStatus(class categorize.LLMErrorClass) int {
	switch class {
	case categorize.LLMErrorClassRateLimit:
		return http.StatusTooManyRequests
	case categorize.LLMErrorClassServer:
		return http.StatusServiceUnavailable
	default:
		return http.StatusUnprocessableEntity
	}
}

// writeWorkUnitExplainUnavailable answers the endpoint's own generic
// fallback, `HTTPException(503, "Explanation unavailable")`
// (main.py:780) -- a DIFFERENT literal from the "Data unavailable" every
// non-LLM REST route degrades to, which is why this route cannot use the
// shared writeRESTDataUnavailable helper. The cause is logged before the
// response is written, so a live 503 correlates back to the failure that
// produced it.
func writeWorkUnitExplainUnavailable(w http.ResponseWriter, r *http.Request, orgID string, err error) {
	log.Printf("query-api: work_unit_explain: degraded to 503 Explanation unavailable: org_id=%s request_id=%s err=%v",
		orgID, envelopeRequestID(r), err)
	writeRESTError(w, r, "work_unit_explain", orgID, http.StatusServiceUnavailable, "Explanation unavailable")
}
