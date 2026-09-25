package admin

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

const (
	// spendDefaultLimit, spendMaxLimit and spendDefaultDays are
	// llm_tokens.py's DEFAULT_LLM_SPEND_LIMIT, MAX_LLM_SPEND_LIMIT and
	// DEFAULT_LLM_SPEND_DAYS.
	spendDefaultLimit = 20
	spendMaxLimit     = 100
	spendDefaultDays  = 30
)

// spendRun is one LLMTokenSpendRunRecord: a run's usage by provider and model.
type spendRun struct {
	runID, provider, model string
	calls, in, out         uint64
	computedAt             time.Time
	failures               *pyjson.Object
}

// spendLegacy is one LLMTokenSpendLegacyRecord: usage written without a run id.
type spendLegacy struct {
	provider, model string
	calls, in, out  uint64
	computedAt      time.Time
}

// spendSummary is LLMTokenSpendSummaryRecord.
type spendSummary struct {
	runs   []spendRun
	legacy []spendLegacy
}

// spendReader is the ClickHouse side of read_llm_token_spend.
type spendReader interface {
	readSpend(ctx context.Context, orgID string, limit uint32, since time.Time) (spendSummary, error)
}

var errNoClickHouse = errors.New("ClickHouse URI not configured")

// getLLMSpend is settings.py's get_llm_settings_spend: the org's LLM token
// usage since a moment, newest runs first, with each run's categorization
// failures by class. An org whose BYO settings are not active answers an
// empty window, without reading ClickHouse.
func (h *handlers) getLLMSpend(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	values := r.URL.Query()
	var errs pybody.Errors
	one := int64(1)
	limitValue, _ := errs.QueryInt("limit", queryPtr(values, "limit"), spendDefaultLimit, &one, nil)
	sinceValue, _ := errs.QueryDatetime("since", queryPtr(values, "since"))
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	if !h.requireBYOLLMAccess(ctx, w, orgID, false) {
		return
	}
	limit := uint32(spendMaxLimit)
	if limitValue.Cmp(big.NewInt(spendMaxLimit)) < 0 {
		limit = uint32(limitValue.Int64())
	}
	now := h.store.now()
	since := pytime.UTC(now.Add(-spendDefaultDays * 24 * time.Hour))
	if sinceValue != nil {
		since = *sinceValue
	}
	respond := func(summary spendSummary) {
		out := pyjson.NewObject()
		out.Set("since", pytime.Pydantic(since))
		out.Set("limit", pyjson.IntOf(int64(limit)))
		runs := []pyjson.Value{}
		for _, run := range summary.runs {
			item := pyjson.NewObject()
			item.Set("run_id", run.runID)
			item.Set("provider", run.provider)
			item.Set("model", run.model)
			item.Set("calls", uintValue(run.calls))
			item.Set("input_tokens", uintValue(run.in))
			item.Set("output_tokens", uintValue(run.out))
			item.Set("computed_at", spendTime(run.computedAt))
			item.Set("failures_by_class", run.failures)
			runs = append(runs, item)
		}
		out.Set("runs", runs)
		legacy := []pyjson.Value{}
		for _, row := range summary.legacy {
			item := pyjson.NewObject()
			item.Set("run_id", "")
			item.Set("marker", "legacy_empty_run_id")
			item.Set("provider", row.provider)
			item.Set("model", row.model)
			item.Set("calls", uintValue(row.calls))
			item.Set("input_tokens", uintValue(row.in))
			item.Set("output_tokens", uintValue(row.out))
			item.Set("computed_at", spendTime(row.computedAt))
			legacy = append(legacy, item)
		}
		out.Set("legacy", legacy)
		policy.WriteModel(w, http.StatusOK, out, nil)
	}
	active, err := h.orgLLMActive(ctx, orgID)
	if err != nil {
		h.internalError(ctx, w, "evaluate org llm status", err)
		return
	}
	if !active {
		respond(spendSummary{})
		return
	}
	if h.spend == nil {
		h.internalError(ctx, w, "read llm spend", errNoClickHouse)
		return
	}
	windowStart, err := spendWindowStart(since)
	if err != nil {
		h.internalError(ctx, w, "bind llm spend window", err)
		return
	}
	summary, err := h.spend.readSpend(ctx, orgID, limit, windowStart)
	if err != nil {
		h.internalError(ctx, w, "read llm spend", err)
		return
	}
	respond(summary)
}

func uintValue(value uint64) pyjson.Value {
	return pyjson.Int{Int: new(big.Int).SetUint64(value)}
}

// spendTime is a ClickHouse DateTime column value as the response carries it.
func spendTime(at time.Time) string {
	return pytime.Pydantic(pytime.DateTime{Time: at.UTC()})
}

// spendWindowStart is what clickhouse-connect binds for a `{since:DateTime}`
// parameter: `value.astimezone(server_tz)` (UTC) in whole seconds. Python
// refuses two ranges with an unhandled error, an unhandled 500 here too: an
// aware value whose UTC instant leaves years 1..9999 (OverflowError), and a
// naive value on the first or last day the datetime type holds (its local-time
// lookup steps a day outside the type). A naive value is read as UTC.
func spendWindowStart(since pytime.DateTime) (time.Time, error) {
	instant := since.Time.UTC()
	if since.Aware {
		if instant.Year() < 1 || instant.Year() > 9999 {
			return time.Time{}, errSpendWindowRange
		}
	} else if (instant.Year() == 1 && instant.YearDay() == 1) || (instant.Year() == 9999 && instant.Month() == time.December && instant.Day() == 31) {
		return time.Time{}, errSpendWindowRange
	}
	return instant.Truncate(time.Second), nil
}

var errSpendWindowRange = errors.New("since is outside the range astimezone converts")

// orgLLMActive is evaluate_org_llm_status(...).active: the org's own LLM
// provider, credentials and base_url settings, without the byo_llm flag gate
// the runtime resolver applies.
func (h *handlers) orgLLMActive(ctx context.Context, orgID string) (bool, error) {
	providerText, err := h.settingText(ctx, orgID, llmCategory, "provider")
	if err != nil {
		return false, err
	}
	provider := llmorgsettings.NormalizeProvider(orEmpty(providerText))
	if provider == "" || provider == "auto" || provider == "mock" || provider == "none" {
		return false, nil
	}
	if !llmorgsettings.IsKnownProvider(provider) {
		return false, nil
	}
	apiKey, err := h.settingText(ctx, orgID, llmCategory, "api_key")
	if err != nil {
		return false, err
	}
	baseURL, err := h.settingText(ctx, orgID, llmCategory, "base_url")
	if err != nil {
		return false, err
	}
	if orEmpty(apiKey) == "" && orEmpty(baseURL) == "" {
		return false, nil
	}
	if !llmorgsettings.CredentialsComplete(provider, orEmpty(apiKey)) {
		return false, nil
	}
	ok, _, err := llmorgsettings.ValidateBaseURLChecked(ctx, orEmpty(baseURL))
	if err != nil {
		return false, err
	}
	return ok, nil
}

// clickhouseSpend reads llm_token_usage and work_unit_investments.
type clickhouseSpend struct{ conn driver.Conn }

const (
	spendLatestRunsQuery = `SELECT run_id, max(computed_at) AS last_at
FROM llm_token_usage
WHERE org_id = {org_id:String}
  AND run_id != ''
  AND computed_at >= {since:DateTime}
GROUP BY run_id
ORDER BY last_at DESC, run_id DESC
LIMIT {limit:UInt32}`
	spendRunsQuery = `SELECT run_id, provider, model, sum(calls) AS calls, sum(input_tokens) AS input_tokens,
    sum(output_tokens) AS output_tokens, max(computed_at) AS last_computed_at
FROM llm_token_usage
WHERE org_id = {org_id:String}
  AND run_id IN {run_ids:Array(String)}
  AND computed_at >= {since:DateTime}
GROUP BY run_id, provider, model
ORDER BY last_computed_at DESC, run_id DESC, model ASC`
	spendLegacyQuery = `SELECT provider, model, sum(calls) AS calls, sum(input_tokens) AS input_tokens,
    sum(output_tokens) AS output_tokens, max(computed_at) AS last_computed_at
FROM llm_token_usage
WHERE org_id = {org_id:String}
  AND run_id = ''
  AND computed_at >= {since:DateTime}
GROUP BY provider, model
ORDER BY last_computed_at DESC, model ASC`
	spendFailuresQuery = `SELECT categorization_run_id, categorization_status, categorization_errors_json
FROM work_unit_investments
WHERE org_id = {org_id:String}
  AND categorization_run_id IN {run_ids:Array(String)}
  AND computed_at >= {since:DateTime}`
)

func (c clickhouseSpend) readSpend(ctx context.Context, orgID string, limit uint32, since time.Time) (spendSummary, error) {
	sinceText := since.Format("2006-01-02 15:04:05")
	var summary spendSummary
	rows, err := c.conn.Query(ctx, spendLatestRunsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("since", sinceText), clickhouse.Named("limit", limit))
	if err != nil {
		return summary, err
	}
	runIDs := []string{}
	for rows.Next() {
		var runID string
		var last time.Time
		if err := rows.Scan(&runID, &last); err != nil {
			_ = rows.Close()
			return summary, err
		}
		if runID != "" {
			runIDs = append(runIDs, runID)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return summary, err
	}
	_ = rows.Close()
	if len(runIDs) > 0 {
		summary.runs, err = c.runs(ctx, orgID, sinceText, runIDs)
		if err != nil {
			return summary, err
		}
	}
	summary.legacy, err = c.legacy(ctx, orgID, sinceText)
	return summary, err
}

func (c clickhouseSpend) runs(ctx context.Context, orgID, since string, runIDs []string) ([]spendRun, error) {
	params := []any{clickhouse.Named("org_id", orgID), clickhouse.Named("since", since), clickhouse.Named("run_ids", runIDs)}
	rows, err := c.conn.Query(ctx, spendRunsQuery, params...)
	if err != nil {
		return nil, err
	}
	var out []spendRun
	for rows.Next() {
		var run spendRun
		if err := rows.Scan(&run.runID, &run.provider, &run.model, &run.calls, &run.in, &run.out, &run.computedAt); err != nil {
			_ = rows.Close()
			return nil, err
		}
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()
	failures, err := c.failures(ctx, params)
	if err != nil {
		return nil, err
	}
	for index := range out {
		out[index].failures = failures[out[index].runID]
		if out[index].failures == nil {
			out[index].failures = pyjson.NewObject()
		}
	}
	return out, nil
}

func (c clickhouseSpend) failures(ctx context.Context, params []any) (map[string]*pyjson.Object, error) {
	rows, err := c.conn.Query(ctx, spendFailuresQuery, params...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]*pyjson.Object{}
	for rows.Next() {
		var runID, status, errorsJSON string
		if err := rows.Scan(&runID, &status, &errorsJSON); err != nil {
			return nil, err
		}
		class, err := categorizationOutcomeClass(status, errorsJSON)
		if err != nil {
			return nil, err
		}
		if class == "" {
			continue
		}
		counts := out[runID]
		if counts == nil {
			counts = pyjson.NewObject()
			out[runID] = counts
		}
		count := int64(1)
		if previous, ok := counts.Get(class); ok {
			count = previous.(pyjson.Int).Int64() + 1
		}
		counts.Set(class, pyjson.IntOf(count))
	}
	return out, rows.Err()
}

func (c clickhouseSpend) legacy(ctx context.Context, orgID, since string) ([]spendLegacy, error) {
	rows, err := c.conn.Query(ctx, spendLegacyQuery, clickhouse.Named("org_id", orgID), clickhouse.Named("since", since))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []spendLegacy
	for rows.Next() {
		var row spendLegacy
		if err := rows.Scan(&row.provider, &row.model, &row.calls, &row.in, &row.out, &row.computedAt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// categorizationOutcomeClass is llm_tokens.py's _categorization_outcome_class.
// A document json.loads refuses with a ValueError that is not a
// JSONDecodeError (an integer of more than 4300 digits) is an unhandled
// error there, so it is one here.
func categorizationOutcomeClass(status, errorsJSON string) (string, error) {
	if status == "ok" {
		return "", nil
	}
	if status != "" {
		return status, nil
	}
	document := errorsJSON
	if document == "" {
		document = "[]"
	}
	parsed, err := pyjson.DecodeString(document)
	if err != nil {
		var syntax *pyjson.SyntaxError
		if errors.As(err, &syntax) {
			return "categorization_error", nil
		}
		return "", err
	}
	if list, ok := parsed.([]pyjson.Value); ok && len(list) > 0 {
		return "categorization_error", nil
	}
	return "unknown_outcome", nil
}
