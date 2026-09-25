package llmbudget

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	// Category and LimitKey are llm/budget.py's BUDGET_CATEGORY and
	// BUDGET_LIMIT_KEY: the setting that holds an organization's ceiling.
	Category = "llm_budget"
	LimitKey = "limit_micro_usd"
	// PricingVersion is PRICING_VERSION.
	PricingVersion = "openai-public-2025-08-07.v1"
	// Window is BUDGET_WINDOW.
	Window = "calendar_month_utc"
	// DefaultOperatorMaxMicroUSD is DEFAULT_OPERATOR_MAX_MICRO_USD.
	DefaultOperatorMaxMicroUSD = 100_000_000
	// LicenseLimitKey is LICENSE_LIMIT_KEY: the org licence override that can
	// lower the ceiling.
	LicenseLimitKey = "byo_llm_budget_micro_usd"

	operatorMaxEnv = "BYO_LLM_MAX_BUDGET_MICRO_USD"
)

// LookupEnv reads one environment variable and says whether it is set.
type LookupEnv func(string) (string, bool)

// RowQuerier is the part of a pgx pool or transaction the ceiling read needs.
type RowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// DB is RowQuerier plus the multi-row read of the reservations.
type DB interface {
	RowQuerier
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// OperatorMaximum is operator_maximum_micro_usd: the env ceiling, the default
// when unset, and 0 when the value is not an integer or is negative.
func OperatorMaximum(lookup LookupEnv) *big.Int {
	raw, set := lookup(operatorMaxEnv)
	if !set {
		return big.NewInt(DefaultOperatorMaxMicroUSD)
	}
	value, err := pythonparity.ParseInt(raw)
	if err != nil || value.Sign() < 0 {
		return new(big.Int)
	}
	return value
}

// ProvisionedMaximum is provisioned_maximum_micro_usd: the lower of the
// operator ceiling and the org licence's byo_llm_budget_micro_usd override.
func ProvisionedMaximum(ctx context.Context, db RowQuerier, lookup LookupEnv, orgID string) (*big.Int, error) {
	operator := OperatorMaximum(lookup)
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		return operator, nil
	}
	var raw []byte
	err = db.QueryRow(ctx, `SELECT limits_override FROM org_licenses WHERE org_id = $1`, org).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return operator, nil
	}
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return operator, nil
	}
	value, err := pyjson.DecodeString(string(raw))
	if err != nil {
		return nil, err
	}
	object, isObject := value.(*pyjson.Object)
	if !isObject {
		return operator, nil
	}
	item, present := object.Get(LicenseLimitKey)
	if !present {
		return operator, nil
	}
	licensed, err := pythonparity.ParseInt(pyjson.Str(item))
	if err != nil || licensed.Sign() < 0 {
		return new(big.Int), nil
	}
	if licensed.Cmp(operator) < 0 {
		return licensed, nil
	}
	return operator, nil
}

// LockKey is llm/budget.py's advisory-lock key: the first eight bytes of
// sha256("byo-llm-budget:<org>") as a big-endian integer, masked to 63 bits.
func LockKey(orgID string) int64 {
	digest := sha256.Sum256([]byte("byo-llm-budget:" + orgID))
	return int64(binary.BigEndian.Uint64(digest[:8]) & (1<<63 - 1))
}

// MonthWindow is _window: the start of the UTC calendar month containing now
// and the start of the next.
func MonthWindow(now time.Time) (start, reset time.Time) {
	utc := now.UTC()
	start = time.Date(utc.Year(), utc.Month(), 1, 0, 0, 0, 0, time.UTC)
	return start, start.AddDate(0, 1, 0)
}

// DefaultModelByProvider is llm/providers/base.py's DEFAULT_MODEL_BY_PROVIDER.
var DefaultModelByProvider = map[string]string{
	"openai":        "gpt-5-mini",
	"anthropic":     "claude-3-haiku-20240307",
	"gemini":        "gemini-3",
	"local":         "llama3.2",
	"ollama":        "llama3.2",
	"lmstudio":      "local-model",
	"qwen":          "qwen-plus",
	"qwen-local":    "qwen2.5:7b",
	"qwen-lmstudio": "local-model",
}

// Reason is BudgetReason.
type Reason string

const (
	ReasonAvailable           Reason = "available"
	ReasonBudgetNotConfigured Reason = "budget_not_configured"
	ReasonPricingUnavailable  Reason = "pricing_unavailable"
	ReasonUsageUnavailable    Reason = "usage_unavailable"
	ReasonBudgetExhausted     Reason = "budget_exhausted"
)

// Status is BYOBudgetStatus. A nil integer is the JSON null.
type Status struct {
	Used             *big.Int
	Limit            *big.Int
	Remaining        *big.Int
	Window           string
	ResetAt          time.Time
	EnforcementAvail bool
	Reason           Reason
	MaximumLimit     *big.Int
	PricingVersion   *string
}

// Input is what get_budget_status is given besides the database: the org's
// BYO provider, model and base URL as stored, and the raw stored limit (nil
// when the setting does not exist).
type Input struct {
	Provider string
	Model    string
	BaseURL  *string
	RawLimit *string
}

// nonNegativeInt is _nonnegative_int: nil for an absent or unparsable or
// negative value.
func nonNegativeInt(value *string) *big.Int {
	if value == nil {
		return nil
	}
	parsed, err := pythonparity.ParseInt(*value)
	if err != nil || parsed.Sign() < 0 {
		return nil
	}
	return parsed
}

// Compute is get_budget_status. An error is what Python lets escape as an
// unhandled exception (a database failure, an unparsable port in an
// acceptance-transport URL): the caller answers the generic 500.
func Compute(ctx context.Context, db DB, lookup LookupEnv, orgID string, in Input, now time.Time) (Status, error) {
	windowStart, resetAt := MonthWindow(now)
	maximum, err := ProvisionedMaximum(ctx, db, lookup, orgID)
	if err != nil {
		return Status{}, err
	}
	parsed := nonNegativeInt(in.RawLimit)
	var limit *big.Int
	if parsed != nil {
		limit = parsed
		if maximum.Cmp(parsed) < 0 {
			limit = maximum
		}
	}
	baseURL := ""
	if in.BaseURL != nil {
		baseURL = *in.BaseURL
	}
	priced, err := ReliablePrice(in.Provider, in.Model, baseURL)
	if err != nil {
		return Status{}, err
	}
	base := Status{Window: Window, ResetAt: resetAt, MaximumLimit: maximum}
	if !priced {
		base.Limit = limit
		base.EnforcementAvail = false
		base.Reason = ReasonPricingUnavailable
		if limit == nil {
			base.Reason = ReasonBudgetNotConfigured
		}
		return base, nil
	}
	version := PricingVersion
	base.PricingVersion = &version

	used := new(big.Int)
	usageUnavailable := false
	if org, err := pythonparity.ParseUUID(orgID); err == nil {
		rows, err := db.Query(ctx, `SELECT status, actual_micro_usd, reserved_micro_usd FROM byo_llm_budget_reservations WHERE org_id = $1 AND window_start = $2`, org, windowStart)
		if err != nil {
			return Status{}, err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				status   string
				actual   *int64
				reserved int64
			)
			if err := rows.Scan(&status, &actual, &reserved); err != nil {
				return Status{}, err
			}
			if status == "usage_unavailable" {
				usageUnavailable = true
			}
			if status == "voided" {
				continue
			}
			if actual != nil {
				used.Add(used, big.NewInt(*actual))
			} else {
				used.Add(used, big.NewInt(reserved))
			}
		}
		if err := rows.Err(); err != nil {
			return Status{}, err
		}
	}
	if usageUnavailable {
		base.Limit = limit
		base.EnforcementAvail = false
		base.Reason = ReasonUsageUnavailable
		return base, nil
	}
	base.Used = used
	if limit == nil {
		base.EnforcementAvail = false
		base.Reason = ReasonBudgetNotConfigured
		return base, nil
	}
	remaining := new(big.Int).Sub(limit, used)
	if remaining.Sign() < 0 {
		remaining = new(big.Int)
	}
	base.Limit = limit
	base.Remaining = remaining
	base.EnforcementAvail = true
	base.Reason = ReasonAvailable
	if remaining.Sign() == 0 {
		base.Reason = ReasonBudgetExhausted
	}
	return base, nil
}

// normalizedModel is _normalized_model: strip, lower, and every dated
// gpt-5-mini variant is gpt-5-mini.
func normalizedModel(model string) string {
	value := pythonparity.Lower(pythonparity.Strip(model))
	if value == "gpt-5-mini" || strings.HasPrefix(value, "gpt-5-mini-") {
		return "gpt-5-mini"
	}
	return value
}

const (
	scriptedModel        = "ask-dev-scripted-v1"
	scriptedProviderHost = "ask-dev-scripted-openai"
	scriptedProviderPort = 8001
)

// officialOpenAIEndpoint is _official_openai_endpoint: no base URL is the
// official endpoint; a URL that cannot be split is not.
func officialOpenAIEndpoint(baseURL string) bool {
	if pythonparity.Strip(baseURL) == "" {
		return true
	}
	parsed, err := pythonparity.SplitURL(baseURL)
	if err != nil {
		return false
	}
	host, _ := parsed.Hostname()
	return parsed.Scheme == "https" && host == "api.openai.com"
}

// acceptanceScriptedTransport is _is_acceptance_scripted_transport. The port
// read can raise a ValueError Python does not catch; it is returned as err.
func acceptanceScriptedTransport(baseURL string) (bool, error) {
	if baseURL == "" {
		return false, nil
	}
	parsed, err := pythonparity.SplitURL(baseURL)
	if err != nil {
		return false, nil
	}
	if parsed.Scheme != "http" {
		return false, nil
	}
	host, _ := parsed.Hostname()
	if host != scriptedProviderHost {
		return false, nil
	}
	port, present, err := parsed.Port()
	if err != nil {
		return false, err
	}
	return present && port == scriptedProviderPort, nil
}

// platformPriceKeys is openai_compatible._PLATFORM_MODEL_PRICES's keys in
// order; scriptedModel is its carve-out entry (exact match only).
var platformPriceKeys = []string{"gpt-5-mini", "gpt-5-nano", scriptedModel}

// canonicalPricedModel is _canonical_priced_model: the price book key the
// model (stripped, NOT lowered) resolves to, honouring dated variants for
// every entry but the scripted carve-out.
func canonicalPricedModel(model string) (string, bool) {
	model = pythonparity.Strip(model)
	for _, known := range platformPriceKeys {
		if model == known || (known != scriptedModel && strings.HasPrefix(model, known+"-")) {
			return known, true
		}
	}
	return "", false
}

// ReliablePrice is `reliable_price(...) is not None`: whether the price book
// prices this provider, model and endpoint. The price VALUES are not needed by
// a status read; only their existence is. An absent pair is unavailable,
// never zero.
func ReliablePrice(provider, model, baseURL string) (bool, error) {
	normalizedProvider := pythonparity.Lower(pythonparity.Strip(provider))
	normalized := normalizedModel(model)
	if normalizedProvider == "openai" && normalized == scriptedModel {
		scripted, err := acceptanceScriptedTransport(baseURL)
		if err != nil {
			return false, err
		}
		if scripted {
			return true, nil
		}
	}
	if normalizedProvider != "openai" || !officialOpenAIEndpoint(baseURL) {
		return false, nil
	}
	if normalized == "gpt-5-mini" {
		return true, nil
	}
	canonical, ok := canonicalPricedModel(model)
	return ok && canonical != scriptedModel, nil
}
