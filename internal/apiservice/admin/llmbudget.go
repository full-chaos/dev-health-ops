package admin

import (
	"context"
	"math/big"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
)

// settingText is SettingsService.get(key, category): the decrypted value, nil
// when the setting does not exist.
func (h *handlers) settingText(ctx context.Context, orgID, category, key string) (*string, error) {
	row, err := settingByKeyOn(ctx, h.store.Pool, orgID, category, key)
	if err != nil {
		return nil, err
	}
	return h.settingValue(row)
}

func orEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func optionalBig(value *big.Int) pyjson.Value {
	if value == nil {
		return nil
	}
	return pyjson.Int{Int: value}
}

// getLLMBudget is settings.py's get_llm_settings_budget: the org's BYO LLM
// monetary budget for the current UTC calendar month.
func (h *handlers) getLLMBudget(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	if !h.requireBYOLLMAccess(ctx, w, orgID, false) {
		return
	}
	fail := func(what string, err error) { h.internalError(ctx, w, what, err) }
	providerText, err := h.settingText(ctx, orgID, llmCategory, "provider")
	if err != nil {
		fail("read llm provider", err)
		return
	}
	provider := orEmpty(providerText)
	modelText, err := h.settingText(ctx, orgID, llmCategory, "model")
	if err != nil {
		fail("read llm model", err)
		return
	}
	model := orEmpty(modelText)
	if model == "" {
		model = llmbudget.DefaultModelByProvider[provider]
	}
	baseURL, err := h.settingText(ctx, orgID, llmCategory, "base_url")
	if err != nil {
		fail("read llm base url", err)
		return
	}
	rawLimit, err := h.settingText(ctx, orgID, llmbudget.Category, llmbudget.LimitKey)
	if err != nil {
		fail("read llm budget limit", err)
		return
	}
	status, err := llmbudget.Compute(ctx, h.store.Pool, lookupEnv, orgID,
		llmbudget.Input{Provider: provider, Model: model, BaseURL: baseURL, RawLimit: rawLimit}, h.store.now())
	if err != nil {
		fail("compute llm budget status", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("used_micro_usd", optionalBig(status.Used))
	out.Set("limit_micro_usd", optionalBig(status.Limit))
	out.Set("remaining_micro_usd", optionalBig(status.Remaining))
	out.Set("window", status.Window)
	out.Set("reset_at", pyTimeString(status.ResetAt))
	out.Set("enforcement_available", status.EnforcementAvail)
	out.Set("reason", string(status.Reason))
	out.Set("maximum_limit_micro_usd", optionalBig(status.MaximumLimit))
	out.Set("pricing_version", optionalString(status.PricingVersion))
	policy.WriteModel(w, http.StatusOK, out, nil)
}
