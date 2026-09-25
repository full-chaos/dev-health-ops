package admin

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	llmCategory   = "llm"
	byoLLMFeature = "byo_llm"
	byoLLMMinTier = "team"
)

// llmSettingKeys is llm_settings.py's LLM_SETTING_KEYS, in delete order.
var llmSettingKeys = []string{"provider", "model", "api_key", "base_url", "concurrency"}

// lookupEnv reads the process environment; a variable so a test can point
// the budget ceiling at a fixed value without touching the real one.
var lookupEnv = os.LookupEnv

func (h *handlers) llmSettingsRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: governancePrefix + "/llm-settings", Allow: "GET", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getLLMSettings))},
		{Method: http.MethodPut, Pattern: governancePrefix + "/llm-settings", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.putLLMSettings))},
		{Method: http.MethodDelete, Pattern: governancePrefix + "/llm-settings", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.deleteLLMSettings))},
		{Method: http.MethodGet, Pattern: governancePrefix + "/llm-settings/budget", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getLLMBudget))},
		{Method: http.MethodGet, Pattern: governancePrefix + "/llm-settings/spend", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getLLMSpend))},
	}
}

func notFoundOrg() *pyjson.Object {
	detail := pyjson.NewObject()
	detail.Set("error", "organization_not_found")
	detail.Set("message", "Organization not found")
	return detail
}

// requireBYOLLMAccess is llm_settings.py's require_byo_llm_access. It answers
// the refusal itself and returns false; a failed lookup answers the generic
// 500 (the gate fails closed).
func (h *handlers) requireBYOLLMAccess(ctx context.Context, w http.ResponseWriter, orgID string, forCleanup bool) bool {
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		policy.WriteDetail(w, http.StatusNotFound, notFoundOrg(), nil)
		return false
	}
	var found uuid.UUID
	err = h.store.Pool.QueryRow(ctx, `SELECT id FROM organizations WHERE id = $1`, org).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		policy.WriteDetail(w, http.StatusNotFound, notFoundOrg(), nil)
		return false
	}
	if err != nil {
		h.internalError(ctx, w, "look up organization", err)
		return false
	}
	// Cleanup skips both the tier and the flag gate: stored credentials must
	// stay removable after a downgrade or a kill switch.
	if forCleanup {
		return true
	}
	tier, err := licensing.ResolveOrgTier(ctx, h.store.Pool, org)
	if err != nil {
		h.internalError(ctx, w, "resolve org tier", err)
		return false
	}
	have, _ := licensing.TierRank(tier)
	want, _ := licensing.TierRank(byoLLMMinTier)
	if have < want {
		detail := pyjson.NewObject()
		detail.Set("error", "feature_not_licensed")
		detail.Set("feature", byoLLMFeature)
		detail.Set("required_tier", byoLLMMinTier)
		detail.Set("current_tier", tier)
		policy.WriteDetail(w, http.StatusPaymentRequired, detail, nil)
		return false
	}
	state, _, err := licensing.FeatureFlagState(ctx, h.store.Pool, org, byoLLMFeature, byoLLMMinTier, h.store.now())
	if err != nil {
		h.internalError(ctx, w, "resolve byo_llm flag", err)
		return false
	}
	if state == licensing.StateDisabled {
		detail := pyjson.NewObject()
		detail.Set("error", "feature_not_enabled")
		detail.Set("feature", byoLLMFeature)
		detail.Set("message", "BYO LLM is not enabled for this organization")
		policy.WriteDetail(w, http.StatusForbidden, detail, nil)
		return false
	}
	return true
}

// maskAPIKey is llm_settings.py's mask_api_key, by code point.
func maskAPIKey(value string) *string {
	if value == "" {
		return nil
	}
	runes := pyjson.Runes(value)
	var masked string
	if len(runes) <= 8 {
		masked = "********"
	} else {
		masked = pyjson.FromRunes(runes[:4]) + "…" + pyjson.FromRunes(runes[len(runes)-4:])
	}
	return &masked
}

// settingValue is SettingsService.get for a loaded row: an encrypted, non-empty
// value is decrypted; a value that cannot be is an unhandled error.
func (h *handlers) settingValue(row *setting) (*string, error) {
	if row == nil {
		return nil, nil
	}
	if !row.IsEncrypted || row.Value == nil || *row.Value == "" {
		return row.Value, nil
	}
	if !h.encryptionConfigured() {
		return nil, errEncryptionKeyMissing
	}
	plain, err := h.decryptor.Decrypt(secrets.NewValue(*row.Value))
	if err != nil {
		return nil, err
	}
	text := string(plain)
	return &text, nil
}

// llmSettingsResponse is get_llm_settings_response over db, serialised with
// response_model_exclude_none: a value that is None is omitted.
func (h *handlers) llmSettingsResponse(ctx context.Context, db pgExecer, orgID string) (*pyjson.Object, error) {
	values := make(map[string]*string, len(llmSettingKeys))
	for _, key := range llmSettingKeys {
		row, err := settingByKeyOn(ctx, db, orgID, llmCategory, key)
		if err != nil {
			return nil, err
		}
		value, err := h.settingValue(row)
		if err != nil {
			return nil, err
		}
		values[key] = value
	}
	out := pyjson.NewObject()
	if values["provider"] != nil {
		out.Set("provider", *values["provider"])
	}
	if values["model"] != nil {
		out.Set("model", *values["model"])
	}
	if key := values["api_key"]; key != nil {
		if masked := maskAPIKey(*key); masked != nil {
			out.Set("api_key", *masked)
		}
	}
	if values["base_url"] != nil {
		out.Set("base_url", *values["base_url"])
	}
	if raw := values["concurrency"]; raw != nil && *raw != "" {
		parsed, err := pythonparity.ParseInt(*raw)
		if err != nil {
			return nil, err
		}
		out.Set("concurrency", pyjson.Int{Int: parsed})
	}
	return out, nil
}

// getLLMSettings is settings.py's get_llm_settings.
func (h *handlers) getLLMSettings(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	if !h.requireBYOLLMAccess(ctx, w, orgID, false) {
		return
	}
	out, err := h.llmSettingsResponse(ctx, h.store.Pool, orgID)
	if err != nil {
		h.internalError(ctx, w, "read llm settings", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

func llmDetail(status int, w http.ResponseWriter, pairs ...string) {
	detail := pyjson.NewObject()
	for index := 0; index+1 < len(pairs); index += 2 {
		detail.Set(pairs[index], pairs[index+1])
	}
	policy.WriteDetail(w, status, detail, nil)
}

// putLLMSettings is settings.py's upsert_llm_settings with llm_settings.py's
// upsert_llm_settings: every write and the budget limit share one transaction
// that a refusal or a failed response rolls back.
func (h *handlers) putLLMSettings(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var (
		provider                   *string
		model, apiKey, baseURL     *string
		concurrency, budgetLimitMu *big.Int
	)
	if ok {
		if v, present := errs.RequiredString(object, "provider", 1, 0); present {
			provider = &v
		}
		if v, present := errs.OptionalString(object, "model", 0, 0); present {
			model = &v
		}
		if v, present := errs.OptionalString(object, "api_key", 0, 0); present {
			apiKey = &v
		}
		if v, present := errs.OptionalString(object, "base_url", 0, 0); present {
			baseURL = &v
		}
		if v, present := errs.OptionalBoundedInt(object, "concurrency", 1, 32); present {
			concurrency = big.NewInt(v)
		}
		if v, present := errs.OptionalMinInt(object, "budget_limit_micro_usd", 0); present {
			budgetLimitMu = v
		}
	}
	user := policy.UserFrom(ctx)
	orgID, ok := adminOrgID(w, user)
	if !ok {
		return
	}
	if len(errs) > 0 || provider == nil {
		writeValidation(w, errs)
		return
	}
	if !h.requireBYOLLMAccess(ctx, w, orgID, false) {
		return
	}
	if budgetLimitMu != nil && user.ImpersonatedBy != nil && *user.ImpersonatedBy != "" {
		llmDetail(http.StatusForbidden, w,
			"error", "impersonated_write_forbidden",
			"message", "BYO LLM budget changes are unavailable while impersonating")
		return
	}
	baseURLText := ""
	if baseURL != nil {
		baseURLText = *baseURL
	}
	valid, reason, splitErr := llmorgsettings.ValidateBaseURLChecked(ctx, baseURLText)
	if splitErr != nil {
		// urlsplit's ValueError escapes validate_llm_base_url: an unhandled 500.
		h.internalError(ctx, w, "validate base url", splitErr)
		return
	}
	if !valid {
		if reason == "" {
			reason = "Invalid LLM base_url"
		}
		llmDetail(http.StatusBadRequest, w, "error", "invalid_base_url", "feature", byoLLMFeature, "message", reason)
		return
	}

	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		h.internalError(ctx, w, "begin llm settings", err)
		return
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	set := func(key string, value *string, encrypt bool, description string) error {
		_, err := h.upsertSetting(ctx, tx, orgID, llmCategory, key, value, encrypt, &description)
		return err
	}
	normalized := pythonparity.Lower(pythonparity.Strip(*provider))
	steps := []struct {
		run func() error
	}{
		{func() error { return set("provider", &normalized, false, "BYO LLM provider for this organization") }},
		{func() error { return set("model", model, false, "BYO LLM model for this organization") }},
	}
	if apiKey != nil {
		steps = append(steps, struct{ run func() error }{func() error {
			return set("api_key", apiKey, true, "Encrypted BYO LLM API key for this organization")
		}})
	}
	steps = append(steps, struct{ run func() error }{func() error {
		return set("base_url", baseURL, false, "BYO LLM base URL for this organization")
	}})
	if concurrency != nil {
		text := concurrency.String()
		steps = append(steps, struct{ run func() error }{func() error {
			return set("concurrency", &text, false, "BYO LLM maximum concurrent categorizations for this organization")
		}})
	}
	for _, step := range steps {
		if err := step.run(); err != nil {
			h.internalError(ctx, w, "write llm setting", err)
			return
		}
	}
	if budgetLimitMu != nil {
		maximum, err := llmbudget.ProvisionedMaximum(ctx, tx, lookupEnv, orgID)
		if err != nil {
			h.internalError(ctx, w, "resolve budget ceiling", err)
			return
		}
		if budgetLimitMu.Sign() < 0 || budgetLimitMu.Cmp(maximum) > 0 {
			llmDetail(http.StatusBadRequest, w,
				"error", "budget_limit_exceeds_maximum",
				"feature", byoLLMFeature,
				"message", "budget_limit_micro_usd must be between 0 and "+maximum.String())
			return
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, llmbudget.LockKey(orgID)); err != nil {
			h.internalError(ctx, w, "lock budget", err)
			return
		}
		limit := budgetLimitMu.String()
		description := "Organization BYO LLM monetary ceiling in integer micro-USD; stored separately from provider credentials"
		if _, err := h.upsertSetting(ctx, tx, orgID, llmbudget.Category, llmbudget.LimitKey, &limit, false, &description); err != nil {
			h.internalError(ctx, w, "write budget limit", err)
			return
		}
	}
	out, err := h.llmSettingsResponse(ctx, tx, orgID)
	if err != nil {
		h.internalError(ctx, w, "build llm settings response", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.internalError(ctx, w, "commit llm settings", err)
		return
	}
	committed = true
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// deleteLLMSettings is settings.py's delete_llm_settings: cleanup access, then
// every llm key deleted; 404 when none existed.
func (h *handlers) deleteLLMSettings(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	if !h.requireBYOLLMAccess(ctx, w, orgID, true) {
		return
	}
	tag, err := h.store.Pool.Exec(ctx,
		`DELETE FROM settings WHERE org_id = $1 AND category = $2 AND key = ANY($3)`,
		orgID, llmCategory, llmSettingKeys)
	if err != nil {
		h.internalError(ctx, w, "delete llm settings", err)
		return
	}
	if tag.RowsAffected() == 0 {
		policy.WriteDetail(w, http.StatusNotFound, "LLM settings not found", nil)
		return
	}
	out := pyjson.NewObject()
	out.Set("deleted", true)
	policy.WriteModel(w, http.StatusOK, out, nil)
}
