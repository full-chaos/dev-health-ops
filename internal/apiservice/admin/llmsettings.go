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

// llmRefusal is one refusal of the BYO LLM settings functions
// (LLMSettingsAccessError): the status the route answers and the detail it
// carries. The route writes it; the operator verb prints its message.
type llmRefusal struct {
	status int
	detail any
}

func (r *llmRefusal) write(w http.ResponseWriter) { policy.WriteDetail(w, r.status, r.detail, nil) }

// message is LLMSettingsAccessError.message: the tier refusal spells out both
// tiers, every other refusal is its detail's message.
func (r *llmRefusal) message() string {
	detail, ok := r.detail.(*pyjson.Object)
	if !ok {
		return "BYO LLM settings access denied"
	}
	text := func(key string) string {
		value, _ := detail.Get(key)
		str, _ := value.(string)
		return str
	}
	if text("error") == "feature_not_licensed" {
		return "BYO LLM settings require " + text("required_tier") + " tier; current tier is " + text("current_tier")
	}
	if message := text("message"); message != "" {
		return message
	}
	return "BYO LLM settings access denied"
}

func newLLMRefusal(status int, pairs ...string) *llmRefusal {
	detail := pyjson.NewObject()
	for index := 0; index+1 < len(pairs); index += 2 {
		detail.Set(pairs[index], pairs[index+1])
	}
	return &llmRefusal{status: status, detail: detail}
}

// llmStepError names the lookup that failed, for the route's log line.
type llmStepError struct {
	step string
	err  error
}

func (e *llmStepError) Error() string { return e.step + ": " + e.err.Error() }
func (e *llmStepError) Unwrap() error { return e.err }

// byoLLMAccess is llm_settings.py's require_byo_llm_access: a refusal, or an
// error when a lookup failed (the gate fails closed).
func (h *handlers) byoLLMAccess(ctx context.Context, orgID string, forCleanup bool) (*llmRefusal, error) {
	notFound := func() *llmRefusal { return &llmRefusal{status: http.StatusNotFound, detail: notFoundOrg()} }
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		return notFound(), nil
	}
	var found uuid.UUID
	err = h.store.Pool.QueryRow(ctx, `SELECT id FROM organizations WHERE id = $1`, org).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFound(), nil
	}
	if err != nil {
		return nil, &llmStepError{"look up organization", err}
	}
	// Cleanup skips both the tier and the flag gate: stored credentials must
	// stay removable after a downgrade or a kill switch.
	if forCleanup {
		return nil, nil
	}
	tier, err := licensing.ResolveOrgTier(ctx, h.store.Pool, org)
	if err != nil {
		return nil, &llmStepError{"resolve org tier", err}
	}
	have, _ := licensing.TierRank(tier)
	want, _ := licensing.TierRank(byoLLMMinTier)
	if have < want {
		detail := pyjson.NewObject()
		detail.Set("error", "feature_not_licensed")
		detail.Set("feature", byoLLMFeature)
		detail.Set("required_tier", byoLLMMinTier)
		detail.Set("current_tier", tier)
		return &llmRefusal{status: http.StatusPaymentRequired, detail: detail}, nil
	}
	state, _, err := licensing.FeatureFlagState(ctx, h.store.Pool, org, byoLLMFeature, byoLLMMinTier, h.store.now())
	if err != nil {
		return nil, &llmStepError{"resolve byo_llm flag", err}
	}
	if state == licensing.StateDisabled {
		detail := pyjson.NewObject()
		detail.Set("error", "feature_not_enabled")
		detail.Set("feature", byoLLMFeature)
		detail.Set("message", "BYO LLM is not enabled for this organization")
		return &llmRefusal{status: http.StatusForbidden, detail: detail}, nil
	}
	return nil, nil
}

// requireBYOLLMAccess answers the route's refusal itself and returns false; a
// failed lookup answers the generic 500.
func (h *handlers) requireBYOLLMAccess(ctx context.Context, w http.ResponseWriter, orgID string, forCleanup bool) bool {
	refusal, err := h.byoLLMAccess(ctx, orgID, forCleanup)
	if err != nil {
		var step *llmStepError
		if errors.As(err, &step) {
			h.internalError(ctx, w, step.step, step.err)
		} else {
			h.internalError(ctx, w, "byo llm access", err)
		}
		return false
	}
	if refusal != nil {
		refusal.write(w)
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
	out, refusal, err := h.applyLLMSettings(ctx, orgID, llmUpsert{
		provider: *provider, model: model, apiKey: apiKey, baseURL: baseURL,
		concurrency: concurrency, budgetLimitMu: budgetLimitMu,
	})
	if err != nil {
		var step *llmStepError
		if errors.As(err, &step) {
			h.internalError(ctx, w, step.step, step.err)
		} else {
			h.internalError(ctx, w, "write llm settings", err)
		}
		return
	}
	if refusal != nil {
		refusal.write(w)
		return
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// llmUpsert is LLMSettingsUpsert as the writes read it.
type llmUpsert struct {
	provider                   string
	model, apiKey, baseURL     *string
	concurrency, budgetLimitMu *big.Int
}

// applyLLMSettings is llm_settings.py's upsert_llm_settings over settings.py's:
// the base URL is checked, then every write and the budget limit share one
// transaction that a refusal or a failed response rolls back. The route and the
// operator verb both run it.
func (h *handlers) applyLLMSettings(ctx context.Context, orgID string, in llmUpsert) (*pyjson.Object, *llmRefusal, error) {
	baseURLText := ""
	if in.baseURL != nil {
		baseURLText = *in.baseURL
	}
	valid, reason, splitErr := llmorgsettings.ValidateBaseURLChecked(ctx, baseURLText)
	if splitErr != nil {
		// urlsplit's ValueError escapes validate_llm_base_url: an unhandled 500.
		return nil, nil, &llmStepError{"validate base url", splitErr}
	}
	if !valid {
		if reason == "" {
			reason = "Invalid LLM base_url"
		}
		return nil, newLLMRefusal(http.StatusBadRequest, "error", "invalid_base_url", "feature", byoLLMFeature, "message", reason), nil
	}

	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		return nil, nil, &llmStepError{"begin llm settings", err}
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
	normalized := pythonparity.Lower(pythonparity.Strip(in.provider))
	steps := []struct {
		run func() error
	}{
		{func() error { return set("provider", &normalized, false, "BYO LLM provider for this organization") }},
		{func() error { return set("model", in.model, false, "BYO LLM model for this organization") }},
	}
	if in.apiKey != nil {
		steps = append(steps, struct{ run func() error }{func() error {
			return set("api_key", in.apiKey, true, "Encrypted BYO LLM API key for this organization")
		}})
	}
	steps = append(steps, struct{ run func() error }{func() error {
		return set("base_url", in.baseURL, false, "BYO LLM base URL for this organization")
	}})
	if in.concurrency != nil {
		text := in.concurrency.String()
		steps = append(steps, struct{ run func() error }{func() error {
			return set("concurrency", &text, false, "BYO LLM maximum concurrent categorizations for this organization")
		}})
	}
	for _, step := range steps {
		if err := step.run(); err != nil {
			return nil, nil, &llmStepError{"write llm setting", err}
		}
	}
	if in.budgetLimitMu != nil {
		maximum, err := llmbudget.ProvisionedMaximum(ctx, tx, lookupEnv, orgID)
		if err != nil {
			return nil, nil, &llmStepError{"resolve budget ceiling", err}
		}
		if in.budgetLimitMu.Sign() < 0 || in.budgetLimitMu.Cmp(maximum) > 0 {
			return nil, newLLMRefusal(http.StatusBadRequest,
				"error", "budget_limit_exceeds_maximum",
				"feature", byoLLMFeature,
				"message", "budget_limit_micro_usd must be between 0 and "+maximum.String()), nil
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, llmbudget.LockKey(orgID)); err != nil {
			return nil, nil, &llmStepError{"lock budget", err}
		}
		limit := in.budgetLimitMu.String()
		description := "Organization BYO LLM monetary ceiling in integer micro-USD; stored separately from provider credentials"
		if _, err := h.upsertSetting(ctx, tx, orgID, llmbudget.Category, llmbudget.LimitKey, &limit, false, &description); err != nil {
			return nil, nil, &llmStepError{"write budget limit", err}
		}
	}
	out, err := h.llmSettingsResponse(ctx, tx, orgID)
	if err != nil {
		return nil, nil, &llmStepError{"build llm settings response", err}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, &llmStepError{"commit llm settings", err}
	}
	committed = true
	return out, nil, nil
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
