package admin

import (
	"context"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Per-purpose BYO LLM overrides (CHAOS-10039) live in the same llm settings
// category as the default row, under "purpose:<purpose>:<field>". The routes
// are the default ones with a purpose: `?purpose=` on GET and DELETE, a
// "purpose" field in the PUT body.

// purposeParam reads an optional purpose; ok=false means the 400 was written.
func purposeParam(w http.ResponseWriter, raw string, present bool) (purpose llmorgsettings.Purpose, set, ok bool) {
	if !present {
		return "", false, true
	}
	purpose, valid := llmorgsettings.ParsePurpose(raw)
	if !valid && raw == "categorize" {
		llmDetail(http.StatusBadRequest, w,
			"error", "purpose_not_configurable",
			"message", "categorize runs on the platform provider and takes no organization setting")
		return "", false, false
	}
	if !valid {
		llmDetail(http.StatusBadRequest, w,
			"error", "invalid_purpose",
			"message", "purpose must be one of investigate, explain")
		return "", false, false
	}
	return purpose, true, true
}

// llmPurposeObject is one override as the API reports it, the api_key masked;
// nil when the purpose has no override.
func (h *handlers) llmPurposeObject(ctx context.Context, db pgExecer, orgID string, purpose llmorgsettings.Purpose) (*pyjson.Object, error) {
	values := map[string]*string{}
	for _, field := range llmorgsettings.Fields {
		row, err := settingByKeyOn(ctx, db, orgID, llmCategory, llmorgsettings.PurposeKey(purpose, field))
		if err != nil {
			return nil, err
		}
		value, err := h.settingValue(row)
		if err != nil {
			return nil, err
		}
		values[field] = value
	}
	if values["provider"] == nil || *values["provider"] == "" {
		return nil, nil
	}
	out := pyjson.NewObject()
	out.Set("purpose", string(purpose))
	out.Set("provider", *values["provider"])
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
	return out, nil
}

// llmPurposesObject maps each purpose with an override to it; nil when none.
func (h *handlers) llmPurposesObject(ctx context.Context, db pgExecer, orgID string) (*pyjson.Object, error) {
	var out *pyjson.Object
	for _, purpose := range llmorgsettings.Purposes {
		item, err := h.llmPurposeObject(ctx, db, orgID, purpose)
		if err != nil {
			return nil, err
		}
		if item == nil {
			continue
		}
		if out == nil {
			out = pyjson.NewObject()
		}
		out.Set(string(purpose), item)
	}
	return out, nil
}

func (h *handlers) getLLMPurpose(w http.ResponseWriter, r *http.Request, orgID string, purpose llmorgsettings.Purpose) {
	ctx := r.Context()
	if !h.requireBYOLLMAccess(ctx, w, orgID, false) {
		return
	}
	out, err := h.llmPurposeObject(ctx, h.store.Pool, orgID, purpose)
	if err != nil {
		h.internalError(ctx, w, "read llm purpose override", err)
		return
	}
	if out == nil {
		policy.WriteDetail(w, http.StatusNotFound, "LLM purpose override not found", nil)
		return
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// llmPurposeUpsert is a purpose override as the write reads it: provider and
// model are both required, so no override falls through to a blank model.
type llmPurposeUpsert struct {
	purpose         llmorgsettings.Purpose
	provider, model string
	apiKey, baseURL *string
}

func (h *handlers) applyLLMPurpose(ctx context.Context, orgID string, in llmPurposeUpsert) (*pyjson.Object, *llmRefusal, error) {
	baseURLText := ""
	if in.baseURL != nil {
		baseURLText = *in.baseURL
	}
	valid, reason, splitErr := llmorgsettings.ValidateBaseURLChecked(ctx, baseURLText)
	if splitErr != nil {
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
		return nil, nil, &llmStepError{"begin llm purpose override", err}
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	set := func(field string, value *string, encrypt bool, description string) error {
		_, err := h.upsertSetting(ctx, tx, orgID, llmCategory, llmorgsettings.PurposeKey(in.purpose, field), value, encrypt, &description)
		return err
	}
	provider := pythonparity.Lower(pythonparity.Strip(in.provider))
	model := in.model
	steps := []func() error{
		func() error { return set("provider", &provider, false, "BYO LLM provider override for one purpose") },
		func() error { return set("model", &model, false, "BYO LLM model override for one purpose") },
	}
	if in.apiKey != nil {
		steps = append(steps, func() error {
			return set("api_key", in.apiKey, true, "Encrypted BYO LLM API key override for one purpose")
		})
	}
	steps = append(steps, func() error {
		return set("base_url", in.baseURL, false, "BYO LLM base URL override for one purpose")
	})
	for _, step := range steps {
		if err := step(); err != nil {
			return nil, nil, &llmStepError{"write llm purpose override", err}
		}
	}
	out, err := h.llmPurposeObject(ctx, tx, orgID, in.purpose)
	if err != nil {
		return nil, nil, &llmStepError{"build llm purpose response", err}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, &llmStepError{"commit llm purpose override", err}
	}
	committed = true
	return out, nil, nil
}

// deleteLLMPurposeRows removes one override and returns how many rows existed.
func (h *handlers) deleteLLMPurposeRows(ctx context.Context, orgID string, purpose llmorgsettings.Purpose) (int, error) {
	tag, err := h.store.Pool.Exec(ctx,
		`DELETE FROM settings WHERE org_id = $1 AND category = $2 AND key = ANY($3)`,
		orgID, llmCategory, llmorgsettings.PurposeKeys(purpose))
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (h *handlers) deleteLLMPurpose(w http.ResponseWriter, r *http.Request, orgID string, purpose llmorgsettings.Purpose) {
	ctx := r.Context()
	if !h.requireBYOLLMAccess(ctx, w, orgID, true) {
		return
	}
	deleted, err := h.deleteLLMPurposeRows(ctx, orgID, purpose)
	if err != nil {
		h.internalError(ctx, w, "delete llm purpose override", err)
		return
	}
	if deleted == 0 {
		policy.WriteDetail(w, http.StatusNotFound, "LLM purpose override not found", nil)
		return
	}
	out := pyjson.NewObject()
	out.Set("deleted", true)
	out.Set("purpose", string(purpose))
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// purposeNeedsKey is whether a purpose write without an api_key must be
// refused: the provider needs a key, the override has none stored, and the
// default row does not supply one (same provider, key stored).
func (h *handlers) purposeNeedsKey(ctx context.Context, orgID string, purpose llmorgsettings.Purpose, providerName string) (bool, error) {
	provider := llmorgsettings.NormalizeProvider(providerName)
	if llmorgsettings.CredentialsComplete(provider, "") {
		return false, nil
	}
	has := func(key string) (bool, *string, error) {
		row, err := settingByKeyOn(ctx, h.store.Pool, orgID, llmCategory, key)
		if err != nil || row == nil || row.Value == nil || *row.Value == "" {
			return false, nil, err
		}
		return true, row.Value, nil
	}
	if ok, _, err := has(llmorgsettings.PurposeKey(purpose, "api_key")); err != nil || ok {
		return false, err
	}
	if ok, _, err := has("api_key"); err != nil || !ok {
		return true, err
	}
	_, defaultProvider, err := has("provider")
	if err != nil {
		return false, err
	}
	return defaultProvider == nil || llmorgsettings.NormalizeProvider(*defaultProvider) != provider, nil
}
