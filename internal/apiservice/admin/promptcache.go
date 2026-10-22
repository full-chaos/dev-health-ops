package admin

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The org-admin prompt-cache opt-in: it asks the Anthropic prompt cache to be
// used for the org's saved BYO model in Ask Dev. It is off until an org admin
// turns it on; the saved BYO rows are never read for a value or written.
func (h *handlers) promptCacheRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: governancePrefix + "/llm-settings/prompt-cache", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getPromptCache))},
		{Method: http.MethodPut, Pattern: governancePrefix + "/llm-settings/prompt-cache", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.putPromptCache))},
	}
}

// promptCacheObject: enabled is the stored opt-in, configured a saved BYO
// config of any purpose, anthropic that the config Ask Dev uses is on the
// anthropic provider (the cache applies to that provider only).
func promptCacheObject(enabled, configured, anthropic bool) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("enabled", enabled)
	out.Set("configured", configured)
	out.Set("anthropic", anthropic)
	return out
}

func (h *handlers) getPromptCache(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	org, ok := h.serviceModelOrg(ctx, w)
	if !ok {
		return
	}
	enabled, err := llmorgsettings.PromptCacheEnabled(ctx, h.store.Pool, org.String())
	if err != nil {
		h.internalError(ctx, w, "read prompt cache opt-in", err)
		return
	}
	configured, err := llmorgsettings.HasSavedBYO(ctx, h.store.Pool, org.String())
	if err != nil {
		h.internalError(ctx, w, "read saved BYO config", err)
		return
	}
	anthropic, err := llmorgsettings.InvestigateProviderIsAnthropic(ctx, h.store.Pool, org.String())
	if err != nil {
		h.internalError(ctx, w, "read saved BYO provider", err)
		return
	}
	policy.WriteJSON(w, http.StatusOK, promptCacheObject(enabled, configured, anthropic), nil)
}

// putPromptCache writes the opt-in row and, on a state change, one audit row in
// the same transaction. Turning it off for an org that never turned it on
// writes nothing.
func (h *handlers) putPromptCache(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := policy.UserFrom(ctx)
	var errs pybody.Errors
	object, objectOK := errs.Object(bodyFromContext(ctx))
	var enabled bool
	if objectOK {
		value, present := errs.DefaultedBool(object, "enabled")
		if !present && len(errs) == 0 {
			errs = append(errs, pybody.Error{Type: "missing", Loc: []pyjson.Value{"body", "enabled"}, Msg: "Field required", Input: object})
		}
		enabled = value
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	org, ok := h.serviceModelOrg(ctx, w)
	if !ok {
		return
	}
	actor, err := pythonparity.ParseUUID(user.UserID)
	if err != nil {
		policy.WriteInternal(w)
		return
	}

	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		h.internalError(ctx, w, "begin prompt cache opt-in", err)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var rowID uuid.UUID
	var stored *string
	err = tx.QueryRow(ctx, `SELECT id, value FROM settings WHERE org_id = $1 AND category = $2 AND key = $3 FOR UPDATE`,
		org.String(), llmCategory, llmorgsettings.KeyPromptCache).Scan(&rowID, &stored)
	found := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		h.internalError(ctx, w, "lock prompt cache opt-in", err)
		return
	}
	before := found && stored != nil && *stored == "true"
	configured, err := llmorgsettings.HasSavedBYO(ctx, tx, org.String())
	if err != nil {
		h.internalError(ctx, w, "read saved BYO config", err)
		return
	}
	anthropic, err := llmorgsettings.InvestigateProviderIsAnthropic(ctx, tx, org.String())
	if err != nil {
		h.internalError(ctx, w, "read saved BYO provider", err)
		return
	}
	logOutcome := func(outcome string) {
		h.logger.InfoContext(ctx, "admin: prompt cache opt-in",
			"org_id", org.String(), "actor_user_id", actor.String(), "old_enabled", before, "new_enabled", enabled,
			"configured", configured, "anthropic", anthropic, "outcome", outcome)
	}
	if before == enabled {
		if err := tx.Commit(ctx); err != nil {
			h.internalError(ctx, w, "commit prompt cache opt-in", err)
			return
		}
		logOutcome("unchanged")
		policy.WriteJSON(w, http.StatusOK, promptCacheObject(enabled, configured, anthropic), nil)
		return
	}

	value := strconv.FormatBool(enabled)
	now := h.store.now().UTC()
	if found {
		if _, err := tx.Exec(ctx, `UPDATE settings SET value = $2, updated_at = $3 WHERE id = $1`, rowID, value, now); err != nil {
			h.internalError(ctx, w, "update prompt cache opt-in", err)
			return
		}
	} else {
		tag, err := tx.Exec(ctx, `
INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, false, NULL, $6, $6)
ON CONFLICT (org_id, category, key) DO NOTHING`, uuid.New(), org.String(), llmCategory, llmorgsettings.KeyPromptCache, value, now)
		if err != nil {
			h.internalError(ctx, w, "insert prompt cache opt-in", err)
			return
		}
		if tag.RowsAffected() != 1 {
			logOutcome("refused_concurrent_write")
			newLLMRefusal(http.StatusConflict,
				"error", "setting_changed",
				"message", "The setting changed while saving. Reload and try again.").write(w)
			return
		}
	}

	changes := pyjson.NewObject()
	changes.Set("setting", llmorgsettings.KeyPromptCache)
	changes.Set("source", serviceModelSource)
	changes.Set("old_enabled", before)
	changes.Set("new_enabled", enabled)
	if user.ImpersonatedBy != nil && *user.ImpersonatedBy != "" {
		changes.Set("impersonated_by", *user.ImpersonatedBy)
	}
	encoded, err := pyjson.Marshal(changes)
	if err != nil {
		h.internalError(ctx, w, "encode prompt cache audit", err)
		return
	}
	description := "BYO LLM prompt cache turned off by an org admin"
	if enabled {
		description = "BYO LLM prompt cache turned on by an org admin"
	}
	if _, err := h.audit.Write(ctx, tx, requestAuditEntry(r, audit.Entry{
		OrgID: org, UserID: &actor, Action: audit.ActionSettingsUpdated,
		ResourceType: audit.ResourceSetting, ResourceID: llmCategory + "/" + llmorgsettings.KeyPromptCache,
		Description: &description, Changes: encoded,
	})); err != nil {
		h.internalError(ctx, w, "write prompt cache audit", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.internalError(ctx, w, "commit prompt cache opt-in", err)
		return
	}
	logOutcome("changed")
	policy.WriteJSON(w, http.StatusOK, promptCacheObject(enabled, configured, anthropic), nil)
}
