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

// The org-admin own-model toggle: it turns the org's saved BYO LLM
// configuration off and on without changing it. Off, the org has no usable own
// model (the acr llm-config route and the explain gate read it); the saved rows
// stay as they are.
func (h *handlers) ownModelRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: governancePrefix + "/llm-settings/own-model", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getOwnModel))},
		{Method: http.MethodPut, Pattern: governancePrefix + "/llm-settings/own-model", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.putOwnModel))},
	}
}

func ownModelObject(enabled, configured bool) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("enabled", enabled)
	out.Set("configured", configured)
	return out
}

func (h *handlers) getOwnModel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	org, ok := h.serviceModelOrg(ctx, w)
	if !ok {
		return
	}
	enabled, err := llmorgsettings.OwnModelEnabled(ctx, h.store.Pool, org.String())
	if err != nil {
		h.internalError(ctx, w, "read own model toggle", err)
		return
	}
	configured, err := llmorgsettings.HasSavedBYO(ctx, h.store.Pool, org.String())
	if err != nil {
		h.internalError(ctx, w, "read saved BYO config", err)
		return
	}
	policy.WriteJSON(w, http.StatusOK, ownModelObject(enabled, configured), nil)
}

// putOwnModel writes the toggle row and, on a state change, one audit row in
// the same transaction. The saved BYO rows are never read for a value or
// written.
func (h *handlers) putOwnModel(w http.ResponseWriter, r *http.Request) {
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
		h.internalError(ctx, w, "begin own model toggle", err)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var rowID uuid.UUID
	var stored *string
	err = tx.QueryRow(ctx, `SELECT id, value FROM settings WHERE org_id = $1 AND category = $2 AND key = $3 FOR UPDATE`,
		org.String(), llmCategory, llmorgsettings.KeyOwnModelEnabled).Scan(&rowID, &stored)
	found := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		h.internalError(ctx, w, "lock own model toggle", err)
		return
	}
	before := !found || stored == nil || *stored != "false"
	configured, err := llmorgsettings.HasSavedBYO(ctx, tx, org.String())
	if err != nil {
		h.internalError(ctx, w, "read saved BYO config", err)
		return
	}
	logOutcome := func(outcome string) {
		h.logger.InfoContext(ctx, "admin: own model toggle",
			"org_id", org.String(), "actor_user_id", actor.String(), "old_enabled", before, "new_enabled", enabled,
			"configured", configured, "outcome", outcome)
	}
	if before == enabled {
		if err := tx.Commit(ctx); err != nil {
			h.internalError(ctx, w, "commit own model toggle", err)
			return
		}
		logOutcome("unchanged")
		policy.WriteJSON(w, http.StatusOK, ownModelObject(enabled, configured), nil)
		return
	}

	value := strconv.FormatBool(enabled)
	now := h.store.now().UTC()
	if found {
		if _, err := tx.Exec(ctx, `UPDATE settings SET value = $2, updated_at = $3 WHERE id = $1`, rowID, value, now); err != nil {
			h.internalError(ctx, w, "update own model toggle", err)
			return
		}
	} else {
		tag, err := tx.Exec(ctx, `
INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, false, NULL, $6, $6)
ON CONFLICT (org_id, category, key) DO NOTHING`, uuid.New(), org.String(), llmCategory, llmorgsettings.KeyOwnModelEnabled, value, now)
		if err != nil {
			h.internalError(ctx, w, "insert own model toggle", err)
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
	changes.Set("setting", llmorgsettings.KeyOwnModelEnabled)
	changes.Set("source", serviceModelSource)
	changes.Set("old_enabled", before)
	changes.Set("new_enabled", enabled)
	if user.ImpersonatedBy != nil && *user.ImpersonatedBy != "" {
		changes.Set("impersonated_by", *user.ImpersonatedBy)
	}
	encoded, err := pyjson.Marshal(changes)
	if err != nil {
		h.internalError(ctx, w, "encode own model audit", err)
		return
	}
	description := "Own BYO LLM model turned off by an org admin"
	if enabled {
		description = "Own BYO LLM model turned on by an org admin"
	}
	if _, err := h.audit.Write(ctx, tx, requestAuditEntry(r, audit.Entry{
		OrgID: org, UserID: &actor, Action: audit.ActionSettingsUpdated,
		ResourceType: audit.ResourceSetting, ResourceID: llmCategory + "/" + llmorgsettings.KeyOwnModelEnabled,
		Description: &description, Changes: encoded,
	})); err != nil {
		h.internalError(ctx, w, "write own model audit", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.internalError(ctx, w, "commit own model toggle", err)
		return
	}
	logOutcome("changed")
	policy.WriteJSON(w, http.StatusOK, ownModelObject(enabled, configured), nil)
}
