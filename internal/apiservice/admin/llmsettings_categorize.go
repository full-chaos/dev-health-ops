package admin

import (
	"context"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The categorize BYO opt-in (D6279, acp rulings on CHAOS-11008): one row in the
// llm settings category, written only by an org admin through the llm settings
// PUT (`categorize_byo`), with one audit row for each change. It has no env var,
// no platform default and no operator path. Off is the default: no row, or any
// value but "true".

// categorizeOptInStored reads the stored opt-in: true only for the value "true".
func (h *handlers) categorizeOptInStored(ctx context.Context, db pgExecer, orgID string) (bool, error) {
	row, err := settingByKeyOn(ctx, db, orgID, llmCategory, llmorgsettings.KeyCategorizeBYOEnabled)
	if err != nil || row == nil || row.Value == nil {
		return false, err
	}
	return *row.Value == "true", nil
}

// purposeParamFor is purposeParam for a route that knows the org: purpose
// "categorize" is accepted only when the org's stored opt-in is "true", or when
// the same request sets it true (bodyOptIn). Every other org keeps the
// purpose_not_configurable 400.
func (h *handlers) purposeParamFor(
	ctx context.Context, w http.ResponseWriter, orgID, raw string, bodyOptIn *bool,
) (llmorgsettings.Purpose, bool) {
	if raw != string(llmorgsettings.PurposeCategorize) {
		purpose, _, ok := purposeParam(w, raw, true)
		return purpose, ok
	}
	optedIn, err := categorizeOptedIn(bodyOptIn, func() (bool, error) {
		return h.categorizeOptInStored(ctx, h.store.Pool, orgID)
	})
	if err != nil {
		h.internalError(ctx, w, "read categorize opt-in", err)
		return "", false
	}
	if !optedIn {
		purpose, _, ok := purposeParam(w, raw, true)
		return purpose, ok
	}
	return llmorgsettings.PurposeCategorize, true
}

// categorizeOptedIn is the one rule of ruling A: the categorize purpose is
// accepted when the same request sets the opt-in true, or when the stored
// opt-in is "true"; every other case is not opted in (the 400 stays). The
// stored value is read only when the request does not set it true.
func categorizeOptedIn(bodyOptIn *bool, stored func() (bool, error)) (bool, error) {
	if bodyOptIn != nil && *bodyOptIn {
		return true, nil
	}
	return stored()
}

// writeCategorizeOptIn stores the opt-in inside tx and, on a change, writes one
// audit row naming the actor. The audit row holds no secret, model or provider.
func (h *handlers) writeCategorizeOptIn(ctx context.Context, tx pgx.Tx, r *http.Request, orgID string, enabled bool) error {
	before, err := h.categorizeOptInStored(ctx, tx, orgID)
	if err != nil {
		return err
	}
	row, err := settingByKeyOn(ctx, tx, orgID, llmCategory, llmorgsettings.KeyCategorizeBYOEnabled)
	if err != nil {
		return err
	}
	if row != nil && before == enabled {
		return nil
	}
	if row == nil && !enabled {
		return nil
	}
	value := strconv.FormatBool(enabled)
	description := "Categorization on the organization's own model, opted in by an org admin"
	if _, err := h.upsertSetting(ctx, tx, orgID, llmCategory, llmorgsettings.KeyCategorizeBYOEnabled, &value, false, &description); err != nil {
		return err
	}
	return h.auditCategorizeOptIn(ctx, tx, r, orgID, before, enabled)
}

// clearCategorizeOptIn removes the stored opt-in (the saved config it belongs
// to is being deleted) and audits the change when it was on.
func (h *handlers) clearCategorizeOptIn(ctx context.Context, r *http.Request, orgID string) error {
	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	before, err := h.categorizeOptInStored(ctx, tx, orgID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM settings WHERE org_id = $1 AND category = $2 AND key = $3`,
		orgID, llmCategory, llmorgsettings.KeyCategorizeBYOEnabled); err != nil {
		return err
	}
	if before {
		if err := h.auditCategorizeOptIn(ctx, tx, r, orgID, true, false); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (h *handlers) auditCategorizeOptIn(ctx context.Context, tx pgx.Tx, r *http.Request, orgID string, before, after bool) error {
	user := policy.UserFrom(ctx)
	actor, err := pythonparity.ParseUUID(user.UserID)
	if err != nil {
		return err
	}
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		return err
	}
	changes := pyjson.NewObject()
	changes.Set("setting", llmorgsettings.KeyCategorizeBYOEnabled)
	changes.Set("source", serviceModelSource)
	changes.Set("old_enabled", before)
	changes.Set("new_enabled", after)
	if user.ImpersonatedBy != nil && *user.ImpersonatedBy != "" {
		changes.Set("impersonated_by", *user.ImpersonatedBy)
	}
	encoded, err := pyjson.Marshal(changes)
	if err != nil {
		return err
	}
	description := "Categorization on the organization's own model turned off by an org admin"
	if after {
		description = "Categorization on the organization's own model turned on by an org admin"
	}
	_, err = h.audit.Write(ctx, tx, requestAuditEntry(r, audit.Entry{
		OrgID: org, UserID: &actor, Action: audit.ActionSettingsUpdated,
		ResourceType: audit.ResourceSetting, ResourceID: llmCategory + "/" + llmorgsettings.KeyCategorizeBYOEnabled,
		Description: &description, Changes: encoded,
	}))
	return err
}

// categorizeOptInStep is the write step of a PUT that carries categorize_byo;
// nil when the body has no such field.
func (h *handlers) categorizeOptInStep(r *http.Request, orgID string, enabled *bool) func(context.Context, pgx.Tx) error {
	if enabled == nil {
		return nil
	}
	value := *enabled
	return func(ctx context.Context, tx pgx.Tx) error {
		return h.writeCategorizeOptIn(ctx, tx, r, orgID, value)
	}
}
