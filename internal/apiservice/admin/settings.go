package admin

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// settingCategories is SettingCategory's values, in enum order.
var settingCategories = []string{
	"general", "github", "gitlab", "jira", "linear", "atlassian", "llm", "ask_dev", "sync", "notifications", "retention",
}

// encryptedMask is what every response shows for an encrypted setting.
const encryptedMask = "[ENCRYPTED]"

func (h *handlers) settingsRoutes() []httpapi.Route {
	return []httpapi.Route{
		// GET /settings/categories is a literal beside GET /settings/{category},
		// which the route table cannot register side by side (see
		// checkOrMethodNotAllowed); it is mounted on the wildcard's GET and
		// told apart by the segment.
		{Method: http.MethodGet, Pattern: governancePrefix + "/settings/{category}", Handler: h.categoriesOrListSettings()},
		{Method: http.MethodGet, Pattern: governancePrefix + "/settings/{category}/{key}", Allow: "GET", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getSetting))},
		{Method: http.MethodPut, Pattern: governancePrefix + "/settings/{category}/{key}", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.putSetting))},
		{Method: http.MethodPost, Pattern: governancePrefix + "/settings", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.postSetting))},
		{Method: http.MethodDelete, Pattern: governancePrefix + "/settings/{category}/{key}", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.deleteSetting))},
	}
}

// categoriesOrListSettings answers GET /settings/categories and
// GET /settings/{category}, both guarded at Admin.
func (h *handlers) categoriesOrListSettings() http.Handler {
	categories := h.guard.Wrap(policy.Admin, http.HandlerFunc(h.listSettingCategories))
	list := h.guard.Wrap(policy.Admin, http.HandlerFunc(h.listSettingsByCategory))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("category") == "categories" {
			categories.ServeHTTP(w, r)
			return
		}
		list.ServeHTTP(w, r)
	})
}

// setting is a settings row.
type setting struct {
	ID          uuid.UUID
	Key         string
	Value       *string
	IsEncrypted bool
	Description *string
}

func (s pgStore) settingsByCategory(ctx context.Context, orgID, category string) ([]setting, error) {
	rows, err := s.Pool.Query(ctx,
		`SELECT id, key, value, is_encrypted, description FROM settings WHERE org_id = $1 AND category = $2`, orgID, category)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []setting
	for rows.Next() {
		var row setting
		if err := rows.Scan(&row.ID, &row.Key, &row.Value, &row.IsEncrypted, &row.Description); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s pgStore) settingByKey(ctx context.Context, orgID, category, key string) (*setting, error) {
	return settingByKeyOn(ctx, s.Pool, orgID, category, key)
}

// settingByKeyOn reads one settings row over db (the pool or an open
// transaction).
func settingByKeyOn(ctx context.Context, db pgExecer, orgID, category, key string) (*setting, error) {
	var row setting
	err := db.QueryRow(ctx,
		`SELECT id, key, value, is_encrypted, description FROM settings WHERE org_id = $1 AND category = $2 AND key = $3`,
		orgID, category, key).Scan(&row.ID, &row.Key, &row.Value, &row.IsEncrypted, &row.Description)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// encryptionConfigured is whether SETTINGS_ENCRYPTION_KEY reached this api:
// without it Python's encrypt_value and decrypt_value raise RuntimeError, an
// unhandled 500.
func (h *handlers) encryptionConfigured() bool {
	return h.decryptor != (providerfoundation.FernetDecryptor{})
}

// rejectSettingsCategory is settings.py's _reject_llm_category: the dedicated
// endpoints own the llm, llm_budget and ask_dev categories.
func rejectSettingsCategory(w http.ResponseWriter, category string) bool {
	switch category {
	case "llm", "llm_budget":
		detail := pyjson.NewObject()
		detail.Set("error", "use_llm_settings_endpoint")
		detail.Set("message", "LLM settings and budgets must be managed via /admin/llm-settings (tier-gated, validated, and masked).")
		policy.WriteDetail(w, http.StatusForbidden, detail, nil)
		return true
	case "ask_dev":
		detail := pyjson.NewObject()
		detail.Set("error", "use_ask_dev_settings_endpoint")
		detail.Set("message", "Ask Dev settings must be managed via /admin/ask-dev/settings.")
		policy.WriteDetail(w, http.StatusForbidden, detail, nil)
		return true
	}
	return false
}

// settingObject is SettingResponse for a row: an encrypted setting's value is
// always the mask, whatever it holds.
func settingObject(key, category string, value *string, isEncrypted bool, description *string) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("key", key)
	if isEncrypted {
		out.Set("value", encryptedMask)
	} else {
		out.Set("value", optionalString(value))
	}
	out.Set("category", category)
	out.Set("is_encrypted", isEncrypted)
	out.Set("description", optionalString(description))
	return out
}

// listSettingCategories is settings.py's list_setting_categories.
func (h *handlers) listSettingCategories(w http.ResponseWriter, r *http.Request) {
	list := make([]pyjson.Value, len(settingCategories))
	for index, name := range settingCategories {
		list[index] = name
	}
	policy.WriteModel(w, http.StatusOK, list, nil)
}

// loadCategory is SettingsService.list_by_category's read: every row of the
// category, with each encrypted non-empty value decrypted. A value that will
// not decrypt is "[DECRYPTION_FAILED]" (a ValueError Python catches), but with
// no encryption key the decrypt raises RuntimeError, which it does not: 500.
func (h *handlers) loadCategory(ctx context.Context, w http.ResponseWriter, orgID, category string) ([]setting, bool) {
	rows, err := h.store.settingsByCategory(ctx, orgID, category)
	if err != nil {
		h.internalError(ctx, w, "list settings", err)
		return nil, false
	}
	for _, row := range rows {
		if row.IsEncrypted && row.Value != nil && *row.Value != "" && !h.encryptionConfigured() {
			h.internalError(ctx, w, "decrypt setting", errors.New("SETTINGS_ENCRYPTION_KEY is not configured"))
			return nil, false
		}
	}
	return rows, true
}

// listSettingsByCategory is settings.py's list_settings_by_category. Python
// builds SettingResponse(**row) from a row that has no `category`, so any
// category with at least one setting fails validation (an unhandled 500); an
// empty category answers 200 with an empty list.
func (h *handlers) listSettingsByCategory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	category := r.PathValue("category")
	if rejectSettingsCategory(w, category) {
		return
	}
	rows, ok := h.loadCategory(ctx, w, orgID, category)
	if !ok {
		return
	}
	if len(rows) > 0 {
		h.internalError(ctx, w, "build setting response", errors.New("setting rows carry no category"))
		return
	}
	out := pyjson.NewObject()
	out.Set("category", category)
	out.Set("settings", []pyjson.Value{})
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// getSetting is settings.py's get_setting.
func (h *handlers) getSetting(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	category, key := r.PathValue("category"), r.PathValue("key")
	if rejectSettingsCategory(w, category) {
		return
	}
	rows, ok := h.loadCategory(ctx, w, orgID, category)
	if !ok {
		return
	}
	for _, row := range rows {
		if row.Key == key {
			policy.WriteModel(w, http.StatusOK, settingObject(key, category, row.Value, row.IsEncrypted, row.Description), nil)
			return
		}
	}
	policy.WriteDetail(w, http.StatusNotFound, "Setting not found", nil)
}

// errEncryptionKeyMissing is Python's RuntimeError when SETTINGS_ENCRYPTION_KEY
// is not configured: an unhandled 500.
var errEncryptionKeyMissing = errors.New("SETTINGS_ENCRYPTION_KEY is not configured")

// upsertSetting is SettingsService.set over db (the pool or an open
// transaction): it encrypts a non-empty value when asked, stores the flag
// whatever the value, and keeps the old description when none is sent. The
// returned row is what the ORM object holds after the flush (an encrypted
// value is its ciphertext).
func (h *handlers) upsertSetting(ctx context.Context, db pgExecer, orgID, category, key string, value *string, encrypt bool, description *string) (*setting, error) {
	stored := value
	if encrypt && value != nil && *value != "" {
		if !h.encryptionConfigured() {
			return nil, errEncryptionKeyMissing
		}
		sealed, err := h.decryptor.Encrypt([]byte(*value))
		if err != nil {
			return nil, err
		}
		text := revealSecret(sealed)
		stored = &text
	}
	existing, err := settingByKeyOn(ctx, db, orgID, category, key)
	if err != nil {
		return nil, err
	}
	now := h.store.now().UTC()
	if existing == nil {
		row := &setting{ID: uuid.New(), Key: key, Value: stored, IsEncrypted: encrypt, Description: description}
		if _, err := db.Exec(ctx, `
INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`, row.ID, orgID, category, key, stored, encrypt, description, now); err != nil {
			return nil, err
		}
		return row, nil
	}
	changed := !sameOptionalString(existing.Value, stored) || existing.IsEncrypted != encrypt
	existing.Value, existing.IsEncrypted = stored, encrypt
	if description != nil {
		if !sameOptionalString(existing.Description, description) {
			changed = true
		}
		existing.Description = description
	}
	// The ORM only UPDATEs (and fires updated_at's onupdate) when an assigned
	// attribute really differs from the loaded one.
	if changed {
		if _, err := db.Exec(ctx, `
UPDATE settings SET value = $2, is_encrypted = $3, description = $4, updated_at = $5 WHERE id = $1`,
			existing.ID, existing.Value, existing.IsEncrypted, existing.Description, now); err != nil {
			return nil, err
		}
	}
	return existing, nil
}

// setSetting answers the 500 for a failed upsertSetting.
func (h *handlers) setSetting(ctx context.Context, w http.ResponseWriter, orgID, category, key string, value *string, encrypt bool, description *string) (*setting, bool) {
	row, err := h.upsertSetting(ctx, h.store.Pool, orgID, category, key, value, encrypt, description)
	if err != nil {
		h.internalError(ctx, w, "set setting", err)
		return nil, false
	}
	return row, true
}

func sameOptionalString(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func revealSecret(value secrets.Value) string { return value.Reveal() }

// putSetting is settings.py's set_setting.
func (h *handlers) putSetting(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var (
		value, description *string
		encrypt            bool
	)
	if ok {
		if v, present := errs.OptionalString(object, "value", 0, 0); present {
			value = &v
		}
		if v, present := errs.OptionalBool(object, "encrypt"); present {
			encrypt = v
		}
		if v, present := errs.OptionalString(object, "description", 0, 0); present {
			description = &v
		}
	}
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	category, key := r.PathValue("category"), r.PathValue("key")
	if rejectSettingsCategory(w, category) {
		return
	}
	row, ok := h.setSetting(ctx, w, orgID, category, key, value, encrypt, description)
	if !ok {
		return
	}
	policy.WriteModel(w, http.StatusOK, settingObject(key, category, row.Value, row.IsEncrypted, row.Description), nil)
}

// postSetting is settings.py's create_setting.
func (h *handlers) postSetting(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var (
		key, description *string
		value            *string
		category         = "general"
		encrypt          bool
	)
	if ok {
		if v, present := errs.RequiredString(object, "key", 1, 255); present {
			key = &v
		}
		if v, present := errs.OptionalString(object, "value", 0, 0); present {
			value = &v
		}
		if v, present := errs.DefaultedString(object, "category", 0, 0); present {
			category = v
		}
		if v, present := errs.DefaultedBool(object, "encrypt"); present {
			encrypt = v
		}
		if v, present := errs.OptionalString(object, "description", 0, 0); present {
			description = &v
		}
	}
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	if len(errs) > 0 || key == nil {
		writeValidation(w, errs)
		return
	}
	if rejectSettingsCategory(w, category) {
		return
	}
	row, ok := h.setSetting(ctx, w, orgID, category, *key, value, encrypt, description)
	if !ok {
		return
	}
	policy.WriteModel(w, http.StatusOK, settingObject(*key, category, row.Value, row.IsEncrypted, row.Description), nil)
}

// deleteSetting is settings.py's delete_setting.
func (h *handlers) deleteSetting(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	category, key := r.PathValue("category"), r.PathValue("key")
	if rejectSettingsCategory(w, category) {
		return
	}
	existing, err := h.store.settingByKey(ctx, orgID, category, key)
	if err != nil {
		h.internalError(ctx, w, "look up setting", err)
		return
	}
	if existing == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Setting not found", nil)
		return
	}
	if _, err := h.store.Pool.Exec(ctx, `DELETE FROM settings WHERE id = $1`, existing.ID); err != nil {
		h.internalError(ctx, w, "delete setting", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("deleted", true)
	policy.WriteModel(w, http.StatusOK, out, nil)
}
