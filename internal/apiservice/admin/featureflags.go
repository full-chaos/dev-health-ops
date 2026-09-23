package admin

import (
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

func (h *handlers) featureRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: governancePrefix + "/feature-flags", Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.listFeatureFlags))},
		{Method: http.MethodGet, Pattern: governancePrefix + "/orgs/{org_id}/feature-overrides", Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.listFeatureOverrides))},
		{Method: http.MethodPost, Pattern: governancePrefix + "/orgs/{org_id}/feature-overrides", Handler: h.bodyFirst(policy.Superuser, http.HandlerFunc(h.createFeatureOverride))},
		{Method: http.MethodPatch, Pattern: governancePrefix + "/orgs/{org_id}/feature-overrides/{override_id}", Handler: h.bodyFirst(policy.Superuser, http.HandlerFunc(h.updateFeatureOverride))},
		{Method: http.MethodDelete, Pattern: governancePrefix + "/orgs/{org_id}/feature-overrides/{override_id}", Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.deleteFeatureOverride))},
		{Method: http.MethodPatch, Pattern: governancePrefix + "/feature-flags/{flag_id}", Handler: h.bodyFirst(policy.Superuser, http.HandlerFunc(h.updateFeatureFlag))},
	}
}

type featureFlag struct {
	ID                              uuid.UUID
	Key, Name                       string
	Description                     *string
	Category, MinTier               string
	IsEnabled, IsBeta, IsDeprecated bool
	CreatedAt                       time.Time
}

const featureFlagColumns = `id, key, name, description, category, min_tier, is_enabled, is_beta, is_deprecated, created_at`

func scanFeatureFlag(row pgx.Row) (*featureFlag, error) {
	var flag featureFlag
	err := row.Scan(&flag.ID, &flag.Key, &flag.Name, &flag.Description, &flag.Category, &flag.MinTier,
		&flag.IsEnabled, &flag.IsBeta, &flag.IsDeprecated, &flag.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &flag, nil
}

func featureFlagObject(flag *featureFlag) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", flag.ID.String())
	out.Set("key", flag.Key)
	out.Set("name", flag.Name)
	out.Set("description", optionalString(flag.Description))
	out.Set("category", flag.Category)
	out.Set("min_tier", flag.MinTier)
	out.Set("is_enabled", flag.IsEnabled)
	out.Set("is_beta", flag.IsBeta)
	out.Set("is_deprecated", flag.IsDeprecated)
	out.Set("created_at", pyTimeString(flag.CreatedAt))
	return out
}

// featureOverride is an org_feature_overrides row with its flag's key. The
// Expires and Config fields hold the response's own values: after a write
// they are the request's (a naive or offset datetime, a JSON object), not a
// database round trip -- the ORM object the route serializes was never
// re-read.
type featureOverride struct {
	ID, OrgID, FeatureID uuid.UUID
	FeatureKey           string
	IsEnabled            bool
	Expires              *pytime.DateTime
	Config               pyjson.Value
	Reason               *string
	CreatedBy, UpdatedBy *uuid.UUID
	CreatedAt            time.Time
}

const overrideJoinColumns = `o.id, o.org_id, o.feature_id, f.key, o.is_enabled, o.expires_at, o.config, o.reason, o.created_by, o.updated_by, o.created_at`

func scanOverride(row pgx.Row) (*featureOverride, error) {
	var (
		o         featureOverride
		expiresAt *time.Time
		config    []byte
	)
	err := row.Scan(&o.ID, &o.OrgID, &o.FeatureID, &o.FeatureKey, &o.IsEnabled, &expiresAt, &config,
		&o.Reason, &o.CreatedBy, &o.UpdatedBy, &o.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if expiresAt != nil {
		value := pytime.UTC(*expiresAt)
		o.Expires = &value
	}
	value, err := jsonColumnValue(config)
	if err != nil {
		return nil, err
	}
	o.Config = value
	return &o, nil
}

func uuidOrNil(id *uuid.UUID) pyjson.Value {
	if id == nil {
		return nil
	}
	return id.String()
}

func featureOverrideObject(o *featureOverride) (*pyjson.Object, error) {
	out := pyjson.NewObject()
	out.Set("id", o.ID.String())
	out.Set("org_id", o.OrgID.String())
	out.Set("feature_id", o.FeatureID.String())
	out.Set("feature_key", o.FeatureKey)
	out.Set("is_enabled", o.IsEnabled)
	if o.Expires == nil {
		out.Set("expires_at", nil)
	} else {
		out.Set("expires_at", pytime.Pydantic(*o.Expires))
	}
	if o.Config != nil {
		if _, isObject := o.Config.(*pyjson.Object); !isObject {
			return nil, errors.New("feature override config is not an object")
		}
	}
	out.Set("config", o.Config)
	out.Set("reason", optionalString(o.Reason))
	out.Set("created_by", uuidOrNil(o.CreatedBy))
	out.Set("updated_by", uuidOrNil(o.UpdatedBy))
	out.Set("created_at", pyTimeString(o.CreatedAt))
	return out, nil
}

// listFeatureFlags is features.py's list_feature_flags.
func (h *handlers) listFeatureFlags(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rows, err := h.store.Pool.Query(ctx, `SELECT `+featureFlagColumns+` FROM feature_flags ORDER BY key ASC`)
	if err != nil {
		h.internalError(ctx, w, "list feature flags", err)
		return
	}
	defer rows.Close()
	list := []pyjson.Value{}
	for rows.Next() {
		flag, err := scanFeatureFlag(rows)
		if err != nil {
			h.internalError(ctx, w, "scan feature flag", err)
			return
		}
		list = append(list, featureFlagObject(flag))
	}
	if err := rows.Err(); err != nil {
		h.internalError(ctx, w, "list feature flags", err)
		return
	}
	policy.WriteJSON(w, http.StatusOK, list, nil)
}

// listFeatureOverrides is features.py's list_feature_overrides.
func (h *handlers) listFeatureOverrides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := pathUUID(r, "org_id")
	if !ok {
		policy.WriteInternal(w)
		return
	}
	rows, err := h.store.Pool.Query(ctx, `SELECT `+overrideJoinColumns+`
FROM org_feature_overrides o JOIN feature_flags f ON o.feature_id = f.id
WHERE o.org_id = $1 ORDER BY o.created_at DESC`, orgID)
	if err != nil {
		h.internalError(ctx, w, "list feature overrides", err)
		return
	}
	defer rows.Close()
	list := []pyjson.Value{}
	for rows.Next() {
		override, err := scanOverride(rows)
		if err != nil {
			h.internalError(ctx, w, "scan feature override", err)
			return
		}
		object, err := featureOverrideObject(override)
		if err != nil {
			h.internalError(ctx, w, "encode feature override", err)
			return
		}
		list = append(list, object)
	}
	if err := rows.Err(); err != nil {
		h.internalError(ctx, w, "list feature overrides", err)
		return
	}
	policy.WriteJSON(w, http.StatusOK, list, nil)
}

// optionalDatetimeField is a `datetime | None = None` body field: absent and
// null are nil, a value that is not a datetime is a pydantic error.
func optionalDatetimeField(errs *pybody.Errors, object *pyjson.Object, name string) *pytime.DateTime {
	raw, present := object.Get(name)
	if !present || raw == nil {
		return nil
	}
	parsed, failure := pytime.ParseDatetime(datetimeInput(raw))
	if failure != nil {
		*errs = append(*errs, pybody.DatetimeError([]pyjson.Value{"body", name}, raw, failure))
		return nil
	}
	return &parsed
}

// datetimeInput converts a decoded JSON value to the form ParseDatetime takes.
func datetimeInput(raw pyjson.Value) any {
	switch typed := raw.(type) {
	case pyjson.Int:
		return typed.Int
	case pyjson.Float:
		return float64(typed)
	default:
		return raw
	}
}

// createFeatureOverride is features.py's create_feature_override.
func (h *handlers) createFeatureOverride(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := policy.UserFrom(ctx)
	orgID, orgOK := pathUUID(r, "org_id")
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var (
		featureIDRaw string
		isEnabled    = true
		expires      *pytime.DateTime
		config       *pyjson.Object
		reason       *string
	)
	if ok {
		featureIDRaw, _ = errs.RequiredString(object, "feature_id", 0, 0)
		if value, present := errs.DefaultedBool(object, "is_enabled"); present {
			isEnabled = value
		}
		expires = optionalDatetimeField(&errs, object, "expires_at")
		if value, present := errs.OptionalAnyDict(object, "config"); present {
			config = value
		}
		if value, present := errs.OptionalString(object, "reason", 0, 0); present {
			reason = &value
		}
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	featureID, featureOK := pythonparity.ParseUUID(featureIDRaw)
	if !orgOK || featureOK != nil {
		policy.WriteInternal(w)
		return
	}
	creator, err := pythonparity.ParseUUID(user.UserID)
	if err != nil {
		policy.WriteInternal(w)
		return
	}
	flag, err := scanFeatureFlag(h.store.Pool.QueryRow(ctx, `SELECT `+featureFlagColumns+` FROM feature_flags WHERE id = $1`, featureID))
	if err != nil {
		h.internalError(ctx, w, "look up feature flag", err)
		return
	}
	if flag == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Feature flag not found", nil)
		return
	}
	var exists bool
	if err := h.store.Pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM org_feature_overrides WHERE org_id = $1 AND feature_id = $2)`, orgID, featureID).Scan(&exists); err != nil {
		h.internalError(ctx, w, "look up feature override", err)
		return
	}
	if exists {
		policy.WriteDetail(w, http.StatusConflict, "Feature override already exists", nil)
		return
	}
	// OrgFeatureOverride stores `config or {}`: an absent or null config is
	// the empty object.
	configText := []byte("{}")
	var configValue pyjson.Value = pyjson.NewObject()
	if config != nil {
		// The json column stores json.dumps' default form (", " and ": "
		// separators, ASCII escapes), which the comparison of stored rows
		// reads as raw text.
		encoded, err := pyjson.Dumps(config)
		if err != nil {
			h.internalError(ctx, w, "encode override config", err)
			return
		}
		configText, configValue = []byte(encoded), config
	}
	now := h.store.now().UTC()
	override := &featureOverride{
		ID: uuid.New(), OrgID: orgID, FeatureID: featureID, FeatureKey: flag.Key, IsEnabled: isEnabled,
		Expires: expires, Config: configValue, Reason: reason, CreatedBy: &creator, CreatedAt: now,
	}
	var expiresAt *time.Time
	if expires != nil {
		instant := expires.Time.UTC()
		expiresAt = &instant
	}
	if _, err := h.store.Pool.Exec(ctx, `
INSERT INTO org_feature_overrides
	(id, org_id, feature_id, is_enabled, expires_at, config, reason, created_by, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6::json, $7, $8, $9, $9)`,
		override.ID, orgID, featureID, isEnabled, expiresAt, string(configText), reason, creator, now); err != nil {
		h.internalError(ctx, w, "insert feature override", err)
		return
	}
	object2, err := featureOverrideObject(override)
	if err != nil {
		h.internalError(ctx, w, "encode feature override", err)
		return
	}
	policy.WriteJSON(w, http.StatusCreated, object2, nil)
}

// updateFeatureOverride is features.py's update_feature_override.
func (h *handlers) updateFeatureOverride(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := policy.UserFrom(ctx)
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var (
		isEnabled *bool
		expires   *pytime.DateTime
		config    *pyjson.Object
		reason    *string
	)
	if ok {
		if value, present := errs.OptionalBool(object, "is_enabled"); present {
			isEnabled = &value
		}
		expires = optionalDatetimeField(&errs, object, "expires_at")
		if value, present := errs.OptionalAnyDict(object, "config"); present {
			config = value
		}
		if value, present := errs.OptionalString(object, "reason", 0, 0); present {
			reason = &value
		}
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	overrideID, idOK := pathUUID(r, "override_id")
	orgID, orgOK := pathUUID(r, "org_id")
	if !idOK || !orgOK {
		policy.WriteInternal(w)
		return
	}
	updater, err := pythonparity.ParseUUID(user.UserID)
	if err != nil {
		policy.WriteInternal(w)
		return
	}
	current, err := scanOverride(h.store.Pool.QueryRow(ctx, `SELECT `+overrideJoinColumns+`
FROM org_feature_overrides o JOIN feature_flags f ON o.feature_id = f.id
WHERE o.id = $1 AND o.org_id = $2`, overrideID, orgID))
	if err != nil {
		h.internalError(ctx, w, "look up feature override", err)
		return
	}
	if current == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Feature override not found", nil)
		return
	}
	// The ORM only UPDATEs (and fires updated_at's onupdate) when an
	// assigned attribute really differs from the loaded one.
	changed := current.UpdatedBy == nil || *current.UpdatedBy != updater
	current.UpdatedBy = &updater
	if isEnabled != nil && *isEnabled != current.IsEnabled {
		current.IsEnabled, changed = *isEnabled, true
	}
	if expires != nil {
		if current.Expires == nil || !sameDatetime(*current.Expires, *expires) {
			changed = true
		}
		current.Expires = expires
	}
	if config != nil {
		if !pyValuesEqual(current.Config, config) {
			changed = true
		}
		current.Config = config
	}
	if reason != nil {
		if current.Reason == nil || *current.Reason != *reason {
			changed = true
		}
		current.Reason = reason
	}
	if changed {
		var expiresAt *time.Time
		if current.Expires != nil {
			instant := current.Expires.Time.UTC()
			expiresAt = &instant
		}
		configText := []byte("null")
		if current.Config != nil {
			encoded, err := pyjson.Dumps(current.Config)
			if err != nil {
				h.internalError(ctx, w, "encode override config", err)
				return
			}
			configText = []byte(encoded)
		}
		if _, err := h.store.Pool.Exec(ctx, `
UPDATE org_feature_overrides
SET is_enabled = $2, expires_at = $3, config = $4::json, reason = $5, updated_by = $6, updated_at = $7
WHERE id = $1`, current.ID, current.IsEnabled, expiresAt, string(configText), current.Reason, updater, h.store.now().UTC()); err != nil {
			h.internalError(ctx, w, "update feature override", err)
			return
		}
	}
	encoded, err := featureOverrideObject(current)
	if err != nil {
		h.internalError(ctx, w, "encode feature override", err)
		return
	}
	policy.WriteJSON(w, http.StatusOK, encoded, nil)
}

// sameDatetime is Python's == on two datetimes: an aware and a naive value
// are never equal.
func sameDatetime(a, b pytime.DateTime) bool {
	return a.Aware == b.Aware && a.Time.Equal(b.Time)
}

// pyValuesEqual is Python's == on two decoded JSON values.
func pyValuesEqual(a, b pyjson.Value) bool {
	left, errLeft := pyjson.Marshal(a)
	right, errRight := pyjson.Marshal(b)
	return errLeft == nil && errRight == nil && string(left) == string(right)
}

// deleteFeatureOverride is features.py's delete_feature_override.
func (h *handlers) deleteFeatureOverride(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	overrideID, idOK := pathUUID(r, "override_id")
	orgID, orgOK := pathUUID(r, "org_id")
	if !idOK || !orgOK {
		policy.WriteInternal(w)
		return
	}
	tag, err := h.store.Pool.Exec(ctx, `DELETE FROM org_feature_overrides WHERE id = $1 AND org_id = $2`, overrideID, orgID)
	if err != nil {
		h.internalError(ctx, w, "delete feature override", err)
		return
	}
	if tag.RowsAffected() == 0 {
		policy.WriteDetail(w, http.StatusNotFound, "Feature override not found", nil)
		return
	}
	// FastAPI's 204 still carries the route's JSON content type.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNoContent)
}

// updateFeatureFlag is features.py's update_feature_flag.
func (h *handlers) updateFeatureFlag(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var isEnabled, isBeta, isDeprecated *bool
	if ok {
		if value, present := errs.OptionalBool(object, "is_enabled"); present {
			isEnabled = &value
		}
		if value, present := errs.OptionalBool(object, "is_beta"); present {
			isBeta = &value
		}
		if value, present := errs.OptionalBool(object, "is_deprecated"); present {
			isDeprecated = &value
		}
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	flagID, idOK := pathUUID(r, "flag_id")
	if !idOK {
		policy.WriteInternal(w)
		return
	}
	flag, err := scanFeatureFlag(h.store.Pool.QueryRow(ctx, `SELECT `+featureFlagColumns+` FROM feature_flags WHERE id = $1`, flagID))
	if err != nil {
		h.internalError(ctx, w, "look up feature flag", err)
		return
	}
	if flag == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Feature flag not found", nil)
		return
	}
	changed := false
	if isEnabled != nil && *isEnabled != flag.IsEnabled {
		flag.IsEnabled, changed = *isEnabled, true
	}
	if isBeta != nil && *isBeta != flag.IsBeta {
		flag.IsBeta, changed = *isBeta, true
	}
	if isDeprecated != nil && *isDeprecated != flag.IsDeprecated {
		flag.IsDeprecated, changed = *isDeprecated, true
	}
	if changed {
		if _, err := h.store.Pool.Exec(ctx, `
UPDATE feature_flags SET is_enabled = $2, is_beta = $3, is_deprecated = $4, updated_at = $5 WHERE id = $1`,
			flag.ID, flag.IsEnabled, flag.IsBeta, flag.IsDeprecated, h.store.now().UTC()); err != nil {
			h.internalError(ctx, w, "update feature flag", err)
			return
		}
	}
	policy.WriteJSON(w, http.StatusOK, featureFlagObject(flag), nil)
}
