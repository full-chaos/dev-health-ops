// Package orgs is plan area L: the self-service org profile
// (api/orgs/router.py) and the org entitlements read
// (api/licensing/router.py), ported 1:1.
package orgs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// Routes returns the area's routes. The Python router registers GET before
// PATCH on /api/v1/orgs/me, so a 405 there says "Allow: GET" (Starlette
// reports the first matching route's methods).
func Routes(pool *pgxpool.Pool, guard *policy.Guard, logger *slog.Logger) []httpapi.Route {
	h := handlers{pool: pool, logger: logger, now: time.Now}
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: "/api/v1/orgs/me", Allow: http.MethodGet,
			Handler: guard.Wrap(policy.Authenticated, http.HandlerFunc(h.getOwnOrg))},
		{Method: http.MethodPatch, Pattern: "/api/v1/orgs/me",
			Handler: h.decodeFirst(guard, http.HandlerFunc(h.updateOwnOrg))},
		{Method: http.MethodGet, Pattern: "/api/v1/licensing/entitlements/{org_id}",
			Handler: guard.Wrap(policy.Authenticated, http.HandlerFunc(h.entitlements))},
	}
}

type handlers struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
	now    func() time.Time
}

// internal logs err with the route's fields and writes the generic 500.
func (h handlers) internal(w http.ResponseWriter, r *http.Request, what string, err error) {
	h.logger.ErrorContext(r.Context(), "api route failed", slog.String("path", r.URL.Path),
		slog.String("step", what), slog.String("error", err.Error()))
	policy.WriteInternal(w)
}

type orgProfile struct {
	id, slug, name, tier string
	description          *string
	isActive             bool
}

func (p orgProfile) json() *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", p.id)
	out.Set("slug", p.slug)
	out.Set("name", p.name)
	if p.description != nil {
		out.Set("description", *p.description)
	} else {
		out.Set("description", nil)
	}
	out.Set("tier", p.tier)
	out.Set("is_active", p.isActive)
	return out
}

func writeJSON(w http.ResponseWriter, status int, body pyjson.Value) {
	payload, err := pyjson.Marshal(body)
	if err != nil {
		policy.WriteInternal(w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", itoa(len(payload)))
	w.WriteHeader(status)
	_, _ = io.Copy(w, bytes.NewReader(payload))
}

// loadOrg is OrganizationService.get_by_id: uuid.UUID(org_id) raising on a
// malformed id is an unhandled error (500) in Python, so it is here too.
func (h handlers) loadOrg(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, orgID string, lock bool) (*orgProfile, error) {
	id, ok := policy.ParsePyUUID(orgID)
	if !ok {
		return nil, errBadOrgID
	}
	sql := `SELECT id, slug, name, description, tier, is_active FROM organizations WHERE id = $1`
	if lock {
		sql += ` FOR UPDATE`
	}
	var profile orgProfile
	var rowID uuid.UUID
	var active *bool
	err := q.QueryRow(ctx, sql, id).Scan(&rowID, &profile.slug, &profile.name, &profile.description, &profile.tier, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	profile.id = rowID.String()
	profile.isActive = active != nil && *active
	return &profile, nil
}

var errBadOrgID = errors.New("orgs: org id is not a UUID")

// getOwnOrg is get_own_org.
func (h handlers) getOwnOrg(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	if user.OrgID == "" {
		policy.WriteDetail(w, http.StatusBadRequest, "No organization context", nil)
		return
	}
	profile, err := h.loadOrg(r.Context(), h.pool, user.OrgID, false)
	if err != nil {
		h.internal(w, r, "load organization", err)
		return
	}
	if profile == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
		return
	}
	writeJSON(w, http.StatusOK, profile.json())
}

type bodyKey struct{}

// decodeFirst reads the body before authentication, as FastAPI does: a
// JSON decode failure is answered 422 (bytes that are not UTF-8, 400)
// before the credential is looked at.
func (h handlers) decodeFirst(guard *policy.Guard, next http.Handler) http.Handler {
	guarded := guard.Wrap(policy.Authenticated, next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, outcome, failure, err := pybody.Read(r)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				policy.WriteDetail(w, http.StatusRequestEntityTooLarge, "Request Entity Too Large", nil)
				return
			}
			h.internal(w, r, "read body", err)
			return
		}
		switch outcome {
		case pybody.DecodeFailed:
			writeJSON(w, http.StatusUnprocessableEntity, pybody.Detail([]pybody.Error{*failure}))
			return
		case pybody.ParseFailed:
			policy.WriteDetail(w, http.StatusBadRequest, "There was an error parsing the body", nil)
			return
		}
		guarded.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), bodyKey{}, body)))
	})
}

// updateOwnOrg is update_own_org.
func (h handlers) updateOwnOrg(w http.ResponseWriter, r *http.Request) {
	body, _ := r.Context().Value(bodyKey{}).(pybody.Body)
	var problems pybody.Errors
	var name, description string
	var hasName, hasDescription bool
	if object, ok := problems.Object(body); ok {
		name, hasName = problems.OptionalString(object, "name", 1, 255)
		description, hasDescription = problems.OptionalString(object, "description", 0, 0)
	}
	if len(problems) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems))
		return
	}

	user := policy.UserFrom(r.Context())
	if user.OrgID == "" {
		policy.WriteDetail(w, http.StatusBadRequest, "No organization context", nil)
		return
	}
	orgID, okOrg := policy.ParsePyUUID(user.OrgID)
	if !okOrg {
		h.internal(w, r, "parse org id", errBadOrgID)
		return
	}
	var role *string
	err := h.pool.QueryRow(r.Context(),
		`SELECT role FROM memberships WHERE org_id = $1 AND user_id = $2`, orgID, user.ID).Scan(&role)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		h.internal(w, r, "load membership", err)
		return
	}
	if errors.Is(err, pgx.ErrNoRows) || role == nil || (*role != "admin" && *role != "owner") {
		policy.WriteDetail(w, http.StatusForbidden,
			"You must be an org admin or owner to update organization settings", nil)
		return
	}

	tx, err := h.pool.Begin(r.Context())
	if err != nil {
		h.internal(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(r.Context())) }()
	profile, err := h.loadOrg(r.Context(), tx, user.OrgID, true)
	if err != nil {
		h.internal(w, r, "load organization", err)
		return
	}
	if profile == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
		return
	}
	if hasName {
		profile.name = name
	}
	if hasDescription {
		profile.description = &description
	}
	if _, err := tx.Exec(r.Context(),
		`UPDATE organizations SET name = $2, description = $3, updated_at = $4 WHERE id = $1`,
		orgID, profile.name, profile.description, h.now().UTC()); err != nil {
		h.internal(w, r, "update organization", err)
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		h.internal(w, r, "commit", err)
		return
	}
	writeJSON(w, http.StatusOK, profile.json())
}

// entitlements is licensing/router.py get_entitlements.
func (h handlers) entitlements(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	orgIDText := r.PathValue("org_id")
	if !user.IsSuperuser && user.OrgID != orgIDText {
		policy.WriteDetail(w, http.StatusForbidden, "Access forbidden", nil)
		return
	}
	orgID, ok := policy.ParsePyUUID(orgIDText)
	if !ok {
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
		return
	}
	ctx := r.Context()
	var orgTier string
	var rowID uuid.UUID
	err := h.pool.QueryRow(ctx, `SELECT id, tier FROM organizations WHERE id = $1`, orgID).Scan(&rowID, &orgTier)
	if errors.Is(err, pgx.ErrNoRows) {
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
		return
	}
	if err != nil {
		h.internal(w, r, "load organization", err)
		return
	}
	license, err := LoadLicense(ctx, h.pool, orgID)
	if err != nil {
		h.internal(w, r, "load license", err)
		return
	}
	rows, err := h.pool.Query(ctx, `SELECT key FROM feature_flags`)
	if err != nil {
		h.internal(w, r, "load feature keys", err)
		return
	}
	storedKeys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		h.internal(w, r, "load feature keys", err)
		return
	}

	tier := orgTier
	if license != nil {
		tier = license.Tier
	}
	tierEnum := CoerceTier(tier)
	keys := append(append([]string(nil), StandardFeatureKeys...), storedKeys...)
	if license != nil && Truthy(license.FeaturesOverride) {
		keys = append(keys, LicenseOverrides(license.FeaturesOverride).Keys()...)
	}
	keys = UniqueSorted(keys)
	decisionRows, loadErr := LoadRows(ctx, h.pool, orgID, keys)
	var decisions map[string]bool
	if loadErr != nil {
		h.logger.WarnContext(ctx, "entitlements: feature rows unavailable; every feature closed",
			slog.String("org_id", orgID.String()), slog.String("error", loadErr.Error()))
		decisions = map[string]bool{} // STORAGE_ERROR: every feature closed
	} else {
		decisions = Decisions(keys, decisionRows, h.now().UTC())
	}

	features := pyjson.NewObject()
	for _, key := range keys {
		features.Set(key, decisions[key])
	}
	limits := pyjson.NewObject()
	for _, limit := range TierLimits[tierEnum] {
		limits.Set(limit.Key, limitJSON(limit.Value))
	}

	out := pyjson.NewObject()
	out.Set("org_id", rowID.String())
	out.Set("tier", tier)
	out.Set("licensed_users", optionalInt(license, func(l *LicenseRow) *int64 { return l.LicensedUsers }))
	out.Set("licensed_repos", optionalInt(license, func(l *LicenseRow) *int64 { return l.LicensedRepos }))
	out.Set("features", features)
	if license != nil && Truthy(license.FeaturesOverride) {
		out.Set("features_override", LicenseOverrides(license.FeaturesOverride))
	} else {
		out.Set("features_override", nil)
	}
	if license != nil && Truthy(license.LimitsOverride) {
		override := coerceLimits(license.LimitsOverride)
		out.Set("limits_override", override)
		for _, key := range override.Keys() {
			value, _ := override.Get(key)
			limits.Set(key, value)
		}
	} else {
		out.Set("limits_override", nil)
	}
	if license != nil && license.ExpiresAt != nil {
		out.Set("expires_at", pytime.Pydantic(pytime.UTC(*license.ExpiresAt)))
	} else {
		out.Set("expires_at", nil)
	}
	out.Set("is_valid", license == nil || license.IsValid)
	out.Set("limits", limits)
	writeJSON(w, http.StatusOK, out)
}

func optionalInt(license *LicenseRow, field func(*LicenseRow) *int64) pyjson.Value {
	if license == nil {
		return nil
	}
	if value := field(license); value != nil {
		return *value
	}
	return nil
}

func limitJSON(value LimitValue) pyjson.Value {
	switch typed := value.(type) {
	case int64:
		return typed
	case float64:
		return pyjson.Float(typed)
	default:
		return nil
	}
}

// coerceLimits is _coerce_limits_map followed by pydantic's
// dict[str, int | float | None] validation: a JSON object's None, int and
// float values are kept (bool, an int subclass, becomes 0/1), everything
// else dropped; a non-object is {}.
func coerceLimits(value pyjson.Value) *pyjson.Object {
	out := pyjson.NewObject()
	object, ok := value.(*pyjson.Object)
	if !ok {
		return out
	}
	for _, key := range object.Keys() {
		item, _ := object.Get(key)
		switch typed := item.(type) {
		case nil, pyjson.Int, pyjson.Float:
			out.Set(key, typed)
		case bool:
			if typed {
				out.Set(key, int64(1))
			} else {
				out.Set(key, int64(0))
			}
		}
	}
	return out
}
