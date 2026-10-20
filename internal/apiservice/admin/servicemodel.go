package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The self-service acr_service_model toggle (CHAOS-10243): an org admin on a
// team or enterprise plan with agent_context_runtime turns ACR AI features on
// or off for their own org. It writes the org's org_feature_overrides row,
// which the acr entitlement route (internal/apiservice/acr) already serves.
const (
	serviceModelFeature        = "acr_service_model"
	serviceModelACRFeature     = "agent_context_runtime"
	serviceModelMinTier        = "team"
	serviceModelSource         = "self_service"
	serviceModelMarkerSource   = "self_service_toggle"
	serviceModelOverrideReason = "Self-service: org admin toggle (AI Setup)"
)

const (
	serviceModelControlNone        = "none"
	serviceModelControlSelfService = "self_service"
	serviceModelControlManaged     = "managed"
)

func (h *handlers) serviceModelRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: governancePrefix + "/llm-settings/service-model", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getServiceModel))},
		{Method: http.MethodPut, Pattern: governancePrefix + "/llm-settings/service-model", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.putServiceModel))},
	}
}

// serviceModelMarker is the config the toggle writes on its own row. The row
// is the toggle's only while SetAt equals the row's updated_at: any other
// writer (the superuser override routes, the dho admin verbs) moves
// updated_at, and from then on the row belongs to billing or support.
type serviceModelMarker struct {
	Source string `json:"source"`
	SetAt  string `json:"set_at"`
}

func serviceModelStamp(at time.Time) string {
	return at.UTC().Format(time.RFC3339Nano)
}

type serviceModelOverride struct {
	ID        uuid.UUID
	IsEnabled bool
	Config    []byte
	Reason    *string
	UpdatedAt time.Time
}

func (o *serviceModelOverride) ownedByToggle() bool {
	if o == nil || o.Reason == nil || *o.Reason != serviceModelOverrideReason {
		return false
	}
	var marker serviceModelMarker
	if err := json.Unmarshal(o.Config, &marker); err != nil {
		return false
	}
	return marker.Source == serviceModelMarkerSource && marker.SetAt == serviceModelStamp(o.UpdatedAt)
}

type serviceModelQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// loadServiceModelOverride reads the org's acr_service_model override; lock
// adds FOR UPDATE (inside the caller's transaction).
func loadServiceModelOverride(ctx context.Context, q serviceModelQueryer, org uuid.UUID, lock bool) (*serviceModelOverride, error) {
	sql := `SELECT o.id, o.is_enabled, COALESCE(o.config::text, 'null'), o.reason, o.updated_at
FROM org_feature_overrides o JOIN feature_flags f ON f.id = o.feature_id
WHERE o.org_id = $1 AND f.key = $2`
	if lock {
		sql += ` FOR UPDATE OF o`
	}
	var row serviceModelOverride
	var config string
	err := q.QueryRow(ctx, sql, org, serviceModelFeature).Scan(&row.ID, &row.IsEnabled, &config, &row.Reason, &row.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	row.Config = []byte(config)
	return &row, nil
}

type serviceModelState struct {
	enabled     bool
	registered  bool
	tier        string
	tierAllowed bool
	acrAllowed  bool
	control     string
	override    *bool
}

func (s serviceModelState) ineligibleReason() string {
	switch {
	case !s.registered:
		return "feature_not_registered"
	case !s.tierAllowed:
		return "tier_required"
	case !s.acrAllowed:
		return "agent_context_runtime_required"
	}
	return ""
}

func (h *handlers) loadServiceModelState(ctx context.Context, q serviceModelQueryer, org uuid.UUID, override *serviceModelOverride) (serviceModelState, error) {
	now := h.store.now().UTC()
	tier, err := licensing.ResolveOrgTier(ctx, q, org)
	if err != nil {
		return serviceModelState{}, &llmStepError{"resolve org tier", err}
	}
	have, _ := licensing.TierRank(tier)
	want, _ := licensing.TierRank(serviceModelMinTier)
	service, err := licensing.LoadState(ctx, q, org.String(), serviceModelFeature, now)
	if err != nil {
		return serviceModelState{}, &llmStepError{"load acr_service_model state", err}
	}
	acr, err := licensing.LoadState(ctx, q, org.String(), serviceModelACRFeature, now)
	if err != nil {
		return serviceModelState{}, &llmStepError{"load agent_context_runtime state", err}
	}
	enabled, err := llmorgsettings.EffectiveServiceModel(ctx, q, org.String(), licensing.Decide(serviceModelFeature, service))
	if err != nil {
		return serviceModelState{}, &llmStepError{"resolve effective acr_service_model", err}
	}
	control := serviceModelControlNone
	var overrideEnabled *bool
	if override != nil {
		overrideEnabled = &override.IsEnabled
		control = serviceModelControlManaged
		if override.ownedByToggle() {
			control = serviceModelControlSelfService
		}
	}
	return serviceModelState{
		enabled:     enabled,
		registered:  service.Registered,
		tier:        tier,
		tierAllowed: have >= want,
		acrAllowed:  licensing.Decide(serviceModelACRFeature, acr).Allowed,
		control:     control,
		override:    overrideEnabled,
	}, nil
}

func serviceModelObject(s serviceModelState) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("feature", serviceModelFeature)
	out.Set("enabled", s.enabled)
	reason := s.ineligibleReason()
	out.Set("eligible", reason == "")
	if reason == "" {
		out.Set("ineligible_reason", nil)
	} else {
		out.Set("ineligible_reason", reason)
	}
	out.Set("control", s.control)
	if s.override == nil {
		out.Set("override", nil)
	} else {
		out.Set("override", *s.override)
	}
	out.Set("required_tier", serviceModelMinTier)
	out.Set("current_tier", s.tier)
	return out
}

func serviceModelRefusal(status int, pairs ...string) *llmRefusal {
	return newLLMRefusal(status, pairs...)
}

// serviceModelOrg is the caller's org from the session, 404 when it has no
// organizations row.
func (h *handlers) serviceModelOrg(ctx context.Context, w http.ResponseWriter) (uuid.UUID, bool) {
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return uuid.Nil, false
	}
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		policy.WriteDetail(w, http.StatusNotFound, notFoundOrg(), nil)
		return uuid.Nil, false
	}
	var found uuid.UUID
	err = h.store.Pool.QueryRow(ctx, `SELECT id FROM organizations WHERE id = $1`, org).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		policy.WriteDetail(w, http.StatusNotFound, notFoundOrg(), nil)
		return uuid.Nil, false
	}
	if err != nil {
		h.internalError(ctx, w, "look up organization", err)
		return uuid.Nil, false
	}
	return org, true
}

func (h *handlers) getServiceModel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	org, ok := h.serviceModelOrg(ctx, w)
	if !ok {
		return
	}
	override, err := loadServiceModelOverride(ctx, h.store.Pool, org, false)
	if err != nil {
		h.internalError(ctx, w, "read acr_service_model override", err)
		return
	}
	state, err := h.loadServiceModelState(ctx, h.store.Pool, org, override)
	if err != nil {
		h.internalError(ctx, w, "read acr_service_model state", err)
		return
	}
	policy.WriteJSON(w, http.StatusOK, serviceModelObject(state), nil)
}

// putServiceModel writes the org's explicit acr_service_model override, on or
// off: off must stay readable as an explicit off (it overrides the auto-enroll
// a saved BYO key gives). It only ever writes a row it owns: an override that
// billing or support wrote (or changed after the toggle) is answered 409 and
// left as it is.
func (h *handlers) putServiceModel(w http.ResponseWriter, r *http.Request) {
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
		h.internalError(ctx, w, "begin acr_service_model toggle", err)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	override, err := loadServiceModelOverride(ctx, tx, org, true)
	if err != nil {
		h.internalError(ctx, w, "lock acr_service_model override", err)
		return
	}
	before, err := h.loadServiceModelState(ctx, tx, org, override)
	if err != nil {
		h.internalError(ctx, w, "read acr_service_model state", err)
		return
	}
	logOutcome := func(outcome string, after bool) {
		h.logger.InfoContext(ctx, "admin: acr_service_model self-service toggle",
			"org_id", org.String(), "actor_user_id", actor.String(), "requested_enabled", enabled,
			"old_enabled", before.enabled, "new_enabled", after, "control", before.control, "outcome", outcome)
	}
	if override != nil && before.control != serviceModelControlSelfService {
		logOutcome("refused_managed", before.enabled)
		serviceModelRefusal(http.StatusConflict,
			"error", "override_managed",
			"feature", serviceModelFeature,
			"message", "This setting is managed by your plan or support. Contact support to change it.").write(w)
		return
	}
	if !before.registered {
		logOutcome("refused_not_registered", before.enabled)
		serviceModelRefusal(http.StatusConflict,
			"error", "feature_not_registered",
			"feature", serviceModelFeature,
			"message", "ACR AI features are not available on this deployment").write(w)
		return
	}
	if enabled {
		switch before.ineligibleReason() {
		case "tier_required":
			logOutcome("refused_tier", before.enabled)
			serviceModelRefusal(http.StatusPaymentRequired,
				"error", "feature_not_licensed",
				"feature", serviceModelFeature,
				"required_tier", serviceModelMinTier,
				"current_tier", before.tier).write(w)
			return
		case "agent_context_runtime_required":
			logOutcome("refused_no_acr", before.enabled)
			serviceModelRefusal(http.StatusForbidden,
				"error", "feature_not_enabled",
				"feature", serviceModelACRFeature,
				"message", "Agent Context Runtime is not enabled for this organization").write(w)
			return
		}
	}

	now := h.store.now().UTC().Truncate(time.Microsecond)
	config, err := json.Marshal(serviceModelMarker{Source: serviceModelMarkerSource, SetAt: serviceModelStamp(now)})
	if err != nil {
		h.internalError(ctx, w, "encode acr_service_model marker", err)
		return
	}
	changed := false
	switch {
	case override == nil:
		reason := serviceModelOverrideReason
		tag, err := tx.Exec(ctx, `
INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, expires_at, config, reason, created_by, updated_by, created_at, updated_at)
SELECT $1, $2, f.id, $3, NULL, $4::json, $5, $6, $6, $7, $7 FROM feature_flags f WHERE f.key = $8
ON CONFLICT (org_id, feature_id) DO NOTHING`, uuid.New(), org, enabled, string(config), reason, actor, now, serviceModelFeature)
		if err != nil {
			h.internalError(ctx, w, "insert acr_service_model override", err)
			return
		}
		if tag.RowsAffected() != 1 {
			logOutcome("refused_concurrent_write", before.enabled)
			serviceModelRefusal(http.StatusConflict,
				"error", "override_changed",
				"feature", serviceModelFeature,
				"message", "The setting changed while saving. Reload and try again.").write(w)
			return
		}
		changed = true
	case override.IsEnabled != enabled:
		if _, err := tx.Exec(ctx, `
UPDATE org_feature_overrides SET is_enabled = $2, expires_at = NULL, config = $3::json, updated_by = $4, updated_at = $5
WHERE id = $1`, override.ID, enabled, string(config), actor, now); err != nil {
			h.internalError(ctx, w, "update acr_service_model override", err)
			return
		}
		changed = true
	}

	if changed {
		after, err := h.loadServiceModelState(ctx, tx, org, nil)
		if err != nil {
			h.internalError(ctx, w, "read acr_service_model state", err)
			return
		}
		changes := pyjson.NewObject()
		changes.Set("feature", serviceModelFeature)
		changes.Set("source", serviceModelSource)
		changes.Set("old_enabled", before.enabled)
		changes.Set("new_enabled", after.enabled)
		if override == nil {
			changes.Set("old_override", nil)
		} else {
			changes.Set("old_override", override.IsEnabled)
		}
		changes.Set("new_override", enabled)
		if user.ImpersonatedBy != nil && *user.ImpersonatedBy != "" {
			changes.Set("impersonated_by", *user.ImpersonatedBy)
		}
		encoded, err := pyjson.Marshal(changes)
		if err != nil {
			h.internalError(ctx, w, "encode acr_service_model audit", err)
			return
		}
		action := audit.ActionFeatureDisabled
		description := "ACR AI features turned off by an org admin"
		if enabled {
			action = audit.ActionFeatureEnabled
			description = "ACR AI features turned on by an org admin"
		}
		if _, err := h.audit.Write(ctx, tx, requestAuditEntry(r, audit.Entry{
			OrgID: org, UserID: &actor, Action: action,
			ResourceType: audit.ResourceFeatureFlag, ResourceID: serviceModelFeature,
			Description: &description, Changes: encoded,
		})); err != nil {
			h.internalError(ctx, w, "write acr_service_model audit", err)
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		h.internalError(ctx, w, "commit acr_service_model toggle", err)
		return
	}

	override, err = loadServiceModelOverride(ctx, h.store.Pool, org, false)
	if err != nil {
		h.internalError(ctx, w, "read acr_service_model override", err)
		return
	}
	state, err := h.loadServiceModelState(ctx, h.store.Pool, org, override)
	if err != nil {
		h.internalError(ctx, w, "read acr_service_model state", err)
		return
	}
	outcome := "unchanged"
	if changed {
		outcome = "changed"
	}
	logOutcome(outcome, state.enabled)
	policy.WriteJSON(w, http.StatusOK, serviceModelObject(state), nil)
}
