package billing

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/orgs"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

var accessForbidden = detail(http.StatusForbidden, "Access forbidden")

// pathUUID validates a `name: uuid.UUID` path parameter as FastAPI does
// (422 uuid_parsing at ["path", name]).
func pathUUID(errs *pybody.Errors, name, raw string) (uuid.UUID, bool) {
	value, failure := pybody.ParsePydanticUUID(raw)
	if failure != "" {
		ctx := pyjson.NewObject()
		ctx.Set("error", failure)
		*errs = append(*errs, pybody.Error{Type: "uuid_parsing", Loc: []pyjson.Value{"path", name},
			Msg: "Input should be a valid UUID, " + failure, Input: raw, Ctx: ctx})
		return uuid.Nil, false
	}
	return value, true
}

// entitlements is get_org_entitlements behind
// require_billing_entitlement_access.
func (h handlers) entitlements(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	orgID, valid := pathUUID(&errs, "org_id", r.PathValue("org_id"))
	if !valid {
		// FastAPI validates org_id once for the access dependency, which
		// declares it too, and once for the route: the 422 lists the same
		// error twice.
		errs = append(errs, errs[0])
		h.write(w, validation(errs))
		return
	}
	user := policy.UserFrom(r.Context())
	h.serve(w, r, "billing entitlements", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		if answer, err := h.entitlementAccess(ctx, tx, user, orgID); answer != nil || err != nil {
			return derefReply(answer), err
		}
		body, err := h.orgEntitlements(ctx, tx, orgID)
		if err != nil {
			return reply{}, err
		}
		return ok(body), nil
	})
}

// entitlementAccess is require_billing_entitlement_access: the caller's
// users row must exist and be active; a superuser passes; anyone else must
// carry this org in the token and hold a membership row for it.
func (h handlers) entitlementAccess(ctx context.Context, q querier, user *policy.User, orgID uuid.UUID) (*reply, error) {
	userID, err := pythonparity.ParseUUID(user.UserID)
	if err != nil {
		return &accessForbidden, nil
	}
	var active, superuser *bool
	switch err := q.QueryRow(ctx, `SELECT is_active, is_superuser FROM users WHERE id = $1`, userID).Scan(&active, &superuser); {
	case err == pgx.ErrNoRows:
		return &accessForbidden, nil
	case err != nil:
		return nil, err
	}
	if active == nil || !*active {
		return &accessForbidden, nil
	}
	if superuser != nil && *superuser {
		return nil, nil
	}
	if user.OrgID != orgID.String() {
		return &accessForbidden, nil
	}
	var member bool
	if err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM memberships WHERE user_id = $1 AND org_id = $2)`,
		userID, orgID).Scan(&member); err != nil {
		return nil, err
	}
	if !member {
		return &accessForbidden, nil
	}
	return nil, nil
}

// licenseTier is LicenseTier(value): an exact member, else community.
func licenseTier(value string) string {
	if _, ok := licensing.TierRank(value); ok {
		return value
	}
	return "community"
}

// orgEntitlements is licensing.gating.get_org_entitlements_from_db.
func (h handlers) orgEntitlements(ctx context.Context, q querier, orgID uuid.UUID) (*pyjson.Object, error) {
	license, err := orgs.LoadLicense(ctx, q, orgID)
	if err != nil {
		return nil, err
	}
	tier := "community"
	if license != nil {
		if tier = licenseTier(license.Tier); tier != license.Tier {
			h.logger.WarnContext(ctx, "billing entitlements: invalid org_licenses tier; community", "org_id", orgID.String())
		}
	} else {
		var orgTier *string
		switch err := q.QueryRow(ctx, `SELECT tier FROM organizations WHERE id = $1`, orgID).Scan(&orgTier); {
		case err == pgx.ErrNoRows:
		case err != nil:
			return nil, err
		default:
			if orgTier != nil {
				if tier = licenseTier(*orgTier); tier != *orgTier {
					h.logger.WarnContext(ctx, "billing entitlements: invalid organizations tier; community", "org_id", orgID.String())
				}
			}
		}
	}
	features := pyjson.NewObject()
	for _, feature := range licensing.FeaturesForTier(tier) {
		features.Set(feature.Key, feature.Enabled)
	}
	keys := licensing.EntitlementDecisionKeys()
	rows, err := orgs.LoadRows(ctx, q, orgID, keys)
	if err != nil {
		// evaluate_org_features_async closes every key on a storage error.
		h.logger.WarnContext(ctx, "billing entitlements: feature rows unavailable; decisions closed", "error", err.Error())
		for _, key := range keys {
			features.Set(key, false)
		}
	} else {
		decisions := orgs.Decisions(keys, rows, h.now().UTC())
		for _, key := range keys {
			features.Set(key, decisions[key])
		}
	}
	limits, _ := licensing.DefaultLimits(tier)
	limitObject := pyjson.NewObject()
	limitObject.Set("users", limits.Users)
	limitObject.Set("repos", limits.Repos)
	limitObject.Set("api_rate", limits.APIRate)

	var status *string
	var trialEnd *time.Time
	err = q.QueryRow(ctx, `SELECT status, trial_end FROM subscriptions WHERE org_id = $1
		ORDER BY updated_at DESC, created_at DESC LIMIT 1`, orgID).Scan(&status, &trialEnd)
	hasSubscription := err == nil
	if err != nil && err != pgx.ErrNoRows {
		return nil, err
	}
	out := pyjson.NewObject()
	out.Set("tier", tier)
	out.Set("features", features)
	out.Set("limits", limitObject)
	out.Set("is_licensed", license != nil && license.IsValid)
	out.Set("in_grace_period", false)
	out.Set("is_trialing", hasSubscription && status != nil && pythonparity.Lower(*status) == "trialing")
	if hasSubscription && trialEnd != nil {
		out.Set("trial_ends_at", pytime.ISOFormat(trialEnd.UTC()))
	} else {
		out.Set("trial_ends_at", nil)
	}
	return out, nil
}
