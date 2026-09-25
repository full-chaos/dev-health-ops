package session

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// onboardingState is OnboardingStateResponse. The routes' `-> Any` return
// annotation makes FastAPI serialize the returned model as its response
// model: every field, defaults included, in declaration order.
type onboardingState struct {
	needsOnboarding, orgCreated              bool
	orgID, orgName                           *string
	integrationConnected, integrationSkipped bool
	nextStep                                 string
	blocker                                  *string
}

func (s onboardingState) object() *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("needs_onboarding", s.needsOnboarding)
	out.Set("org_created", s.orgCreated)
	out.Set("org_id", optionalPtr(s.orgID))
	out.Set("org_name", optionalPtr(s.orgName))
	out.Set("first_integration_connected", s.integrationConnected)
	out.Set("integration_skipped", s.integrationSkipped)
	out.Set("recommended_provider", "github")
	out.Set("next_step", s.nextStep)
	out.Set("blocker", optionalPtr(s.blocker))
	return out
}

// refuseText writes {"detail": text}: the onboarding routes raise
// HTTPException with a bare string detail, not error_detail().
func refuseText(w http.ResponseWriter, status int, text string) {
	policy.WriteDetail(w, status, text, nil)
}

// onboardingMembership is onboarding.py _load_membership: the user's
// earliest membership (joined_at, then created_at, NULLs last), limited to
// the token's organization when it names one. A token organization that is
// not a UUID matches nothing.
func onboardingMembership(ctx context.Context, tx pgx.Tx, userID uuid.UUID, tokenOrgID string) (*uuid.UUID, error) {
	query := `SELECT org_id FROM memberships WHERE user_id = $1::uuid`
	args := []any{userID}
	if tokenOrgID != "" {
		orgID := parseUUID(&tokenOrgID)
		if orgID == nil {
			return nil, nil
		}
		query += ` AND org_id = $2::uuid`
		args = append(args, *orgID)
	}
	var orgID uuid.UUID
	err := tx.QueryRow(ctx, query+` ORDER BY joined_at ASC, created_at ASC LIMIT 1`, args...).Scan(&orgID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &orgID, nil
}

// buildOnboardingState is onboarding.py _build_onboarding_state. It writes
// the refusal itself and returns nil for one.
func (h handlers) buildOnboardingState(w http.ResponseWriter, r *http.Request, tx pgx.Tx) *onboardingState {
	ctx := r.Context()
	caller := policy.UserFrom(ctx)
	id := parseUUID(&caller.UserID)
	if id == nil {
		refuseText(w, http.StatusUnauthorized, "invalid_user")
		return nil
	}
	user, err := userByID(ctx, tx, *id)
	if err != nil {
		h.fail(w, r, "load user", err)
		return nil
	}
	if user == nil {
		refuseText(w, http.StatusUnauthorized, "user_not_found")
		return nil
	}
	if !user.IsVerified {
		refuseText(w, http.StatusForbidden, "email_unverified")
		return nil
	}
	orgID, err := onboardingMembership(ctx, tx, user.ID, caller.OrgID)
	if err != nil {
		h.fail(w, r, "load membership", err)
		return nil
	}
	if orgID == nil {
		if user.IsSuperuser {
			return &onboardingState{nextStep: "dashboard"}
		}
		return &onboardingState{needsOnboarding: true, nextStep: "workspace"}
	}
	var name string
	var skippedAt *time.Time
	err = tx.QueryRow(ctx, `SELECT name, onboarding_integration_skipped_at FROM organizations WHERE id = $1::uuid`, *orgID).Scan(&name, &skippedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		blocker := "organization_not_found"
		return &onboardingState{needsOnboarding: true, nextStep: "workspace", blocker: &blocker}
	}
	if err != nil {
		h.fail(w, r, "load organization", err)
		return nil
	}
	var connected bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM integration_credentials WHERE org_id = $1::text AND is_active = true)`,
		orgID.String()).Scan(&connected); err != nil {
		h.fail(w, r, "load integrations", err)
		return nil
	}
	orgText := orgID.String()
	state := &onboardingState{orgCreated: true, orgID: &orgText, orgName: &name,
		integrationConnected: connected, integrationSkipped: skippedAt != nil && !connected}
	switch {
	case user.IsSuperuser || caller.Role == "admin":
		state.nextStep = "dashboard"
	case state.integrationConnected || state.integrationSkipped:
		state.nextStep = "complete"
	default:
		state.nextStep, state.needsOnboarding = "integration", true
	}
	return state
}

// onboardingStateRoute is GET /api/v1/auth/onboarding/state.
func (h handlers) onboardingStateRoute(w http.ResponseWriter, r *http.Request) {
	tx, err := h.Pool.Begin(r.Context())
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if state := h.buildOnboardingState(w, r, tx); state != nil {
		policy.WriteModel(w, http.StatusOK, state.object(), nil)
	}
}

// skipIntegration is POST /api/v1/auth/onboarding/skip-integration. The
// skip is committed before the state is rebuilt, so a refusal from the
// rebuild still leaves it written, as in Python.
func (h handlers) skipIntegration(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	caller := policy.UserFrom(ctx)
	if caller.OrgID == "" {
		refuseText(w, http.StatusBadRequest, "organization_required")
		return
	}
	orgID := parseUUID(&caller.OrgID)
	if orgID == nil {
		refuseText(w, http.StatusBadRequest, "organization_required")
		return
	}
	userID := parseUUID(&caller.UserID)
	if userID == nil {
		refuseText(w, http.StatusUnauthorized, "invalid_user")
		return
	}
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var member bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM memberships WHERE org_id = $1::uuid AND user_id = $2::uuid)`,
		*orgID, *userID).Scan(&member); err != nil {
		h.fail(w, r, "load membership", err)
		return
	}
	if !member {
		refuseText(w, http.StatusForbidden, "organization_membership_required")
		return
	}
	var skippedAt *time.Time
	err = tx.QueryRow(ctx, `SELECT onboarding_integration_skipped_at FROM organizations WHERE id = $1::uuid`, *orgID).Scan(&skippedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		refuseText(w, http.StatusNotFound, "organization_not_found")
		return
	}
	if err != nil {
		h.fail(w, r, "load organization", err)
		return
	}
	if skippedAt == nil {
		// Setting the attribute makes the ORM flush write the onupdate
		// updated_at too.
		now := h.Now().UTC()
		if _, err := tx.Exec(ctx, `UPDATE organizations SET onboarding_integration_skipped_at = $2, updated_at = $2 WHERE id = $1::uuid`,
			*orgID, now); err != nil {
			h.fail(w, r, "skip integration", err)
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit", err)
		return
	}
	read, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = read.Rollback(context.Background()) }()
	if state := h.buildOnboardingState(w, r, read); state != nil {
		policy.WriteModel(w, http.StatusOK, state.object(), nil)
	}
}
