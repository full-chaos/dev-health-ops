package session

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// verificationTTL is create_email_verification_token's ttl_hours=24.
const verificationTTL = 24 * time.Hour

// autoCreateOrgEnv is api/auth/config.py's AUTH_AUTO_CREATE_ORG_ENV.
const autoCreateOrgEnv = "AUTH_AUTO_CREATE_ORG_ON_REGISTER"

type registerInputKey struct{}

// registerInput is RegisterRequest after validation.
type registerInput struct {
	email, password   string
	fullName, orgName *string
}

// validateRegister is RegisterRequest: EmailStr, a password of 8 to 128
// characters, and an optional full name and organization name.
func validateRegister(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	var input registerInput
	if object, ok := errs.Object(body); ok {
		input.email, _ = errs.RequiredEmailStr(object, "email")
		input.password, _ = errs.RequiredString(object, "password", 8, 128)
		if value, present := errs.OptionalString(object, "full_name", 0, 0); present {
			input.fullName = &value
		}
		if value, present := errs.OptionalString(object, "org_name", 0, 0); present {
			input.orgName = &value
		}
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return nil, false
	}
	return r.WithContext(context.WithValue(r.Context(), registerInputKey{}, input)), true
}

// autoCreateOrg is auth_auto_create_org_on_register(): unset or an
// unrecognized value keeps the default (true).
func (h handlers) autoCreateOrg() bool {
	switch pythonparity.Lower(pythonparity.Strip(h.Getenv(autoCreateOrgEnv))) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

// refuseViolations writes error_detail(message, errors=violations).
func refuseViolations(w http.ResponseWriter, status int, message string, violations []string) {
	detail := policy.ErrorDetail(message)
	list := make([]pyjson.Value, len(violations))
	for index, violation := range violations {
		list[index] = violation
	}
	detail.Set("errors", list)
	policy.WriteDetail(w, status, detail, nil)
}

// firstRunes is text[:n] on a Python str: the first n code points.
func firstRunes(text string, n int) string {
	runes := pyjson.Runes(text)
	if len(runes) > n {
		runes = runes[:n]
	}
	return pyjson.FromRunes(runes)
}

// insertOrganization adds an organizations row with the model's defaults.
func insertOrganization(ctx context.Context, tx pgx.Tx, id uuid.UUID, slug, name string, now time.Time) error {
	_, err := tx.Exec(ctx, `INSERT INTO organizations
	(id, slug, name, description, settings, tier, stripe_customer_id, managed_by, is_active,
	 created_at, updated_at, onboarding_integration_skipped_at)
VALUES ($1, $2, $3, NULL, '{}', 'community', NULL, 'stripe', true, $4, $4, NULL)`, id, slug, name, now.UTC())
	return err
}

// insertMembership adds a memberships row joined at now.
func insertMembership(ctx context.Context, tx pgx.Tx, id, userID, orgID uuid.UUID, role string, invitedBy *uuid.UUID, now time.Time) error {
	_, err := tx.Exec(ctx, `INSERT INTO memberships
	(id, user_id, org_id, role, invited_by_id, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $6, $6)`, id, userID, orgID, role, invitedBy, now.UTC())
	return err
}

// register is POST /api/v1/auth/register (register.py).
func (h handlers) register(w http.ResponseWriter, r *http.Request) {
	input, _ := r.Context().Value(registerInputKey{}).(registerInput)
	ctx := r.Context()
	email := pythonparity.Strip(pythonparity.Lower(input.email))
	if violations := passwordpolicy.Validate(input.password); len(violations) > 0 {
		refuseViolations(w, http.StatusUnprocessableEntity, "Password validation failed", violations)
		return
	}
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	existing, err := userByEmail(ctx, tx, email)
	if err != nil {
		h.fail(w, r, "load user", err)
		return
	}
	if existing != nil {
		// register.py also emits a failure audit row here, but the
		// HTTPException that follows rolls the session back, so no row
		// is ever written.
		refuse(w, http.StatusBadRequest, "Email already registered")
		return
	}
	hash, err := passwordhash.Hash(input.password)
	if err != nil {
		h.fail(w, r, "hash password", err)
		return
	}
	now := h.Now()
	user := &userRow{ID: h.NewUUID(), Email: email, FullName: input.fullName, IsActive: true}
	if _, err := tx.Exec(ctx, `INSERT INTO users
	(id, email, username, password_hash, full_name, avatar_url, auth_provider, auth_provider_id,
	 is_active, is_verified, is_superuser, token_version, last_login_at, created_at, updated_at)
VALUES ($1, $2, NULL, $3, $4, NULL, 'local', NULL, true, false, false, 0, NULL, $5, $5)`,
		user.ID, user.Email, hash, user.FullName, now.UTC()); err != nil {
		h.fail(w, r, "insert user", err)
		return
	}

	var orgID *uuid.UUID
	if h.autoCreateOrg() {
		name := "My Organization"
		if input.orgName != nil && *input.orgName != "" {
			name = *input.orgName
		}
		slug := firstRunes(strings.ReplaceAll(pythonparity.Lower(name), " ", "-"), 50) + "-" + user.ID.String()[:8]
		id := h.NewUUID()
		if err := insertOrganization(ctx, tx, id, slug, name, now); err != nil {
			h.fail(w, r, "insert organization", err)
			return
		}
		if err := insertMembership(ctx, tx, h.NewUUID(), user.ID, id, "owner", nil, now); err != nil {
			h.fail(w, r, "insert membership", err)
			return
		}
		changes := emailChanges(email)
		changes.Set("organization_id", id.String())
		userID := user.ID
		if err := h.emitAudit(ctx, tx, r, auditEntry{orgID: id, action: audit.ActionCreate, resourceType: audit.ResourceUser,
			resourceID: user.ID.String(), userID: &userID, description: "User registered", changes: changes}); err != nil {
			h.fail(w, r, "audit", err)
			return
		}
		orgID = &id
	}

	token, err := h.replaceLinkToken(ctx, tx, "email_verification_tokens", user.ID, now, verificationTTL)
	if err != nil {
		h.fail(w, r, "verification token", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit", err)
		return
	}
	h.sendVerification(ctx, user, token)

	out := pyjson.NewObject()
	out.Set("message", "Registration successful")
	out.Set("user_id", user.ID.String())
	if orgID != nil {
		out.Set("org_id", orgID.String())
	} else {
		out.Set("org_id", nil)
	}
	policy.WriteModel(w, http.StatusCreated, out, nil)
}
