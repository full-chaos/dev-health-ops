package session

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/registerorg"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// verificationTTL is create_email_verification_token's ttl_hours=24.
const verificationTTL = 24 * time.Hour

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
		input.email, _ = errs.RequiredRegisterEmailStr(object, "email")
		input.password, _ = errs.RequiredString(object, "password", 8, 128)
		if value, present := errs.OptionalDisplayText(object, "full_name"); present {
			input.fullName = &value
		}
		if value, present := errs.OptionalDisplayText(object, "org_name"); present {
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
	return registerorg.Enabled(h.Getenv(registerorg.EnvName))
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
	if h.refuseWhileUsersOwned(w) {
		return
	}
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

	existing, err := h.userByEmail(ctx, tx, email)
	if err != nil {
		h.fail(w, r, "load user", err)
		return
	}
	if existing != nil {
		// The answer, the hash work and the queued mail match a new
		// sign-up's; the hash is discarded.
		if _, err := passwordhash.Hash(input.password); err != nil {
			h.fail(w, r, "hash password", err)
			return
		}
		h.queueMail(ctx, mailAccountExists, func(ctx context.Context) error { return h.sendAccountExists(ctx, existing) })
		writeRegisterAccepted(w)
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
		if isUniqueViolation(err) {
			// A concurrent sign-up for the same address won the unique index:
			// this one is a taken address, answered and mailed as one.
			h.queueMail(ctx, mailAccountExists, func(ctx context.Context) error {
				return h.sendAccountExists(ctx, &userRow{Email: email})
			})
			writeRegisterAccepted(w)
			return
		}
		h.fail(w, r, "insert user", err)
		return
	}

	if h.autoCreateOrg() {
		name := registerorg.Name(input.orgName)
		slug := registerorg.Slug(name, user.ID)
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
	h.queueMail(ctx, mailVerification, func(ctx context.Context) error { return h.sendRegisterVerification(ctx, user, token) })
	writeRegisterAccepted(w)
}

// isUniqueViolation reports a Postgres unique_violation (SQLSTATE 23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// registerAcceptedMessage is the one answer to every sign-up that passed
// validation.
const registerAcceptedMessage = "Check your email to continue"

func writeRegisterAccepted(w http.ResponseWriter) {
	out := pyjson.NewObject()
	out.Set("message", registerAcceptedMessage)
	policy.WriteModel(w, http.StatusAccepted, out, nil)
}

// queueMail sends one best-effort mail off the request; the send outlives it.
func (h handlers) queueMail(ctx context.Context, purpose string, send func(context.Context) error) {
	detached := context.WithoutCancel(ctx)
	run := func() error {
		jobCtx, cancel := linkMailQueue.bind(detached, authmail.SendTimeout)
		defer cancel()
		err := send(jobCtx)
		if err != nil {
			h.Logger.ErrorContext(detached, "api: failed to send sign-up email", "purpose", purpose, "error", err)
		}
		return err
	}
	linkMailQueue.submit(purpose, run)
}

// sendRegisterVerification is sendVerification with the failure returned.
func (h handlers) sendRegisterVerification(ctx context.Context, user *userRow, token string) error {
	name := user.Email
	if user.FullName != nil && *user.FullName != "" {
		name = *user.FullName
	}
	return h.Mail.Try(ctx, user.Email, "Verify your email address", "verification", map[string]string{
		"full_name": name, "verification_url": h.Mail.Link("/auth/verify", token),
	})
}

// sendAccountExists tells the owner of an existing account that a sign-up was
// started with the address. It names nothing about the requester.
func (h handlers) sendAccountExists(ctx context.Context, user *userRow) error {
	name := user.Email
	if user.FullName != nil && *user.FullName != "" {
		name = *user.FullName
	}
	return h.Mail.Try(ctx, user.Email, "You already have an account", "account_exists", map[string]string{
		"full_name":  name,
		"signin_url": h.Mail.BaseURL() + "/auth/signin",
		"reset_url":  h.Mail.BaseURL() + "/auth/forgot-password",
	})
}

// refreshOrgGone reports the refresh token write's foreign key violation on
// its organization: the organization row is gone.
func refreshOrgGone(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23503" && pg.ConstraintName == "refresh_tokens_org_id_fkey"
}
