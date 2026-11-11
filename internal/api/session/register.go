package session

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

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

// register is POST /api/v1/auth/register. auth-service serves it (the
// credentials forward); go-api has no local sign-up, so the route answers the
// moved-operation refusal with the users-owned switch off or on.
func (h handlers) register(w http.ResponseWriter, _ *http.Request) {
	usersowned.Write(w)
}

// refreshOrgGone reports the refresh token write's foreign key violation on
// its organization: the organization row is gone.
func refreshOrgGone(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23503" && pg.ConstraintName == "refresh_tokens_org_id_fkey"
}
