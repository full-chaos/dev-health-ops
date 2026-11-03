// Package copyproject inserts the auth copies a session row points at (D6651):
// auth.principals, auth.users and auth.organizations rows made from the public
// rows, inside the caller's transaction. A copy is a foreign-key target only and
// is never a decision input. It imports no identity, session or membership code,
// so the session store can use it without an import cycle.
package copyproject

import (
	"context"
	"errors"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity/emailvalidator"
)

var (
	// ErrUserNotFound reports a public user that public.users does not hold.
	ErrUserNotFound = errors.New("identity: user not found")
	// ErrEmailCollision reports a projection whose login key another principal
	// already holds. Nothing is merged and nothing is written.
	ErrEmailCollision = errors.New("identity: the email belongs to another principal")
	// ErrOrganizationNotFound reports an organization public.organizations does
	// not hold.
	ErrOrganizationNotFound = errors.New("identity: organization not found")
	// ErrOrganizationSlugCollision reports a projection whose slug another
	// organization already holds in the auth schema. Nothing is written.
	ErrOrganizationSlugCollision = errors.New("identity: the organization slug belongs to another organization")
)

const usersEmailLowerKey = "users_email_lower_key"

type source struct {
	email      string
	emailLower string
	active     bool
	verified   bool
}

type orgState struct {
	slug   string
	name   string
	active bool
}

// LoginKey is the auth.users.email_lower of a public.users email: go-api finds
// a user by lower(email), and public.users holds addresses pydantic EmailStr
// refuses, which keep Python strip(lower()) as their key.
func LoginKey(email string) string {
	if utf8.ValidString(email) {
		if normalized, _, ok := emailvalidator.ValidateEmail([]rune(email)); ok {
			return pythonparity.Strip(pythonparity.Lower(string(normalized)))
		}
	}
	return pythonparity.Strip(pythonparity.Lower(email))
}

func isEmailCollision(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == usersEmailLowerKey
}

// PrincipalEvent is a principal event: the status after it.
func PrincipalEvent(principal, verb string, active bool) audit.OutboxEvent {
	status := "disabled"
	if active {
		status = "active"
	}
	return audit.OutboxEvent{AggregateType: "principal", AggregateID: provisioning.WireID("principal", principal),
		EventType: verb, Change: map[string]any{"status": status}}
}

// UserEvent is a user event; a user event carries no change member.
func UserEvent(principal, verb string) audit.OutboxEvent {
	return audit.OutboxEvent{AggregateType: "user", AggregateID: provisioning.WireID("user", principal), EventType: verb}
}

// OrganizationAudit is the audit row of an organization projection.
func OrganizationAudit(eventType, reason, org string) audit.AuditEvent {
	return audit.AuditEvent{
		EventType: eventType, Outcome: audit.OutcomeAllowed, OrganizationID: &org,
		Attributes: map[string]any{"reason_code": reason, "target_kind": "organization", "target_id": org},
	}
}

// EnsureUserIn inserts the auth.principals and auth.users copies of a public
// user inside tx, only when the principal row is missing; an existing copy is
// never read for a value or changed. q is the quoted auth schema. The copy is
// the foreign-key target of auth.sessions and never a decision input (D6651):
// the caller decides on public.users. A public user that is not there is
// ErrUserNotFound; an auth.users row with the same login key under another id
// is ErrEmailCollision and writes nothing. The audit row and the
// principal.created and user.created events are queued on tx, as the
// standalone projection writes them.
func EnsureUserIn(ctx context.Context, tx audit.TxOps, q string, id uuid.UUID) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+q+`.principals WHERE id = $1)`, id).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	var email string
	var src source
	err := tx.QueryRow(ctx, `SELECT email, is_active, is_verified FROM public.users WHERE id = $1`, id).Scan(&email, &src.active, &src.verified)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrUserNotFound
	}
	if err != nil {
		return err
	}
	src.email, src.emailLower = email, LoginKey(email)
	var inserted uuid.UUID
	err = tx.QueryRow(ctx, InsertPrincipalSQL(q), id, src.active).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, InsertUserSQL(q), id, src.email, src.emailLower, src.verified); err != nil {
		if isEmailCollision(err) {
			return ErrEmailCollision
		}
		return err
	}
	principal := id.String()
	events := []audit.OutboxEvent{PrincipalEvent(principal, "principal.created", src.active), UserEvent(principal, "user.created")}
	if src.verified {
		events = append(events, UserEvent(principal, "user.email_verified"))
	}
	if err := tx.AddAudit(PrincipalProjectedAudit(principal)); err != nil {
		return err
	}
	return tx.AddEvent(events...)
}

// EnsureOrganizationIn inserts the auth.organizations copy of a public
// organization inside tx, only when the row is missing; an existing copy is
// never read for a value or changed. Like EnsureUserIn it is independent of
// the organizations-owned switch: public.organizations is the source. A
// public organization that is not there is ErrOrganizationNotFound.
func EnsureOrganizationIn(ctx context.Context, tx audit.TxOps, q string, orgID uuid.UUID) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+q+`.organizations WHERE id = $1)`, orgID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	var src orgState
	err := tx.QueryRow(ctx, `SELECT slug, name, is_active FROM public.organizations WHERE id = $1`, orgID).Scan(&src.slug, &src.name, &src.active)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrOrganizationNotFound
	}
	if err != nil {
		return err
	}
	var inserted uuid.UUID
	err = tx.QueryRow(ctx, InsertOrganizationSQL(q), orgID, src.slug, src.name, src.active).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+q+`.organizations WHERE id = $1)`, orgID).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		}
		return ErrOrganizationSlugCollision
	}
	if err != nil {
		return err
	}
	org := orgID.String()
	if err := tx.AddAudit(OrganizationAudit("auth.organization.projected", "organization_created", org)); err != nil {
		return err
	}
	change := map[string]any{"status": "suspended"}
	if src.active {
		change["status"] = "active"
	}
	if provisioning.ChangeAllows("slug", src.slug) {
		change["slug"] = src.slug
	}
	return tx.AddEvent(audit.OutboxEvent{AggregateType: "organization", AggregateID: provisioning.WireID("organization", org),
		EventType: "organization.created", Change: change})
}

func InsertPrincipalSQL(q string) string {
	return `INSERT INTO ` + q + `.principals (id, kind, disabled_at)
VALUES ($1, 'user', CASE WHEN $2::boolean THEN NULL ELSE clock_timestamp() END)
ON CONFLICT (id) DO NOTHING
RETURNING id`
}

func InsertUserSQL(q string) string {
	return `INSERT INTO ` + q + `.users (principal_id, email, email_lower, email_verified_at)
VALUES ($1, $2, $3, CASE WHEN $4::boolean THEN clock_timestamp() ELSE NULL END)`
}

func InsertOrganizationSQL(q string) string {
	return `INSERT INTO ` + q + `.organizations (id, slug, name, disabled_at)
VALUES ($1, $2, $3, CASE WHEN $4::boolean THEN NULL ELSE clock_timestamp() END)
ON CONFLICT DO NOTHING
RETURNING id`
}

// PrincipalProjectedAudit is the audit row of a user projection.
func PrincipalProjectedAudit(principal string) audit.AuditEvent {
	return audit.AuditEvent{
		EventType: "auth.principal.projected", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &principal,
		Attributes: map[string]any{"reason_code": "principal_projected", "target_kind": "principal", "target_id": principal},
	}
}
