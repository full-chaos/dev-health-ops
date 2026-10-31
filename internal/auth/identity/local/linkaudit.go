package local

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/publicaudit"
)

// resetRequestedEvent is the audit row of a password-reset request. A known
// principal is both subject and actor, in the organization of its earliest
// membership. An unknown address leaves a row that holds no principal, no
// organization and no address: the decoy's random id never reaches it.
func resetRequestedEvent(principal uuid.UUID, known bool, org *string) audit.AuditEvent {
	if !known {
		return audit.AuditEvent{
			EventType: audit.EventPasswordResetRequested, Outcome: audit.OutcomeDenied,
			Attributes: map[string]any{"reason_code": "principal_not_found", "target_kind": "principal"},
		}
	}
	id := principal.String()
	return audit.AuditEvent{
		EventType: audit.EventPasswordResetRequested, Outcome: audit.OutcomeAllowed,
		ActorPrincipalID: &id, SubjectPrincipalID: &id, OrganizationID: org,
		Attributes: map[string]any{"reason_code": "password_reset_requested", "target_kind": "principal", "target_id": id},
	}
}

// issueResetRequest is IssueBound for a reset link: the link rows and the audit
// row of the request are one transaction (audit.AppendWith). A decoy runs the
// same statements as a real request and writes the unknown-address row.
func (l *PGLinks) issueResetRequest(ctx context.Context, req IssueRequest, principal uuid.UUID, real bool) error {
	org, err := identity.AuditOrganization(ctx, l.pool, l.q, principal)
	if err != nil {
		return fmt.Errorf("local: reading the membership of a reset request: %w", err)
	}
	err = audit.AppendWith(ctx, l.pool, l.schema, resetRequestedEvent(principal, real, org), func(ctx context.Context, tx audit.TxOps) error {
		if err := l.writeBoundLink(ctx, tx, req, principal, real); err != nil {
			return err
		}
		if !real {
			return nil
		}
		return resetRequestedPublicRow(ctx, tx, principal)
	})
	if errors.Is(err, errResetRequestPublicRow) {
		// The request answers as every other address's does (no enumeration);
		// nothing was committed: no link, no auth event, no public row.
		resetRequestSkipped.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reasonPublicAuditRefused)))
		slog.WarnContext(ctx, "local: reset request not recorded; nothing committed", "reason", reasonPublicAuditRefused)
		return ErrResetRequestNotRecorded
	}
	if err != nil {
		return fmt.Errorf("local: storing a link credential: %w", err)
	}
	return nil
}

// errResetRequestPublicRow wraps a failed write of a reset request's go-api row.
var errResetRequestPublicRow = errors.New("local: the reset request's public audit row was refused")

// ErrResetRequestNotRecorded is IssueBound's answer to a reset request whose
// go-api public.audit_logs row could not be written: nothing was committed,
// and the flow answers as it answers any address (a decoy, no mail delivered).
var ErrResetRequestNotRecorded = errors.New("local: the reset request was not recorded")

// reasonPublicAuditRefused is the one closed reason of a skipped reset request.
const reasonPublicAuditRefused = "public_audit_refused"

// resetRequestSkipped counts reset requests answered without a commit.
var resetRequestSkipped = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/identity/local").Int64Counter(
		"auth.local.reset_request_skipped", metric.WithDescription("Password reset requests answered without a commit, by closed reason."))
	if err != nil {
		counter, _ = noop.NewMeterProvider().Meter("noop").Int64Counter("auth.local.reset_request_skipped")
	}
	return counter
}()

// resetRequestedPublicRow is go-api's row of a reset request (internal/api/session verify.go:231 auditUserEvent):
// password_reset_requested "Password reset requested", resource user/<user>, changes {}, in the user's first public
// membership's organization in the login order; none for a user with no membership (CHAOS-11400).
func resetRequestedPublicRow(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error {
	org, err := publicaudit.FirstOrganization(ctx, tx, principal)
	if err != nil || org == nil {
		return err
	}
	if err := publicaudit.UserRow(ctx, tx, *org, principal, "password_reset_requested", "Password reset requested", nil); err != nil {
		return fmt.Errorf("%w: %w", errResetRequestPublicRow, err)
	}
	return nil
}

// PeekVerificationLink reads the principal of a live verification credential
// and consumes nothing.
func (l *PGLinks) PeekVerificationLink(ctx context.Context, tokenHash [32]byte) (uuid.UUID, bool, error) {
	var principal uuid.UUID
	err := l.pool.QueryRow(ctx, `SELECT principal_id FROM `+l.q+`.link_credentials
WHERE token_hash = $1 AND purpose = $2 AND consumed_at IS NULL AND expires_at > now()`, tokenHash[:], string(PurposeEmailVerification)).Scan(&principal)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("local: reading a verification link: %w", err)
	}
	return principal, true, nil
}

// ConsumeVerificationLinkInTx marks the principal's verification credential
// consumed inside the caller's transaction, on the database clock. It returns
// identity.ErrLinkUnusable, writing nothing, when the credential is not exactly
// one live row of that principal.
func (l *PGLinks) ConsumeVerificationLinkInTx(ctx context.Context, tx audit.TxOps, principalID uuid.UUID, tokenHash [32]byte) error {
	tag, err := tx.Exec(ctx, `UPDATE `+l.q+`.link_credentials SET consumed_at = now()
WHERE token_hash = $1 AND purpose = $2 AND principal_id = $3::uuid AND consumed_at IS NULL AND expires_at > now()`,
		tokenHash[:], string(PurposeEmailVerification), principalID.String())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return identity.ErrLinkUnusable
	}
	return nil
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}
