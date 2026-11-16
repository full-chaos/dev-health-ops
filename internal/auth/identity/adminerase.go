package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrReferenceBlocked reports an erase that a row naming the user refuses: a foreign key with ON DELETE RESTRICT or
// NO ACTION into auth.principals (a platform role the user granted, a grant it holds or revoked, a service account it
// owns, a credential projection) or into public.users (an impersonation, billing-audit or refund row). Nothing is
// written.
var ErrReferenceBlocked = errors.New("identity: a row that names the user blocks its erase")

// AdminForceDeleteRequest is go-api's force delete (userdelete.go forceDeleteUser) while auth-service owns users.
type AdminForceDeleteRequest struct {
	Principal uuid.UUID
	Actor     uuid.UUID
	RequestID *string
	Caller    audit.Caller
	// Authorize is the route's actor rule and the last platform_admin guard, under the platform role write lock.
	Authorize func(ctx context.Context, tx audit.TxOps) error
	// RevokeRoles revokes the user's own platform role assignments on tx, after Authorize (their audit rows and events
	// are queued on tx).
	RevokeRoles func(ctx context.Context, tx audit.TxOps) error
}

// AdminForceDeleteResult is what the erase found: WasActive is the public.users is_active it deleted.
type AdminForceDeleteResult struct {
	WasActive bool
}

// AdminForceDeleteUser erases the user in one audit.Commit with the soft delete's events (principal.disabled,
// user.tokens_invalidated: a consumer re-reads and finds no principal): its own platform role assignments are revoked,
// its own auth memberships deleted (membership.removed each), then public.users, auth.users and the principal; the foreign keys decide the rest as their owner (sessions, refresh and link credentials, external
// identities (external_identity.unlinked each), OAuth grants and go-api's tokens, memberships and conversations go;
// audit, invite, override and allowlist rows stay with the reference set to NULL). The queued audit rows are written
// before the principal goes, so the key keeps them with the id set to NULL; the erase row itself names the admin as
// actor and the user by id in its attributes. A RESTRICT or NO ACTION row refuses the erase (ErrReferenceBlocked).
func (s *Store) AdminForceDeleteUser(ctx context.Context, req AdminForceDeleteRequest) (result AdminForceDeleteResult, err error) {
	defer func() { s.countLink(ctx, "admin_force_delete", err) }()
	if !s.ownsUsers {
		return result, ErrSignupNotAvailable
	}
	if req.Authorize == nil || req.RevokeRoles == nil {
		return result, errors.New("identity: an admin erase needs its actor rule and its role revoke")
	}
	subject := req.Principal.String()
	actor := req.Actor.String()
	event := audit.AuditEvent{EventType: "auth.user.erased", Outcome: audit.OutcomeAllowed, ActorPrincipalID: &actor,
		RequestID: req.RequestID, Attributes: map[string]any{"reason_code": "principal_erased", "target_kind": "principal", "target_id": subject}}
	req.Caller.Annotate(event.Attributes)
	err = commitMutation(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := req.Authorize(ctx, tx); err != nil {
				return err
			}
			r, err := applyAdminErase(ctx, tx, s.q, req.Principal, func(ctx context.Context) error { return req.RevokeRoles(ctx, tx) })
			result = r
			return err
		},
		Audit:    event,
		Events:   []audit.OutboxEvent{principalEvent(subject, "principal.disabled", false), userEvent(subject, "user.tokens_invalidated")},
		Refusals: []error{ErrUserNotFound, ErrReferenceBlocked},
	})
	return result, err
}

// eraseTx is the part of the audited transaction the erase runs on: audit.TxOps, or a test double.
type eraseTx interface {
	AdminTx
	AddEvent(events ...audit.OutboxEvent) error
	FlushAudit(ctx context.Context) error
}

func applyAdminErase(ctx context.Context, tx eraseTx, q string, principal uuid.UUID, revokeRoles func(context.Context) error) (AdminForceDeleteResult, error) {
	var result AdminForceDeleteResult
	var locked uuid.UUID
	err := tx.QueryRow(ctx, `SELECT id FROM `+q+`.principals WHERE id = $1 AND kind = 'user' FOR UPDATE`, principal).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, ErrUserNotFound
	}
	if err != nil {
		return result, err
	}
	err = tx.QueryRow(ctx, `SELECT COALESCE(is_active, false) FROM public.users WHERE id = $1 FOR UPDATE`, principal).Scan(&result.WasActive)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, ErrLegacyProjection
	}
	if err != nil {
		return result, err
	}
	if err := revokeRoles(ctx); err != nil {
		return result, err
	}
	subject := principal.String()
	var memberships []string
	if err := tx.QueryRow(ctx, `SELECT COALESCE(array_agg(id::text ORDER BY id), '{}') FROM `+q+`.memberships WHERE principal_id = $1`,
		principal).Scan(&memberships); err != nil {
		return result, err
	}
	var identities, providers []string
	if err := tx.QueryRow(ctx, `SELECT COALESCE(array_agg(id::text ORDER BY id), '{}'), COALESCE(array_agg(provider ORDER BY id), '{}')
FROM `+q+`.external_identities WHERE principal_id = $1`, principal).Scan(&identities, &providers); err != nil {
		return result, err
	}
	for _, id := range memberships {
		if err := tx.AddEvent(removedMembershipEvent(id)); err != nil {
			return result, err
		}
	}
	for i, id := range identities {
		if err := tx.AddEvent(identityEvent("external_identity.unlinked", ExternalKey{Provider: Provider(providers[i])}, id, subject)); err != nil {
			return result, err
		}
	}
	// The queued rows name the user; after the principal goes they could not be written.
	if err := tx.FlushAudit(ctx); err != nil {
		return result, err
	}
	statements := []string{`DELETE FROM ` + q + `.memberships WHERE principal_id = $1`}
	statements = append(statements, publicNullStatements()...)
	for _, statement := range append(statements,
		`DELETE FROM public.users WHERE id = $1`,
		`DELETE FROM `+q+`.users WHERE principal_id = $1`,
		`DELETE FROM `+q+`.principals WHERE id = $1`,
	) {
		if _, err := tx.Exec(ctx, statement, principal); err != nil {
			if isReferenceBlocked(err) {
				return result, ErrReferenceBlocked
			}
			return result, err
		}
	}
	return result, nil
}

// publicNullStatements set each RESTRICT or NO ACTION reference into public.users to NULL, as go-api's force delete
// does, so the impersonation, billing-audit and refund records stay with no user.
func publicNullStatements() []string {
	var statements []string
	for _, c := range authschema.UserEraseNullColumns() {
		table, column := pgx.Identifier{"public", c.Table}.Sanitize(), pgx.Identifier{c.Column}.Sanitize()
		statements = append(statements, `UPDATE `+table+` SET `+column+` = NULL WHERE `+column+` = $1`)
	}
	return statements
}

// removedMembershipEvent is the provisioning event of an auth membership the erase deletes.
func removedMembershipEvent(id string) audit.OutboxEvent {
	return membershipEvent(uuid.MustParse(id), "membership.removed", map[string]any{"status": "removed"})
}

// isReferenceBlocked is a foreign key that refuses the delete: 23001 (RESTRICT) or 23503 (NO ACTION).
func isReferenceBlocked(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "23001" || pgErr.Code == "23503")
}
