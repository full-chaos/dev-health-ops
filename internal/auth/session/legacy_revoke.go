package session

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// RevokeLegacy ends the go-api lineage of a refresh token the caller verified:
// every live public.refresh_tokens row of the token's family and every live
// session exchanged from that family, in one audited transaction. It reports
// false, writing nothing, for a token whose row is unknown, belongs to another
// principal or organization, has expired, or whose lineage is already ended.
// This is stricter than go-api's logout, which revokes only the presented
// token: a sign-out must leave no token of the lineage that refresh accepts.
func (s *Store) RevokeLegacy(ctx context.Context, legacy LegacyRefresh, actor Actor) (bool, error) {
	return s.RevokeLegacyWith(ctx, legacy, actor, publicRowOf(ctx))
}

// RevokeLegacyWith is RevokeLegacy with public, when not nil, run in the audited
// transaction after an auth-schema session of the lineage is revoked, with the
// token's principal and organization: go-api's public.audit_logs row of a
// compatibility sign-out. A lineage with no auth-schema session to revoke writes
// no auth event and so no public row.
func (s *Store) RevokeLegacyWith(ctx context.Context, legacy LegacyRefresh, actor Actor, public PublicRow) (bool, error) {
	hexDigest := hex.EncodeToString(legacyJTIDigest(legacy.JTI))
	var row *legacyRow
	if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) (err error) {
		row, err = readLegacyRow(ctx, tx.Query, hexDigest)
		return err
	}); err != nil {
		return false, fmt.Errorf("%w: reading the go-api refresh token: %w", ErrStore, err)
	}
	if row == nil || row.userID != legacy.PrincipalID || !sameOrganization(row.orgID, legacy.OrganizationID) || row.expired {
		return false, nil
	}
	principal := legacy.PrincipalID.String()
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			ids, err := s.revokeFamilyTx(ctx, tx, row.familyID, revokedByCaller)
			if err != nil || len(ids) == 0 || public == nil {
				return err
			}
			return public(ctx, tx, legacy.PrincipalID, legacy.OrganizationID)
		},
		Audit: audit.AuditEvent{
			EventType: "auth.session.revoked", Outcome: audit.OutcomeAllowed,
			ActorPrincipalID: actorID(actor), SubjectPrincipalID: &principal, RequestID: requestIDOf(ctx, actor),
			Attributes: map[string]any{
				"reason_code": "session_revoked", "target_kind": "principal", "target_id": principal,
				"credential_class": "refresh_credential",
			},
		},
		OffStream: audit.OffStreamSessionLifecycle,
	})
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, audit.ErrNoStateChange):
		// No auth-schema session changed, so the audited transaction rolled back; the
		// go-api rows are public state with no audit row, as in revokeLegacyFamily.
		var affected int64
		if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			tag, err := tx.Exec(ctx, revokeLegacyFamilyRows, row.familyID)
			affected = tag.RowsAffected()
			return err
		}); err != nil {
			return false, fmt.Errorf("%w: revoking a go-api refresh lineage: %w", ErrStore, err)
		}
		return affected > 0, nil
	default:
		return false, fmt.Errorf("%w: revoking a go-api refresh lineage: %w", ErrStore, err)
	}
}
