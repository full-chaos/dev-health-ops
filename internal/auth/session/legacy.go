package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// LegacyRefresh is a go-api refresh token whose signature, type and expiry the
// caller verified: its subject, its org_id claim and its jti.
type LegacyRefresh struct {
	PrincipalID    uuid.UUID
	OrganizationID *uuid.UUID
	JTI            contracts.SecretString
}

// legacyAMR is the amr of a session exchanged from a go-api refresh token,
// which records no sign-in method.
var legacyAMR = []string{"pwd"}

// legacyJTIDigest is go-api's token_hash of a jti (the SHA-256 of its text):
// raw for refresh_credentials.legacy_jti_hash, hex for public.refresh_tokens.
func legacyJTIDigest(jti contracts.SecretString) []byte {
	digest := sha256.Sum256([]byte(jti.Reveal()))
	return digest[:]
}

// legacyRow is the public.refresh_tokens row of a jti, read FOR UPDATE.
type legacyRow struct {
	userID       uuid.UUID
	orgID        *uuid.UUID
	familyID     uuid.UUID
	revoked      bool
	revokedGrace bool
	expired      bool
}

const legacyRowQuery = `
	SELECT user_id, org_id, family_id, revoked_at IS NOT NULL,
	       revoked_at IS NOT NULL AND revoked_at > clock_timestamp() - make_interval(secs => $2::double precision),
	       expires_at <= clock_timestamp()
	FROM public.refresh_tokens WHERE token_hash = $1::text FOR UPDATE`

// errMultipleLegacyRows is a jti matching more than one go-api row, which
// go-api's own lookup refuses as well.
var errMultipleLegacyRows = errors.New("session: more than one public.refresh_tokens row for a jti")

// errDeactivatedFirstExchange rolls back a first exchange whose principal is
// disabled or missing, so the family is revoked in a transaction of its own.
var errDeactivatedFirstExchange = errors.New("session: first legacy exchange for an unavailable principal")

func readLegacyRow(ctx context.Context, tx queryer, hexDigest string) (*legacyRow, error) {
	rows, err := tx.Query(ctx, legacyRowQuery, hexDigest, GraceWindow.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var found *legacyRow
	for rows.Next() {
		if found != nil {
			return nil, errMultipleLegacyRows
		}
		var row legacyRow
		if err := rows.Scan(&row.userID, &row.orgID, &row.familyID, &row.revoked, &row.revokedGrace, &row.expired); err != nil {
			return nil, err
		}
		found = &row
	}
	return found, rows.Err()
}

// queryer is a pgx.Tx or an audit.TxOps that can return rows.
type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// ExchangeLegacy exchanges a go-api refresh token once for a new session.
//
// A live public.refresh_tokens row (unrevoked, unexpired, the token's own
// subject and organization) is revoked and a session is created whose first
// credential records the jti's digest (legacy_jti_hash), in one transaction.
// The session's pin is the user's token_version, as for every session.
//
// The session's first credential is derived from the jti (deriveSuccessor), so
// a jti presented again within GraceWindow of its exchange, while that
// credential is unused and its session live, is answered with the same
// credential and nothing is written (ReasonGraceReplay), as go-api replays a
// successor. A derived credential that no longer matches the stored one (the
// successor key changed) is ErrGraceUnavailable, a retry that revokes nothing.
// Any other revoked row is reuse: its go-api family and every session
// exchanged from a jti of that family are revoked (ReasonRefreshReused). A
// first exchange or a replay for a deactivated (or missing) principal revokes
// the same family and answers ReasonPrincipalDeactivated, as go-api's
// allowActive does.
func (s *Store) ExchangeLegacy(ctx context.Context, legacy LegacyRefresh) (result Result, err error) {
	defer func() { s.recordExchange(ctx, result, err) }()
	digest := legacyJTIDigest(legacy.JTI)
	hexDigest := hex.EncodeToString(digest)
	var reusedFamily *uuid.UUID
	var deactivated bool
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		row, err := readLegacyRow(ctx, tx, hexDigest)
		if err != nil {
			return err
		}
		switch {
		case row == nil || row.userID != legacy.PrincipalID || !sameOrganization(row.orgID, legacy.OrganizationID):
			result.Reason = ReasonRefreshUnknown
		case row.revoked && row.revokedGrace:
			replay, err := s.replayLegacy(ctx, tx, digest, legacy.JTI)
			if err != nil {
				return err
			}
			switch {
			case replay.deactivated:
				reusedFamily, deactivated = &row.familyID, true
			case replay.issued != nil:
				result = Result{Reason: ReasonGraceReplay, Issued: *replay.issued}
			default:
				reusedFamily = &row.familyID
			}
		case row.revoked:
			family := row.familyID
			reusedFamily = &family
		case row.expired:
			result.Reason = ReasonRefreshExpired
		default:
			tag, err := tx.Exec(ctx, `UPDATE public.refresh_tokens SET revoked_at = clock_timestamp()
				WHERE token_hash = $1::text AND revoked_at IS NULL`, hexDigest)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return fmt.Errorf("revoking the go-api refresh token changed %d rows under its lock", tag.RowsAffected())
			}
			issued, err := s.insertSession(ctx, tx, legacy.PrincipalID, legacy.OrganizationID, legacyAMR, legacy.JTI)
			if errors.Is(err, ErrPrincipalUnavailable) {
				reusedFamily, deactivated = &row.familyID, true
				return errDeactivatedFirstExchange
			}
			if err != nil {
				return err
			}
			result = Result{Reason: ReasonAccepted, Issued: issued}
		}
		return nil
	})
	switch {
	case errors.Is(err, ErrGraceUnavailable):
		return Result{Reason: ReasonDependency}, err
	case errors.Is(err, errDeactivatedFirstExchange):
		err = nil
	case err != nil:
		return Result{Reason: ReasonStoreUnavailable}, fmt.Errorf("%w: legacy refresh exchange: %w", ErrStore, err)
	}
	if reusedFamily == nil {
		return result, nil
	}
	revoked, err := s.revokeLegacyFamily(ctx, legacy.PrincipalID, *reusedFamily)
	if err == nil && deactivated {
		revoked.Reason = ReasonPrincipalDeactivated
	}
	return revoked, err
}

// firstCredential is the first refresh credential of a session, with its row id
// and, for a legacy exchange, the digest of the jti. A legacy credential is
// derived from the jti, as a rotated one is from its parent, so a second
// presentation of the jti inside the grace window recomputes the same value.
func (s *Store) firstCredential(legacyJTI contracts.SecretString) (credential contracts.SecretString, id uuid.UUID, legacyDigest []byte, err error) {
	if id, err = uuid.NewRandom(); err != nil {
		return contracts.SecretString{}, uuid.UUID{}, nil, err
	}
	if legacyJTI.Reveal() == "" {
		credential, err = newCredential()
		return credential, id, nil, err
	}
	return deriveSuccessor(s.key, id, legacyJTI), id, legacyJTIDigest(legacyJTI), nil
}

// legacyReplay is the decision on a jti revoked inside the grace window: the
// credential to answer again, a deactivated principal, or neither (reuse).
type legacyReplay struct {
	issued      *Issued
	deactivated bool
}

// replayLegacy reads the credential exchanged from a jti under its session
// lock and recomputes it. It writes nothing.
func (s *Store) replayLegacy(ctx context.Context, tx pgx.Tx, digest []byte, jti contracts.SecretString) (legacyReplay, error) {
	var sessionID string
	err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT c.session_id::text FROM %[1]s.refresh_credentials c WHERE c.legacy_jti_hash = $1`, s.q), digest).Scan(&sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return legacyReplay{}, nil
	}
	if err != nil {
		return legacyReplay{}, err
	}
	locked, err := s.lockSession(ctx, tx, sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return legacyReplay{}, nil
	}
	if err != nil {
		return legacyReplay{}, err
	}
	var credentialID string
	var storedHash []byte
	var live bool
	var expires time.Time
	err = tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT c.id::text, c.token_hash,
		       c.consumed_at IS NULL AND c.revoked_at IS NULL AND c.expires_at > clock_timestamp(), c.expires_at
		FROM %[1]s.refresh_credentials c WHERE c.legacy_jti_hash = $1 AND c.session_id = $2`, s.q), digest, sessionID).Scan(&credentialID, &storedHash, &live, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return legacyReplay{}, nil
	}
	if err != nil {
		return legacyReplay{}, err
	}
	if !live || locked.RevokedAt != nil || locked.expired || locked.LegacyTokenVersion == nil {
		return legacyReplay{}, nil
	}
	if locked.principalDisabled {
		return legacyReplay{deactivated: true}, nil
	}
	id, err := uuid.Parse(credentialID)
	if err != nil {
		return legacyReplay{}, err
	}
	credential := deriveSuccessor(s.key, id, jti)
	if !equalHash(hashCredential(credential), storedHash) {
		return legacyReplay{}, ErrGraceUnavailable
	}
	return legacyReplay{issued: &Issued{Session: locked.Record, Credential: credential, ExpiresAt: expires}}, nil
}

func sameOrganization(a, b *uuid.UUID) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// legacyFamilySessions selects, under their locks, the live sessions whose
// credentials were exchanged from a jti of go-api family $1.
const legacyFamilySessions = `
	SELECT DISTINCT c.session_id::text
	FROM %[1]s.refresh_credentials c
	JOIN public.refresh_tokens r ON c.legacy_jti_hash = decode(r.token_hash, 'hex')
	WHERE r.family_id = $1`

// revokeLegacyFamilyRows revokes the live rows of go-api family $1, as go-api's
// revoke_family does.
const revokeLegacyFamilyRows = `UPDATE public.refresh_tokens SET revoked_at = clock_timestamp()
	WHERE family_id = $1 AND revoked_at IS NULL`

// revokeLegacyFamily revokes go-api family familyID and the sessions exchanged
// from it. With a session to revoke both commit in one audit.Commit
// transaction with the audit row; without one only go-api's rows change, which
// is no auth-schema state change and has no audit row.
func (s *Store) revokeLegacyFamily(ctx context.Context, principalID uuid.UUID, familyID uuid.UUID) (Result, error) {
	principal := principalID.String()
	var revoked []string
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, err := tx.Exec(ctx, revokeLegacyFamilyRows, familyID); err != nil {
				return err
			}
			rows, err := tx.Query(ctx, fmt.Sprintf(legacyFamilySessions, s.q), familyID)
			if err != nil {
				return err
			}
			var ids []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					rows.Close()
					return err
				}
				ids = append(ids, id)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
			for _, id := range ids {
				locked, err := s.lockSession(ctx, tx, id)
				if err != nil {
					return err
				}
				if locked.RevokedAt == nil {
					revoked = append(revoked, id)
				}
			}
			if len(revoked) == 0 {
				return nil
			}
			return s.revokeSessions(ctx, tx, revoked, revokedByReuse)
		},
		Audit: audit.AuditEvent{
			EventType:          "auth.session.refresh_reuse_revoked",
			Outcome:            audit.OutcomeDenied,
			SubjectPrincipalID: &principal,
			RequestID:          requestid.Audit(ctx),
			Attributes: map[string]any{
				"reason_code": "session_revoked", "target_kind": "principal", "target_id": principal,
				"credential_class": "refresh_credential",
			},
		},
		OffStream: audit.OffStreamSessionLifecycle,
	})
	switch {
	case err == nil:
		s.revocations.Add(ctx, int64(len(revoked)), metric.WithAttributes(attribute.String("reason", revokedByReuse)))
		s.logger.WarnContext(ctx, "legacy refresh token reuse: go-api family and its exchanged sessions revoked",
			"family_id", familyID.String(), "sessions", len(revoked), "reason_code", string(ReasonRefreshReused))
		return Result{Reason: ReasonRefreshReused}, nil
	case errors.Is(err, audit.ErrNoStateChange):
		if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, revokeLegacyFamilyRows, familyID)
			return err
		}); err != nil {
			return Result{Reason: ReasonStoreUnavailable}, fmt.Errorf("%w: revoking a reused go-api family: %w", ErrStore, err)
		}
		s.logger.WarnContext(ctx, "legacy refresh token reuse: go-api family revoked",
			"family_id", familyID.String(), "reason_code", string(ReasonRefreshReused))
		return Result{Reason: ReasonRefreshReused}, nil
	default:
		return Result{Reason: ReasonStoreUnavailable}, fmt.Errorf("%w: revoking a reused go-api family: %w", ErrStore, err)
	}
}
