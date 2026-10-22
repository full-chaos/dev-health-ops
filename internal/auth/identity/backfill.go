package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// BackfillTarget is the state of one public.users row that the users backfill
// writes into auth.principals, auth.users and auth.external_identities.
type BackfillTarget struct {
	ID          uuid.UUID
	Email       string
	DisplayName string
	Active      bool
	Verified    bool
	// PasswordHash is a bcrypt verifier, or nil for a user without a local
	// credential. The caller has already refused any other format.
	PasswordHash *string
	// Identity is the external identity of a social-provider user, or nil.
	Identity *ExternalKey
}

// BackfillResult says what one BackfillUser call wrote (or, in a dry run,
// would write).
type BackfillResult struct {
	Inserted       bool
	Updated        bool
	IdentityLinked bool
	// DisplayNameDrift is set when only the display name differs: a rename
	// has no provisioning event, so it cannot commit alone.
	DisplayNameDrift bool
}

type backfillState struct {
	kind        string
	disabled    bool
	displayName string
	hasUser     bool
	email       string
	emailLower  string
	verified    bool
	hash        *string
	algorithm   *string
}

// BackfillUser makes the auth rows of one user agree with t. A missing user is
// inserted; an existing one changes only the fields that differ, each commit
// through audit.Commit; an equal user writes nothing. With dryRun set it reads
// and reports, and writes nothing.
func (s *Store) BackfillUser(ctx context.Context, t BackfillTarget, dryRun bool) (BackfillResult, error) {
	var res BackfillResult
	st, found, err := s.readBackfillState(ctx, t.ID)
	if err != nil {
		return res, err
	}
	src := source{email: t.Email, emailLower: storedLoginKey(t.Email), active: t.Active, verified: t.Verified}
	if !found {
		res.Inserted = true
		if !dryRun {
			if err := s.insertBackfill(ctx, t, src); err != nil {
				if !errors.Is(err, errPrincipalExists) {
					return BackfillResult{}, err
				}
				return s.BackfillUser(ctx, t, dryRun)
			}
		}
	} else {
		if st.kind != "user" || !st.hasUser {
			return res, ErrPrincipalKind
		}
		cur := projected{kind: st.kind, email: st.email, emailLower: st.emailLower, disabled: st.disabled, verified: st.verified}
		if changed := changedFields(src, cur); len(changed) > 0 {
			res.Updated = true
			if !dryRun {
				if err := s.applyProjectionChange(ctx, t.ID, src, cur, changed, nil); err != nil {
					return BackfillResult{}, err
				}
			}
		}
		hashDiffers := differsString(st.hash, t.PasswordHash)
		nameDiffers := st.displayName != t.DisplayName
		switch {
		case hashDiffers:
			res.Updated = true
			if !dryRun {
				if err := s.applyBackfillCredentials(ctx, t); err != nil {
					return BackfillResult{}, err
				}
			}
		case nameDiffers:
			res.DisplayNameDrift = true
		}
	}
	if t.Identity != nil {
		linked, err := s.backfillIdentity(ctx, t, dryRun)
		if err != nil {
			return res, err
		}
		res.IdentityLinked = linked
	}
	return res, nil
}

func differsString(a, b *string) bool {
	if a == nil || b == nil {
		return a != b
	}
	return *a != *b
}

func (s *Store) readBackfillState(ctx context.Context, id uuid.UUID) (backfillState, bool, error) {
	var st backfillState
	var email, emailLower *string
	var verified *bool
	err := s.pool.QueryRow(ctx, `SELECT p.kind, p.disabled_at IS NOT NULL, p.display_name,
       u.email, u.email_lower, u.email_verified_at IS NOT NULL, u.password_hash, u.password_algorithm
FROM `+s.q+`.principals p LEFT JOIN `+s.q+`.users u ON u.principal_id = p.id
WHERE p.id = $1`, id).Scan(&st.kind, &st.disabled, &st.displayName, &email, &emailLower, &verified, &st.hash, &st.algorithm)
	if errors.Is(err, pgx.ErrNoRows) {
		return st, false, nil
	}
	if err != nil {
		return st, false, s.storeErr(ctx, "read backfill state", err)
	}
	if email != nil && emailLower != nil && verified != nil {
		st.hasUser, st.email, st.emailLower, st.verified = true, *email, *emailLower, *verified
	}
	return st, true, nil
}

func (s *Store) insertBackfill(ctx context.Context, t BackfillTarget, src source) error {
	principal := t.ID.String()
	events := []audit.OutboxEvent{principalEvent(principal, "principal.created", t.Active), userEvent(principal, "user.created")}
	if t.Verified {
		events = append(events, userEvent(principal, "user.email_verified"))
	}
	var algorithm *string
	if t.PasswordHash != nil {
		a := passwordAlgorithm
		algorithm = &a
	}
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var inserted uuid.UUID
			err := tx.QueryRow(ctx, `INSERT INTO `+s.q+`.principals (id, kind, display_name, disabled_at)
VALUES ($1, 'user', $2, CASE WHEN $3::boolean THEN NULL ELSE clock_timestamp() END)
ON CONFLICT (id) DO NOTHING
RETURNING id`, t.ID, t.DisplayName, t.Active).Scan(&inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				return errPrincipalExists
			}
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `INSERT INTO `+s.q+`.users
	(principal_id, email, email_lower, email_verified_at, password_hash, password_algorithm)
VALUES ($1, $2, $3, CASE WHEN $4::boolean THEN clock_timestamp() ELSE NULL END, $5, $6)`,
				t.ID, src.email, src.emailLower, t.Verified, t.PasswordHash, algorithm)
			return err
		},
		Audit: audit.AuditEvent{
			EventType: "auth.principal.projected", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &principal,
			Attributes: map[string]any{"reason_code": "principal_projected", "target_kind": "principal", "target_id": principal},
		},
		Events: events,
	})
	switch {
	case err == nil, errors.Is(err, errPrincipalExists):
		return err
	case isEmailCollision(err):
		return ErrEmailCollision
	}
	return s.storeErr(ctx, "insert backfill", err)
}

// applyBackfillCredentials copies the verifier (and the display name with it)
// onto an existing auth.users row in one audit.Commit.
func (s *Store) applyBackfillCredentials(ctx context.Context, t BackfillTarget) error {
	principal := t.ID.String()
	var algorithm *string
	if t.PasswordHash != nil {
		a := passwordAlgorithm
		algorithm = &a
	}
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := s.lockUser(ctx, tx, t.ID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.users
SET password_hash = $2, password_algorithm = $3, updated_at = clock_timestamp()
WHERE principal_id = $1 AND password_hash IS DISTINCT FROM $2::text`, t.ID, t.PasswordHash, algorithm); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE `+s.q+`.principals SET display_name = $2, updated_at = clock_timestamp()
WHERE id = $1 AND display_name IS DISTINCT FROM $2`, t.ID, t.DisplayName)
			return err
		},
		Audit:  localAudit("auth.principal.projection_updated", "principal_projected", principal, Actor{}),
		Events: []audit.OutboxEvent{userEvent(principal, "user.credentials_changed")},
	})
	if err == nil || errors.Is(err, audit.ErrNoStateChange) {
		return nil
	}
	if errors.Is(err, ErrUserNotFound) {
		return ErrUserNotFound
	}
	return s.storeErr(ctx, "update backfill credentials", err)
}

// backfillIdentity links t.Identity to the principal unless the principal
// already holds it. It reports whether a link was (or, in a dry run, would
// be) written.
func (s *Store) backfillIdentity(ctx context.Context, t BackfillTarget, dryRun bool) (bool, error) {
	key := *t.Identity
	var holder uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT principal_id FROM `+s.q+`.external_identities
WHERE issuer = $1 AND provider = $2 AND subject = $3`, key.Issuer, string(key.Provider), key.Subject).Scan(&holder)
	switch {
	case err == nil && holder == t.ID:
		return false, nil
	case err == nil:
		return false, ErrIdentityConflict
	case !errors.Is(err, pgx.ErrNoRows):
		return false, s.storeErr(ctx, "read backfill identity", err)
	}
	if dryRun {
		return true, nil
	}
	if err := s.projectIdentity(ctx, key, t.ID); err != nil {
		return false, err
	}
	return true, nil
}
