package admin

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// DeleteMode is how a user delete runs (CHAOS-10019, D6051): Soft keeps the user row and turns the login off; Force removes
// the row and every personal field and keeps the audit, billing-audit and refund rows with the user reference set to null.
type DeleteMode string

const (
	DeleteSoft  DeleteMode = "soft"
	DeleteForce DeleteMode = "force"
)

// userRefDecision is what a permanent delete does with one foreign key that references users(id).
type userRefDecision struct {
	// Table is the quoted, schema-qualified name for SQL; Name is the same table as a reader writes it.
	Table  string
	Name   string
	Column string
	// OnDelete is the constraint's action as the catalog spells it: a, r, c, n or d.
	OnDelete string
	Rows     int64
	Action   string
	// Blocked is why a row that references the user could not be kept; it only matters when such a row exists.
	Blocked string
}

// userRefs reads every single-column foreign key that references users(id) from the catalog, so a foreign key added later is
// decided the same way without a code change.
func (s pgStore) userRefs(ctx context.Context) ([]userRefDecision, error) {
	rows, err := s.Pool.Query(ctx, `
SELECT n.nspname, cl.relname, c.conname, c.confdeltype::text, cardinality(c.conkey),
       COALESCE(a.attname, ''), COALESCE(a.attnotnull, false)
FROM pg_constraint c
JOIN pg_class cl ON cl.oid = c.conrelid
JOIN pg_namespace n ON n.oid = cl.relnamespace
LEFT JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
WHERE c.contype = 'f' AND c.confrelid = 'users'::regclass
ORDER BY n.nspname, cl.relname, c.conname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var refs []userRefDecision
	for rows.Next() {
		var schema, table, constraint, onDelete, column string
		var width int
		var notNull bool
		if err := rows.Scan(&schema, &table, &constraint, &onDelete, &width, &column, &notNull); err != nil {
			return nil, err
		}
		ref := userRefDecision{Table: pgx.Identifier{schema, table}.Sanitize(), Name: table, Column: column, OnDelete: onDelete}
		if schema != "public" {
			ref.Name = schema + "." + table
		}
		switch {
		case onDelete == "c":
			ref.Action = "database-cascade"
		case onDelete == "n":
			ref.Action = "database-set-null"
		case width != 1:
			return nil, refuse("Cannot anonymize %s: foreign key %s has %d columns. Nothing was deleted.", ref.Name, constraint, width)
		case onDelete != "a" && onDelete != "r":
			ref.Action = "blocked"
			ref.Blocked = fmt.Sprintf("foreign key %s has the delete action %q", constraint, onDelete)
		case notNull:
			ref.Action = "blocked"
			ref.Blocked = "the column is NOT NULL"
		default:
			ref.Action = "null"
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

// softDeleteResult is what a soft delete found and did. AlreadyDone is true when the user was inactive and held no live token
// (the call still advanced the token version).
type softDeleteResult struct {
	Found, WasActive, AlreadyDone bool
}

// softDeleteUser turns the login off and keeps the row: is_active false, the access tokens fail on token_version, and the
// refresh and one-time tokens are revoked or removed. It does all of this on every call, for an inactive user too (a user
// an operator deactivated first must not keep credentials that work again on reactivation), so it is idempotent.
func (s pgStore) softDeleteUser(ctx context.Context, logger *slog.Logger, id uuid.UUID) (softDeleteResult, error) {
	now := s.now().UTC()
	var wasActive bool
	err := s.Pool.QueryRow(ctx, `SELECT COALESCE(is_active, false) FROM users WHERE id = $1 FOR UPDATE`, id).Scan(&wasActive)
	if err == pgx.ErrNoRows {
		return softDeleteResult{}, nil
	}
	if err != nil {
		return softDeleteResult{}, err
	}
	result := softDeleteResult{Found: true, WasActive: wasActive}
	revoked, err := s.Pool.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = $2 WHERE user_id = $1 AND revoked_at IS NULL`, id, now)
	if err != nil {
		return result, err
	}
	reset, err := s.Pool.Exec(ctx, `DELETE FROM password_reset_tokens WHERE user_id = $1`, id)
	if err != nil {
		return result, err
	}
	verification, err := s.Pool.Exec(ctx, `DELETE FROM email_verification_tokens WHERE user_id = $1`, id)
	if err != nil {
		return result, err
	}
	if _, err = s.Pool.Exec(ctx, `UPDATE users SET is_active = false, token_version = token_version + 1, updated_at = $2 WHERE id = $1`, id, now); err != nil {
		return result, err
	}
	// AlreadyDone only describes the answer: the user was inactive and no token was left to end. The credentials are ended on every call.
	result.AlreadyDone = !wasActive && revoked.RowsAffected()+reset.RowsAffected()+verification.RowsAffected() == 0
	logger.DebugContext(ctx, "admin: user delete (soft): tokens ended", "user_id", id.String(), "was_active", wasActive,
		"refresh_tokens_revoked", revoked.RowsAffected(), "password_reset_tokens_deleted", reset.RowsAffected(),
		"email_verification_tokens_deleted", verification.RowsAffected())
	return result, nil
}

// forceDeleteUser removes the user row. Every foreign key that references the user is decided from the catalog: CASCADE and
// SET NULL are left to the database; a RESTRICT or NO ACTION key (impersonation sessions, billing audit, refunds) has its
// column set to null so the record stays. A key that cannot be nulled fails the whole delete (the caller's transaction rolls
// back and the user stays). found is false when no user has the id.
func (s pgStore) forceDeleteUser(ctx context.Context, logger *slog.Logger, id uuid.UUID) (found bool, decisions []userRefDecision, err error) {
	refs, err := s.userRefs(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "admin: user delete (force) refused", "user_id", id.String(), "mode", string(DeleteForce), "error", err)
		return false, nil, err
	}
	for i := range refs {
		ref := &refs[i]
		if ref.Action == "database-set-null" {
			// the database nulls these rows when the user goes; count them first so the outcome names every kept record
			if err := s.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s = $1`, ref.Table, pgx.Identifier{ref.Column}.Sanitize()), id).Scan(&ref.Rows); err != nil {
				logger.ErrorContext(ctx, "admin: user delete (force) could not count a reference", "user_id", id.String(),
					"table", ref.Name, "column", ref.Column, "error", err)
				return false, nil, fmt.Errorf("count %s.%s: %w", ref.Name, ref.Column, err)
			}
		}
		if ref.Action == "blocked" {
			// only a row that references this user is a problem: a NOT NULL table nobody in it points at this user is left alone
			var referencing int64
			if err := s.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s = $1`, ref.Table, pgx.Identifier{ref.Column}.Sanitize()), id).Scan(&referencing); err != nil {
				logger.ErrorContext(ctx, "admin: user delete (force) could not count a reference", "user_id", id.String(),
					"table", ref.Name, "column", ref.Column, "error", err)
				return false, nil, fmt.Errorf("count %s.%s: %w", ref.Name, ref.Column, err)
			}
			logger.DebugContext(ctx, "admin: user delete (force): reference that cannot be nulled", "user_id", id.String(),
				"table", ref.Name, "column", ref.Column, "reason", ref.Blocked, "referencing_rows", referencing)
			if referencing > 0 {
				err := refuse("Cannot anonymize %s.%s: %s, so %d row(s) that reference the user cannot be kept. Nothing was deleted.", ref.Name, ref.Column, ref.Blocked, referencing)
				logger.ErrorContext(ctx, "admin: user delete (force) refused", "user_id", id.String(), "mode", string(DeleteForce),
					"table", ref.Name, "column", ref.Column, "reason", ref.Blocked, "referencing_rows", referencing)
				return false, nil, err
			}
			continue
		}
		if ref.Action == "null" {
			tag, err := s.Pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET %s = NULL WHERE %s = $1`, ref.Table, pgx.Identifier{ref.Column}.Sanitize(), pgx.Identifier{ref.Column}.Sanitize()), id)
			if err != nil {
				logger.ErrorContext(ctx, "admin: user delete (force) could not anonymize a reference", "user_id", id.String(),
					"table", ref.Name, "column", ref.Column, "error", err)
				return false, nil, fmt.Errorf("anonymize %s.%s: %w", ref.Name, ref.Column, err)
			}
			ref.Rows = tag.RowsAffected()
		}
		logger.DebugContext(ctx, "admin: user delete (force): reference decided", "user_id", id.String(),
			"table", ref.Name, "column", ref.Column, "on_delete", ref.OnDelete, "action", ref.Action, "rows_nulled", ref.Rows)
	}
	tag, err := s.Pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, id)
	if err != nil {
		logger.ErrorContext(ctx, "admin: user delete (force) failed", "user_id", id.String(), "error", err)
		return false, nil, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil, nil
	}
	return true, refs, nil
}

// logSoftDeleted and logForceDeleted write the Info outcome line. They run AFTER the transaction commits (a failed commit
// rolls the delete back, and must leave no success line).
func logSoftDeleted(ctx context.Context, logger *slog.Logger, id uuid.UUID, result softDeleteResult) {
	logger.InfoContext(ctx, "admin: user soft-deleted", "user_id", id.String(), "mode", string(DeleteSoft),
		"was_active", result.WasActive, "already_done", result.AlreadyDone)
}

func logForceDeleted(ctx context.Context, logger *slog.Logger, id uuid.UUID, refs []userRefDecision) {
	var kept int64
	for _, ref := range refs {
		kept += ref.Rows
	}
	logger.InfoContext(ctx, "admin: user permanently deleted", "user_id", id.String(), "mode", string(DeleteForce),
		"references_checked", len(refs), "rows_kept_with_null_user", kept)
}
