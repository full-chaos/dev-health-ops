// Package aclguard serialises every writer of table privileges on the shared Postgres database. A GRANT or REVOKE
// rewrites the table's pg_class row; two transactions that do it to one table at once fail the second with SQLSTATE
// XX000 "tuple concurrently updated" even when they name different roles. Each writer takes Lock as the first statement
// of its transaction, after any lock of its own, so the writers queue instead of failing.
package aclguard

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// WriterLockKey is the transaction-scoped advisory lock key of the privilege writers ("ACLW" + 1). It differs from the
// session keys of auth-migrate and river-migrate: those serialise a tool's own runs, this one serialises the tools.
const WriterLockKey int64 = 0x4143_4C57_0001

// Lock blocks until no other transaction holds the privilege-writer lock, then holds it until tx ends.
func Lock(ctx context.Context, tx pgx.Tx) error {
	if tx == nil {
		return errors.New("aclguard: nil transaction")
	}
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, WriterLockKey)
	return err
}
