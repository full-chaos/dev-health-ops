package authschema

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrOwnerMove reports an owner move this package refuses or could not finish.
var ErrOwnerMove = errors.New("auth owner move")

// ErrSchemaOwnerPin reports an auth schema whose objects are not all owned by the pinned owner role.
var ErrSchemaOwnerPin = errors.New("auth schema owner pin")

// PublicPrivilege is one privilege on one ops-schema table that the auth migrator holds WITH GRANT OPTION.
// Columns is nil for a table-level privilege.
type PublicPrivilege struct {
	Table     string
	Privilege string
	Columns   []string
}

// PublicPrivilegeSet is everything the grants pass may give the runtime role in the ops schema, under every switch.
func PublicPrivilegeSet() []PublicPrivilege { return nil }

// OwnerMoveOptions configures one OwnerMove run.
type OwnerMoveOptions struct {
	Schema      string
	From, To    string
	RuntimeRole string
	Apply       bool
	GrantsOnly  bool
	LockTimeout time.Duration

	hookBeforeCommit func(context.Context)
	hookAfterCommit  func(context.Context)
}

// OwnerMoveReport counts what a run changed (or, without Apply, would change). It carries counts only.
type OwnerMoveReport struct {
	Applied        bool
	Moved          map[string]int
	Followed       map[string]int
	PublicTables   int
	GrantRowsMoved int
}

// Validate checks the options without touching the database.
func (o OwnerMoveOptions) Validate() error { return nil }

// OwnerMove moves the auth schema, and every object in it owned by From, to To, and makes To the grantor of the
// runtime role's privileges in the ops schema, in one transaction.
func OwnerMove(ctx context.Context, pool *pgxpool.Pool, options OwnerMoveOptions) (OwnerMoveReport, error) {
	return OwnerMoveReport{}, errors.New("not implemented")
}
