package authapi

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UserState is the live public.users state an access token is minted from.
type UserState struct {
	Email        string
	Username     *string
	FullName     *string
	IsActive     bool
	IsSuperuser  bool
	TokenVersion int64
}

// Users reads the go-api-owned user and membership rows. Until the users
// cutover (D3) go-api is their only writer and auth-service reads them live.
type Users interface {
	// State returns the user's row; found is false for an unknown id.
	State(ctx context.Context, userID uuid.UUID) (state UserState, found bool, err error)
	// Role returns the membership role in org; found is false without one.
	// A NULL role is found with a nil role.
	Role(ctx context.Context, userID, orgID uuid.UUID) (role *string, found bool, err error)
}

// PGUsers reads public.users and public.memberships. The runtime role needs
// column SELECT on users (id, email, username, full_name, is_active,
// is_superuser, token_version) and memberships (user_id, org_id, role), and
// nothing else (CHAOS-10224). The schema is named, so the role's search_path
// does not decide which table is read.
type PGUsers struct{ Pool *pgxpool.Pool }

// errMultipleRows is a lookup that must match at most one row matching more.
var errMultipleRows = errors.New("authapi: more than one row where one or none was expected")

// errUsersStore wraps every failed read of the go-api tables.
var errUsersStore = errors.New("authapi: users store unavailable")

// State reads the user as go-api's policy reader does: a NULL is_active or
// is_superuser reads false and a NULL token_version reads 0
// (internal/api/policy/store.go UserState).
func (p PGUsers) State(ctx context.Context, userID uuid.UUID) (UserState, bool, error) {
	var s UserState
	err := p.Pool.QueryRow(ctx, `
		SELECT email, username, full_name, COALESCE(is_active, false), COALESCE(is_superuser, false),
		       COALESCE(token_version, 0)
		FROM public.users WHERE id = $1`, userID).
		Scan(&s.Email, &s.Username, &s.FullName, &s.IsActive, &s.IsSuperuser, &s.TokenVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return UserState{}, false, nil
	}
	if err != nil {
		return UserState{}, false, fmt.Errorf("%w: reading public.users: %w", errUsersStore, err)
	}
	return s, true, nil
}

// Role is go-api's membershipRole (internal/api/session/store.go): more than
// one row is an error, as SQLAlchemy's one_or_none raises.
func (p PGUsers) Role(ctx context.Context, userID, orgID uuid.UUID) (*string, bool, error) {
	rows, err := p.Pool.Query(ctx,
		`SELECT role FROM public.memberships WHERE user_id = $1 AND org_id = $2`, userID, orgID)
	if err != nil {
		return nil, false, fmt.Errorf("%w: reading public.memberships: %w", errUsersStore, err)
	}
	defer rows.Close()
	var (
		role  *string
		found bool
	)
	for rows.Next() {
		if found {
			return nil, false, errMultipleRows
		}
		if err := rows.Scan(&role); err != nil {
			return nil, false, fmt.Errorf("%w: reading public.memberships: %w", errUsersStore, err)
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("%w: reading public.memberships: %w", errUsersStore, err)
	}
	return role, found, nil
}
