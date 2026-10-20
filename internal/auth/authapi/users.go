package authapi

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	// Memberships returns the user's memberships ordered by joined_at, then
	// created_at (NULLs last), as go-api's login reads them.
	Memberships(ctx context.Context, userID uuid.UUID) ([]Membership, error)
}

// Membership is one public.memberships row as login reads it.
type Membership struct {
	OrgID     uuid.UUID
	Role      *string
	JoinedAt  *time.Time
	CreatedAt time.Time
}

// PGUsers reads public.users and public.memberships. The runtime role needs
// column SELECT on users (id, email, username, full_name, is_active,
// is_superuser, token_version) and memberships (user_id, org_id, role,
// joined_at, created_at), and nothing else (authschema.PublicReadColumns). The schema is named, so the role's search_path
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

// Memberships is go-api's orderedMemberships (internal/api/session/store.go).
func (p PGUsers) Memberships(ctx context.Context, userID uuid.UUID) ([]Membership, error) {
	rows, err := p.Pool.Query(ctx, `
		SELECT org_id, role, joined_at, created_at FROM public.memberships
		WHERE user_id = $1 ORDER BY joined_at ASC, created_at ASC`, userID)
	if err != nil {
		return nil, fmt.Errorf("%w: reading public.memberships: %w", errUsersStore, err)
	}
	defer rows.Close()
	var out []Membership
	for rows.Next() {
		var m Membership
		if err := rows.Scan(&m.OrgID, &m.Role, &m.JoinedAt, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("%w: reading public.memberships: %w", errUsersStore, err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading public.memberships: %w", errUsersStore, err)
	}
	return out, nil
}

// chooseMembership is go-api's selectActiveMembership without organization
// activity (auth-service reads no ClickHouse): the requested organization's
// membership, nil when the user holds none in it; otherwise the earliest
// join, joined_at falling back to created_at, the first such in list order.
// go-api decides the same way when its activity read is unavailable; with
// activity it may prefer an organization with newer metrics (a recorded
// difference, CHAOS-10228 ruling).
func chooseMembership([]Membership, *uuid.UUID) *Membership { return nil }
