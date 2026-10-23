package authapi

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
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
type PGUsers struct {
	Pool *pgxpool.Pool
	// UsersOwned is the users-cutover mode: the active memberships of Schema
	// (the auth schema) are read as well as public.memberships, and an auth row
	// wins on role. Off, only public.memberships is read.
	UsersOwned bool
	Schema     authschema.ValidatedIdentifier
}

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
	if p.UsersOwned {
		var role *string
		err := p.Pool.QueryRow(ctx, `SELECT role FROM `+authschema.Quote(p.Schema)+`.memberships
WHERE principal_id = $1 AND organization_id = $2 AND status = 'active'`, userID, orgID).Scan(&role)
		switch {
		case err == nil:
			return role, true, nil
		case !errors.Is(err, pgx.ErrNoRows):
			return nil, false, fmt.Errorf("%w: reading auth memberships: %w", errUsersStore, err)
		}
	}
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
	public, err := p.publicMemberships(ctx, userID)
	if err != nil || !p.UsersOwned {
		return public, err
	}
	rows, err := p.Pool.Query(ctx, `SELECT organization_id, role, joined_at, created_at FROM `+authschema.Quote(p.Schema)+`.memberships
WHERE principal_id = $1 AND status = 'active'`, userID)
	if err != nil {
		return nil, fmt.Errorf("%w: reading auth memberships: %w", errUsersStore, err)
	}
	defer rows.Close()
	byOrg := map[uuid.UUID]Membership{}
	for _, m := range public {
		byOrg[m.OrgID] = m
	}
	for rows.Next() {
		var m Membership
		if err := rows.Scan(&m.OrgID, &m.Role, &m.JoinedAt, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("%w: reading auth memberships: %w", errUsersStore, err)
		}
		byOrg[m.OrgID] = m
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading auth memberships: %w", errUsersStore, err)
	}
	out := make([]Membership, 0, len(byOrg))
	for _, m := range byOrg {
		out = append(out, m)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		switch {
		case a.JoinedAt != nil && b.JoinedAt == nil:
			return true
		case a.JoinedAt == nil && b.JoinedAt != nil:
			return false
		case a.JoinedAt != nil && !a.JoinedAt.Equal(*b.JoinedAt):
			return a.JoinedAt.Before(*b.JoinedAt)
		case !a.CreatedAt.Equal(b.CreatedAt):
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.OrgID.String() < b.OrgID.String()
	})
	return out, nil
}

func (p PGUsers) publicMemberships(ctx context.Context, userID uuid.UUID) ([]Membership, error) {
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
func chooseMembership(memberships []Membership, requested *uuid.UUID) *Membership {
	if requested != nil {
		for index := range memberships {
			if memberships[index].OrgID == *requested {
				return &memberships[index]
			}
		}
		return nil
	}
	var best *Membership
	var bestJoined time.Time
	for index := range memberships {
		joined := memberships[index].CreatedAt
		if memberships[index].JoinedAt != nil {
			joined = *memberships[index].JoinedAt
		}
		if best == nil || joined.Before(bestJoined) {
			best, bestJoined = &memberships[index], joined
		}
	}
	return best
}
