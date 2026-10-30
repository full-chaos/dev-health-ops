package acr

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/repoinventory"
)

// askDevFeatureKey is the second entitlement the principal access read serves.
const askDevFeatureKey = "ask_dev"

// AccessReads is what the principal access read needs from go-api's policy
// store: the users row, the membership, the admin-keyed impersonation and the
// auth-service session the sid names. policy.PGStore implements it with
// go-api's existing grants (the session read is the session reader's
// sessions(id, principal_id, revoked_at, legacy_token_version)).
type AccessReads interface {
	UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error)
	Membership(ctx context.Context, userID, orgID uuid.UUID) (string, bool, error)
	ActiveImpersonation(ctx context.Context, adminID uuid.UUID) (*policy.Impersonation, error)
	SessionState(ctx context.Context, id uuid.UUID) (policy.SessionRow, bool, error)
}

// FeatureDecider decides one org feature; licensing.PostgresStore implements it.
type FeatureDecider interface {
	Decide(ctx context.Context, orgID, featureKey string) (licensing.Decision, error)
}

// OrganizationLookup reports whether an organizations row exists.
type OrganizationLookup interface {
	OrganizationExists(ctx context.Context, orgID uuid.UUID) (bool, error)
}

// RepositoryInventory reads an org's repository inventory (repoinventory).
// An error is a failed read, never an empty inventory.
type RepositoryInventory interface {
	Repositories(ctx context.Context, orgID uuid.UUID) ([]string, error)
}

// AccessStore is the production PrincipalAccessStore. Every read runs on every
// call: there is no cache (check 18). The ClickHouse inventory read runs
// beside the Postgres reads; any failed read fails the whole answer
// (ErrUnavailable), so no field is ever served from a partial read.
type AccessStore struct {
	Organizations OrganizationLookup
	Reads         AccessReads
	Features      FeatureDecider
	Inventory     RepositoryInventory
}

var _ PrincipalAccessStore = AccessStore{}

// NewPostgresAccessStore is AccessStore over go-api's pool and policy store,
// with inventory as the repository read (nil answers 503 on every call).
func NewPostgresAccessStore(pool *pgxpool.Pool, reads policy.PGStore, inventory RepositoryInventory) AccessStore {
	return AccessStore{
		Organizations: postgresOrganizations{pool: pool},
		Reads:         reads,
		Features:      licensing.PostgresStore{Pool: pool},
		Inventory:     inventory,
	}
}

func (s AccessStore) Access(ctx context.Context, query AccessQuery) (PrincipalAccess, error) {
	if s.Organizations == nil || s.Reads == nil || s.Features == nil || s.Inventory == nil {
		return PrincipalAccess{}, fmt.Errorf("%w: principal access store not fully configured", ErrUnavailable)
	}
	found, err := s.Organizations.OrganizationExists(ctx, query.OrgID)
	if err != nil {
		return PrincipalAccess{}, fmt.Errorf("%w: organization read: %w", ErrUnavailable, err)
	}
	if !found {
		return PrincipalAccess{}, ErrOrgUnknown
	}
	group, groupCtx := errgroup.WithContext(ctx)
	var repositories []string
	group.Go(func() error {
		repos, err := s.Inventory.Repositories(groupCtx, query.OrgID)
		if err != nil {
			return fmt.Errorf("repository inventory read: %w", err)
		}
		repositories = repos
		return nil
	})
	var access PrincipalAccess
	group.Go(func() error {
		var err error
		access, err = s.postgresFacts(groupCtx, query)
		return err
	})
	if err := group.Wait(); err != nil {
		return PrincipalAccess{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	access.Repositories = repositories
	return access, nil
}

// postgresFacts reads membership, session, impersonation and both entitlements.
func (s AccessStore) postgresFacts(ctx context.Context, query AccessQuery) (PrincipalAccess, error) {
	user, userFound, err := s.Reads.UserState(ctx, query.UserID)
	if err != nil {
		return PrincipalAccess{}, fmt.Errorf("users read: %w", err)
	}
	_, membership, err := s.Reads.Membership(ctx, query.UserID, query.OrgID)
	if err != nil {
		return PrincipalAccess{}, fmt.Errorf("memberships read: %w", err)
	}
	impersonation, err := s.Reads.ActiveImpersonation(ctx, query.UserID)
	if err != nil {
		return PrincipalAccess{}, fmt.Errorf("impersonation read: %w", err)
	}
	access := PrincipalAccess{Member: userFound && user.IsActive && membership, Impersonating: impersonation != nil}
	if query.SessionID != nil {
		row, sessionFound, err := s.Reads.SessionState(ctx, *query.SessionID)
		if err != nil {
			return PrincipalAccess{}, fmt.Errorf("session read: %w", err)
		}
		live, reason := sessionLive(query.UserID, row, sessionFound, user, userFound)
		access.Session = &SessionAccess{Live: live, Reason: reason, Impersonating: access.Impersonating}
	}
	orgID := query.OrgID.String()
	askDev, err := s.Features.Decide(ctx, orgID, askDevFeatureKey)
	if err != nil {
		return PrincipalAccess{}, fmt.Errorf("%s decision: %w", askDevFeatureKey, err)
	}
	runtime, err := s.Features.Decide(ctx, orgID, agentContextRuntimeFeatureKey)
	if err != nil {
		return PrincipalAccess{}, fmt.Errorf("%s decision: %w", agentContextRuntimeFeatureKey, err)
	}
	access.AskDev, access.AgentContextRuntime = askDev.Allowed, runtime.Allowed
	return access, nil
}

// Session reasons, logged with the answer (never served).
const (
	sessionReasonLive         = "live"
	sessionReasonNotFound     = "session_not_found"
	sessionReasonOtherUser    = "session_of_another_user"
	sessionReasonRevoked      = "session_revoked"
	sessionReasonPinMissing   = "session_pin_missing"
	sessionReasonUserUnknown  = "user_unknown"
	sessionReasonVersionStale = "token_version_stale"
)

// sessionLive is policy's session check (internal/api/policy/session.go) for a
// named user rather than a token subject: the session exists, belongs to the
// user, is not revoked, and its token_version pin equals the user's current
// users.token_version. A password change (which adds 1 to token_version) or
// any other token_version bump therefore ends it.
func sessionLive(user uuid.UUID, row policy.SessionRow, found bool, state policy.UserState, userFound bool) (bool, string) {
	switch {
	case !found:
		return false, sessionReasonNotFound
	case row.PrincipalID != user:
		return false, sessionReasonOtherUser
	case row.Revoked:
		return false, sessionReasonRevoked
	case row.LegacyTokenVersion == nil:
		return false, sessionReasonPinMissing
	case !userFound:
		return false, sessionReasonUserUnknown
	case *row.LegacyTokenVersion != state.TokenVersion:
		return false, sessionReasonVersionStale
	}
	return true, sessionReasonLive
}

type postgresOrganizations struct{ pool *pgxpool.Pool }

func (o postgresOrganizations) OrganizationExists(ctx context.Context, orgID uuid.UUID) (bool, error) {
	if o.pool == nil {
		return false, errors.New("no pool")
	}
	return organizationExists(ctx, o.pool, orgID.String())
}

// inventoryTimeout is the max_execution_time of the inventory read.
const inventoryTimeout = 5 * time.Second

// ClickHouseRepositoryInventory runs repoinventory's read on go-api's
// ClickHouse login (chclickhouse.APIPosture grants SELECT on repos for it).
type ClickHouseRepositoryInventory struct {
	Conn driver.Conn
}

func (c ClickHouseRepositoryInventory) Repositories(ctx context.Context, orgID uuid.UUID) ([]string, error) {
	if c.Conn == nil {
		return nil, errors.New("no ClickHouse connection configured")
	}
	statement := repoinventory.Statement(fmt.Sprintf("SETTINGS max_execution_time = %d", int(inventoryTimeout.Seconds())))
	rows, err := c.Conn.Query(ctx, statement,
		clickhouse.Named("org_id", orgID.String()), clickhouse.Named("limit_n", uint32(repoinventory.QueryLimit)))
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var read []repoinventory.Row
	for rows.Next() {
		var row repoinventory.Row
		if err := rows.Scan(&row.Value, &row.Count); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		read = append(read, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	items, err := repoinventory.Canonical(read)
	if err != nil {
		return nil, err
	}
	slugs := make([]string, 0, len(items))
	for _, item := range items {
		slugs = append(slugs, item.Slug)
	}
	return slugs, nil
}
