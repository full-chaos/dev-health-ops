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
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/projectinventory"
	"github.com/full-chaos/dev-health-ops/internal/repoinventory"
)

// askDevFeatureKey is the second entitlement the principal access read serves.
const askDevFeatureKey = "ask_dev"

// inventoryOverLimit names an access read refused because an inventory is
// above its limit: the 503's code, the counter's outcome, the inventory and
// its limit (names and counts only).
type inventoryOverLimit struct {
	code, outcome, inventory string
	limit                    int
}

// inventoryOverLimitOf reports whether err is an inventory over its limit
// (repoinventory or projectinventory ErrOverLimit anywhere in its chain). Any
// other error, nil included, is false and stays the code-less 503.
func inventoryOverLimitOf(err error) (inventoryOverLimit, bool) {
	switch {
	case errors.Is(err, repoinventory.ErrOverLimit):
		return inventoryOverLimit{RepositoryScopeOverLimitCode, outcomeReposOverLimit, "repository", repoinventory.Limit}, true
	case errors.Is(err, projectinventory.ErrOverLimit):
		return inventoryOverLimit{ProjectScopeOverLimitCode, outcomeProjectsOverLimit, "project", projectinventory.Limit}, true
	}
	return inventoryOverLimit{}, false
}

// AccessReads is what the principal access read needs from go-api's policy
// store: the users row, the membership and the admin-keyed impersonation.
// policy.PGStore implements it with go-api's existing grants. The session the
// sid names is never read here: auth-service answers it (SessionStates).
type AccessReads interface {
	UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error)
	Membership(ctx context.Context, userID, orgID uuid.UUID) (string, bool, error)
	ActiveImpersonation(ctx context.Context, adminID uuid.UUID) (*policy.Impersonation, error)
}

// SessionStates reads one session's state from auth-service's session state
// route (sessionissue.Client, on go-api's session issuer credential): owner,
// organization, effective revoke and effective expiry, as auth-service decides
// them (acp ruling D6559 on CHAOS-12122). sessionissue.ErrSessionNotFound is a
// session it does not hold; every other error is a failed read.
type SessionStates interface {
	State(ctx context.Context, sessionID uuid.UUID) (sessionissue.State, error)
}

// FeatureDecider decides one org feature; licensing.PostgresStore implements it.
type FeatureDecider interface {
	Decide(ctx context.Context, orgID, featureKey string) (licensing.Decision, error)
}

// OrganizationLookup reports whether an organizations row exists.
type OrganizationLookup interface {
	OrganizationExists(ctx context.Context, orgID uuid.UUID) (bool, error)
}

// ProjectInventory reads an org's project inventory (projectinventory): the
// (provider, id) pairs of its projects rows, sorted. An error is a failed or
// over-limit read, never an empty inventory.
type ProjectInventory interface {
	Projects(ctx context.Context, orgID uuid.UUID) ([]projectinventory.Item, error)
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
	// Sessions answers the session a sid names. Nil: a read with a sid is
	// unavailable (503); a read without one never needs it.
	Sessions SessionStates
	// Projects is the project inventory read behind the project scope flag
	// (ACR_ACCESS_PROJECT_SCOPE, ProjectScopeFlagEnv). Nil, the default, is the
	// flag off: no project read and no project_scope in the answer, so the read
	// is byte-identical to the one before project_scope existed. Set, an org
	// with no repository inventory also gets its project inventory.
	Projects ProjectInventory
}

var _ PrincipalAccessStore = AccessStore{}

// NewPostgresAccessStore is AccessStore over go-api's pool and policy store,
// with inventory as the repository read (nil answers 503 on every call) and
// sessions as auth-service's session state read (nil answers 503 on every read
// with a sid).
func NewPostgresAccessStore(pool *pgxpool.Pool, reads policy.PGStore, inventory RepositoryInventory, sessions SessionStates) AccessStore {
	return AccessStore{
		Organizations: postgresOrganizations{pool: pool},
		Reads:         reads,
		Features:      licensing.PostgresStore{Pool: pool},
		Inventory:     inventory,
		Sessions:      sessions,
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
	var state sessionissue.State
	stateFound := false
	if query.SessionID != nil {
		group.Go(func() error {
			if s.Sessions == nil {
				return errors.New("session state read: no auth-service session state reader configured")
			}
			read, err := s.Sessions.State(groupCtx, *query.SessionID)
			switch {
			case errors.Is(err, sessionissue.ErrSessionNotFound):
				return nil
			case err != nil:
				return fmt.Errorf("session state read: %w", err)
			}
			state, stateFound = read, true
			return nil
		})
	}
	var access PrincipalAccess
	var user policy.UserState
	var userFound bool
	group.Go(func() error {
		var err error
		access, user, userFound, err = s.postgresFacts(groupCtx, query)
		return err
	})
	if err := group.Wait(); err != nil {
		return PrincipalAccess{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if query.SessionID != nil {
		live, reason := sessionLive(query, state, stateFound, user, userFound)
		access.Session = &SessionAccess{Live: live, Reason: reason, Impersonating: access.Impersonating}
	}
	access.Repositories = repositories
	if s.Projects != nil && len(repositories) == 0 {
		// Only an org with no repository inventory gets a project scope
		// (CHAOS-12224; acr turns it into project_only for a "*" consent grant
		// only, design CHAOS-12107 D2). One more live read, after the others.
		projects, err := s.Projects.Projects(ctx, query.OrgID)
		if err != nil {
			return PrincipalAccess{}, fmt.Errorf("%w: project inventory read: %w", ErrUnavailable, err)
		}
		if projects == nil {
			projects = []projectinventory.Item{}
		}
		access.ProjectScope = &ProjectScope{Projects: projects}
	}
	return access, nil
}

// postgresFacts reads membership, impersonation and both entitlements, and
// returns the users row the session check compares the session's pin with.
func (s AccessStore) postgresFacts(ctx context.Context, query AccessQuery) (PrincipalAccess, policy.UserState, bool, error) {
	user, userFound, err := s.Reads.UserState(ctx, query.UserID)
	if err != nil {
		return PrincipalAccess{}, policy.UserState{}, false, fmt.Errorf("users read: %w", err)
	}
	_, membership, err := s.Reads.Membership(ctx, query.UserID, query.OrgID)
	if err != nil {
		return PrincipalAccess{}, policy.UserState{}, false, fmt.Errorf("memberships read: %w", err)
	}
	impersonation, err := s.Reads.ActiveImpersonation(ctx, query.UserID)
	if err != nil {
		return PrincipalAccess{}, policy.UserState{}, false, fmt.Errorf("impersonation read: %w", err)
	}
	access := PrincipalAccess{Member: userFound && user.IsActive && membership, Impersonating: impersonation != nil}
	orgID := query.OrgID.String()
	askDev, err := s.Features.Decide(ctx, orgID, askDevFeatureKey)
	if err != nil {
		return PrincipalAccess{}, policy.UserState{}, false, fmt.Errorf("%s decision: %w", askDevFeatureKey, err)
	}
	runtime, err := s.Features.Decide(ctx, orgID, agentContextRuntimeFeatureKey)
	if err != nil {
		return PrincipalAccess{}, policy.UserState{}, false, fmt.Errorf("%s decision: %w", agentContextRuntimeFeatureKey, err)
	}
	// CHAOS-3695 (chris 2026-08-10, "ACR = Ask Dev"): agent_context_runtime is
	// ask_dev OR the legacy agent_context_runtime grant. No org loses ACR while
	// the legacy grants retire (CHAOS-12292); an org with ask_dev alone gains it.
	access.AskDev, access.AgentContextRuntime = askDev.Allowed, askDev.Allowed || runtime.Allowed
	return access, user, userFound, nil
}

// Session reasons, logged with the answer (never served).
const (
	sessionReasonLive         = "live"
	sessionReasonNotFound     = "session_not_found"
	sessionReasonOtherUser    = "session_of_another_user"
	sessionReasonOrgMismatch  = "session_org_mismatch"
	sessionReasonRevoked      = "session_revoked"
	sessionReasonExpired      = "session_expired"
	sessionReasonPinMissing   = "session_pin_missing"
	sessionReasonUserUnknown  = "user_unknown"
	sessionReasonVersionStale = "token_version_stale"
)

// sessionLive decides session.live from auth-service's answer for the sid: the
// session exists, belongs to the user, is bound to the requested organization
// (a session bound to no organization is not, D6559), is not revoked (the
// effective revoke, a password change included) and not expired (the effective
// expiry, absolute or idle), and its token_version pin equals the user's
// current users.token_version.
func sessionLive(query AccessQuery, state sessionissue.State, found bool, user policy.UserState, userFound bool) (bool, string) {
	switch {
	case !found:
		return false, sessionReasonNotFound
	case state.PrincipalID != query.UserID:
		return false, sessionReasonOtherUser
	case state.OrganizationID == nil || *state.OrganizationID != query.OrgID:
		return false, sessionReasonOrgMismatch
	case state.Revoked:
		return false, sessionReasonRevoked
	case state.Expired:
		return false, sessionReasonExpired
	case state.LegacyTokenVersion == nil:
		return false, sessionReasonPinMissing
	case !userFound:
		return false, sessionReasonUserUnknown
	case *state.LegacyTokenVersion != user.TokenVersion:
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

// ClickHouseProjectInventory runs projectinventory's read on go-api's
// ClickHouse login. The login holds SELECT on projects only with the project
// scope flag on (chclickhouse.APIPostureFor), so this reader is built only
// then.
type ClickHouseProjectInventory struct {
	Conn driver.Conn
}

func (c ClickHouseProjectInventory) Projects(ctx context.Context, orgID uuid.UUID) ([]projectinventory.Item, error) {
	if c.Conn == nil {
		return nil, errors.New("no ClickHouse connection configured")
	}
	statement := projectinventory.Statement(fmt.Sprintf("SETTINGS max_execution_time = %d", int(inventoryTimeout.Seconds())))
	rows, err := c.Conn.Query(ctx, statement,
		clickhouse.Named("org_id", orgID.String()), clickhouse.Named("limit_n", uint32(projectinventory.QueryLimit)))
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var read []projectinventory.Item
	for rows.Next() {
		var row projectinventory.Item
		if err := rows.Scan(&row.Provider, &row.ID); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		read = append(read, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return projectinventory.Canonical(read)
}
