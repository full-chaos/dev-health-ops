package acr

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// agentContextRuntimeFeatureKey is the one feature this route decides.
// Python: dev_health_ops.licensing.registry.py, "agent_context_runtime".
const agentContextRuntimeFeatureKey = "agent_context_runtime"

// ErrOrgNotFound means the organization row does not exist. Python:
// api/internal/acr.py:181-192's 404 ("Not found") on both an unparseable
// org_id and a missing Organization row.
var ErrOrgNotFound = errors.New("organization not found")

// ErrUnavailable means the decision could not be READ. Python:
// api/internal/acr.py:196-204's 503 ("Service unavailable") on the
// get_org_entitlements_from_db call failing. Deliberately distinct from
// ErrOrgNotFound and from a closed (Allowed: false) decision -- a decision
// that was correctly evaluated and came back closed is not an error.
var ErrUnavailable = errors.New("agent_context_runtime entitlement could not be evaluated")

// Entitlement is the one fact this package's routes serve.
type Entitlement struct {
	// OrgID echoes the caller's own org_id path value verbatim, exactly as
	// Python's route does (api/internal/acr.py:223 returns the org_id
	// function parameter, not str(org_uuid)) -- acr's client asserts this
	// equals the org_id IT sent (internal/entitlements/response.go:137),
	// which trivially holds by construction on both planes.
	OrgID               string
	AgentContextRuntime bool
}

// EntitlementStore looks up one org's agent_context_runtime entitlement.
type EntitlementStore interface {
	Lookup(ctx context.Context, orgID string) (Entitlement, error)
	// Ready reports whether the store can be read now; the health route
	// answers 503 when it cannot.
	Ready(ctx context.Context) error
}

// PostgresEntitlementStore is the production EntitlementStore. Unlike
// licensing.PostgresStore, which never checks whether the org exists (see
// its own doc comment), this route's contract requires a 404 distinct from
// a closed decision -- api/internal/acr.py:181-192 looks the Organization
// row up explicitly before ever computing entitlements. This type owns
// exactly that one extra fact; the decision itself is licensing.Decide via
// licensing.PostgresStore.
type PostgresEntitlementStore struct {
	Pool *pgxpool.Pool
	// Now is injectable so a test can drive the override-expiry clock. Nil
	// means time.Now.
	Now func() time.Time
}

var _ EntitlementStore = PostgresEntitlementStore{}

func (store PostgresEntitlementStore) Lookup(ctx context.Context, orgID string) (Entitlement, error) {
	// Format validation runs before touching the pool, matching Python's own
	// order: uuid.UUID(org_id) is checked with no query issued for the check
	// itself (api/internal/acr.py:169-180), so a malformed org_id is 404 on
	// both planes even when the store is not configured at all.
	parsedOrgID, err := uuid.Parse(orgID)
	if err != nil {
		return Entitlement{}, ErrOrgNotFound
	}
	if store.Pool == nil {
		return Entitlement{}, ErrUnavailable
	}
	found, err := organizationExists(ctx, store.Pool, parsedOrgID.String())
	if err != nil {
		return Entitlement{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if !found {
		return Entitlement{}, ErrOrgNotFound
	}
	decisionStore := licensing.PostgresStore{Pool: store.Pool, Now: store.Now}
	decision, err := decisionStore.Decide(ctx, parsedOrgID.String(), agentContextRuntimeFeatureKey)
	if err != nil {
		return Entitlement{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return Entitlement{OrgID: orgID, AgentContextRuntime: decision.Allowed}, nil
}

// readyTimeout bounds the health route's ping, so an unreachable store is a
// prompt 503 rather than a hung probe.
const readyTimeout = 2 * time.Second

// Ready makes the entitlement route's decision read (licensing.LoadState,
// which reads feature_flags, org_feature_overrides, org_licenses and
// organizations in one statement) for the nil org id, which matches no org
// row. So it fails when Postgres is unreachable and when any table the
// entitlement route reads cannot be read, as Python's health read fails in
// both cases.
func (store PostgresEntitlementStore) Ready(ctx context.Context) error {
	if store.Pool == nil {
		return ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	decisionStore := licensing.PostgresStore{Pool: store.Pool, Now: store.Now}
	if _, err := decisionStore.Decide(ctx, uuid.Nil.String(), agentContextRuntimeFeatureKey); err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return nil
}

type existsQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func organizationExists(ctx context.Context, queryer existsQueryer, orgID string) (bool, error) {
	var exists bool
	err := queryer.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM organizations WHERE id = $1::uuid)`, orgID,
	).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}
