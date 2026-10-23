package policy

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const legacyPathMetric = "dev_health_api_legacy_path_total"

const (
	legacyPathGuardLevel = "guard_level"
	legacyPathRoleClaim  = "role_claim"
	legacyPathDirectRead = "direct_read"

	roleClaimByRole      = "admin_by_role"
	roleClaimBySuperuser = "admin_by_superuser"
	roleClaimDenied      = "role_denied"

	directReadUsers         = "users"
	directReadMemberships   = "memberships"
	directReadImpersonation = "impersonation_sessions"
)

type legacyAdder func(ctx context.Context, path, kind string)

// legacyPathAdd counts one use of a legacy auth path. Tests replace it.
var legacyPathAdd = newLegacyAdder()

func newLegacyAdder() legacyAdder {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/api/policy").Int64Counter(
		legacyPathMetric,
		metric.WithDescription("Uses of legacy API auth paths: guard levels, role-claim admin checks, direct auth-table reads"))
	if err != nil {
		return func(context.Context, string, string) {}
	}
	return func(ctx context.Context, path, kind string) {
		counter.Add(ctx, 1, metric.WithAttributes(attribute.String("path", path), attribute.String("kind", kind)))
	}
}

// countLegacy never changes a decision: a counter failure is swallowed.
func countLegacy(ctx context.Context, path, kind string) {
	defer func() { _ = recover() }()
	if legacyPathAdd != nil {
		legacyPathAdd(ctx, path, kind)
	}
}

// countGuardLevel counts each request that reaches next through level.
func countGuardLevel(level Authz, next http.Handler) http.Handler {
	kind := level.String()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		countLegacy(r.Context(), legacyPathGuardLevel, kind)
		next.ServeHTTP(w, r)
	})
}

// countRoleClaim counts how an Admin or AdminOrg decision reads the role claim.
func countRoleClaim(ctx context.Context, user *User) {
	kind := roleClaimDenied
	switch {
	case user.Role == "owner" || user.Role == "admin":
		kind = roleClaimByRole
	case user.IsSuperuser:
		kind = roleClaimBySuperuser
	}
	countLegacy(ctx, legacyPathRoleClaim, kind)
}

// countingStore counts every read of an auth-owned table.
type countingStore struct{ Store }

func (s countingStore) UserState(ctx context.Context, id uuid.UUID) (UserState, bool, error) {
	countLegacy(ctx, legacyPathDirectRead, directReadUsers)
	return s.Store.UserState(ctx, id)
}

func (s countingStore) Membership(ctx context.Context, userID, orgID uuid.UUID) (string, bool, error) {
	countLegacy(ctx, legacyPathDirectRead, directReadMemberships)
	return s.Store.Membership(ctx, userID, orgID)
}

func (s countingStore) ActiveImpersonation(ctx context.Context, adminID uuid.UUID) (*Impersonation, error) {
	countLegacy(ctx, legacyPathDirectRead, directReadImpersonation)
	return s.Store.ActiveImpersonation(ctx, adminID)
}

// countingSessionStore keeps the SessionStore half of a wrapped Store.
type countingSessionStore struct {
	countingStore
	sessions SessionStore
}

func (s countingSessionStore) SessionState(ctx context.Context, id uuid.UUID) (SessionRow, bool, error) {
	return s.sessions.SessionState(ctx, id)
}

// countStore wraps store with the direct-read counters and keeps whether it
// is a SessionStore.
func countStore(store Store) Store {
	counted := countingStore{store}
	if sessions, ok := store.(SessionStore); ok {
		return countingSessionStore{counted, sessions}
	}
	return counted
}
