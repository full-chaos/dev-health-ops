package policy

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const legacyPathMetric = "dev_health_api_legacy_path_total"

// Surfaces of the legacy-path counter. Every surface and kind is a constant:
// no request, user or organization value is ever a label.
const (
	surfaceGuardLevel = "guard_level"
	surfaceRoleSource = "role_source"
	surfaceDirectRead = "direct_read"
)

const (
	kindGuardPublic        = "public"
	kindGuardAuthenticated = "authenticated"
	kindGuardAdmin         = "admin"
	kindGuardAdminOrg      = "admin_org"
	kindGuardSuperuser     = "superuser"
	kindGuardOptional      = "optional"
	kindGuardUnknown       = "unknown"

	kindRoleMembershipRow  = "membership_row"
	kindRoleClaimFallback  = "claim_fallback"
	kindRolePlatformBypass = "platform_bypass"
	kindRolePlatformGate   = "platform_gate"

	kindReadUsers          = "users"
	kindReadMemberships    = "memberships"
	kindReadOrgMemberships = "org_memberships"
	kindReadImpersonation  = "impersonation_sessions"
	kindReadAuthSessions   = "auth_sessions"
)

type legacyAdder func(ctx context.Context, surface, kind string)

// legacyPathAdd counts one use of a legacy auth path. Tests replace it.
var legacyPathAdd = newLegacyAdder()

func newLegacyAdder() legacyAdder {
	legacyPathCounter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/api/policy").Int64Counter(
		legacyPathMetric,
		metric.WithDescription("Uses of legacy API auth paths: guard levels, role sources, direct auth-table reads"))
	if err != nil {
		return func(context.Context, string, string) {}
	}
	return func(ctx context.Context, surface, kind string) {
		legacyPathCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("surface", surface), attribute.String("kind", kind)))
	}
}

// countLegacy never changes a decision: a counter failure is swallowed.
func countLegacy(ctx context.Context, surface, kind string) {
	defer func() { _ = recover() }()
	if legacyPathAdd != nil {
		legacyPathAdd(ctx, surface, kind)
	}
}

func countDirectRead(ctx context.Context, kind string) { countLegacy(ctx, surfaceDirectRead, kind) }

func countRoleSource(ctx context.Context, kind string) { countLegacy(ctx, surfaceRoleSource, kind) }

func guardLevelKind(level Authz) string {
	switch level {
	case Public:
		return kindGuardPublic
	case Authenticated:
		return kindGuardAuthenticated
	case Admin:
		return kindGuardAdmin
	case AdminOrg:
		return kindGuardAdminOrg
	case Superuser:
		return kindGuardSuperuser
	case Optional:
		return kindGuardOptional
	}
	return kindGuardUnknown
}

// Wrap returns next behind level, counting each request that enters the
// route, refused callers included.
func (g *Guard) Wrap(level Authz, next http.Handler) http.Handler {
	kind := guardLevelKind(level)
	guarded := g.wrap(level, next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		countLegacy(r.Context(), surfaceGuardLevel, kind)
		guarded.ServeHTTP(w, r)
	})
}
