package authapi

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// PatternCompatSwitchOrg is go-api's organization switch, POST
// /api/v1/auth/switch-org. Not served yet: the route is never mounted.
const PatternCompatSwitchOrg = "/api/v1/auth/switch-org"

// switchOrgLimit is the switch budget; not set yet.
var switchOrgLimit = httpapi.Limit{}

// SwitchOrgSessions is the session domain of the switch (*session.Store).
type SwitchOrgSessions interface {
	CreateSupersedingGuarded(ctx context.Context, principalID uuid.UUID, organizationID *uuid.UUID, amr []string,
		predecessor uuid.UUID, event audit.AuditEvent, guard session.Guard) (session.Issued, error)
}

// SwitchOrgReads are the reads the switch makes inside the session
// transaction (PGUsers).
type SwitchOrgReads interface {
	StateOn(ctx context.Context, q session.Queryer, userID uuid.UUID) (UserState, bool, error)
	OrganizationActiveOn(ctx context.Context, q session.Queryer, orgID uuid.UUID) (bool, error)
	MemberRoleLockedOn(ctx context.Context, q session.Queryer, userID, orgID uuid.UUID) (*string, bool, error)
}

// SwitchOrgConfig mounts POST /api/v1/auth/switch-org (CompatDeps.SwitchOrg).
type SwitchOrgConfig struct {
	Sessions SwitchOrgSessions
	Reads    SwitchOrgReads
}

var errSwitchOrgUnbuilt = errors.New("authapi: the switch-org reads are not built")

// OrganizationActiveOn is not built yet.
func (p PGUsers) OrganizationActiveOn(context.Context, session.Queryer, uuid.UUID) (bool, error) {
	return false, errSwitchOrgUnbuilt
}

// MemberRoleLockedOn is not built yet.
func (p PGUsers) MemberRoleLockedOn(context.Context, session.Queryer, uuid.UUID, uuid.UUID) (*string, bool, error) {
	return nil, false, errSwitchOrgUnbuilt
}

// withSwitchOrgRoute mounts nothing yet.
func (h compatHandlers) withSwitchOrgRoute(routes []httpapi.Route, _ []byte) ([]httpapi.Route, error) {
	return routes, nil
}
