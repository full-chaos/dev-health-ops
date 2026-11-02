package authapi

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/grant"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/publicread"
)

// The versioned read routes: POST get and list of principals, organizations, memberships and grants. Each is mounted only
// when the embedded endpoint-profile document holds its row, so without the rows they answer 404.
const (
	PatternPrincipalsGet     = "/internal/v1/principals/get"
	PatternPrincipalsList    = "/internal/v1/principals/list"
	PatternOrganizationsGet  = "/internal/v1/organizations/get"
	PatternOrganizationsList = "/internal/v1/organizations/list"
	PatternMembershipsGet    = "/internal/v1/memberships/get"
	PatternMembershipsList   = "/internal/v1/memberships/list"
	PatternGrantsGet         = "/internal/v1/grants/get"
	PatternGrantsList        = "/internal/v1/grants/list"
)

// ReadStore is the read side of organizations, memberships and user principals (*publicread.Reader): the public tables
// are its record.
type ReadStore interface {
	Organization(ctx context.Context, id uuid.UUID) (publicread.Organization, error)
	OrganizationsOf(ctx context.Context, principal uuid.UUID, after *uuid.UUID, limit int) ([]publicread.OrganizationOf, bool, error)
	Membership(ctx context.Context, org, principal uuid.UUID) (publicread.Membership, error)
	Memberships(ctx context.Context, org uuid.UUID, after *uuid.UUID, limit int) ([]publicread.Membership, bool, error)
	Principal(ctx context.Context, id uuid.UUID) (publicread.Principal, error)
	Principals(ctx context.Context, org uuid.UUID, kind string, after *uuid.UUID, limit int) ([]publicread.Principal, bool, error)
}

// GrantReads is the read side of the resource grant aggregate (*grant.Store).
type GrantReads interface {
	Get(ctx context.Context, orgID, id uuid.UUID) (*grant.Grant, error)
	ListForOrg(ctx context.Context, orgID uuid.UUID, f grant.ListFilter) ([]grant.Grant, error)
}

func (h provisioningHandlers) withReadRoutes(routes []httpapi.Route, rows []byte) []httpapi.Route {
	return routes
}
