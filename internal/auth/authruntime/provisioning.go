package authruntime

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/grant"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/orgdeprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/publicread"
	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// provisioningProfileRows is the endpoint-profile document the provisioning routes are checked against; nil is the
// document the binary embeds. A test plants another one (the suspend route is mounted only with its row).
var provisioningProfileRows []byte

// provisioningRoutes builds the internal membership, invite and organization
// deprovision routes and, behind the same switch, the internal read routes (each
// mounted only with its endpoint-profile row) when AUTH_MEMBERSHIP_ROUTES is on, and none otherwise.
// Every verb behind them refuses while auth-service does not own users. They
// take the session issue route's workload path (nil refuses workload tokens).
// A membership removal ends the member's sessions of the organization, and an
// organization suspend or deprovision every session of the organization, through
// sessions, in the verb's transaction.
func provisioningRoutes(cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	identities *identity.Store, workload *authapi.SessionIssueWorkload, sessions *session.Store) ([]httpapi.Route, error) {
	if !cfg.Provisioning.Routes {
		return nil, nil
	}
	members, err := membership.New(membership.Config{Pool: pool, Schema: schema, OwnsUsers: cfg.UsersOwned, Sessions: sessions, Logger: logger})
	if err != nil {
		return nil, err
	}
	invites, err := invite.New(invite.Config{Pool: pool, Schema: schema, OwnsUsers: cfg.UsersOwned, Memberships: members,
		Secret: cfg.Provisioning.InviteSecret.Reveal(), AcceptSecrets: cfg.InviteAcceptSecrets(), Logger: logger})
	if err != nil {
		return nil, err
	}
	orgs, err := org.New(org.Config{Pool: pool, Schema: schema, Sessions: sessions, Logger: logger})
	if err != nil {
		return nil, err
	}
	deprovision, err := orgdeprovision.New(orgdeprovision.Config{Pool: pool, Schema: schema, Orgs: orgs, Memberships: members,
		Invites: invites, Sessions: sessions, Logger: logger})
	if err != nil {
		return nil, err
	}
	accounts, err := serviceaccount.New(serviceaccount.Config{Pool: pool, Schema: schema, Locks: members, OwnsUsers: cfg.UsersOwned, Logger: logger})
	if err != nil {
		return nil, err
	}
	grants, err := grant.New(grant.Config{Pool: pool, Schema: schema, OwnsUsers: cfg.UsersOwned, Members: members, Logger: logger})
	if err != nil {
		return nil, err
	}
	routes, err := authapi.ProvisioningRoutes(authapi.ProvisioningDeps{
		Users: pgUsers(cfg, pool, schema), Reader: pool, Organizations: identities,
		Memberships: members, Invites: invites, Deprovision: deprovision, Orgs: orgs, ServiceAccounts: accounts, Grants: grants, Workload: workload, Logger: logger, ProfileRows: provisioningProfileRows,
	})
	if err != nil {
		return nil, err
	}
	reader, err := publicread.New(publicread.Config{Querier: pool, AuthSchema: schema.String(), Logger: logger})
	if err != nil {
		return nil, err
	}
	reads, err := authapi.ReadRoutes(authapi.ReadDeps{
		Reads: reader, GrantReads: grants, Workload: workload, Logger: logger, ProfileRows: provisioningProfileRows,
	})
	if err != nil {
		return nil, err
	}
	return append(routes, reads...), nil
}

// usersAdminRoutes builds the user-admin routes go-api calls for its admin users writes while auth-service owns users
// (AUTH_USERS_OWNED), and none otherwise. They are not behind AUTH_MEMBERSHIP_ROUTES: each mounts only with its
// endpoint-profile row (authapi.UsersAdminRoutes), on the provisioning routes' workload path.
func usersAdminRoutes(cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	identities *identity.Store, workload *authapi.SessionIssueWorkload) ([]httpapi.Route, error) {
	if !cfg.UsersOwned {
		return nil, nil
	}
	roles, err := platformrole.New(platformrole.Config{Pool: pool, Schema: schema, Logger: logger})
	if err != nil {
		return nil, err
	}
	return authapi.UsersAdminRoutes(authapi.ProvisioningDeps{
		Users: pgUsers(cfg, pool, schema), Reader: pool, Organizations: identities, Workload: workload, Logger: logger,
		ProfileRows:      provisioningProfileRows,
		UsersAdminStores: authapi.UsersAdminStores{UsersAdmin: identities, PlatformRoles: authapi.PlatformRoleStore{Store: roles}},
	})
}

// goAPIInternalRoutes are the internal routes go-api calls: the provisioning routes (AUTH_MEMBERSHIP_ROUTES) and, apart
// from them, the user-admin routes (AUTH_USERS_OWNED).
func goAPIInternalRoutes(cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	identities *identity.Store, workload *authapi.SessionIssueWorkload, sessions *session.Store) ([]httpapi.Route, error) {
	provisioning, err := provisioningRoutes(cfg, logger, pool, schema, identities, workload, sessions)
	if err != nil {
		return nil, err
	}
	users, err := usersAdminRoutes(cfg, logger, pool, schema, identities, workload)
	if err != nil {
		return nil, err
	}
	return append(provisioning, users...), nil
}
