package authruntime

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/orgdeprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// provisioningProfileRows is the endpoint-profile document the provisioning routes are checked against; nil is the
// document the binary embeds. A test plants another one (the suspend route is mounted only with its row).
var provisioningProfileRows []byte

// provisioningRoutes builds the internal membership, invite and organization
// deprovision routes when AUTH_MEMBERSHIP_ROUTES is on, and none otherwise.
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
	return authapi.ProvisioningRoutes(authapi.ProvisioningDeps{
		Credentials: servicecred.PG{Pool: pool}, Users: pgUsers(cfg, pool, schema), Reader: pool, Organizations: identities,
		Memberships: members, Invites: invites, Deprovision: deprovision, Orgs: orgs, ServiceAccounts: accounts, Workload: workload, Logger: logger, ProfileRows: provisioningProfileRows,
	})
}
