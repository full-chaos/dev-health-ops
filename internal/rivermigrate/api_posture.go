package rivermigrate

import (
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// apiPostureFromLookup is the api role manifest under go-api's two switches,
// AUTH_PROXY_SESSIONS and AUTH_ORGS_OWNED, read as go-api reads them. It also
// returns the sessions switch, which the query-api leg reads.
func apiPostureFromLookup(lookup func(string) (string, bool)) (postgresstore.RolePosture, bool, error) {
	sessions, err := authProxySessions(lookup)
	if err != nil {
		return postgresstore.RolePosture{}, false, err
	}
	orgsOwned, err := authconfig.OrgsOwnedFromLookup(lookup)
	if err != nil {
		return postgresstore.RolePosture{}, false, err
	}
	return postgresstore.APIPostureFor(sessions, orgsOwned), sessions, nil
}
