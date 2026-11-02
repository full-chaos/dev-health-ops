package rivermigrate

import postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"

func apiPostureFromLookup(lookup func(string) (string, bool)) (postgresstore.RolePosture, bool, error) {
	sessions, err := authProxySessions(lookup)
	if err != nil {
		return postgresstore.RolePosture{}, false, err
	}
	return postgresstore.APIPosture(sessions), sessions, nil
}
