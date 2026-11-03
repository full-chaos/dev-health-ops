package apiservice

import authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"

// goAPIDelegatedRows is the guard's delegated-bearer lookup: whether the
// go-api row of a route template ("METHOD /pattern") admits a delegated
// session's bearer.
func goAPIDelegatedRows() (func(profile string) bool, error) {
	allowed, err := parseDelegatedRows(authv1.GoAPIEndpointProfiles)
	if err != nil {
		return nil, err
	}
	return func(profile string) bool { return allowed[profile] }, nil
}

func parseDelegatedRows([]byte) (map[string]bool, error) { return map[string]bool{}, nil }
