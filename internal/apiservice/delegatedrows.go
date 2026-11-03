package apiservice

import (
	"encoding/json"
	"errors"
	"fmt"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

// goAPIDelegatedRows is the Guard's delegated-bearer lookup: whether the go-api
// row of a route template ("METHOD /pattern") admits a delegated session's
// bearer, read from the rows' delegated_bearer field. A row with no value, or
// no row, does not.
func goAPIDelegatedRows() (func(profile string) bool, error) {
	allowed, err := parseDelegatedRows(authv1.GoAPIEndpointProfiles)
	if err != nil {
		return nil, err
	}
	return func(profile string) bool { return allowed[profile] }, nil
}

func parseDelegatedRows(raw []byte) (map[string]bool, error) {
	var document struct {
		Rows []struct {
			Service         string  `json:"service"`
			Method          string  `json:"method"`
			Route           string  `json:"route"`
			DelegatedBearer *string `json:"delegated_bearer"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("go-api endpoint profiles: %w", err)
	}
	seen := map[string]bool{}
	allowed := map[string]bool{}
	for _, entry := range document.Rows {
		if entry.Service != goAPIAccessService {
			continue
		}
		if entry.Method == "" || entry.Route == "" {
			return nil, errors.New("go-api endpoint profiles: a row has no method or route")
		}
		profile := entry.Method + " " + entry.Route
		if seen[profile] {
			return nil, fmt.Errorf("go-api endpoint profiles: %s has two rows", profile)
		}
		seen[profile] = true
		if entry.DelegatedBearer == nil {
			continue
		}
		switch *entry.DelegatedBearer {
		case "allowed":
			allowed[profile] = true
		case "denied":
		default:
			return nil, fmt.Errorf("go-api endpoint profiles: %s has delegated_bearer %q", profile, *entry.DelegatedBearer)
		}
	}
	return allowed, nil
}
