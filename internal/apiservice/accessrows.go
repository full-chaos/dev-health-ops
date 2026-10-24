package apiservice

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

// goAPIAccessService is the service name of the api listener's
// endpoint-profile rows.
const goAPIAccessService = "dev-health-ops-api"

// goAPIAccessRows is the FULL go-api row table for the access-token
// validator: every dev-health-ops-api row, keyed by the route template the
// mux registers it under.
func goAPIAccessRows() ([]accessverify.Row, error) {
	return parseAccessRows(authv1.GoAPIEndpointProfiles)
}

func parseAccessRows(raw []byte) ([]accessverify.Row, error) {
	var document struct {
		Rows []struct {
			Service               string   `json:"service"`
			Method                string   `json:"method"`
			Route                 string   `json:"route"`
			AcceptedClasses       []string `json:"accepted_credential_classes"`
			AccessTokenValidation *struct {
				LegacyHS256 bool `json:"legacy_hs256"`
			} `json:"access_token_validation"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("go-api endpoint profiles: %w", err)
	}
	seen := map[string]bool{}
	var rows []accessverify.Row
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
		rows = append(rows, accessverify.Row{
			Service:     goAPIAccessService,
			Profile:     profile,
			AcceptsUser: slices.Contains(entry.AcceptedClasses, accessverify.ClassUser),
			LegacyHS256: entry.AccessTokenValidation != nil && entry.AccessTokenValidation.LegacyHS256,
		})
	}
	if len(rows) == 0 {
		return nil, errors.New("go-api endpoint profiles: no row for the api service")
	}
	return rows, nil
}
