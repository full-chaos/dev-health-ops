package apiservice

import (
	"encoding/json"
	"errors"
	"fmt"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// goAPIScopeRows is the Principal step's row lookup: what each go-api
// endpoint-profile row declares about the Authorization bearer, keyed by the
// route template, read from the class field the access-token validator reads.
func goAPIScopeRows() (func(profile string) (policy.BearerRow, bool), error) {
	rows, err := parseScopeRows(authv1.GoAPIEndpointProfiles)
	if err != nil {
		return nil, err
	}
	return func(profile string) (policy.BearerRow, bool) {
		row, ok := rows[profile]
		return row, ok
	}, nil
}

func parseScopeRows(raw []byte) (map[string]policy.BearerRow, error) {
	var document struct {
		Rows []struct {
			Service               string   `json:"service"`
			Method                string   `json:"method"`
			Route                 string   `json:"route"`
			AcceptedClasses       []string `json:"accepted_credential_classes"`
			AccessTokenValidation *struct {
				LegacyHS256 bool `json:"legacy_hs256"`
			} `json:"access_token_validation"`
			TenantRequirement string `json:"tenant_requirement"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("go-api endpoint profiles: %w", err)
	}
	rows := map[string]policy.BearerRow{}
	for _, entry := range document.Rows {
		if entry.Service != goAPIAccessService {
			continue
		}
		if entry.Method == "" || entry.Route == "" {
			return nil, errors.New("go-api endpoint profiles: a row has no method or route")
		}
		profile := entry.Method + " " + entry.Route
		if _, seen := rows[profile]; seen {
			return nil, fmt.Errorf("go-api endpoint profiles: %s has two rows", profile)
		}
		row := policy.BearerRowFor(entry.AcceptedClasses, entry.AccessTokenValidation != nil && entry.AccessTokenValidation.LegacyHS256)
		switch entry.TenantRequirement {
		case "org_scoped":
		case "none", "cross_org_superuser_only":
			row.NotOrgScoped = true
		default:
			return nil, fmt.Errorf("go-api endpoint profiles: %s has tenant_requirement %q", profile, entry.TenantRequirement)
		}
		rows[profile] = row
	}
	if len(rows) == 0 {
		return nil, errors.New("go-api endpoint profiles: no row for the api service")
	}
	return rows, nil
}
