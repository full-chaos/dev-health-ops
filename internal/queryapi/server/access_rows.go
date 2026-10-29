package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

// queryAPIAccessService is the service name of query-api's endpoint-profile
// rows.
const queryAPIAccessService = "dev-health-query-api"

// queryAPIRowListeners is the listener list a row id ends with:
// "METHOD /route [dev-health-query-api @query,internal]".
var queryAPIRowListeners = regexp.MustCompile(`@([a-z,]+)\]$`)

// queryAPIAccessRows is the access-token validator's row table: every
// dev-health-query-api row served on the query or the internal listener,
// keyed by the route template the request carries ("METHOD /route"). Rows of
// the MCP or the operator listener alone are left out: the MCP listener takes
// the internal identity headers only (its POST /query row would collide with
// the query listener's), and the operator listener reads no bearer.
func queryAPIAccessRows() ([]accessverify.Row, error) {
	return parseQueryAPIAccessRows(authv1.QueryAPIEndpointProfiles)
}

func parseQueryAPIAccessRows(raw []byte) ([]accessverify.Row, error) {
	var document struct {
		Rows []struct {
			ID                    string   `json:"id"`
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
		return nil, fmt.Errorf("query-api endpoint profiles: %w", err)
	}
	seen := map[string]bool{}
	var rows []accessverify.Row
	for _, entry := range document.Rows {
		if entry.Service != queryAPIAccessService {
			continue
		}
		match := queryAPIRowListeners.FindStringSubmatch(entry.ID)
		if match == nil {
			return nil, fmt.Errorf("query-api endpoint profiles: row id %q names no listener", entry.ID)
		}
		listeners := strings.Split(match[1], ",")
		if !slices.Contains(listeners, "query") && !slices.Contains(listeners, "internal") {
			continue
		}
		if entry.Method == "" || entry.Route == "" {
			return nil, errors.New("query-api endpoint profiles: a row has no method or route")
		}
		profile := entry.Method + " " + entry.Route
		if seen[profile] {
			return nil, fmt.Errorf("query-api endpoint profiles: %s has two rows", profile)
		}
		seen[profile] = true
		rows = append(rows, accessverify.Row{
			Service:     queryAPIAccessService,
			Profile:     profile,
			AcceptsUser: slices.Contains(entry.AcceptedClasses, accessverify.ClassUser),
			LegacyHS256: entry.AccessTokenValidation != nil && entry.AccessTokenValidation.LegacyHS256,
		})
	}
	if len(rows) == 0 {
		return nil, errors.New("query-api endpoint profiles: no row for the query listener")
	}
	return rows, nil
}
