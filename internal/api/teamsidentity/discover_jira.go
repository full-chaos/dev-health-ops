package teamsidentity

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// jiraProjectSearchResponse is the subset of Jira's GET
// /rest/api/3/project/search response this route reads (values[].key,
// values[].name, values[].description); every other field the real API
// returns is ignored, matching team_discovery.py's own read.
type jiraProjectSearchResponse struct {
	Values []struct {
		Key         string  `json:"key"`
		Name        string  `json:"name"`
		Description *string `json:"description"`
	} `json:"values"`
}

// discoverJira mirrors TeamDiscoveryService.discover_jira
// (team_discovery.py:299-338) EXACTLY, including what it does NOT do: one
// GET request, maxResults=100, no further pagination even though Jira's
// project/search endpoint supports it (startAt/isLast) -- a real,
// deliberate-or-not limitation of the reference implementation this port
// reproduces rather than silently improves on.
func discoverJira(ctx context.Context, credential providerfoundation.Credential) ([]discoveredTeam, error) {
	client, err := providerfoundation.NewJiraClient(credential, discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{})
	if err != nil {
		return nil, err
	}
	// url is whatever base URL NewJiraClient itself resolved (email/token/
	// base_url aliasing already applied); associations.provider_org below
	// must carry the SAME value Python's route does: the credential's own
	// url, not a re-derived one, so reuse the client's own BaseURL.
	providerOrg := ""
	if client.BaseURL != nil {
		providerOrg = client.BaseURL.String()
	}
	response, err := client.Do(ctx, "GET", "/rest/api/3/project/search?maxResults=100", nil)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	var payload jiraProjectSearchResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode jira project/search response: %w", err)
	}
	teams := make([]discoveredTeam, 0, len(payload.Values))
	for _, project := range payload.Values {
		projectKey := project.Key
		projectName := project.Name
		if projectName == "" {
			projectName = projectKey
		}
		if projectKey == "" || projectName == "" {
			continue
		}
		associations := pyjson.NewObject()
		associations.Set("project_keys", []string{projectKey})
		associations.Set("provider_org", providerOrg)
		teams = append(teams, discoveredTeam{
			ProviderType:   "jira",
			ProviderTeamID: projectKey,
			Name:           projectName,
			Description:    project.Description,
			Associations:   associations,
		})
	}
	return teams, nil
}
