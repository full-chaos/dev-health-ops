package workerservice

import (
	"net/http"
	"testing"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"
)

// r1 P1 (CHAOS-7454): the worker's own Atlassian gateway clients must not follow a redirect either.
func TestJiraCollectorGatewayClientsRefuseRedirects(t *testing.T) {
	collector := jiraCombinedTeamCatalogCollector{}
	auth := atlassian.BasicAPITokenAuth{Email: "e@example.test", Token: "t"}
	for name, client := range map[string]any{
		"teams client":          collector.newClient("https://tenant.example.test/gateway/api", auth),
		"organization resolver": collector.newOrganizationResolver("https://tenant.example.test/gateway/api", auth),
	} {
		graphClient, ok := client.(*graph.Client)
		if !ok || graphClient.HTTPClient == nil || graphClient.HTTPClient.CheckRedirect == nil ||
			graphClient.HTTPClient.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
			t.Errorf("%s: the production HTTP client follows redirects", name)
		}
	}
}
