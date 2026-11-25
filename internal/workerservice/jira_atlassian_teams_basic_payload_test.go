package workerservice

import (
	"context"
	"encoding/base64"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"

	"atlassian/atlassian"
)

// The gateway can echo the Authorization header the leg sent, which holds
// base64(email:token) in no spelling of the token or the email. Every value is made up.
func TestAtlassianLegErrorCarriesNoBasicPayloadWhenTheGatewayEchoesIt(t *testing.T) {
	t.Logf("cell: TestAtlassianLegErrorCarriesNoBasicPayloadWhenTheGatewayEchoesIt")
	const token, email = "zz9tok-synthetic-aaaaaaaa", "sync@example.test"
	raw := []byte(email + ":" + token)
	payloads := []string{
		base64.StdEncoding.EncodeToString(raw), base64.RawStdEncoding.EncodeToString(raw),
		base64.URLEncoding.EncodeToString(raw), base64.RawURLEncoding.EncodeToString(raw),
	}
	credential := providerfoundation.NewCredential("jira", "cred-1",
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_organization_id": "org-123", "atlassian_cloud_id": "cloud-123"},
		map[string]secrets.Value{"email": secrets.NewValue(email), "api_token": secrets.NewValue(token)})
	client := &providerfoundation.HTTPClient{BaseURL: &url.URL{Scheme: "https", Host: "acme.atlassian.net"}}
	for _, payload := range payloads {
		collector := jiraCombinedTeamCatalogCollector{
			ScopeCensus:   soleIntegrationCensus{},
			ProjectAsTeam: projectAsTeamStub{result: providersync.TeamCatalogResult{TeamsWritten: 1}},
			Conn:          nopConn{},
			NewClient: func(string, atlassian.AuthProvider) atlassianteams.Client {
				return echoingClient{echo: "the credential " + payload + " was refused"}
			},
		}
		// The leg's own error, before any later sanitizer sees it.
		_, _, err := collector.collectAtlassianTeams(context.Background(),
			providersync.TeamCatalogReference{OrgID: testOrg, SyncRunID: testRun, Strict: true},
			credential, client, providersync.TeamCatalogSelections{Teams: true}, time.Now())
		if err == nil || !strings.Contains(err.Error(), "teamSearchV2") {
			t.Fatalf("err=%v, want the gateway's error", err)
		}
		if strings.Contains(err.Error(), payload) {
			t.Errorf("the Basic payload %q reached the leg's error: %q", payload, err.Error())
		}
	}
}
