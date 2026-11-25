package workerservice

import (
	"context"
	"encoding/base64"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// A project-link read that fails after a successful team search is logged with
// its text in the "error" field. The text can carry the bare Basic payload. Every value is made up.

type linkFailsClient struct{ echo string }

func (linkFailsClient) SearchTeams(context.Context, string, string, string, int) ([]atlassian.AtlassianTeam, error) {
	return []atlassian.AtlassianTeam{{ID: "ari:cloud:identity::team/11111111-1111-4111-8111-111111111111", DisplayName: "Team", State: "ACTIVE"}}, nil
}
func (linkFailsClient) IterTeamUsers(context.Context, string, int) ([]atlassian.TeamworkUserRelation, error) {
	return nil, nil
}
func (c linkFailsClient) IterTeamConnectedContainers(context.Context, string, int) ([]graph.TeamConnectedContainer, error) {
	return nil, errors.New("upstream rejected [" + c.echo + "]")
}

type erroringConn struct{ driver.Conn }

var errNoStore = errors.New("no store in this cell")

func (erroringConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errNoStore
}
func (erroringConn) Select(context.Context, any, string, ...any) error { return errNoStore }
func (erroringConn) Exec(context.Context, string, ...any) error        { return errNoStore }

func TestAtlassianProjectLinkWarningCarriesNoBasicPayload(t *testing.T) {
	t.Logf("cell: TestAtlassianProjectLinkWarningCarriesNoBasicPayload")
	logs := captureWarnings(t)
	const token, email = "zz9tok-synthetic-aaaaaaaa", "sync@example.test"
	payload := base64.StdEncoding.EncodeToString([]byte(email + ":" + token))
	credential := providerfoundation.NewCredential("jira", "cred-1",
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_organization_id": "org-123", "atlassian_cloud_id": "cloud-123"},
		map[string]secrets.Value{"email": secrets.NewValue(email), "api_token": secrets.NewValue(token)})
	collector := jiraCombinedTeamCatalogCollector{
		ScopeCensus:   soleIntegrationCensus{},
		ProjectAsTeam: projectAsTeamStub{result: providersync.TeamCatalogResult{TeamsWritten: 1}},
		Conn:          erroringConn{},
		NewClient: func(string, atlassian.AuthProvider) atlassianteams.Client {
			return linkFailsClient{echo: payload}
		},
	}
	client := &providerfoundation.HTTPClient{BaseURL: &url.URL{Scheme: "https", Host: "acme.atlassian.net"}}
	_, _, _ = collector.collectAtlassianTeams(context.Background(),
		providersync.TeamCatalogReference{OrgID: testOrg, SyncRunID: testRun, Strict: true},
		credential, client, providersync.TeamCatalogSelections{Teams: true, Projects: true}, time.Now())
	if !strings.Contains(logs.String(), atlassianProjectLinksDegradedAt) {
		t.Fatalf("the project-link warning was never logged: %q", logs.String())
	}
	if strings.Contains(logs.String(), payload) {
		t.Errorf("the Basic payload was logged: %q", logs.String())
	}
}
