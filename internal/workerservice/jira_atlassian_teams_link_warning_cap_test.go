package workerservice

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"atlassian/atlassian"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// The project-link failure text is bounded (4000 code points) by the sanitizer. A credential
// that straddles the cut must not leave a prefix in the logged warning or the stored detail.
// Every value is made up.
const capEdgeToken = "zz9tok-synthetic-cutedgevalue0123456789"

func collectLinkFailure(t *testing.T, echo string) string {
	t.Helper()
	logs := captureWarnings(t)
	const email = "sync@example.test"
	credential := providerfoundation.NewCredential("jira", "cred-1",
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_organization_id": "org-123", "atlassian_cloud_id": "cloud-123"},
		map[string]secrets.Value{"email": secrets.NewValue(email), "api_token": secrets.NewValue(capEdgeToken)})
	collector := jiraCombinedTeamCatalogCollector{
		ScopeCensus:   soleIntegrationCensus{},
		ProjectAsTeam: projectAsTeamStub{result: providersync.TeamCatalogResult{TeamsWritten: 1}},
		Conn:          erroringConn{},
		NewClient: func(string, atlassian.AuthProvider) atlassianteams.Client {
			return linkFailsClient{echo: echo}
		},
	}
	client := &providerfoundation.HTTPClient{BaseURL: &url.URL{Scheme: "https", Host: "acme.atlassian.net"}}
	_, _, _ = collector.collectAtlassianTeams(context.Background(),
		providersync.TeamCatalogReference{OrgID: testOrg, SyncRunID: testRun, Strict: true},
		credential, client, providersync.TeamCatalogSelections{Teams: true, Projects: true}, time.Now())
	return logs.String()
}

// filler is plain words, so no credential shape matches it.
func filler(n int) string {
	return strings.Repeat("lorem ", n/6+1)[:n]
}

func TestAtlassianProjectLinkWarningHidesATokenAtTheCapEdge(t *testing.T) {
	t.Logf("cell: TestAtlassianProjectLinkWarningHidesATokenAtTheCapEdge")
	// The failure text is "read connected projects of a team: upstream rejected [" + echo; put the
	// token so the cut at 3986 code points falls inside it.
	prefix := len("read connected projects of a team: upstream rejected [")
	echo := filler(3975-prefix) + capEdgeToken + " tail"
	logged := collectLinkFailure(t, echo)
	if !strings.Contains(logged, atlassianProjectLinksDegradedAt) {
		t.Fatalf("the project-link warning was never logged")
	}
	// The stored form keeps the first 500 bytes of the detail: the same text, cut at the same place.
	failure := errors.New("read connected projects of a team: upstream rejected [" + echo + "]")
	leg := degradedProjectLinksLeg(atlassianteams.Rows{ProjectLinkFailure: failure, ProjectLinks: atlassianteams.ProjectLinkCounts{FailedTeamReads: 1}}, legCredentialValues(capEdgeToken, "sync@example.test"))
	stored := syncdispatchruntime.DegradedLegsPayload([]providersync.DegradedLeg{*leg})
	for _, partial := range []string{capEdgeToken[:8], capEdgeToken[len(capEdgeToken)-8:]} {
		if strings.Contains(logged, partial) {
			t.Errorf("a part %q of the token was logged", partial)
		}
		if strings.Contains(stored[0]["detail"], partial) {
			t.Errorf("a part %q of the token was stored", partial)
		}
	}
}

func TestAtlassianProjectLinkWarningHidesATokenInsideTheCap(t *testing.T) {
	t.Logf("cell: TestAtlassianProjectLinkWarningHidesATokenInsideTheCap")
	logged := collectLinkFailure(t, filler(20)+capEdgeToken+" tail")
	if !strings.Contains(logged, atlassianProjectLinksDegradedAt) || !strings.Contains(logged, "[REDACTED]") {
		t.Fatalf("the warning was not written with the token redacted: %q", logged)
	}
	if strings.Contains(logged, capEdgeToken[:8]) {
		t.Errorf("the token was logged: %q", logged)
	}
}
