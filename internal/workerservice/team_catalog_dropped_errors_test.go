package workerservice

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"log/slog"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// CHAOS-7132: the post-sync team-catalog seam is non-strict, so a failure on its path degrades to a
// counter outcome and the run stays a success. Before this, nothing named the CAUSE: an operator saw
// `native_failed_nonfatal` and no line saying which stage failed or why. Each dropped error now
// leaves one Warn line with the org, run, provider, stage and the error text (names only: the
// errors on this path are credential-free by construction).

func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buffer, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buffer
}

type failingClientResolver struct{ err error }

func (resolver failingClientResolver) ResolveClient(context.Context, string, string, string) (providerfoundation.Credential, *providerfoundation.HTTPClient, string, error) {
	return providerfoundation.Credential{}, nil, "", resolver.err
}

func TestTeamAutoImportLogsTheCauseOfADroppedClientError(t *testing.T) {
	logs := captureWarnings(t)
	cause := errors.New("build a jira client from the stored credential: provider credential is invalid: missing base_url")
	observer := &fakeTeamCatalogObserver{}
	dispatcher := &nativeTeamAutoimportDispatcher{
		resolveProvider: func(context.Context, string, string) (string, error) { return "jira", nil },
		native:          map[string]providersync.TeamCatalogCollector{"jira": &linearCollectorSpy{}},
		clients:         failingClientResolver{err: cause},
		selections:      fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true}},
		observer:        observer,
	}
	if err := dispatcher.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{OrganizationID: testOrg, SyncRunID: testRun}); err != nil {
		t.Fatalf("a non-strict dropped error must not fail the job: %v", err)
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0].outcome != jobruntime.TeamCatalogOutcomeNativeFailedNonfatal {
		t.Fatalf("outcome = %+v, want native_failed_nonfatal", observer.dispatches)
	}
	text := logs.String()
	for _, want := range []string{"level=WARN", "provider=jira", testOrg, testRun, "stage=client", "missing base_url"} {
		if !strings.Contains(text, want) {
			t.Errorf("the dropped client error left no log with %q; log = %q", want, text)
		}
	}
}

func TestTeamAutoImportLogsTheCauseOfADroppedCollectorError(t *testing.T) {
	logs := captureWarnings(t)
	observer := &fakeTeamCatalogObserver{}
	dispatcher := &nativeTeamAutoimportDispatcher{
		resolveProvider: func(context.Context, string, string) (string, error) { return "linear", nil },
		native:          map[string]providersync.TeamCatalogCollector{"linear": &linearCollectorSpy{err: errors.New("linear API rate limited")}},
		clients:         fakeAutoimportClientResolver{integrationID: "integration-1"},
		selections:      fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true}},
		observer:        observer,
	}
	if err := dispatcher.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{OrganizationID: testOrg, SyncRunID: testRun}); err != nil {
		t.Fatal(err)
	}
	text := logs.String()
	for _, want := range []string{"level=WARN", "provider=linear", "stage=collect", "linear API rate limited"} {
		if !strings.Contains(text, want) {
			t.Errorf("the dropped collector error left no log with %q; log = %q", want, text)
		}
	}
}

type projectAsTeamStub struct {
	result providersync.TeamCatalogResult
}

func (stub projectAsTeamStub) CollectTeamCatalog(context.Context, providersync.TeamCatalogReference, providerfoundation.Credential, *providerfoundation.HTTPClient, providersync.TeamCatalogSelections, time.Time) (providersync.TeamCatalogResult, error) {
	return stub.result, nil
}

// The Atlassian Teams leg used to return with no line at all when the client carried no base URL.
func TestJiraCombinedCollectorLogsWhenTheAtlassianLegHasNoClient(t *testing.T) {
	logs := captureWarnings(t)
	collector := jiraCombinedTeamCatalogCollector{ProjectAsTeam: projectAsTeamStub{result: providersync.TeamCatalogResult{TeamsWritten: 3}}}
	result, err := collector.CollectTeamCatalog(context.Background(),
		providersync.TeamCatalogReference{OrgID: testOrg, SyncRunID: testRun},
		providerfoundation.Credential{Provider: "jira"}, &providerfoundation.HTTPClient{},
		providersync.TeamCatalogSelections{Teams: true}, time.Now())
	if err != nil || result.TeamsWritten != 3 {
		t.Fatalf("the project-as-team result must survive: %+v, %v", result, err)
	}
	text := logs.String()
	for _, want := range []string{"level=WARN", "jira_atlassian_teams_walk_skipped", testOrg, "no_base_url"} {
		if !strings.Contains(text, want) {
			t.Errorf("the skipped Atlassian leg left no log with %q; log = %q", want, text)
		}
	}
}

// D2778 + CHAOS-7132: the Atlassian Teams leg is additive. Even in STRICT mode (reference discovery) its
// failure must not fail the collection; it is recorded as a degraded leg with a fixed reason, and the
// project-as-team result survives. Before this, `if ref.Strict { return result, err }` failed every
// jira sync (0 of 14 units on bigboy) over an "Invalid Organization Ari".
func TestJiraCombinedCollectorKeepsTheCatalogWhenTheAtlassianLegFailsInStrictMode(t *testing.T) {
	logs := captureWarnings(t)
	// No ClickHouse connection: the Atlassian leg refuses with a configuration error, a real failure of that leg.
	collector := jiraCombinedTeamCatalogCollector{ProjectAsTeam: projectAsTeamStub{result: providersync.TeamCatalogResult{TeamsWritten: 3}}}
	client := &providerfoundation.HTTPClient{BaseURL: &url.URL{Scheme: "https", Host: "site.example.test"}}
	result, err := collector.CollectTeamCatalog(context.Background(),
		providersync.TeamCatalogReference{OrgID: testOrg, SyncRunID: testRun, Strict: true},
		providerfoundation.Credential{Provider: "jira"}, client, providersync.TeamCatalogSelections{Teams: true}, time.Now())
	if err != nil {
		t.Fatalf("a failed additive leg must not fail a strict collection: %v", err)
	}
	if result.TeamsWritten != 3 {
		t.Errorf("the project-as-team result must survive: %+v", result)
	}
	if len(result.DegradedLegs) != 1 {
		t.Fatalf("DegradedLegs = %+v, want exactly the Atlassian Teams leg", result.DegradedLegs)
	}
	leg := result.DegradedLegs[0]
	if leg.Dataset != "teams" || leg.Leg != "jira_atlassian_teams" || leg.Outcome != "failed" || leg.Reason != "configuration" {
		t.Errorf("degraded leg = %+v", leg)
	}
	if !strings.Contains(logs.String(), "jira_atlassian_teams_walk_skipped") || !strings.Contains(logs.String(), "strict=true") {
		t.Errorf("no Warn line for the failed leg: %q", logs.String())
	}
}

// The stored detail of a failed leg must not carry a provider response body: ProviderError keeps the body
// out of Error() by design, and a wrapped one must not bring it back.
func TestDegradedAtlassianLegDetailCarriesNoProviderResponseBody(t *testing.T) {
	providerErr := &providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication, StatusCode: 401, Path: "/gateway/api/graphql", Body: `{"message":"token secret-body-fragment rejected"}`}
	leg := newDegradedAtlassianLeg(fmt.Errorf("search atlassian teams: %w", providerErr))
	if strings.Contains(leg.Detail, "secret-body-fragment") {
		t.Fatalf("detail carries the response body: %q", leg.Detail)
	}
	if !strings.Contains(leg.Detail, "authentication") || !strings.Contains(leg.Detail, "401") {
		t.Errorf("detail should name the class and status: %q", leg.Detail)
	}
	if leg.Reason != "authentication:401" || leg.Leg != "jira_atlassian_teams" || leg.Outcome != "failed" {
		t.Errorf("leg = %+v", leg)
	}
}

type nopConn struct{ driver.Conn }

type echoingClient struct{ echo string }

func (c echoingClient) SearchTeams(context.Context, string, string, string, int) ([]atlassian.AtlassianTeam, error) {
	return nil, errors.New("Exception while fetching data (/team/teamSearchV2) : rejected request with " + c.echo)
}
func (echoingClient) IterTeamUsers(context.Context, string, int) ([]atlassian.TeamworkUserRelation, error) {
	return nil, nil
}
func (echoingClient) IterTeamConnectedContainers(context.Context, string, int) ([]graph.TeamConnectedContainer, error) {
	return nil, nil
}

// r1 (CHAOS-7132): the gateway can echo the credential it was sent into its error text. The leg's error
// must carry no credential value to the log or to the stored detail, while the reason and errors.Is stay.
func TestAtlassianLegErrorCarriesNoCredentialValueWhenTheGatewayEchoesIt(t *testing.T) {
	logs := captureWarnings(t)
	const token, email = "ATATT-secret-api-token-0123456789", "sync-secret@example.test"
	credential := providerfoundation.NewCredential("jira", "cred-1",
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_organization_id": "org-123", "atlassian_cloud_id": "cloud-123"},
		map[string]secrets.Value{"email": secrets.NewValue(email), "api_token": secrets.NewValue(token)})
	collector := jiraCombinedTeamCatalogCollector{
		ProjectAsTeam: projectAsTeamStub{result: providersync.TeamCatalogResult{TeamsWritten: 1}},
		Conn:          nopConn{},
		NewClient: func(string, atlassian.AuthProvider) atlassianteams.Client {
			return echoingClient{echo: "Basic " + token + " for " + email + " token=" + token}
		},
	}
	client := &providerfoundation.HTTPClient{BaseURL: &url.URL{Scheme: "https", Host: "acme.atlassian.net"}}
	result, err := collector.CollectTeamCatalog(context.Background(),
		providersync.TeamCatalogReference{OrgID: testOrg, SyncRunID: testRun, Strict: true},
		credential, client, providersync.TeamCatalogSelections{Teams: true}, time.Now())
	if err != nil || len(result.DegradedLegs) != 1 {
		t.Fatalf("err=%v legs=%+v, want one degraded leg", err, result.DegradedLegs)
	}
	for _, surface := range []string{result.DegradedLegs[0].Detail, result.DegradedLegs[0].Reason, logs.String()} {
		if strings.Contains(surface, token) || strings.Contains(surface, email) {
			t.Fatalf("a credential value reached %q", surface)
		}
	}
	if !strings.Contains(result.DegradedLegs[0].Detail, "teamSearchV2") {
		t.Errorf("the detail lost the gateway's own message: %q", result.DegradedLegs[0].Detail)
	}
}

// r1: a dropped error of ANY provider is logged now, so the line must not carry a credential-shaped value either.
func TestTeamAutoImportLogOfADroppedErrorIsSanitized(t *testing.T) {
	logs := captureWarnings(t)
	dispatcher := &nativeTeamAutoimportDispatcher{
		resolveProvider: func(context.Context, string, string) (string, error) { return "linear", nil },
		native:          map[string]providersync.TeamCatalogCollector{"linear": &linearCollectorSpy{err: errors.New("request failed: Authorization: Bearer lin_api_secretvalue123456")}},
		clients:         fakeAutoimportClientResolver{integrationID: "integration-1"},
		selections:      fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true}},
	}
	if err := dispatcher.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{OrganizationID: testOrg, SyncRunID: testRun}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "lin_api_secretvalue123456") {
		t.Fatalf("a credential reached the log: %q", logs.String())
	}
}

// r1: the post-sync seam runs after finalize, so a degraded leg it finds must be recorded on the run itself.
func TestTeamAutoImportRecordsADegradedLegOnTheRun(t *testing.T) {
	var gotOrg, gotRun string
	var gotLegs []providersync.DegradedLeg
	spy := &linearCollectorSpy{result: providersync.TeamCatalogResult{TeamsWritten: 2, DegradedLegs: []providersync.DegradedLeg{{Dataset: "teams", Leg: "jira_atlassian_teams", Outcome: "failed", Reason: "unclassified"}}}}
	observer := &fakeTeamCatalogObserver{}
	dispatcher := &nativeTeamAutoimportDispatcher{
		resolveProvider: func(context.Context, string, string) (string, error) { return "linear", nil },
		native:          map[string]providersync.TeamCatalogCollector{"linear": spy},
		clients:         fakeAutoimportClientResolver{integrationID: "integration-1"},
		selections:      fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true}},
		observer:        observer,
		recordDegraded: func(_ context.Context, orgID, runID string, legs []providersync.DegradedLeg) error {
			gotOrg, gotRun, gotLegs = orgID, runID, legs
			return nil
		},
	}
	if err := dispatcher.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{OrganizationID: testOrg, SyncRunID: testRun}); err != nil {
		t.Fatal(err)
	}
	if gotOrg != testOrg || gotRun != testRun || len(gotLegs) != 1 || gotLegs[0].Leg != "jira_atlassian_teams" {
		t.Errorf("the degraded leg was not recorded on the run: org=%q run=%q legs=%+v", gotOrg, gotRun, gotLegs)
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0].outcome != jobruntime.TeamCatalogOutcomeNativeFailedNonfatal {
		t.Errorf("dispatches = %+v", observer.dispatches)
	}
}

// D3678: every stored and logged sink of the dropped error, end to end through the post-sync dispatcher with
// the real jira collector: the gateway echoes the test token AND email; neither may appear in the Warn
// lines, the recorded degraded detail, the reason, or any telemetry outcome.
func TestEveryDegradedLegSinkIsFreeOfTheEchoedCredential(t *testing.T) {
	logs := captureWarnings(t)
	const token, email = "ATATT-test-constant-token-9876543210", "echo-test-constant@example.test"
	credential := providerfoundation.NewCredential("jira", "cred-1",
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_organization_id": "org-123", "atlassian_cloud_id": "cloud-123"},
		map[string]secrets.Value{"email": secrets.NewValue(email), "api_token": secrets.NewValue(token)})
	combined := jiraCombinedTeamCatalogCollector{
		ProjectAsTeam: projectAsTeamStub{result: providersync.TeamCatalogResult{TeamsWritten: 1}},
		Conn:          nopConn{},
		NewClient: func(string, atlassian.AuthProvider) atlassianteams.Client {
			return echoingClient{echo: "body: {\"email\":\"" + email + "\",\"token\":\"" + token + "\"} Basic " + token}
		},
	}
	var recorded []providersync.DegradedLeg
	observer := &fakeTeamCatalogObserver{}
	dispatcher := &nativeTeamAutoimportDispatcher{
		resolveProvider: func(context.Context, string, string) (string, error) { return "jira", nil },
		native:          map[string]providersync.TeamCatalogCollector{"jira": combined},
		clients:         credentialClientResolver{credential: credential},
		selections:      fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true}},
		observer:        observer,
		recordDegraded: func(_ context.Context, _, _ string, legs []providersync.DegradedLeg) error {
			recorded = legs
			return nil
		},
	}
	if err := dispatcher.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{OrganizationID: testOrg, SyncRunID: testRun}); err != nil {
		t.Fatal(err)
	}
	if len(recorded) != 1 {
		t.Fatalf("recorded legs = %+v", recorded)
	}
	sinks := []string{logs.String(), recorded[0].Detail, recorded[0].Reason, recorded[0].Leg, recorded[0].Outcome}
	for _, dispatch := range observer.dispatches {
		sinks = append(sinks, dispatch.provider, string(dispatch.outcome))
	}
	for _, sink := range sinks {
		if strings.Contains(sink, token) || strings.Contains(sink, email) {
			t.Fatalf("a credential value reached a sink: %q", sink)
		}
	}
	if !strings.Contains(recorded[0].Detail, "teamSearchV2") {
		t.Errorf("the detail lost the gateway's own message: %q", recorded[0].Detail)
	}
}

type credentialClientResolver struct{ credential providerfoundation.Credential }

func (resolver credentialClientResolver) ResolveClient(context.Context, string, string, string) (providerfoundation.Credential, *providerfoundation.HTTPClient, string, error) {
	return resolver.credential, &providerfoundation.HTTPClient{BaseURL: &url.URL{Scheme: "https", Host: "acme.atlassian.net"}}, "integration-1", nil
}
