package workerservice

import (
	"bytes"
	"context"
	"errors"
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
