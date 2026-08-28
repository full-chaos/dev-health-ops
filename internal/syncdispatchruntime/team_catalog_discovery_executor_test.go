package syncdispatchruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// fakeTeamCatalogObserver records every call it receives, so dispatch tests
// can pin that the composite executor actually invokes telemetry (not just
// that it WOULD, if wired -- CHAOS-4431's standing order is telemetry ships
// wired in the same PR, and this is the test that proves the wiring, not
// just the Observe method's own existence).
type fakeTeamCatalogObserver struct {
	dispatches []teamCatalogDispatchCall
	rows       []teamCatalogRowsCall
}

type teamCatalogDispatchCall struct {
	provider   string
	entryPoint jobruntime.TeamCatalogEntryPoint
	outcome    jobruntime.TeamCatalogOutcome
}

type teamCatalogRowsCall struct {
	provider string
	table    jobruntime.TeamCatalogTable
	count    int
}

func (observer *fakeTeamCatalogObserver) ObserveTeamCatalogDispatch(provider string, entryPoint jobruntime.TeamCatalogEntryPoint, outcome jobruntime.TeamCatalogOutcome) error {
	observer.dispatches = append(observer.dispatches, teamCatalogDispatchCall{provider, entryPoint, outcome})
	return nil
}

func (observer *fakeTeamCatalogObserver) ObserveTeamCatalogRowsWritten(provider string, table jobruntime.TeamCatalogTable, count int) error {
	observer.rows = append(observer.rows, teamCatalogRowsCall{provider, table, count})
	return nil
}

// fakeTeamCatalogCollector records every call it receives and returns a
// canned result/error, so dispatch tests can pin exactly what the composite
// executor handed it without a real credential resolver, HTTP client, or
// ClickHouse connection.
type fakeTeamCatalogCollector struct {
	gotRef        providersync.TeamCatalogReference
	gotCredential providerfoundation.Credential
	gotSelections providersync.TeamCatalogSelections
	result        providersync.TeamCatalogResult
	err           error
}

func (collector *fakeTeamCatalogCollector) CollectTeamCatalog(
	_ context.Context,
	ref providersync.TeamCatalogReference,
	credential providerfoundation.Credential,
	_ *providerfoundation.HTTPClient,
	selections providersync.TeamCatalogSelections,
	_ time.Time,
) (providersync.TeamCatalogResult, error) {
	collector.gotRef, collector.gotCredential, collector.gotSelections = ref, credential, selections
	return collector.result, collector.err
}

type fakeTeamCatalogFallbackExecutor struct {
	gotOrgID, gotRunID, gotProvider string
	summary                         map[string]any
	err                             error
}

func (executor *fakeTeamCatalogFallbackExecutor) Discover(_ context.Context, orgID, runID, provider string) (map[string]any, error) {
	executor.gotOrgID, executor.gotRunID, executor.gotProvider = orgID, runID, provider
	return executor.summary, executor.err
}

type fakeProviderClientResolver struct {
	credential providerfoundation.Credential
	client     *providerfoundation.HTTPClient
	err        error
}

func (resolver *fakeProviderClientResolver) ResolveClient(context.Context, string, string, string) (providerfoundation.Credential, *providerfoundation.HTTPClient, error) {
	return resolver.credential, resolver.client, resolver.err
}

// TestTeamCatalogDiscoveryExecutorRoutesNativeProvidersToTheirCollector pins
// the core CHAOS-4431 dispatch rule: a provider with a registered native
// collector never reaches the bridge fallback, and the collector receives
// the claim-free reference plus the resolved credential/client. Selections
// are always "every surface" here (Teams/Projects/Members all true):
// src/dev_health_ops/workers/team_autoimport.py:98-103 documents that
// run_team_autoimport_strict -- what this seam mirrors -- never consults
// sync_options, unlike the separate, selection-gated post-sync path.
func TestTeamCatalogDiscoveryExecutorRoutesNativeProvidersToTheirCollector(t *testing.T) {
	collector := &fakeTeamCatalogCollector{result: providersync.TeamCatalogResult{
		TeamsWritten: 3, TeamKeys: []string{"ENG", "OPS"},
	}}
	fallback := &fakeTeamCatalogFallbackExecutor{}
	credential := providerfoundation.Credential{Provider: "linear", ID: "cred-1"}
	observer := &fakeTeamCatalogObserver{}
	executor := &TeamCatalogDiscoveryExecutor{
		Native:   map[string]providersync.TeamCatalogCollector{"linear": collector},
		Fallback: fallback,
		Clients:  &fakeProviderClientResolver{credential: credential},
		Observer: observer,
	}
	summary, err := executor.Discover(context.Background(), testOrg, testRun, "linear")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
		provider: "linear", entryPoint: jobruntime.TeamCatalogEntryPointReferenceDiscovery, outcome: jobruntime.TeamCatalogOutcomeNative,
	}) {
		t.Fatalf("observer dispatches=%+v", observer.dispatches)
	}
	if len(observer.rows) != 5 {
		t.Fatalf("observer rows=%+v, want one call per destination table", observer.rows)
	}
	foundTeamsRow := false
	for _, row := range observer.rows {
		if row.table == jobruntime.TeamCatalogTableTeams {
			foundTeamsRow = true
			if row.count != 3 {
				t.Fatalf("teams row count=%d want=3", row.count)
			}
		}
	}
	if !foundTeamsRow {
		t.Fatalf("no teams row observed: %+v", observer.rows)
	}
	if fallback.gotProvider != "" {
		t.Fatalf("native provider reached the bridge fallback: %+v", fallback)
	}
	if collector.gotRef.OrgID != testOrg || collector.gotRef.SyncRunID != testRun {
		t.Fatalf("collector ref=%+v want org=%q run=%q", collector.gotRef, testOrg, testRun)
	}
	if collector.gotCredential.Provider != credential.Provider || collector.gotCredential.ID != credential.ID {
		t.Fatalf("collector credential=%+v want=%+v", collector.gotCredential, credential)
	}
	if !collector.gotSelections.Teams || !collector.gotSelections.Projects || !collector.gotSelections.Members {
		t.Fatalf("collector selections=%+v want every surface selected (strict semantics)", collector.gotSelections)
	}
	if summary["provider"] != "linear" || summary["outcome"] != "native" {
		t.Fatalf("summary=%#v", summary)
	}
	keys, ok := summary["reference_team_keys"].([]string)
	if !ok || len(keys) != 2 {
		t.Fatalf("summary reference_team_keys=%#v", summary["reference_team_keys"])
	}
}

// TestTeamCatalogDiscoveryExecutorFallsBackForUnregisteredProviders pins the
// complement: a provider with no native collector goes through the bridge,
// exactly as it does today, untouched.
func TestTeamCatalogDiscoveryExecutorFallsBackForUnregisteredProviders(t *testing.T) {
	fallback := &fakeTeamCatalogFallbackExecutor{summary: map[string]any{"provider": "github", "outcome": "bridge"}}
	observer := &fakeTeamCatalogObserver{}
	executor := &TeamCatalogDiscoveryExecutor{
		Native:   map[string]providersync.TeamCatalogCollector{"linear": &fakeTeamCatalogCollector{}},
		Fallback: fallback,
		Observer: observer,
	}
	summary, err := executor.Discover(context.Background(), testOrg, testRun, "github")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if fallback.gotOrgID != testOrg || fallback.gotRunID != testRun || fallback.gotProvider != "github" {
		t.Fatalf("fallback saw org=%q run=%q provider=%q", fallback.gotOrgID, fallback.gotRunID, fallback.gotProvider)
	}
	if summary["outcome"] != "bridge" {
		t.Fatalf("summary=%#v", summary)
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
		provider: "github", entryPoint: jobruntime.TeamCatalogEntryPointReferenceDiscovery, outcome: jobruntime.TeamCatalogOutcomeBridge,
	}) {
		t.Fatalf("observer dispatches=%+v", observer.dispatches)
	}
}

// TestTeamCatalogDiscoveryExecutorFailsClosedWhenUnconstructed matches the
// nil-safety convention every other executor in this package follows.
func TestTeamCatalogDiscoveryExecutorFailsClosedWhenUnconstructed(t *testing.T) {
	var nilExecutor *TeamCatalogDiscoveryExecutor
	if _, err := nilExecutor.Discover(context.Background(), testOrg, testRun, "linear"); !errors.Is(err, ErrReferenceDiscoveryUnavailable) {
		t.Fatalf("nil executor error=%v want=%v", err, ErrReferenceDiscoveryUnavailable)
	}
	zeroExecutor := &TeamCatalogDiscoveryExecutor{}
	if _, err := zeroExecutor.Discover(context.Background(), testOrg, testRun, "github"); !errors.Is(err, ErrReferenceDiscoveryUnavailable) {
		t.Fatalf("zero-value executor (no fallback) error=%v want=%v", err, ErrReferenceDiscoveryUnavailable)
	}
	nativeOnly := &TeamCatalogDiscoveryExecutor{Native: map[string]providersync.TeamCatalogCollector{"linear": &fakeTeamCatalogCollector{}}}
	if _, err := nativeOnly.Discover(context.Background(), testOrg, testRun, "linear"); !errors.Is(err, ErrReferenceDiscoveryUnavailable) {
		t.Fatalf("native executor with no Clients error=%v want=%v", err, ErrReferenceDiscoveryUnavailable)
	}
}
