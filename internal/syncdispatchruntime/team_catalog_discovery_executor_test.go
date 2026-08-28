package syncdispatchruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

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

type fakeDiscoveryExecutor struct {
	gotOrgID, gotRunID, gotProvider string
	summary                         map[string]any
	err                             error
}

func (executor *fakeDiscoveryExecutor) Discover(_ context.Context, orgID, runID, provider string) (map[string]any, error) {
	executor.gotOrgID, executor.gotRunID, executor.gotProvider = orgID, runID, provider
	return executor.summary, executor.err
}

type fakeProviderClientResolver struct {
	credential providerfoundation.Credential
	client     *providerfoundation.HTTPClient
	err        error
}

func (resolver *fakeProviderClientResolver) ResolveClient(context.Context, string, string) (providerfoundation.Credential, *providerfoundation.HTTPClient, error) {
	return resolver.credential, resolver.client, resolver.err
}

type fakeTeamCatalogSelectionsResolver struct {
	selections providersync.TeamCatalogSelections
	err        error
}

func (resolver *fakeTeamCatalogSelectionsResolver) ResolveSelections(context.Context, string, string) (providersync.TeamCatalogSelections, error) {
	return resolver.selections, resolver.err
}

// TestTeamCatalogDiscoveryExecutorRoutesNativeProvidersToTheirCollector pins
// the core CHAOS-4431 dispatch rule: a provider with a registered native
// collector never reaches the bridge fallback, and the collector receives
// the claim-free reference plus the resolved credential/client/selections.
func TestTeamCatalogDiscoveryExecutorRoutesNativeProvidersToTheirCollector(t *testing.T) {
	collector := &fakeTeamCatalogCollector{result: providersync.TeamCatalogResult{
		TeamsWritten: 3, TeamKeys: []string{"ENG", "OPS"},
	}}
	fallback := &fakeDiscoveryExecutor{}
	credential := providerfoundation.Credential{Provider: "linear", ID: "cred-1"}
	executor := &TeamCatalogDiscoveryExecutor{
		Native:     map[string]providersync.TeamCatalogCollector{"linear": collector},
		Fallback:   fallback,
		Clients:    &fakeProviderClientResolver{credential: credential},
		Selections: &fakeTeamCatalogSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true}},
	}
	summary, err := executor.Discover(context.Background(), testOrg, testRun, "linear")
	if err != nil {
		t.Fatalf("Discover: %v", err)
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
	if !collector.gotSelections.Teams || collector.gotSelections.Projects || collector.gotSelections.Members {
		t.Fatalf("collector selections=%+v", collector.gotSelections)
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
	fallback := &fakeDiscoveryExecutor{summary: map[string]any{"provider": "github", "outcome": "bridge"}}
	executor := &TeamCatalogDiscoveryExecutor{
		Native:   map[string]providersync.TeamCatalogCollector{"linear": &fakeTeamCatalogCollector{}},
		Fallback: fallback,
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
}

// TestTeamCatalogDiscoveryExecutorSkipsNativeProviderWithNoSelection pins the
// third outcome: a native provider whose org has every CHAOS-4323 flag off
// writes nothing and never calls the bridge either -- there is nothing to
// import either way, so paying for the Python round trip would be pure waste.
func TestTeamCatalogDiscoveryExecutorSkipsNativeProviderWithNoSelection(t *testing.T) {
	collector := &fakeTeamCatalogCollector{}
	fallback := &fakeDiscoveryExecutor{}
	executor := &TeamCatalogDiscoveryExecutor{
		Native:     map[string]providersync.TeamCatalogCollector{"linear": collector},
		Fallback:   fallback,
		Clients:    &fakeProviderClientResolver{},
		Selections: &fakeTeamCatalogSelectionsResolver{selections: providersync.TeamCatalogSelections{}},
	}
	summary, err := executor.Discover(context.Background(), testOrg, testRun, "linear")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if collector.gotRef.OrgID != "" {
		t.Fatalf("collector was called despite no selection: %+v", collector.gotRef)
	}
	if fallback.gotProvider != "" {
		t.Fatalf("bridge was called despite no selection: %+v", fallback)
	}
	if summary["outcome"] != "skipped_selection" {
		t.Fatalf("summary=%#v", summary)
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
		t.Fatalf("native executor with no Clients/Selections error=%v want=%v", err, ErrReferenceDiscoveryUnavailable)
	}
}
