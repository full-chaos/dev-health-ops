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
	credential    providerfoundation.Credential
	client        *providerfoundation.HTTPClient
	integrationID string
	err           error
}

func (resolver *fakeProviderClientResolver) ResolveClient(context.Context, string, string, string) (providerfoundation.Credential, *providerfoundation.HTTPClient, string, error) {
	return resolver.credential, resolver.client, resolver.integrationID, resolver.err
}

type fakeTeamCatalogSelectionsResolver struct {
	selections  providersync.TeamCatalogSelections
	syncOptions map[string]any
	err         error
}

func (resolver *fakeTeamCatalogSelectionsResolver) ResolveSelections(context.Context, string, string, string) (providersync.TeamCatalogSelections, map[string]any, error) {
	return resolver.selections, resolver.syncOptions, resolver.err
}

type fakeSourceExternalIDsResolver struct {
	ids []string
	err error
}

func (resolver *fakeSourceExternalIDsResolver) ResolveSourceExternalIDs(context.Context, string, string) ([]string, error) {
	return resolver.ids, resolver.err
}

// TestTeamCatalogDiscoveryExecutorRoutesNativeProvidersToTheirCollector pins
// the core CHAOS-4431 dispatch rule: a provider with a registered native
// collector never reaches the bridge fallback, and the collector receives
// the claim-free reference plus the resolved credential/client AND the
// resolved CHAOS-4323 selections (team-lead ruling 2026-08-28: parity with
// run_team_autoimport_strict's selection-blind defect is not the spec --
// this path gates on selections exactly like the post-sync path does).
func TestTeamCatalogDiscoveryExecutorRoutesNativeProvidersToTheirCollector(t *testing.T) {
	collector := &fakeTeamCatalogCollector{result: providersync.TeamCatalogResult{
		TeamsWritten: 3, RepoOwnershipWritten: 9, TeamKeys: []string{"ENG", "OPS"},
	}}
	fallback := &fakeTeamCatalogFallbackExecutor{}
	credential := providerfoundation.Credential{Provider: "linear", ID: "cred-1"}
	observer := &fakeTeamCatalogObserver{}
	syncOptions := map[string]any{"auto_import_teams": true}
	executor := &TeamCatalogDiscoveryExecutor{
		Native:   map[string]providersync.TeamCatalogCollector{"linear": collector},
		Fallback: fallback,
		Clients:  &fakeProviderClientResolver{credential: credential, integrationID: "integration-1"},
		Selections: &fakeTeamCatalogSelectionsResolver{
			selections:  providersync.TeamCatalogSelections{Teams: true, Projects: true, Members: true},
			syncOptions: syncOptions,
		},
		Sources:  &fakeSourceExternalIDsResolver{ids: []string{"src-1", "src-2"}},
		Observer: observer,
	}
	summary, err := executor.Discover(context.Background(), testOrg, testRun, "linear")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if collector.gotRef.IntegrationID != "integration-1" {
		t.Fatalf("collector ref.IntegrationID=%q want=%q", collector.gotRef.IntegrationID, "integration-1")
	}
	if collector.gotRef.SyncOptions["auto_import_teams"] != true {
		t.Fatalf("collector ref.SyncOptions=%+v want the resolved map threaded through", collector.gotRef.SyncOptions)
	}
	if !collector.gotRef.Strict {
		t.Fatalf("collector ref.Strict=%t want=true (reference-discovery mirrors run_team_autoimport_strict)", collector.gotRef.Strict)
	}
	if len(collector.gotRef.SourceExternalIDs) != 2 || collector.gotRef.SourceExternalIDs[0] != "src-1" {
		t.Fatalf("collector ref.SourceExternalIDs=%+v want the resolved ids threaded through", collector.gotRef.SourceExternalIDs)
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
		provider: "linear", entryPoint: jobruntime.TeamCatalogEntryPointReferenceDiscovery, outcome: jobruntime.TeamCatalogOutcomeNative,
	}) {
		t.Fatalf("observer dispatches=%+v", observer.dispatches)
	}
	if len(observer.rows) != 6 {
		t.Fatalf("observer rows=%+v, want one call per destination table", observer.rows)
	}
	foundTeamsRow, foundRepoOwnershipRow := false, false
	for _, row := range observer.rows {
		switch row.table {
		case jobruntime.TeamCatalogTableTeams:
			foundTeamsRow = true
			if row.count != 3 {
				t.Fatalf("teams row count=%d want=3", row.count)
			}
		case jobruntime.TeamCatalogTableTeamRepoOwnership:
			foundRepoOwnershipRow = true
			if row.count != 9 {
				t.Fatalf("team_repo_ownership row count=%d want=9", row.count)
			}
		}
	}
	if !foundTeamsRow {
		t.Fatalf("no teams row observed: %+v", observer.rows)
	}
	if !foundRepoOwnershipRow {
		t.Fatalf("no team_repo_ownership row observed (RepoOwnershipWritten must get its own table label, not share team_project_ownership's): %+v", observer.rows)
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
		t.Fatalf("collector selections=%+v want every surface selected (resolver returned all three true)", collector.gotSelections)
	}
	if summary["provider"] != "linear" || summary["outcome"] != "native" {
		t.Fatalf("summary=%#v", summary)
	}
	keys, ok := summary["reference_team_keys"].([]string)
	if !ok || len(keys) != 2 {
		t.Fatalf("summary reference_team_keys=%#v", summary["reference_team_keys"])
	}
}

// TestTeamCatalogDiscoveryExecutorSkipsNativeProviderWithEverySelectionOff
// pins lane-4437's executed key-resolution answer (relayed by team-lead,
// 2026-08-28): with every CHAOS-4323 selection off, the native path must
// call neither the collector nor the bridge, and its summary must carry NO
// "reference_team_keys"/"reference_sprint_ids" key at all (their absence,
// not an empty slice) -- ReferenceReadbackVerifier.Verify is a no-op
// exactly in that case (clickhouse_readback.go:228-236), so the discovery
// ledger still reaches status=success and unit dispatch still proceeds
// (native_dispatch_sync_run_service.go:242-253), with zero ClickHouse
// writes and zero bridge calls.
// TestTeamCatalogDiscoveryExecutorReportsRosterPreservationFailure pins the
// team-lead ruling (2026-08-28): a collector that returns success but flags
// TeamCatalogResult.RosterPreservationFailed must record the dedicated
// outcome, not the generic "native" one -- so that choice (no collector
// makes it today) is never silently indistinguishable from a clean run.
func TestTeamCatalogDiscoveryExecutorReportsRosterPreservationFailure(t *testing.T) {
	collector := &fakeTeamCatalogCollector{result: providersync.TeamCatalogResult{RosterPreservationFailed: true}}
	observer := &fakeTeamCatalogObserver{}
	executor := &TeamCatalogDiscoveryExecutor{
		Native:   map[string]providersync.TeamCatalogCollector{"linear": collector},
		Fallback: &fakeTeamCatalogFallbackExecutor{},
		Clients:  &fakeProviderClientResolver{credential: providerfoundation.Credential{Provider: "linear"}},
		Selections: &fakeTeamCatalogSelectionsResolver{
			selections: providersync.TeamCatalogSelections{Teams: true},
		},
		Observer: observer,
	}
	if _, err := executor.Discover(context.Background(), testOrg, testRun, "linear"); err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
		provider: "linear", entryPoint: jobruntime.TeamCatalogEntryPointReferenceDiscovery, outcome: jobruntime.TeamCatalogOutcomeRosterPreservationFailed,
	}) {
		t.Fatalf("observer dispatches=%+v", observer.dispatches)
	}
}

func TestTeamCatalogDiscoveryExecutorSkipsNativeProviderWithEverySelectionOff(t *testing.T) {
	collector := &fakeTeamCatalogCollector{}
	fallback := &fakeTeamCatalogFallbackExecutor{}
	observer := &fakeTeamCatalogObserver{}
	executor := &TeamCatalogDiscoveryExecutor{
		Native:     map[string]providersync.TeamCatalogCollector{"linear": collector},
		Fallback:   fallback,
		Clients:    &fakeProviderClientResolver{},
		Selections: &fakeTeamCatalogSelectionsResolver{selections: providersync.TeamCatalogSelections{}},
		Observer:   observer,
	}
	summary, err := executor.Discover(context.Background(), testOrg, testRun, "linear")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if collector.gotRef.OrgID != "" {
		t.Fatalf("collector was called despite every selection being off: %+v", collector.gotRef)
	}
	if fallback.gotProvider != "" {
		t.Fatalf("bridge was called despite every selection being off: %+v", fallback)
	}
	if _, present := summary["reference_team_keys"]; present {
		t.Fatalf("summary must omit reference_team_keys entirely, got=%#v", summary)
	}
	if _, present := summary["reference_sprint_ids"]; present {
		t.Fatalf("summary must omit reference_sprint_ids entirely, got=%#v", summary)
	}
	if summary["provider"] != "linear" {
		t.Fatalf("summary=%#v", summary)
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
		provider: "linear", entryPoint: jobruntime.TeamCatalogEntryPointReferenceDiscovery, outcome: jobruntime.TeamCatalogOutcomeSkipped,
	}) {
		t.Fatalf("observer dispatches=%+v", observer.dispatches)
	}
	if len(observer.rows) != 0 {
		t.Fatalf("observer rows=%+v, want none", observer.rows)
	}

	// Prove the no-op claim directly against the real verifier, not just by
	// asserting the summary's shape: a reviewer should not have to trust
	// that "absent key" is what ReferenceReadbackVerifier.Verify actually
	// treats as vacuous success.
	verifier, err := NewReferenceReadbackVerifier(&alwaysMissingReadbackChecker{})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.Verify(context.Background(), testOrg, "linear", summary); err != nil {
		t.Fatalf("Verify should no-op on a summary with no claimed keys, got: %v", err)
	}
}

// alwaysMissingReadbackChecker would fail any non-vacuous Verify call by
// reporting every expected key as missing -- used to prove
// TestTeamCatalogDiscoveryExecutorSkipsNativeProviderWithEverySelectionOff's
// summary really does short-circuit Verify before it ever queries ClickHouse.
type alwaysMissingReadbackChecker struct{}

func (*alwaysMissingReadbackChecker) MissingTeamKeys(_ context.Context, _, _ string, expected []string) ([]string, error) {
	return expected, nil
}

func (*alwaysMissingReadbackChecker) MissingSprintIDs(_ context.Context, _, _ string, expected []string) ([]string, error) {
	return expected, nil
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
