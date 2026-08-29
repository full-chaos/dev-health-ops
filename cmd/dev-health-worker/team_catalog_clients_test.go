package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// fakeTeamCatalogObserver records every call it receives, so dispatch tests
// can pin that the autoimport bridge actually invokes telemetry (CHAOS-4431
// standing order: telemetry ships wired in the same PR, not just defined).
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

// linearCollectorSpy records whether/how it was called, standing in for a
// real TeamCatalogCollector so dispatch tests need no credential resolver,
// HTTP client, or ClickHouse connection.
type linearCollectorSpy struct {
	gotRef        providersync.TeamCatalogReference
	gotSelections providersync.TeamCatalogSelections
	called        bool
	result        providersync.TeamCatalogResult
	err           error
}

func (collector *linearCollectorSpy) CollectTeamCatalog(
	_ context.Context,
	ref providersync.TeamCatalogReference,
	_ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient,
	selections providersync.TeamCatalogSelections,
	_ time.Time,
) (providersync.TeamCatalogResult, error) {
	collector.called = true
	collector.gotRef, collector.gotSelections = ref, selections
	return collector.result, collector.err
}

type fakeCoordinatorBridge struct {
	teamAutoImportCalled bool
	err                  error
}

func (bridge *fakeCoordinatorBridge) Dispatch(context.Context, syncdispatchruntime.DispatchSyncRunArgs) error {
	return nil
}
func (bridge *fakeCoordinatorBridge) Finalize(context.Context, syncdispatchruntime.FinalizeSyncRunArgs) error {
	return nil
}
func (bridge *fakeCoordinatorBridge) Discover(context.Context, syncdispatchruntime.ReferenceDiscoveryArgs) error {
	return nil
}
func (bridge *fakeCoordinatorBridge) TeamAutoImport(context.Context, syncdispatchruntime.DomainReference) error {
	bridge.teamAutoImportCalled = true
	return bridge.err
}

type fakeAutoimportClientResolver struct {
	integrationID string
}

func (resolver fakeAutoimportClientResolver) ResolveClient(context.Context, string, string, string) (providerfoundation.Credential, *providerfoundation.HTTPClient, string, error) {
	return providerfoundation.Credential{Provider: "linear"}, &providerfoundation.HTTPClient{}, resolver.integrationID, nil
}

const (
	testOrg = "20000000-0000-4000-8000-000000000002"
	testRun = "30000000-0000-4000-8000-000000000003"
)

type fakeAutoimportSelectionsResolver struct {
	selections  providersync.TeamCatalogSelections
	syncOptions map[string]any
	err         error
}

func (resolver fakeAutoimportSelectionsResolver) ResolveSelections(context.Context, string, string, string, bool) (providersync.TeamCatalogSelections, map[string]any, error) {
	return resolver.selections, resolver.syncOptions, resolver.err
}

type fakeAutoimportSourceResolver struct {
	ids []string
	err error
}

func (resolver fakeAutoimportSourceResolver) ResolveSourceExternalIDs(context.Context, string, string) ([]string, error) {
	return resolver.ids, resolver.err
}

func TestTeamCatalogAutoimportBridgeRoutesNativeProviderDirectly(t *testing.T) {
	native := &linearCollectorSpy{result: providersync.TeamCatalogResult{TeamsWritten: 1, MembersWritten: 2, RepoOwnershipWritten: 4}}
	observer := &fakeTeamCatalogObserver{}
	syncOptions := map[string]any{"owner": "acme-group"}
	bridge := &teamCatalogAutoimportBridge{
		CoordinatorBridge: &fakeCoordinatorBridge{},
		resolveProvider:   func(context.Context, string, string) (string, error) { return "linear", nil },
		native:            map[string]providersync.TeamCatalogCollector{"linear": native},
		clients:           fakeAutoimportClientResolver{integrationID: "integration-1"},
		selections: fakeAutoimportSelectionsResolver{
			selections:  providersync.TeamCatalogSelections{Teams: true, Members: true},
			syncOptions: syncOptions,
		},
		sources:  fakeAutoimportSourceResolver{ids: []string{"src-1"}},
		observer: observer,
	}
	err := bridge.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{
		OrganizationID: testOrg, SyncRunID: testRun,
	})
	if err != nil {
		t.Fatalf("TeamAutoImport: %v", err)
	}
	if !native.called {
		t.Fatal("native collector was not called")
	}
	if bridge.CoordinatorBridge.(*fakeCoordinatorBridge).teamAutoImportCalled {
		t.Fatal("wrapped bridge's TeamAutoImport was called despite a native provider")
	}
	if !native.gotSelections.Teams || native.gotSelections.Projects || !native.gotSelections.Members {
		t.Fatalf("selections not threaded through: %+v", native.gotSelections)
	}
	if native.gotRef.IntegrationID != "integration-1" {
		t.Fatalf("ref.IntegrationID=%q want=%q", native.gotRef.IntegrationID, "integration-1")
	}
	if native.gotRef.SyncOptions["owner"] != "acme-group" {
		t.Fatalf("ref.SyncOptions=%+v want the resolved map threaded through", native.gotRef.SyncOptions)
	}
	if native.gotRef.Strict {
		t.Fatalf("ref.Strict=%t want=false (post-sync mirrors non-strict run_team_autoimport)", native.gotRef.Strict)
	}
	if len(native.gotRef.SourceExternalIDs) != 1 || native.gotRef.SourceExternalIDs[0] != "src-1" {
		t.Fatalf("ref.SourceExternalIDs=%+v want the resolved ids threaded through", native.gotRef.SourceExternalIDs)
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
		provider: "linear", entryPoint: jobruntime.TeamCatalogEntryPointPostSync, outcome: jobruntime.TeamCatalogOutcomeNative,
	}) {
		t.Fatalf("observer dispatches=%+v", observer.dispatches)
	}
	if len(observer.rows) != 9 {
		t.Fatalf("observer rows=%+v, want one call per destination table", observer.rows)
	}
	foundRepoOwnershipRow := false
	for _, row := range observer.rows {
		if row.table == jobruntime.TeamCatalogTableTeamRepoOwnership {
			foundRepoOwnershipRow = true
			if row.count != 4 {
				t.Fatalf("team_repo_ownership row count=%d want=4", row.count)
			}
		}
	}
	if !foundRepoOwnershipRow {
		t.Fatalf("no team_repo_ownership row observed: %+v", observer.rows)
	}
}

// TestTeamCatalogAutoimportBridgeDegradesNativeFailureToNonfatal pins the
// team-lead ruling (2026-08-28): a native collector error on the POST-SYNC
// path must not fail/retry the River job -- it mirrors Python's non-strict
// run_team_autoimport, which catches every populator exception and returns
// a zero summary. The strict reference-discovery seam
// (TeamCatalogDiscoveryExecutor) has no such decorator and keeps
// propagating; this distinction is the whole reason the two paths are
// separate types.
func TestTeamCatalogAutoimportBridgeDegradesNativeFailureToNonfatal(t *testing.T) {
	collectErr := errors.New("linear API rate limited")
	native := &linearCollectorSpy{err: collectErr}
	fallback := &fakeCoordinatorBridge{}
	observer := &fakeTeamCatalogObserver{}
	bridge := &teamCatalogAutoimportBridge{
		CoordinatorBridge: fallback,
		resolveProvider:   func(context.Context, string, string) (string, error) { return "linear", nil },
		native:            map[string]providersync.TeamCatalogCollector{"linear": native},
		clients:           fakeAutoimportClientResolver{},
		selections:        fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true}},
		observer:          observer,
	}
	err := bridge.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{
		OrganizationID: testOrg, SyncRunID: testRun,
	})
	if err != nil {
		t.Fatalf("TeamAutoImport must not propagate a native collector error on the post-sync path, got: %v", err)
	}
	if !native.called {
		t.Fatal("native collector was not called")
	}
	if fallback.teamAutoImportCalled {
		t.Fatal("wrapped bridge was called despite a registered native collector")
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
		provider: "linear", entryPoint: jobruntime.TeamCatalogEntryPointPostSync, outcome: jobruntime.TeamCatalogOutcomeNativeFailedNonfatal,
	}) {
		t.Fatalf("observer dispatches=%+v", observer.dispatches)
	}
	if len(observer.rows) != 0 {
		t.Fatalf("observer rows=%+v, want none on a failed collection", observer.rows)
	}
}

// TestTeamCatalogAutoimportBridgeDegradesResolverFailuresToNonfatal pins the
// team-lead ruling (2026-08-28): EVERY failure past provider resolution on
// the post-sync path -- selections, credential/client, or source ids, not
// only the collector call itself -- must degrade to a non-fatal zero
// result, or a resolver blip causes the exact retry storm the collector-
// error degrade (TestTeamCatalogAutoimportBridgeDegradesNativeFailureToNonfatal)
// already exists to prevent.
func TestTeamCatalogAutoimportBridgeDegradesResolverFailuresToNonfatal(t *testing.T) {
	okSelections := fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true}}
	for _, testCase := range []struct {
		name       string
		selections syncdispatchruntime.TeamCatalogSelectionsResolver
		clients    syncdispatchruntime.ProviderClientResolver
		sources    syncdispatchruntime.SourceExternalIDsResolver
	}{
		{
			name:       "selections resolver error",
			selections: fakeAutoimportSelectionsResolver{err: errors.New("sync_configurations query failed")},
			clients:    fakeAutoimportClientResolver{},
		},
		{
			name:       "client resolver error",
			selections: okSelections,
			clients:    erroringClientResolver{err: errors.New("credential resolution failed")},
		},
		{
			name:       "source ids resolver error",
			selections: okSelections,
			clients:    fakeAutoimportClientResolver{},
			sources:    fakeAutoimportSourceResolver{err: errors.New("source inventory incomplete")},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			native := &linearCollectorSpy{}
			fallback := &fakeCoordinatorBridge{}
			observer := &fakeTeamCatalogObserver{}
			bridge := &teamCatalogAutoimportBridge{
				CoordinatorBridge: fallback,
				resolveProvider:   func(context.Context, string, string) (string, error) { return "linear", nil },
				native:            map[string]providersync.TeamCatalogCollector{"linear": native},
				clients:           testCase.clients,
				selections:        testCase.selections,
				sources:           testCase.sources,
				observer:          observer,
			}
			err := bridge.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{
				OrganizationID: testOrg, SyncRunID: testRun,
			})
			if err != nil {
				t.Fatalf("TeamAutoImport must not propagate a resolver error on the post-sync path, got: %v", err)
			}
			if fallback.teamAutoImportCalled {
				t.Fatal("wrapped bridge was called despite a registered native collector")
			}
			if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
				provider: "linear", entryPoint: jobruntime.TeamCatalogEntryPointPostSync, outcome: jobruntime.TeamCatalogOutcomeNativeFailedNonfatal,
			}) {
				t.Fatalf("observer dispatches=%+v", observer.dispatches)
			}
		})
	}
}

type erroringClientResolver struct{ err error }

func (resolver erroringClientResolver) ResolveClient(context.Context, string, string, string) (providerfoundation.Credential, *providerfoundation.HTTPClient, string, error) {
	return providerfoundation.Credential{}, nil, "", resolver.err
}

// TestTeamCatalogAutoimportBridgeReportsRosterPreservationFailure mirrors
// TestTeamCatalogDiscoveryExecutorReportsRosterPreservationFailure for the
// post-sync seam.
func TestTeamCatalogAutoimportBridgeReportsRosterPreservationFailure(t *testing.T) {
	native := &linearCollectorSpy{result: providersync.TeamCatalogResult{RosterPreservationFailed: true}}
	observer := &fakeTeamCatalogObserver{}
	bridge := &teamCatalogAutoimportBridge{
		CoordinatorBridge: &fakeCoordinatorBridge{},
		resolveProvider:   func(context.Context, string, string) (string, error) { return "linear", nil },
		native:            map[string]providersync.TeamCatalogCollector{"linear": native},
		clients:           fakeAutoimportClientResolver{},
		selections:        fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true}},
		observer:          observer,
	}
	if err := bridge.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{
		OrganizationID: testOrg, SyncRunID: testRun,
	}); err != nil {
		t.Fatalf("TeamAutoImport: %v", err)
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
		provider: "linear", entryPoint: jobruntime.TeamCatalogEntryPointPostSync, outcome: jobruntime.TeamCatalogOutcomeRosterPreservationFailed,
	}) {
		t.Fatalf("observer dispatches=%+v", observer.dispatches)
	}
}

// TestTeamCatalogAutoimportBridgeReportsSkippedCollectorResult is the
// CHAOS-4432 regression proof (team-lead confirmation, 2026-08-28): a
// collector that made NO writes and returned a clean, successful zero
// result (providersync.TeamCatalogResult.Skipped -- GitLab's non-strict
// walk-failure Python-parity fix) must record the DEDICATED skipped
// outcome, never the plain "native" success outcome a bare zero result
// would otherwise be indistinguishable from ("zero-row success = defect").
// Distinct from TestTeamCatalogAutoimportBridgeSkipsNativeProviderWithNoSelection,
// which never even calls the collector -- here the collector IS called and
// itself chooses to report a skip.
func TestTeamCatalogAutoimportBridgeReportsSkippedCollectorResult(t *testing.T) {
	native := &linearCollectorSpy{result: providersync.TeamCatalogResult{Skipped: true, SkipReason: "group_projects_fetch_failed"}}
	observer := &fakeTeamCatalogObserver{}
	bridge := &teamCatalogAutoimportBridge{
		CoordinatorBridge: &fakeCoordinatorBridge{},
		resolveProvider:   func(context.Context, string, string) (string, error) { return "linear", nil },
		native:            map[string]providersync.TeamCatalogCollector{"linear": native},
		clients:           fakeAutoimportClientResolver{},
		selections:        fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true}},
		observer:          observer,
	}
	if err := bridge.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{
		OrganizationID: testOrg, SyncRunID: testRun,
	}); err != nil {
		t.Fatalf("TeamAutoImport: %v", err)
	}
	if !native.called {
		t.Fatal("native collector was not called -- this test proves the collector's OWN skip decision, not the pre-collector no-selection skip")
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
		provider: "linear", entryPoint: jobruntime.TeamCatalogEntryPointPostSync, outcome: jobruntime.TeamCatalogOutcomeSkipped,
	}) {
		t.Fatalf("observer dispatches=%+v, want a single TeamCatalogOutcomeSkipped dispatch", observer.dispatches)
	}
	if len(observer.rows) != 0 {
		t.Fatalf("observer rows=%+v, want no rows-written calls for a skipped result", observer.rows)
	}
}

func TestTeamCatalogAutoimportBridgeSkipsNativeProviderWithNoSelection(t *testing.T) {
	native := &linearCollectorSpy{}
	fallback := &fakeCoordinatorBridge{}
	observer := &fakeTeamCatalogObserver{}
	bridge := &teamCatalogAutoimportBridge{
		CoordinatorBridge: fallback,
		resolveProvider:   func(context.Context, string, string) (string, error) { return "linear", nil },
		native:            map[string]providersync.TeamCatalogCollector{"linear": native},
		clients:           fakeAutoimportClientResolver{},
		selections:        fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{}},
		observer:          observer,
	}
	if err := bridge.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{
		OrganizationID: testOrg, SyncRunID: testRun,
	}); err != nil {
		t.Fatalf("TeamAutoImport: %v", err)
	}
	if native.called {
		t.Fatal("native collector was called despite no selection")
	}
	if fallback.teamAutoImportCalled {
		t.Fatal("wrapped bridge was called despite no selection -- nothing to import either way")
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
		provider: "linear", entryPoint: jobruntime.TeamCatalogEntryPointPostSync, outcome: jobruntime.TeamCatalogOutcomeSkipped,
	}) {
		t.Fatalf("observer dispatches=%+v", observer.dispatches)
	}
}

func TestTeamCatalogAutoimportBridgeFallsBackForNonNativeProviders(t *testing.T) {
	fallback := &fakeCoordinatorBridge{}
	observer := &fakeTeamCatalogObserver{}
	bridge := &teamCatalogAutoimportBridge{
		CoordinatorBridge: fallback,
		resolveProvider:   func(context.Context, string, string) (string, error) { return "github", nil },
		native:            map[string]providersync.TeamCatalogCollector{"linear": &linearCollectorSpy{}},
		observer:          observer,
	}
	if err := bridge.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{
		OrganizationID: testOrg, SyncRunID: testRun,
	}); err != nil {
		t.Fatalf("TeamAutoImport: %v", err)
	}
	if !fallback.teamAutoImportCalled {
		t.Fatal("non-native provider did not fall through to the wrapped bridge")
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
		provider: "github", entryPoint: jobruntime.TeamCatalogEntryPointPostSync, outcome: jobruntime.TeamCatalogOutcomeBridge,
	}) {
		t.Fatalf("observer dispatches=%+v", observer.dispatches)
	}
}

func TestTeamCatalogAutoimportBridgeFallsBackWhenProviderResolutionFails(t *testing.T) {
	fallback := &fakeCoordinatorBridge{}
	bridge := &teamCatalogAutoimportBridge{
		CoordinatorBridge: fallback,
		resolveProvider:   func(context.Context, string, string) (string, error) { return "", errors.New("resolution failed") },
		native:            map[string]providersync.TeamCatalogCollector{"linear": &linearCollectorSpy{}},
	}
	if err := bridge.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{
		OrganizationID: testOrg, SyncRunID: testRun,
	}); err != nil {
		t.Fatalf("TeamAutoImport: %v", err)
	}
	if !fallback.teamAutoImportCalled {
		t.Fatal("provider-resolution failure did not fall through to the wrapped bridge")
	}
}

func TestTeamCatalogAutoimportBridgeFailsClosedWhenUnconstructed(t *testing.T) {
	var nilBridge *teamCatalogAutoimportBridge
	if err := nilBridge.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{}); !errors.Is(err, syncdispatchruntime.ErrInvalidBridge) {
		t.Fatalf("nil bridge error=%v want=%v", err, syncdispatchruntime.ErrInvalidBridge)
	}
	zeroBridge := &teamCatalogAutoimportBridge{}
	if err := zeroBridge.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{}); !errors.Is(err, syncdispatchruntime.ErrInvalidBridge) {
		t.Fatalf("zero-value bridge error=%v want=%v", err, syncdispatchruntime.ErrInvalidBridge)
	}
}
