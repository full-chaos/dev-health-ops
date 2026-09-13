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

func (resolver *fakeTeamCatalogSelectionsResolver) ResolveSelections(context.Context, string, string, string, bool) (providersync.TeamCatalogSelections, map[string]any, error) {
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
	credential := providerfoundation.Credential{Provider: "linear", ID: "cred-1"}
	observer := &fakeTeamCatalogObserver{}
	syncOptions := map[string]any{"auto_import_teams": true}
	executor := &TeamCatalogDiscoveryExecutor{
		Native:  map[string]providersync.TeamCatalogCollector{"linear": collector},
		Clients: &fakeProviderClientResolver{credential: credential, integrationID: "integration-1"},
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
	if len(observer.rows) != 13 {
		t.Fatalf("observer rows=%+v, want one call per destination table (CHAOS-4444 added 3: teams_staged_for_review, team_memberships_staged_for_review, team_drift_changes_superseded; CHAOS-4530 added 1: projects_without_key)", observer.rows)
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
// pins the CORRECTED behavior (CHAOS-4431 codex review, converging with
// CHAOS-4437's Python fix independently found by the same review, team-lead
// ruling 2026-08-28): with every CHAOS-4323 selection off, this STRICT-mode
// seam still calls the native collector (never the bridge) -- unlike
// Python's non-strict early exit (team_autoimport_linear.py:421), strict
// mode's sprint/cycle discovery is unconditional reference data, so it must
// never be skipped just because every writable category is off. Only
// teams/members/projects stay at zero. The discovery ledger still reaches
// status=success and unit dispatch still proceeds regardless: with no teams
// selected, "reference_team_keys" is vacuously satisfied (empty), and
// ReferenceReadbackVerifier.Verify checks "reference_sprint_ids" for real.
// TestTeamCatalogDiscoveryExecutorReportsRosterPreservationFailure pins the
// team-lead ruling (2026-08-28): a collector that returns success but flags
// TeamCatalogResult.RosterPreservationFailed must record the dedicated
// outcome, not the generic "native" one -- so that choice (no collector
// makes it today) is never silently indistinguishable from a clean run.
func TestTeamCatalogDiscoveryExecutorReportsRosterPreservationFailure(t *testing.T) {
	collector := &fakeTeamCatalogCollector{result: providersync.TeamCatalogResult{RosterPreservationFailed: true}}
	observer := &fakeTeamCatalogObserver{}
	executor := &TeamCatalogDiscoveryExecutor{
		Native:  map[string]providersync.TeamCatalogCollector{"linear": collector},
		Clients: &fakeProviderClientResolver{credential: providerfoundation.Credential{Provider: "linear"}},
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
	collector := &fakeTeamCatalogCollector{result: providersync.TeamCatalogResult{
		SprintsWritten: 2, SprintIDs: []string{"linear:cycle:1", "linear:cycle:2"},
	}}
	observer := &fakeTeamCatalogObserver{}
	executor := &TeamCatalogDiscoveryExecutor{
		Native:     map[string]providersync.TeamCatalogCollector{"linear": collector},
		Clients:    &fakeProviderClientResolver{},
		Selections: &fakeTeamCatalogSelectionsResolver{selections: providersync.TeamCatalogSelections{}},
		Observer:   observer,
	}
	summary, err := executor.Discover(context.Background(), testOrg, testRun, "linear")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	// The collector IS called -- strict mode's sprint/cycle discovery is
	// unconditional reference data, never gated on selections.
	if collector.gotRef.OrgID != testOrg {
		t.Fatalf("collector was not called despite strict mode: %+v", collector.gotRef)
	}
	if collector.gotSelections.Any() {
		t.Fatalf("resolver selections leaked as non-empty: %+v", collector.gotSelections)
	}
	teamKeys, ok := summary["reference_team_keys"].([]string)
	if !ok || len(teamKeys) != 0 {
		t.Fatalf("summary reference_team_keys=%#v, want an empty slice (teams never selected)", summary["reference_team_keys"])
	}
	sprintIDs, ok := summary["reference_sprint_ids"].([]string)
	if !ok || len(sprintIDs) != 2 {
		t.Fatalf("summary reference_sprint_ids=%#v, want the 2 sprints the (unconditional) collector call reported", summary["reference_sprint_ids"])
	}
	if summary["provider"] != "linear" || summary["outcome"] != "native" {
		t.Fatalf("summary=%#v", summary)
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
		provider: "linear", entryPoint: jobruntime.TeamCatalogEntryPointReferenceDiscovery, outcome: jobruntime.TeamCatalogOutcomeNative,
	}) {
		t.Fatalf("observer dispatches=%+v", observer.dispatches)
	}
	foundSprintsRow := false
	for _, row := range observer.rows {
		if row.table == jobruntime.TeamCatalogTableSprints {
			foundSprintsRow = true
			if row.count != 2 {
				t.Fatalf("sprints row count=%d want=2", row.count)
			}
		} else if row.count != 0 {
			t.Fatalf("non-sprints table reported a nonzero count with every selection off: %+v", row)
		}
	}
	if !foundSprintsRow {
		t.Fatalf("no sprints row observed: %+v", observer.rows)
	}

	// Prove the readback claim directly against the real verifier: teams are
	// vacuously satisfied (empty, never checked), sprints are checked for
	// real against whatever the checker reports missing.
	verifier, err := NewReferenceReadbackVerifier(&alwaysMissingReadbackChecker{})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.Verify(context.Background(), testOrg, "linear", summary); err == nil {
		t.Fatalf("Verify should fail: 2 claimed sprint ids are reported missing by the checker")
	}
}

// alwaysMissingReadbackChecker would fail any non-vacuous Verify call by
// reporting every expected key as missing -- used to prove
// TestTeamCatalogDiscoveryExecutorSkipsNativeProviderWithEverySelectionOff's
// sprint claim really is checked for real, not short-circuited the way the
// (now vacuous) team-keys claim is.
type alwaysMissingReadbackChecker struct{}

func (*alwaysMissingReadbackChecker) MissingTeamKeys(_ context.Context, _, _ string, expected []string) ([]string, error) {
	return expected, nil
}

func (*alwaysMissingReadbackChecker) MissingSprintIDs(_ context.Context, _, _ string, expected []string) ([]string, error) {
	return expected, nil
}

// TestTeamCatalogDiscoveryExecutorNoOpsForProvidersWithNoImportCapability
// pins the complement to the wiring-bug guard above: a provider with no
// import capability at all (never registered a real populate() in Python,
// and never will be in Native -- e.g. atlassian) gets a clean, empty no-op
// instead of an error. There is no Python bridge left to fall through to.
func TestTeamCatalogDiscoveryExecutorNoOpsForProvidersWithNoImportCapability(t *testing.T) {
	observer := &fakeTeamCatalogObserver{}
	executor := &TeamCatalogDiscoveryExecutor{
		Native:   map[string]providersync.TeamCatalogCollector{"linear": &fakeTeamCatalogCollector{}},
		Observer: observer,
	}
	summary, err := executor.Discover(context.Background(), testOrg, testRun, "atlassian")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if summary["provider"] != "atlassian" || summary["outcome"] != "not_import_capable" {
		t.Fatalf("summary=%#v", summary)
	}
	teamKeys, ok := summary["reference_team_keys"].([]string)
	if !ok || len(teamKeys) != 0 {
		t.Fatalf("summary reference_team_keys=%#v, want an empty slice", summary["reference_team_keys"])
	}
	sprintIDs, ok := summary["reference_sprint_ids"].([]string)
	if !ok || len(sprintIDs) != 0 {
		t.Fatalf("summary reference_sprint_ids=%#v, want an empty slice", summary["reference_sprint_ids"])
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0] != (teamCatalogDispatchCall{
		provider: "atlassian", entryPoint: jobruntime.TeamCatalogEntryPointReferenceDiscovery, outcome: jobruntime.TeamCatalogOutcomeNotImportCapable,
	}) {
		t.Fatalf("observer dispatches=%+v", observer.dispatches)
	}

	// The empty claim keys must verify trivially -- prove it against a
	// checker that would fail any non-vacuous claim.
	verifier, err := NewReferenceReadbackVerifier(&alwaysMissingReadbackChecker{})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.Verify(context.Background(), testOrg, "atlassian", summary); err != nil {
		t.Fatalf("Verify should vacuously succeed on an empty claim: %v", err)
	}
}

// TestTeamCatalogDiscoveryExecutorFailsClosedWhenAnImportCapableProviderHasNoCollector
// pins the wiring-bug guard: a provider this codebase KNOWS can write real
// reference data (linear/jira/github/gitlab) missing from Native is a
// registration bug, never a legitimate no-op, and must fail loudly rather
// than silently report an empty, unverified result.
func TestTeamCatalogDiscoveryExecutorFailsClosedWhenAnImportCapableProviderHasNoCollector(t *testing.T) {
	executor := &TeamCatalogDiscoveryExecutor{
		Native: map[string]providersync.TeamCatalogCollector{"linear": &fakeTeamCatalogCollector{}},
	}
	if _, err := executor.Discover(context.Background(), testOrg, testRun, "github"); !errors.Is(err, ErrReferenceDiscoveryUnavailable) {
		t.Fatalf("github missing from Native: error=%v want=%v", err, ErrReferenceDiscoveryUnavailable)
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
