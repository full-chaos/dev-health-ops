//go:build integration

package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// startSourceDiscoveryPostgres provisions a fresh Postgres (same DDL as
// materializer_integration_test.go's fixture, same package) with ONE jira
// integration and sync config that has NO integration_sources rows at all --
// the exact CHAOS-4602 starting condition ("Jira had no sources at all until
// [Python] 4584"). source_id is left NULL (no explicit scope) so the
// materializer's source-discovery call site fires.
func startSourceDiscoveryPostgres(t *testing.T) (materializerFixture, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, materializerFixtureDDL); err != nil {
		t.Fatal(err)
	}
	const (
		orgID         = "00000000-0000-4000-8000-0000000000bb"
		configID      = "00000000-0000-4000-8000-000000002001"
		integrationID = "00000000-0000-4000-8000-000000002002"
		datasetID     = "00000000-0000-4000-8000-000000002004"
		jobID         = "00000000-0000-4000-8000-000000002005"
	)
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO integrations VALUES ($1::uuid,$2,'jira',NULL,TRUE,'{}'::jsonb)`, []any{integrationID, orgID}},
		{`INSERT INTO organizations VALUES ($1::uuid,'community')`, []any{orgID}},
		{`INSERT INTO feature_flags VALUES ('00000000-0000-4000-8000-000000002007','canonical_incident_ingestion','community',TRUE)`, nil},
		// planner_managed=TRUE: a REAL scheduled occurrence always has this
		// true (Materialize's own eligibility gate requires
		// occurrence.ConfigPlannerManaged), and it is exactly the condition
		// under which loadPlanSources' metadata.planner_managed_sync_config_id
		// filter applies (codex review round 1, P1) -- planner_managed=FALSE
		// here would have silently bypassed that filter and hidden the bug.
		{`INSERT INTO sync_configurations (id,org_id,sync_targets,sync_options,integration_id,is_active,source_id,planner_managed,provider,updated_at) VALUES ($1::uuid,$2,'["work-items"]'::jsonb,'{"schedule_cron":"0 * * * *"}'::jsonb,$3::uuid,TRUE,NULL,TRUE,'jira',now())`, []any{configID, orgID, integrationID}},
		{`INSERT INTO integration_datasets VALUES ($1::uuid,$2,$3::uuid,'work-items',TRUE,'{}'::jsonb)`, []any{datasetID, orgID, integrationID}},
		{`INSERT INTO scheduled_jobs (id,org_id,sync_config_id,job_type,schedule_cron,timezone,status,is_running) VALUES ($1::uuid,$2,$3::uuid,'sync','0 * * * *','UTC',0,FALSE)`, []any{jobID, orgID, configID}},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	fixture := materializerFixture{pool: pool, occurrence: PendingOccurrence{
		ID: "occurrence:v1:scheduled", IdentityVersion: OccurrenceIdentityVersion,
		OrgID: orgID, ConfigID: configID, JobID: jobID,
		ScheduledFor: time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC),
		ConfigActive: true, ConfigPlannerManaged: true, JobStatus: 0, JobType: "sync",
	}}
	return fixture, integrationID
}

// fakeSourceDiscoveryExecutor is a SourceDiscoveryExecutor test double: it
// records every call and, when configured, inserts sources directly (via the
// SAME pool the materializer's domain writes use) to simulate what
// NativeSourceDiscoveryService's real provider-API path would produce --
// the provider-API parsing itself is covered separately (source_discovery_
// test.go's fake-HTTPDoer unit tests), so this fixture isolates exactly what
// this file exists to prove: the materializer calls the step at the right
// time, with the right args, and survives it failing.
type fakeSourceDiscoveryExecutor struct {
	t             *testing.T
	pool          *pgxpool.Pool
	calls         []SourceDiscoveryArgs
	sourcesAtCall []int
	sourcesToSeed int
	failWith      error
}

func (fake *fakeSourceDiscoveryExecutor) Discover(ctx context.Context, args SourceDiscoveryArgs) (SourceDiscoveryReport, error) {
	fake.t.Helper()
	fake.calls = append(fake.calls, args)
	var existingCount int
	if err := fake.pool.QueryRow(ctx, `SELECT count(*) FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid`, args.OrgID, args.IntegrationID).Scan(&existingCount); err != nil {
		fake.t.Fatal(err)
	}
	fake.sourcesAtCall = append(fake.sourcesAtCall, existingCount)
	if fake.failWith != nil {
		return SourceDiscoveryReport{}, fake.failWith
	}
	if args.ExplicitScope {
		// Mirrors the REAL NativeSourceDiscoveryService.Discover's own
		// early return (source_discovery.go): an explicit-scope config
		// never seeds anything, it just reports skipped. The materializer
		// now calls Discover unconditionally (codex review finding: the
		// old caller-side sourceID==nil bypass made this skip invisible to
		// telemetry) and relies on THIS early return, not on never being
		// called at all.
		return SourceDiscoveryReport{Outcome: SourceDiscoveryOutcomeSkipped}, nil
	}
	created := 0
	for i := 0; i < fake.sourcesToSeed; i++ {
		// Mirrors NativeSourceDiscoveryService.upsertSources' own
		// planner_managed_sync_config_id stamping (codex review round 1,
		// P1): without it, loadPlanSources would never see these rows for a
		// planner-managed occurrence and the "units get planned" assertion
		// below would fail even though sources exist.
		metadataFields := map[string]any{}
		if args.PlannerManaged && args.ConfigID != "" {
			metadataFields["planner_managed_sync_config_id"] = args.ConfigID
		}
		metadata, _ := json.Marshal(metadataFields)
		if _, err := fake.pool.Exec(ctx, `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES (gen_random_uuid(),$1,$2::uuid,$3,'project',$4,$4,$4,TRUE,$5::json,now(),now())`,
			args.OrgID, args.IntegrationID, args.Provider, "PROJ"+string(rune('A'+i)), metadata,
		); err != nil {
			fake.t.Fatal(err)
		}
		created++
	}
	return SourceDiscoveryReport{Outcome: SourceDiscoveryOutcomeCreated, Created: created}, nil
}

func TestMaterializeRunsSourceDiscoveryBeforeUnitPlanningAndCreatesSources(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeSourceDiscoveryExecutor{t: t, pool: fixture.pool, sourcesToSeed: 3}
	materializer.WithSourceDiscovery(fake)

	plan, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence)
	if err != nil {
		t.Fatal(err)
	}

	if len(fake.calls) != 1 {
		t.Fatalf("Discover called %d times, want exactly 1", len(fake.calls))
	}
	call := fake.calls[0]
	if call.Provider != "jira" || call.IntegrationID != integrationID || call.ExplicitScope {
		t.Fatalf("Discover args = %#v", call)
	}
	if !call.PlannerManaged || call.ConfigID != fixture.occurrence.ConfigID {
		t.Fatalf("Discover args PlannerManaged/ConfigID = %v/%q, want true/%q", call.PlannerManaged, call.ConfigID, fixture.occurrence.ConfigID)
	}
	// The core CHAOS-4602 ordering claim: at the moment Discover ran, this
	// occurrence's integration had ZERO integration_sources rows -- discovery
	// really did run BEFORE unit planning read sources, not as an
	// afterthought once sources already existed some other way.
	if fake.sourcesAtCall[0] != 0 {
		t.Fatalf("integration_sources count at Discover() call = %d, want 0 (discovery must run before any source exists)", fake.sourcesAtCall[0])
	}

	var sourceCount int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT count(*) FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid`, fixture.occurrence.OrgID, integrationID).Scan(&sourceCount); err != nil {
		t.Fatal(err)
	}
	if sourceCount != 3 {
		t.Fatalf("integration_sources count after Materialize = %d, want 3 (the discovery step's own creations)", sourceCount)
	}

	// The assertion codex review (round 1, P1) proved missing: sources
	// existing is not the same as sources being PLANNABLE. Without the
	// planner_managed_sync_config_id tag, loadPlanSources would see 3 rows
	// in integration_sources and still plan ZERO units for this
	// planner-managed occurrence.
	var unitCount int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT count(*) FROM public.sync_run_units WHERE sync_run_id=$1::uuid`, plan.SyncRunID).Scan(&unitCount); err != nil {
		t.Fatal(err)
	}
	if unitCount == 0 {
		t.Fatal("sync_run_units count = 0 after Materialize -- discovered sources exist but were never planned (the planner_managed_sync_config_id tagging bug this test exists to catch)")
	}
}

func TestMaterializeSkipsSourceDiscoveryForExplicitScopeConfig(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	// Pin the config to one explicit source (config.source_id set) --
	// discovery must not run at all, matching loadPlanSources' own
	// pre-existing sourceID branch.
	sourceID := "00000000-0000-4000-8000-000000002099"
	if _, err := fixture.pool.Exec(context.Background(), `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES ($1::uuid,$2,$3::uuid,'jira','project','PINNED','PINNED','PINNED',TRUE,'{}'::jsonb,now(),now())`,
		sourceID, fixture.occurrence.OrgID, integrationID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(context.Background(), `UPDATE public.sync_configurations SET source_id=$1::uuid WHERE id=$2::uuid`, sourceID, fixture.occurrence.ConfigID); err != nil {
		t.Fatal(err)
	}
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeSourceDiscoveryExecutor{t: t, pool: fixture.pool, sourcesToSeed: 5}
	materializer.WithSourceDiscovery(fake)

	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); err != nil {
		t.Fatal(err)
	}

	// codex review finding: Discover IS now called (so its own skip path
	// records the telemetry an operator needs), just with ExplicitScope
	// true -- it must still never touch the source rows, asserted below.
	if len(fake.calls) != 1 {
		t.Fatalf("Discover called %d times for an explicit-scope config, want exactly 1", len(fake.calls))
	}
	if !fake.calls[0].ExplicitScope {
		t.Fatalf("Discover call args = %#v, want ExplicitScope=true", fake.calls[0])
	}
	var sourceCount int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT count(*) FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid`, fixture.occurrence.OrgID, integrationID).Scan(&sourceCount); err != nil {
		t.Fatal(err)
	}
	if sourceCount != 1 {
		t.Fatalf("integration_sources count = %d, want exactly the pinned row (discovery must not have widened it)", sourceCount)
	}
}

func TestMaterializeSourceDiscoveryFailureDoesNotFailTheOccurrence(t *testing.T) {
	fixture, _ := startSourceDiscoveryPostgres(t)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeSourceDiscoveryExecutor{t: t, pool: fixture.pool, failWith: errSourceDiscoveryTestFailure}
	materializer.WithSourceDiscovery(fake)

	// A discovery failure is loud (logged), never fatal to the occurrence --
	// same posture as executedProof refresh failures. This config plans zero
	// units (no sources exist and discovery just failed to create any), which
	// is itself a legitimate, already-supported outcome, not an error.
	if _, err := materializeAndCommit(t, fixture, materializer, fixture.occurrence); err != nil {
		t.Fatalf("Materialize() with a failing discovery step = %v, want nil (discovery failure must not fail the occurrence)", err)
	}
	if len(fake.calls) != 1 {
		t.Fatalf("Discover called %d times, want exactly 1", len(fake.calls))
	}
}

var errSourceDiscoveryTestFailure = &sourceDiscoveryTestError{"simulated jira API failure"}

type sourceDiscoveryTestError struct{ message string }

func (err *sourceDiscoveryTestError) Error() string { return err.message }

func TestUpsertSourcesIsIdempotentAndNeverFlipsIsEnabled(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	service := &NativeSourceDiscoveryService{domainPool: fixture.pool, telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	ctx := context.Background()
	orgID := fixture.occurrence.OrgID

	first := []discoveredSource{
		{ExternalID: "CHAOS", SourceType: "project", Name: "Chaos", FullName: "Chaos Engineering", Metadata: map[string]any{"project_id": "1"}},
		{ExternalID: "PLAT", SourceType: "project", Name: "Platform", FullName: "Platform", Metadata: map[string]any{"project_id": "2"}},
	}
	const configID = "00000000-0000-4000-8000-000000002001"
	created, existing, err := service.upsertSources(ctx, orgID, integrationID, "jira", first, time.Now().UTC(), configID, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if created != 2 || existing != 0 {
		t.Fatalf("first upsertSources() = created=%d existing=%d, want 2,0", created, existing)
	}
	// Codex review (round 1, P1): loadPlanSources only admits a
	// planner-managed parent's sources tagged with its own config id.
	var taggedConfigID string
	if err := fixture.pool.QueryRow(ctx, `SELECT metadata->>'planner_managed_sync_config_id' FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND external_id='CHAOS'`, orgID, integrationID).Scan(&taggedConfigID); err != nil {
		t.Fatal(err)
	}
	if taggedConfigID != configID {
		t.Fatalf("planner_managed_sync_config_id = %q, want %q -- a planner-managed parent's own loadPlanSources query would never see this row", taggedConfigID, configID)
	}

	// Simulate an operator disabling one of the two discovered sources.
	if _, err := fixture.pool.Exec(ctx, `UPDATE public.integration_sources SET is_enabled=FALSE WHERE org_id=$1 AND integration_id=$2::uuid AND external_id='PLAT'`, orgID, integrationID); err != nil {
		t.Fatal(err)
	}

	// Re-discovery: same two projects, PLAT's name changed upstream.
	second := []discoveredSource{
		{ExternalID: "CHAOS", SourceType: "project", Name: "Chaos", FullName: "Chaos Engineering", Metadata: map[string]any{"project_id": "1"}},
		{ExternalID: "PLAT", SourceType: "project", Name: "Platform Renamed", FullName: "Platform Renamed", Metadata: map[string]any{"project_id": "2"}},
	}
	created, existing, err = service.upsertSources(ctx, orgID, integrationID, "jira", second, time.Now().UTC(), configID, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if created != 0 || existing != 2 {
		t.Fatalf("second upsertSources() = created=%d existing=%d, want 0,2 (both rows already existed)", created, existing)
	}

	var isEnabled bool
	var name string
	if err := fixture.pool.QueryRow(ctx, `SELECT is_enabled, name FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND external_id='PLAT'`, orgID, integrationID).Scan(&isEnabled, &name); err != nil {
		t.Fatal(err)
	}
	if isEnabled {
		t.Fatal("re-discovery flipped is_enabled back to TRUE for an operator-disabled source; discovery must never touch is_enabled on an existing row")
	}
	if name != "Platform Renamed" {
		t.Fatalf("re-discovery did not refresh name for an existing row: got %q", name)
	}

	var total int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid`, orgID, integrationID).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("total integration_sources rows = %d, want 2 (idempotent -- no duplicate rows from re-discovery)", total)
	}
}

// TestUpsertSourcesTagsANewlyVisibleSourceForAnUnboundedConfig is the codex
// gate-round P1 case ledger row 8's original test did not prove: for a
// genuinely unbounded (all_repos-equivalent) config, CHAOS and PLAT are
// already tagged from an earlier pass, then a THIRD project (NEWPROJ)
// becomes visible on a later discovery pass. Gating the tag decision on
// "does any tagged row already exist" (the round-2 fix) would leave NEWPROJ
// untagged forever, invisible to loadPlanSources' tag-filtered SELECT even
// though it was validly discovered. Gating on the caller's durable
// `unbounded` signal instead must tag it.
func TestUpsertSourcesTagsANewlyVisibleSourceForAnUnboundedConfig(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	service := &NativeSourceDiscoveryService{domainPool: fixture.pool, telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	ctx := context.Background()
	orgID := fixture.occurrence.OrgID
	const configID = "00000000-0000-4000-8000-000000002002"

	bootstrap := []discoveredSource{
		{ExternalID: "CHAOS", SourceType: "project", Name: "Chaos", FullName: "Chaos Engineering", Metadata: map[string]any{"project_id": "1"}},
		{ExternalID: "PLAT", SourceType: "project", Name: "Platform", FullName: "Platform", Metadata: map[string]any{"project_id": "2"}},
	}
	if created, existing, err := service.upsertSources(ctx, orgID, integrationID, "jira", bootstrap, time.Now().UTC(), configID, true, true); err != nil {
		t.Fatal(err)
	} else if created != 2 || existing != 0 {
		t.Fatalf("bootstrap upsertSources() = created=%d existing=%d, want 2,0", created, existing)
	}

	// A new project becomes visible on a later pass. CHAOS/PLAT are already
	// tagged; NEWPROJ is not.
	later := []discoveredSource{
		{ExternalID: "CHAOS", SourceType: "project", Name: "Chaos", FullName: "Chaos Engineering", Metadata: map[string]any{"project_id": "1"}},
		{ExternalID: "PLAT", SourceType: "project", Name: "Platform", FullName: "Platform", Metadata: map[string]any{"project_id": "2"}},
		{ExternalID: "NEWPROJ", SourceType: "project", Name: "New Project", FullName: "New Project", Metadata: map[string]any{"project_id": "3"}},
	}
	created, existing, err := service.upsertSources(ctx, orgID, integrationID, "jira", later, time.Now().UTC(), configID, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if created != 1 || existing != 2 {
		t.Fatalf("later upsertSources() = created=%d existing=%d, want 1,2", created, existing)
	}

	var taggedConfigID string
	if err := fixture.pool.QueryRow(ctx, `SELECT metadata->>'planner_managed_sync_config_id' FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND external_id='NEWPROJ'`, orgID, integrationID).Scan(&taggedConfigID); err != nil {
		t.Fatal(err)
	}
	if taggedConfigID != configID {
		t.Fatalf("NEWPROJ planner_managed_sync_config_id = %q, want %q -- a newly-visible source for an unbounded config must be tagged just like the bootstrap rows, or loadPlanSources will never see it", taggedConfigID, configID)
	}
}

// TestUpsertSourcesNeverTagsANewlyVisibleSourceForABoundedJiraConfig is
// TestUpsertSourcesTagsANewlyVisibleSourceForAnUnboundedConfig's sibling for
// the opposite, previously-unproven direction (codex gate-round-8 P1): a
// Jira config explicitly scoped to ONE project (sync_options.project_key,
// isUnboundedDiscovery's new jira branch) must NOT tag a later-discovered
// project it never asked for, exactly like an explicit github/gitlab repo
// selection already doesn't. CHAOS is already tagged (the config's own
// explicit scope); PLAT becomes visible on a later discovery pass (a repo
// jira discovery has no owner/pattern filter to exclude by, unlike
// github/gitlab, so it is discovered but must stay untagged) and must never
// become plannable through loadPlanSources' tag-filtered SELECT.
func TestUpsertSourcesNeverTagsANewlyVisibleSourceForABoundedJiraConfig(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	service := &NativeSourceDiscoveryService{domainPool: fixture.pool, telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	ctx := context.Background()
	orgID := fixture.occurrence.OrgID
	const configID = "00000000-0000-4000-8000-000000002007"

	bootstrap := []discoveredSource{
		{ExternalID: "CHAOS", SourceType: "project", Name: "Chaos", FullName: "Chaos Engineering", Metadata: map[string]any{"project_id": "1"}},
	}
	unbounded := isUnboundedDiscovery("jira", map[string]any{"project_key": "CHAOS"})
	if unbounded {
		t.Fatal("test setup bug: isUnboundedDiscovery(\"jira\", project_key=\"CHAOS\") = true, want false")
	}
	if created, existing, err := service.upsertSources(ctx, orgID, integrationID, "jira", bootstrap, time.Now().UTC(), configID, true, unbounded); err != nil {
		t.Fatal(err)
	} else if created != 1 || existing != 0 {
		t.Fatalf("bootstrap upsertSources() = created=%d existing=%d, want 1,0", created, existing)
	}

	// PLAT becomes visible on a later pass -- the credential can now access
	// it, but this config was never scoped to it.
	later := []discoveredSource{
		{ExternalID: "CHAOS", SourceType: "project", Name: "Chaos", FullName: "Chaos Engineering", Metadata: map[string]any{"project_id": "1"}},
		{ExternalID: "PLAT", SourceType: "project", Name: "Platform", FullName: "Platform", Metadata: map[string]any{"project_id": "2"}},
	}
	created, existing, err := service.upsertSources(ctx, orgID, integrationID, "jira", later, time.Now().UTC(), configID, true, unbounded)
	if err != nil {
		t.Fatal(err)
	}
	if created != 1 || existing != 1 {
		t.Fatalf("later upsertSources() = created=%d existing=%d, want 1,1", created, existing)
	}

	var taggedConfigID *string
	if err := fixture.pool.QueryRow(ctx, `SELECT metadata->>'planner_managed_sync_config_id' FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND external_id='PLAT'`, orgID, integrationID).Scan(&taggedConfigID); err != nil {
		t.Fatal(err)
	}
	if taggedConfigID != nil {
		t.Fatalf("PLAT planner_managed_sync_config_id = %q, want NULL -- a Jira config explicitly scoped to CHAOS must never widen to a later-discovered project it never asked for", *taggedConfigID)
	}
}

// --- CHAOS-4629: Jira parity with #2036/CHAOS-4584's hardened Python
// discover_sources_for_integration behaviors -----------------------------

// mustUpsertJiraSourcesInOwnTx opens, runs, and commits a transaction for a
// direct upsertJiraSources call in a test -- mirroring how Discover itself
// now wraps the upsert and rebalanceJiraSourceRepoLimit in ONE transaction
// (codex review P1: two separate commits left a window where a rebalance
// failure after a successful over-limit upsert committed left the org over
// its entitlement with no automatic retry).
func mustUpsertJiraSourcesInOwnTx(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, service *NativeSourceDiscoveryService,
	orgID, integrationID string, sources []discoveredSource, now time.Time, configID string, plannerManaged, unbounded bool,
) (created, existing int, createdLower, discoveredLower map[string]struct{}, superseded int) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	created, existing, createdLower, discoveredLower, superseded, err = service.upsertJiraSources(ctx, tx, orgID, integrationID, sources, now, configID, plannerManaged, unbounded)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return created, existing, createdLower, discoveredLower, superseded
}

// mustRebalanceJiraSourceRepoLimitInOwnTx is mustUpsertJiraSourcesInOwnTx's
// sibling for a direct rebalanceJiraSourceRepoLimit call.
func mustRebalanceJiraSourceRepoLimitInOwnTx(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, service *NativeSourceDiscoveryService,
	orgID, integrationID string, createdLower, discoveredLower map[string]struct{},
) (capped, recovered int) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	capped, recovered, err = service.rebalanceJiraSourceRepoLimit(ctx, tx, orgID, integrationID, createdLower, discoveredLower)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return capped, recovered
}

// TestUpsertJiraSourcesCaseInsensitiveMatchUpdatesExistingRowInsteadOfDuplicating
// is the exact latent case-duplicate bug #2036 fixed on the Python side
// (CHAOS-4584 round 2): a pre-existing row was created (or manually
// inserted) with one casing; Jira's API always canonicalizes project keys
// on return, so re-discovery sees a different casing for the SAME project.
// Go's plain ON CONFLICT (org_id,integration_id,provider,external_id) is
// exact-match and would insert a second, case-variant row that
// double-schedules the same project -- upsertJiraSources' case-insensitive
// matching must update the existing row instead.
func TestUpsertJiraSourcesCaseInsensitiveMatchUpdatesExistingRowInsteadOfDuplicating(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	service := &NativeSourceDiscoveryService{domainPool: fixture.pool, telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	ctx := context.Background()
	orgID := fixture.occurrence.OrgID

	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES (gen_random_uuid(),$1,$2::uuid,'jira','project','eng','Eng (stale)','Eng (stale)',TRUE,'{}'::jsonb,now(),now())`,
		orgID, integrationID); err != nil {
		t.Fatal(err)
	}

	discovered := []discoveredSource{
		{ExternalID: "ENG", SourceType: "project", Name: "Engineering", FullName: "Engineering", Metadata: map[string]any{"project_id": "1"}},
	}
	created, existing, _, _, superseded := mustUpsertJiraSourcesInOwnTx(t, ctx, fixture.pool, service, orgID, integrationID, discovered, time.Now().UTC(), "", false, true)
	if created != 0 || existing != 1 || superseded != 0 {
		t.Fatalf("upsertJiraSources() = created=%d existing=%d superseded=%d, want 0,1,0 (case-insensitive match, no duplicate)", created, existing, superseded)
	}

	var total int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid`, orgID, integrationID).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("integration_sources count = %d, want 1 (no case-variant duplicate inserted)", total)
	}

	var externalID, name string
	if err := fixture.pool.QueryRow(ctx, `SELECT external_id, name FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid`, orgID, integrationID).Scan(&externalID, &name); err != nil {
		t.Fatal(err)
	}
	if externalID != "eng" {
		t.Fatalf("external_id = %q, want %q -- the SURVIVING row's external_id is never rewritten by a re-discovery pass", externalID, "eng")
	}
	if name != "Engineering" {
		t.Fatalf("name = %q, want %q -- an existing row's mutable fields ARE refreshed", name, "Engineering")
	}
}

// TestUpsertJiraSourcesSelfRepairsPreexistingCaseVariantPair is CHAOS-4584
// round 3's self-repair case: a case-variant PAIR already exists (e.g. one
// row created before this fix landed, one after). Re-discovery must fold
// every extra candidate into ONE surviving row rather than raising on
// (or silently picking an arbitrary member of) more than one match --
// preferring the row that is ALREADY ENABLED as the survivor, so an
// in-progress sync is never silently stopped by which row happened to sort
// first.
func TestUpsertJiraSourcesSelfRepairsPreexistingCaseVariantPair(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	service := &NativeSourceDiscoveryService{domainPool: fixture.pool, telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	ctx := context.Background()
	orgID := fixture.occurrence.OrgID

	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES (gen_random_uuid(),$1,$2::uuid,'jira','project','eng','Eng (disabled dup)','Eng',FALSE,'{}'::jsonb,now()-interval '1 day',now())`, []any{orgID, integrationID}},
		{`INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES (gen_random_uuid(),$1,$2::uuid,'jira','project','ENG','Eng (enabled survivor)','ENG',TRUE,'{}'::jsonb,now(),now())`, []any{orgID, integrationID}},
	}
	for _, statement := range statements {
		if _, err := fixture.pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}

	discovered := []discoveredSource{
		{ExternalID: "Eng", SourceType: "project", Name: "Engineering", FullName: "Engineering", Metadata: map[string]any{"project_id": "1"}},
	}
	mustUpsertJiraSourcesInOwnTx(t, ctx, fixture.pool, service, orgID, integrationID, discovered, time.Now().UTC(), "", false, true)

	var enabledCount int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND is_enabled`, orgID, integrationID).Scan(&enabledCount); err != nil {
		t.Fatal(err)
	}
	if enabledCount != 1 {
		t.Fatalf("enabled integration_sources count = %d, want exactly 1", enabledCount)
	}

	var survivorExternalID string
	if err := fixture.pool.QueryRow(ctx, `SELECT external_id FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND is_enabled`, orgID, integrationID).Scan(&survivorExternalID); err != nil {
		t.Fatal(err)
	}
	if survivorExternalID != "ENG" {
		t.Fatalf("surviving enabled row external_id = %q, want %q -- the ALREADY-ENABLED candidate must survive, never the disabled one", survivorExternalID, "ENG")
	}

	var loserMetadata []byte
	if err := fixture.pool.QueryRow(ctx, `SELECT metadata FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND external_id='eng'`, orgID, integrationID).Scan(&loserMetadata); err != nil {
		t.Fatal(err)
	}
	var loserFields map[string]any
	if err := json.Unmarshal(loserMetadata, &loserFields); err != nil {
		t.Fatal(err)
	}
	if loserFields["duplicate_of_external_id"] != "ENG" {
		t.Fatalf("loser metadata = %#v, want duplicate_of_external_id=%q", loserFields, "ENG")
	}
}

// TestRebalanceJiraSourceRepoLimitCapsOverflowPreferringNewlyCreatedSources
// ports discovery.py::_rebalance_jira_sources_against_repo_limit's core
// claim: an org already at its max_repos entitlement must not have real,
// already-relied-upon (pre-existing) sources silently disabled just because
// a discovery run ALSO discovered new ones -- the newly created rows are
// capped first.
func TestRebalanceJiraSourceRepoLimitCapsOverflowPreferringNewlyCreatedSources(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	service := &NativeSourceDiscoveryService{domainPool: fixture.pool, telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	ctx := context.Background()
	orgID := fixture.occurrence.OrgID
	// org tier is 'community' (startSourceDiscoveryPostgres's own seed) and
	// no tier_limits row exists, so resolveMaxReposLimit falls back to the
	// hardcoded default: max_repos=3.

	preexisting := []struct{ id, externalID string }{
		{"00000000-0000-4000-8000-0000000030a1", "AAA"},
		{"00000000-0000-4000-8000-0000000030a2", "BBB"},
	}
	for _, source := range preexisting {
		if _, err := fixture.pool.Exec(ctx, `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES ($1::uuid,$2,$3::uuid,'jira','project',$4,$4,$4,TRUE,'{}'::jsonb,now(),now())`,
			source.id, orgID, integrationID, source.externalID); err != nil {
			t.Fatal(err)
		}
	}

	discovered := []discoveredSource{
		{ExternalID: "AAA", SourceType: "project", Name: "AAA", FullName: "AAA", Metadata: map[string]any{}},
		{ExternalID: "BBB", SourceType: "project", Name: "BBB", FullName: "BBB", Metadata: map[string]any{}},
		{ExternalID: "CCC", SourceType: "project", Name: "CCC", FullName: "CCC", Metadata: map[string]any{}},
		{ExternalID: "DDD", SourceType: "project", Name: "DDD", FullName: "DDD", Metadata: map[string]any{}},
	}
	created, _, createdLower, discoveredLower, _ := mustUpsertJiraSourcesInOwnTx(t, ctx, fixture.pool, service, orgID, integrationID, discovered, time.Now().UTC(), "", false, true)
	if created != 2 {
		t.Fatalf("upsertJiraSources() created = %d, want 2 (CCC, DDD)", created)
	}

	mustRebalanceJiraSourceRepoLimitInOwnTx(t, ctx, fixture.pool, service, orgID, integrationID, createdLower, discoveredLower)

	var enabledCount int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND is_enabled`, orgID, integrationID).Scan(&enabledCount); err != nil {
		t.Fatal(err)
	}
	if enabledCount != 3 {
		t.Fatalf("enabled integration_sources count = %d, want 3 (max_repos)", enabledCount)
	}

	// Deterministic pick: capping sorts by external_id DESC and prefers
	// newly-created rows first, so of {DDD,CCC} (created) and {BBB,AAA}
	// (pre-existing), DDD is capped first.
	var isEnabled bool
	var metadataJSON []byte
	if err := fixture.pool.QueryRow(ctx, `SELECT is_enabled, metadata FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND external_id='DDD'`, orgID, integrationID).Scan(&isEnabled, &metadataJSON); err != nil {
		t.Fatal(err)
	}
	if isEnabled {
		t.Fatal("DDD is still enabled, want it capped (the newly-created row, preferred for capping over pre-existing AAA/BBB)")
	}
	var metadataFields map[string]any
	if err := json.Unmarshal(metadataJSON, &metadataFields); err != nil {
		t.Fatal(err)
	}
	if metadataFields["capped_by_repo_limit"] != true {
		t.Fatalf("DDD metadata = %#v, want capped_by_repo_limit=true", metadataFields)
	}

	for _, externalID := range []string{"AAA", "BBB", "CCC"} {
		var stillEnabled bool
		if err := fixture.pool.QueryRow(ctx, `SELECT is_enabled FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND external_id=$3`, orgID, integrationID, externalID).Scan(&stillEnabled); err != nil {
			t.Fatal(err)
		}
		if !stillEnabled {
			t.Fatalf("%s was disabled by capping, want it to stay enabled (only 1 row should be capped for a 1-row overflow)", externalID)
		}
	}
}

// TestRebalanceJiraSourceRepoLimitRecoversPreviouslyCappedRowWhenReconfirmed
// ports the recovery half of _rebalance_jira_sources_against_repo_limit: a
// previously cap-disabled row is re-enabled once the org's allowance
// (headroom) returns -- but ONLY if THIS discovery run reconfirmed the
// project still exists (discoveredLower), never a row discovery didn't even
// see this pass.
func TestRebalanceJiraSourceRepoLimitRecoversPreviouslyCappedRowWhenReconfirmed(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	service := &NativeSourceDiscoveryService{domainPool: fixture.pool, telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	ctx := context.Background()
	orgID := fixture.occurrence.OrgID

	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES (gen_random_uuid(),$1,$2::uuid,'jira','project','AAA','AAA','AAA',FALSE,'{"capped_by_repo_limit":true}'::jsonb,now(),now())`,
		orgID, integrationID); err != nil {
		t.Fatal(err)
	}
	// One enabled row keeps usage below max_repos=3 (headroom=2).
	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES (gen_random_uuid(),$1,$2::uuid,'jira','project','BBB','BBB','BBB',TRUE,'{}'::jsonb,now(),now())`,
		orgID, integrationID); err != nil {
		t.Fatal(err)
	}

	discoveredLower := map[string]struct{}{"aaa": {}, "bbb": {}}
	mustRebalanceJiraSourceRepoLimitInOwnTx(t, ctx, fixture.pool, service, orgID, integrationID, map[string]struct{}{}, discoveredLower)

	var isEnabled bool
	var metadataJSON []byte
	if err := fixture.pool.QueryRow(ctx, `SELECT is_enabled, metadata FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND external_id='AAA'`, orgID, integrationID).Scan(&isEnabled, &metadataJSON); err != nil {
		t.Fatal(err)
	}
	if !isEnabled {
		t.Fatal("AAA is still disabled, want it recovered (headroom exists and discovery just reconfirmed it)")
	}
	var metadataFields map[string]any
	if err := json.Unmarshal(metadataJSON, &metadataFields); err != nil {
		t.Fatal(err)
	}
	if _, stillCapped := metadataFields["capped_by_repo_limit"]; stillCapped {
		t.Fatalf("AAA metadata = %#v, want capped_by_repo_limit removed", metadataFields)
	}
}

// TestUpsertJiraSourcesSupersedesAMixedCaseWhitespacePaddedProviderRowOnRescope
// covers TWO codex findings against the SAME root cause: provider is
// unconstrained text, not write-time validated anywhere in this codebase
// (Integration.provider accepts "JIRA" or " JIRA " as freely as "jira", the
// same latent shape CHAOS-4584 itself had to account for on
// external_id/project_key). Round 3 found case (`provider='jira'` exact
// match); round 5 found whitespace surviving THAT fix
// (`lower(provider)='jira'` still doesn't trim). Both are fixed by the same
// change: no SQL-side provider predicate at all, filtered in Go via
// normalizeSourceKey like every other comparison in this file. A tagged
// legacy row with provider=" JIRA " must still be found and superseded on a
// bounded rescope -- otherwise OLD stays enabled (and permanently
// uncountable for repo-limit capping, since activeRepoUsageCountForLimit
// counts it toward usage with no provider filter at all).
func TestUpsertJiraSourcesSupersedesAMixedCaseWhitespacePaddedProviderRowOnRescope(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	service := &NativeSourceDiscoveryService{domainPool: fixture.pool, telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	ctx := context.Background()
	orgID := fixture.occurrence.OrgID
	const configID = "00000000-0000-4000-8000-000000003102"

	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES (gen_random_uuid(),$1,$2::uuid,' JIRA ','project','OLD','Old Project','OLD',TRUE,$3::jsonb,now(),now())`,
		orgID, integrationID, fmt.Sprintf(`{"planner_managed_sync_config_id":"%s"}`, configID)); err != nil {
		t.Fatal(err)
	}

	discovered := []discoveredSource{
		{ExternalID: "NEW", SourceType: "project", Name: "New Project", FullName: "New Project", Metadata: map[string]any{"project_id": "2"}},
	}
	_, _, _, _, superseded := mustUpsertJiraSourcesInOwnTx(t, ctx, fixture.pool, service, orgID, integrationID, discovered, time.Now().UTC(), configID, true, false)
	if superseded != 1 {
		t.Fatalf("superseded = %d, want 1 -- a mixed-case/whitespace-padded provider=' JIRA ' row must still be found and disabled on rescope", superseded)
	}

	var oldEnabled bool
	if err := fixture.pool.QueryRow(ctx, `SELECT is_enabled FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND external_id='OLD'`, orgID, integrationID).Scan(&oldEnabled); err != nil {
		t.Fatal(err)
	}
	if oldEnabled {
		t.Fatal("OLD (provider=' JIRA ') is still enabled -- a SQL-side provider predicate silently skipped it")
	}
}

// TestRebalanceJiraSourceRepoLimitCountsWhitespacePaddedProviderRowAsCandidate
// is round 5's other half: a whitespace-padded provider row must also be
// visible to capping/recovery, not just supersede.
func TestRebalanceJiraSourceRepoLimitCountsWhitespacePaddedProviderRowAsCandidate(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	service := &NativeSourceDiscoveryService{domainPool: fixture.pool, telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	ctx := context.Background()
	orgID := fixture.occurrence.OrgID
	// org tier 'community' -> max_repos=3.

	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES (gen_random_uuid(),$1,$2::uuid,' JIRA ','project','WSP','WSP','WSP',TRUE,'{}'::jsonb,now(),now())`,
		orgID, integrationID); err != nil {
		t.Fatal(err)
	}
	for _, externalID := range []string{"AAA", "BBB", "CCC"} {
		if _, err := fixture.pool.Exec(ctx, `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES (gen_random_uuid(),$1,$2::uuid,'jira','project',$3,$3,$3,TRUE,'{}'::jsonb,now(),now())`,
			orgID, integrationID, externalID); err != nil {
			t.Fatal(err)
		}
	}
	// 4 enabled sources (1 whitespace-padded + 3 canonical) for a
	// max_repos=3 org -- overflow=1. No new discovery this pass.
	mustRebalanceJiraSourceRepoLimitInOwnTx(t, ctx, fixture.pool, service, orgID, integrationID, map[string]struct{}{}, map[string]struct{}{})
	var enabledCount int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND is_enabled`, orgID, integrationID).Scan(&enabledCount); err != nil {
		t.Fatal(err)
	}
	if enabledCount != 3 {
		t.Fatalf("enabled count = %d, want 3 (max_repos) -- the whitespace-padded provider row must be a valid capping candidate, not invisible to the SELECT", enabledCount)
	}
}

// TestUpsertJiraSourcesSupersedesStaleScopedSourceOnRescope ports
// discovery.py::_supersede_stale_scoped_jira_sources: an explicitly-scoped
// planner-managed config (sync_options.project_key/project_id set, i.e.
// unbounded=false) that gets rescoped to a DIFFERENT project must disable
// the OLD project's source -- otherwise trigger_routing.py's own tag-scoped
// planning keeps syncing the project the operator just moved away from.
func TestUpsertJiraSourcesSupersedesStaleScopedSourceOnRescope(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	service := &NativeSourceDiscoveryService{domainPool: fixture.pool, telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	ctx := context.Background()
	orgID := fixture.occurrence.OrgID
	const configID = "00000000-0000-4000-8000-000000003100"

	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES (gen_random_uuid(),$1,$2::uuid,'jira','project','OLD','Old Project','OLD',TRUE,$3::jsonb,now(),now())`,
		orgID, integrationID, fmt.Sprintf(`{"planner_managed_sync_config_id":"%s"}`, configID)); err != nil {
		t.Fatal(err)
	}

	// Simulates the operator's PATCH moving project_key OLD -> NEW: this
	// discovery pass (already filtered upstream by discoverJira, see
	// TestDiscoverJiraFiltersToExplicitScope) returns only the NEW project.
	discovered := []discoveredSource{
		{ExternalID: "NEW", SourceType: "project", Name: "New Project", FullName: "New Project", Metadata: map[string]any{"project_id": "2"}},
	}
	created, existing, _, _, superseded := mustUpsertJiraSourcesInOwnTx(t, ctx, fixture.pool, service, orgID, integrationID, discovered, time.Now().UTC(), configID, true, false)
	if created != 1 || existing != 0 {
		t.Fatalf("upsertJiraSources() = created=%d existing=%d, want 1,0", created, existing)
	}
	if superseded != 1 {
		t.Fatalf("upsertJiraSources() superseded = %d, want 1 (OLD)", superseded)
	}

	var oldEnabled bool
	var oldMetadataJSON []byte
	if err := fixture.pool.QueryRow(ctx, `SELECT is_enabled, metadata FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND external_id='OLD'`, orgID, integrationID).Scan(&oldEnabled, &oldMetadataJSON); err != nil {
		t.Fatal(err)
	}
	if oldEnabled {
		t.Fatal("OLD is still enabled, want it disabled by the rescope")
	}
	var oldMetadata map[string]any
	if err := json.Unmarshal(oldMetadataJSON, &oldMetadata); err != nil {
		t.Fatal(err)
	}
	if oldMetadata["superseded_by_scope_change"] != true {
		t.Fatalf("OLD metadata = %#v, want superseded_by_scope_change=true", oldMetadata)
	}

	// codex review P1: NEW must ALSO get tagged, or loadPlanSources' own
	// tag-filtered SELECT never sees it and the rescoped project silently
	// plans zero units forever -- the original bug was gating this tag on
	// `unbounded` (bounded rescopes never got it); discoverJira's own
	// project_key/project_id filtering (TestDiscoverJiraFiltersToExplicitScope)
	// is what makes tagging unconditionally-on-planner-managed safe now.
	var newTaggedConfigID string
	if err := fixture.pool.QueryRow(ctx, `SELECT metadata->>'planner_managed_sync_config_id' FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND external_id='NEW'`, orgID, integrationID).Scan(&newTaggedConfigID); err != nil {
		t.Fatal(err)
	}
	if newTaggedConfigID != configID {
		t.Fatalf("NEW planner_managed_sync_config_id = %q, want %q so loadPlanSources can plan the rescoped project", newTaggedConfigID, configID)
	}
}

// TestUpsertJiraSourcesNeverSupersedesOnAnUnboundedConfig is the negative
// case discovery.py's own docstring calls out by name: this module's
// documented stale-handling policy (a project transiently missing from one
// run is never auto-disabled) must hold for an UNBOUNDED config -- supersede
// only ever applies to an explicitly-scoped one.
func TestUpsertJiraSourcesNeverSupersedesOnAnUnboundedConfig(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	service := &NativeSourceDiscoveryService{domainPool: fixture.pool, telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	ctx := context.Background()
	orgID := fixture.occurrence.OrgID
	const configID = "00000000-0000-4000-8000-000000003101"

	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES (gen_random_uuid(),$1,$2::uuid,'jira','project','STALE','Stale Project','STALE',TRUE,$3::jsonb,now(),now())`,
		orgID, integrationID, fmt.Sprintf(`{"planner_managed_sync_config_id":"%s"}`, configID)); err != nil {
		t.Fatal(err)
	}

	discovered := []discoveredSource{
		{ExternalID: "OTHER", SourceType: "project", Name: "Other Project", FullName: "Other Project", Metadata: map[string]any{}},
	}
	// unbounded=true: STALE is absent from this pass but must NOT be
	// superseded -- this is an unscoped "discover everything" config.
	_, _, _, _, superseded := mustUpsertJiraSourcesInOwnTx(t, ctx, fixture.pool, service, orgID, integrationID, discovered, time.Now().UTC(), configID, true, true)
	if superseded != 0 {
		t.Fatalf("upsertJiraSources() superseded = %d, want 0 (unbounded configs never supersede on absence)", superseded)
	}

	var stillEnabled bool
	if err := fixture.pool.QueryRow(ctx, `SELECT is_enabled FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND external_id='STALE'`, orgID, integrationID).Scan(&stillEnabled); err != nil {
		t.Fatal(err)
	}
	if !stillEnabled {
		t.Fatal("STALE was disabled, want it left untouched (unbounded config, absence is not disable-worthy)")
	}
}

// TestJiraDiscoveryRebalanceFailureRollsBackTheWholeUpsertAtomically is the
// GREEN counterpart to the codex round-1 P1 finding this session's
// TestRedRebalanceFailureLeavesOrgOverLimit proved on the pre-fix commit
// (5fc9c8c48): upsertJiraSources and rebalanceJiraSourceRepoLimit used to
// commit in two SEPARATE transactions, so a rebalance failure after a
// successful over-limit upsert left the org over max_repos with no
// automatic retry. They now share ONE transaction (mirroring Discover's own
// jira branch exactly): a rebalance failure must roll back the upsert too,
// leaving NOTHING durable -- not "upsert kept, cap skipped".
func TestJiraDiscoveryRebalanceFailureRollsBackTheWholeUpsertAtomically(t *testing.T) {
	fixture, integrationID := startSourceDiscoveryPostgres(t)
	service := &NativeSourceDiscoveryService{domainPool: fixture.pool, telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	ctx := context.Background()
	orgID := fixture.occurrence.OrgID
	// org tier 'community' (startSourceDiscoveryPostgres's own seed) -> max_repos=3.

	discovered := []discoveredSource{
		{ExternalID: "AAA", SourceType: "project", Name: "AAA", FullName: "AAA", Metadata: map[string]any{}},
		{ExternalID: "BBB", SourceType: "project", Name: "BBB", FullName: "BBB", Metadata: map[string]any{}},
		{ExternalID: "CCC", SourceType: "project", Name: "CCC", FullName: "CCC", Metadata: map[string]any{}},
		{ExternalID: "DDD", SourceType: "project", Name: "DDD", FullName: "DDD", Metadata: map[string]any{}},
	}

	// Mirrors Discover()'s own jira branch exactly: ONE transaction shared
	// by both steps.
	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	created, _, createdLower, discoveredLower, _, err := service.upsertJiraSources(ctx, tx, orgID, integrationID, discovered, time.Now().UTC(), "", false, true)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if created != 4 {
		_ = tx.Rollback(ctx)
		t.Fatalf("upsertJiraSources() created = %d, want 4", created)
	}

	// Force the rebalance step to fail: an already-canceled context makes
	// its own tx.QueryRow fail immediately and deterministically -- standing
	// in for any real transient failure between what used to be two
	// separately-committed transactions.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, rebalanceErr := service.rebalanceJiraSourceRepoLimit(canceledCtx, tx, orgID, integrationID, createdLower, discoveredLower)
	if rebalanceErr == nil {
		_ = tx.Rollback(ctx)
		t.Fatal("test setup bug: expected rebalanceJiraSourceRepoLimit to fail with a canceled context")
	}
	t.Logf("rebalanceJiraSourceRepoLimit() error = %v", rebalanceErr)

	// Exactly what Discover()'s defer does on a non-nil err: roll back
	// instead of committing.
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var total int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid`, orgID, integrationID).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 0 {
		t.Fatalf("GREEN did not hold: %d rows persisted after rollback, want 0 -- a rebalance failure must roll back the upsert atomically, not leave it committed uncapped", total)
	}
}
