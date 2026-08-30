//go:build integration

package sync

import (
	"context"
	"encoding/json"
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
