//go:build integration

package daily

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// stubSourceDataConn answers ClickHouse Query calls by matching the query
// text against a table name -> "has rows" map. Every one of the checker's
// source and output queries names its table exactly once and each table name
// is unique across the four families, so a single substring match
// unambiguously distinguishes them without needing to parse the query.
type stubSourceDataConn struct {
	hasRows map[string]bool
	queries []string
}

// strictSourceDataConn models the clickhouse-go named-argument boundary. The
// production source/output columns are UUID, so the checker must send a
// []uuid.UUID value for its Array(UUID) parameter. The old []string binding is
// deliberately rejected here: this makes the handler regression red on the
// pre-fix tip instead of allowing a table-name-only stub to pass.
type strictSourceDataConn struct {
	queries []string
}

func (conn *strictSourceDataConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	conn.queries = append(conn.queries, query)
	if strings.Contains(query, "repo_ids:") {
		if !strings.Contains(query, "repo_ids:Array(UUID)") {
			return nil, fmt.Errorf("repository filter is not Array(UUID): %s", query)
		}
		for _, arg := range args {
			named, ok := arg.(driver.NamedValue)
			if !ok || named.Name != "repo_ids" {
				continue
			}
			if _, ok := named.Value.([]uuid.UUID); !ok {
				return nil, fmt.Errorf("repo_ids binding type = %T, want []uuid.UUID", named.Value)
			}
		}
	}
	if strings.Contains(query, "FROM ci_pipeline_runs") {
		return &boolRows{present: true}, nil
	}
	return &boolRows{present: false}, nil
}

// incidentScopeConn returns an incident only when the repository is linked by
// the canonical service mapping. The mapped/unmapped split is the production
// shape that an org-wide operational_incidents probe cannot represent.
type incidentScopeConn struct {
	mappedRepo uuid.UUID
}

func (conn *incidentScopeConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	if !strings.Contains(query, "repo_ids:") {
		if strings.Contains(query, "FROM operational_incidents") {
			// Baseline behavior: org-wide incident presence would make this true
			// for every partition. The fixed query always has repo_ids and does
			// not take this branch.
			return &boolRows{present: true}, nil
		}
		return &boolRows{present: false}, nil
	}
	if !strings.Contains(query, "operational_service_repository_mappings") {
		return &boolRows{present: false}, nil
	}
	if !strings.Contains(query, "repo_ids:Array(UUID)") ||
		!strings.Contains(query, "is_active = 1") ||
		!strings.Contains(query, "valid_from <=") ||
		!strings.Contains(query, "valid_to IS NULL") ||
		!strings.Contains(query, "resolved_at >= {start:DateTime64(3, 'UTC')}") ||
		!strings.Contains(query, "resolved_at < {end:DateTime64(3, 'UTC')}") {
		return nil, fmt.Errorf("incident query does not use canonical mapped projection: %s", query)
	}
	for _, arg := range args {
		named, ok := arg.(driver.NamedValue)
		if !ok || named.Name != "repo_ids" {
			continue
		}
		ids, ok := named.Value.([]uuid.UUID)
		if !ok {
			return nil, fmt.Errorf("incident repo_ids binding type = %T, want []uuid.UUID", named.Value)
		}
		for _, id := range ids {
			if id == conn.mappedRepo {
				return &boolRows{present: true}, nil
			}
		}
	}
	return &boolRows{present: false}, nil
}

func (conn *stubSourceDataConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	conn.queries = append(conn.queries, query)
	for table, present := range conn.hasRows {
		if strings.Contains(query, table) {
			return &boolRows{present: present}, nil
		}
	}
	return &boolRows{present: false}, nil
}

// boolRows is the minimal driver.Rows the checker's exists/existsOrgScoped
// helpers need: they only ever call Next and Err, never Scan.
type boolRows struct {
	present bool
	yielded bool
}

func (rows *boolRows) Next() bool {
	if rows.yielded || !rows.present {
		return false
	}
	rows.yielded = true
	return true
}
func (*boolRows) Scan(...any) error                { return nil }
func (*boolRows) ScanStruct(any) error             { return nil }
func (*boolRows) ColumnTypes() []driver.ColumnType { return nil }
func (*boolRows) Totals(...any) error              { return nil }
func (*boolRows) Columns() []string                { return nil }
func (*boolRows) Close() error                     { return nil }
func (*boolRows) Err() error                       { return nil }
func (*boolRows) HasData() bool                    { return true }

func insertPartitionScope(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	runID, partitionID, orgID, day string, repoIDs []RepositoryID,
) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_runs (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3::date, 'zero-row-check-test', 'running', 'pending', $4, $4)`,
		runID, orgID, day, now); err != nil {
		t.Fatal(err)
	}
	if repoIDs == nil {
		repoIDs = []RepositoryID{}
	}
	raw, err := json.Marshal(repoIDs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_partitions (id, run_id, ordinal, repo_ids, status, attempt_count, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, 0, $3::jsonb, 'pending', 0, $4, $4)`,
		partitionID, runID, raw, now); err != nil {
		t.Fatal(err)
	}
}

// TestClickHouseSourceDataCheckerFlagsOnlyFamiliesWithSourceButNoOutput pins
// the CHAOS-4263 ruling's core invariant: a family whose source data exists
// but whose output is empty is flagged; a family with no source data at all
// (a genuinely empty day) is not, even though its output is equally empty.
func TestClickHouseSourceDataCheckerFlagsOnlyFamiliesWithSourceButNoOutput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)

	const (
		runID       = "00000000-0000-4000-8000-000000000101"
		partitionID = "00000000-0000-4000-8000-000000000102"
		orgID       = "00000000-0000-4000-8000-000000000009"
		day         = "2026-08-25"
	)
	repoIDs := []RepositoryID{"00000000-0000-4000-8000-000000000002"}
	insertPartitionScope(t, ctx, pool, runID, partitionID, orgID, day, repoIDs)

	// cicd: source present, output missing -> flagged.
	// deploy: neither present -> genuinely empty day, not flagged.
	// testops_risk: source present, all three independent outputs present
	// -> not flagged.
	// incident: source present, output missing -> flagged.
	conn := &stubSourceDataConn{hasRows: map[string]bool{
		"ci_pipeline_runs":           true,
		"cicd_metrics_daily":         false,
		"deployments":                false,
		"deploy_metrics_daily":       false,
		"test_suite_results":         true,
		"testops_release_confidence": true,
		"testops_quality_drag":       true,
		"testops_pipeline_stability": true,
		"operational_incidents":      true,
		"incident_metrics_daily":     false,
	}}
	checker, err := NewClickHouseSourceDataChecker(pool, conn)
	if err != nil {
		t.Fatal(err)
	}
	families, err := checker.ZeroRowFamiliesWithSourceData(ctx, partitionID)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, family := range families {
		got[family] = true
	}
	if !got["cicd"] || got["deploy"] || got["testops_risk"] || !got["incident"] {
		t.Fatalf("flagged families=%v, want exactly cicd and incident", families)
	}
}

// TestPartitionHandlerWorkFlagsTestopsRiskWhenOnlyOneOutputTableIsEmpty pins
// the codex adversarial-review finding (round 1, CHAOS-4263): testops_risk
// writes THREE independent output tables (release_confidence, quality_drag,
// pipeline_stability -- job_daily.py:1481-1483 logs their zero-row state
// separately), and a UNION-based existence check let one populated table mask
// another's empty one. Seeds source data plus two of the three outputs
// present and the third empty, and drives the real PartitionHandler.Work path
// (not just the checker function) to prove the partition is released and
// retried instead of silently completing.
func TestPartitionHandlerWorkFlagsTestopsRiskWhenOnlyOneOutputTableIsEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)

	const (
		runID       = "00000000-0000-4000-8000-000000000114"
		partitionID = "00000000-0000-4000-8000-000000000115"
		orgID       = "00000000-0000-4000-8000-000000000009"
		day         = "2026-08-25"
	)
	insertPartitionScope(t, ctx, pool, runID, partitionID, orgID, day,
		[]RepositoryID{"00000000-0000-4000-8000-000000000002"})

	// testops_risk source present (test_suite_results); release_confidence
	// and quality_drag populated but pipeline_stability empty -- the exact
	// shape a UNION ALL existence check cannot distinguish from "all three
	// present".
	conn := &stubSourceDataConn{hasRows: map[string]bool{
		"test_suite_results":         true,
		"testops_release_confidence": true,
		"testops_quality_drag":       true,
		"testops_pipeline_stability": false,
	}}
	checker, err := NewClickHouseSourceDataChecker(pool, conn)
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition:     Partition{ID: partitionID, RunID: runID},
			Token:         "00000000-0000-4000-8000-000000000116",
			LeaseDuration: time.Second,
		},
		run: Run{ID: runID, OrganizationID: orgID, Status: "running"},
	}
	observer := &recordingZeroRowsObserver{}
	handler, err := NewPartitionHandler(store, fakePublisher{}, fakeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetSourceDataChecker(checker)
	handler.SetZeroRowsObserver(observer)

	err = handler.Work(ctx, partitionExecutionFor(partitionID, runID, orgID))
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("handler error = %v, want retryable zero-row error", err)
	}
	if store.partitionCompletions != 0 {
		t.Fatalf("CompletePartition calls = %d, want 0", store.partitionCompletions)
	}
	if store.partitionReleases != 1 {
		t.Fatalf("ReleasePartition calls = %d, want 1", store.partitionReleases)
	}
	if len(observer.families) != 1 || observer.families[0] != "testops_risk" {
		t.Fatalf("observed families = %v, want [testops_risk]", observer.families)
	}
}

// TestClickHouseSourceDataCheckerSkipsPartitionsWithNoRepositories pins that a
// no_repositories partition (empty repo_ids) is never checked: that terminal
// state is MaterializeScheduledFanout's, not this checker's, to report.
func TestClickHouseSourceDataCheckerSkipsPartitionsWithNoRepositories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)

	const (
		runID       = "00000000-0000-4000-8000-000000000103"
		partitionID = "00000000-0000-4000-8000-000000000104"
		orgID       = "00000000-0000-4000-8000-000000000009"
		day         = "2026-08-25"
	)
	insertPartitionScope(t, ctx, pool, runID, partitionID, orgID, day, nil)

	conn := &stubSourceDataConn{hasRows: map[string]bool{
		"ci_pipeline_runs": true, "cicd_metrics_daily": false,
	}}
	checker, err := NewClickHouseSourceDataChecker(pool, conn)
	if err != nil {
		t.Fatal(err)
	}
	families, err := checker.ZeroRowFamiliesWithSourceData(ctx, partitionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 0 {
		t.Fatalf("no_repositories partition was checked, flagged=%v", families)
	}
	if len(conn.queries) != 0 {
		t.Fatalf("no_repositories partition issued %d ClickHouse queries, want 0", len(conn.queries))
	}
}

// TestPartitionHandlerWorkRetriesAndObservesZeroRowsWithSourceData proves the
// causal boundary: source data plus empty output reaches the real partition
// handler, increments the family observer, releases the claim for retry, and
// never calls CompletePartition. It also proves the production UUID binding
// rather than accepting a table-name-only answer.
func TestPartitionHandlerWorkRetriesAndObservesZeroRowsWithSourceData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)

	const (
		runID       = "00000000-0000-4000-8000-000000000105"
		partitionID = "00000000-0000-4000-8000-000000000106"
		orgID       = "00000000-0000-4000-8000-000000000009"
		day         = "2026-08-25"
	)
	insertPartitionScope(t, ctx, pool, runID, partitionID, orgID, day, []RepositoryID{
		"00000000-0000-4000-8000-000000000002",
	})

	conn := &strictSourceDataConn{}
	checker, err := NewClickHouseSourceDataChecker(pool, conn)
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition:     Partition{ID: partitionID, RunID: runID},
			Token:         "00000000-0000-4000-8000-000000000107",
			LeaseDuration: time.Second,
		},
		run: Run{ID: runID, OrganizationID: orgID, Status: "running"},
	}
	observer := &recordingZeroRowsObserver{}
	handler, err := NewPartitionHandler(store, fakePublisher{}, fakeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetSourceDataChecker(checker)
	handler.SetZeroRowsObserver(observer)

	err = handler.Work(ctx, partitionExecutionFor(partitionID, runID, orgID))
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("handler error = %v, want retryable zero-row error", err)
	}
	if store.partitionCompletions != 0 {
		t.Fatalf("CompletePartition calls = %d, want 0", store.partitionCompletions)
	}
	if store.partitionReleases != 1 {
		t.Fatalf("ReleasePartition calls = %d, want 1", store.partitionReleases)
	}
	if len(observer.families) != 2 || observer.families[0] != "cicd" || observer.families[1] != "testops_risk" {
		t.Fatalf("observed families = %v, want [cicd testops_risk]", observer.families)
	}
}

// TestClickHouseSourceDataCheckerScopesIncidentSourceThroughCurrentMapping
// proves that an incident linked to one repository cannot block an unrelated
// repository's partition. The fixture carries one mapped and one unmapped repo
// in the same organization and feeds the checker through the production-shaped
// service/mapping projection boundary.
func TestClickHouseSourceDataCheckerScopesIncidentSourceThroughCurrentMapping(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)

	const (
		orgID        = "00000000-0000-4000-8000-000000000009"
		day          = "2026-08-25"
		mappedRepo   = "00000000-0000-4000-8000-000000000108"
		unmappedRepo = "00000000-0000-4000-8000-000000000109"
	)
	insertPartitionScope(t, ctx, pool,
		"00000000-0000-4000-8000-000000000110",
		"00000000-0000-4000-8000-000000000111", orgID, day,
		[]RepositoryID{mappedRepo})
	insertPartitionScope(t, ctx, pool,
		"00000000-0000-4000-8000-000000000112",
		"00000000-0000-4000-8000-000000000113", orgID, day,
		[]RepositoryID{unmappedRepo})

	parsedMappedRepo, err := uuid.Parse(mappedRepo)
	if err != nil {
		t.Fatal(err)
	}
	checker, err := NewClickHouseSourceDataChecker(pool, &incidentScopeConn{mappedRepo: parsedMappedRepo})
	if err != nil {
		t.Fatal(err)
	}
	mappedFamilies, err := checker.ZeroRowFamiliesWithSourceData(ctx, "00000000-0000-4000-8000-000000000111")
	if err != nil {
		t.Fatal(err)
	}
	if len(mappedFamilies) != 1 || mappedFamilies[0] != "incident" {
		t.Fatalf("mapped incident families = %v, want [incident]", mappedFamilies)
	}
	unmappedFamilies, err := checker.ZeroRowFamiliesWithSourceData(ctx, "00000000-0000-4000-8000-000000000113")
	if err != nil {
		t.Fatal(err)
	}
	if len(unmappedFamilies) != 0 {
		t.Fatalf("unmapped incident families = %v, want none", unmappedFamilies)
	}
}

// TestClickHouseSourceDataCheckerBindsUUIDArrayAgainstRealUUIDColumn executes
// the checker query against a real ClickHouse UUID column. A substring stub can
// accept []string forever; this test reaches the driver encoder and server type
// checker, proving the Array(UUID) binding matches the production schema.
func TestClickHouseSourceDataCheckerBindsUUIDArrayAgainstRealUUIDColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, `
CREATE TABLE ci_pipeline_runs (
    org_id String,
    repo_id UUID,
    finished_at DateTime64(3, 'UTC')
) ENGINE = MergeTree ORDER BY (org_id, repo_id, finished_at)`); err != nil {
		t.Fatal(err)
	}
	const (
		orgID  = "00000000-0000-4000-8000-000000000009"
		repoID = "00000000-0000-4000-8000-000000000002"
	)
	if err := conn.Exec(ctx, `
INSERT INTO ci_pipeline_runs (org_id, repo_id, finished_at)
VALUES ('00000000-0000-4000-8000-000000000009', toUUID('00000000-0000-4000-8000-000000000002'), toDateTime64('2026-08-25 12:00:00', 3, 'UTC'))`); err != nil {
		t.Fatal(err)
	}
	checker := &ClickHouseSourceDataChecker{conn: conn}
	repositoryID, err := uuid.Parse(repoID)
	if err != nil {
		t.Fatal(err)
	}
	found, err := checker.exists(ctx, `
SELECT 1 FROM ci_pipeline_runs
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
  AND finished_at IS NOT NULL AND toDate(finished_at) = {day:Date}
LIMIT 1`, orgID, "2026-08-25", []uuid.UUID{repositoryID})
	if err != nil {
		t.Fatalf("UUID source query: %v", err)
	}
	if !found {
		t.Fatal("UUID source query did not find the inserted row")
	}
}

// TestClickHouseIncidentProjectionUsesMappedRepositories executes the
// canonical current-row incident projection against ClickHouse. Only the
// mapped repository has a service mapping; the same org/day incident must not
// be treated as source data for the unmapped repository.
func TestClickHouseIncidentProjectionUsesMappedRepositories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, statement := range []string{
		`CREATE TABLE operational_incidents (
    org_id String, id String, service_id Nullable(String),
    source_revision DateTime64(6, 'UTC'), source_conflict_key String, ingest_revision UInt128,
    is_deleted UInt8, started_at Nullable(DateTime64(3, 'UTC')),
    resolved_at Nullable(DateTime64(3, 'UTC')), normalized_status String,
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree ORDER BY (org_id, id)`,
		`CREATE TABLE operational_service_repository_mappings (
    org_id String, id String, service_id String, repo_id Nullable(UUID),
    source_revision DateTime64(6, 'UTC'), source_conflict_key String, ingest_revision UInt128,
    is_deleted UInt8, is_active UInt8, valid_from Nullable(DateTime64(6, 'UTC')),
    valid_to Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree ORDER BY (org_id, id)`,
		`CREATE TABLE repos (
    org_id String, id UUID
) ENGINE = ReplacingMergeTree ORDER BY (org_id, id)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	const (
		orgID        = "00000000-0000-4000-8000-000000000009"
		mappedRepo   = "00000000-0000-4000-8000-000000000108"
		unmappedRepo = "00000000-0000-4000-8000-000000000109"
	)
	if err := conn.Exec(ctx, `
INSERT INTO operational_incidents
    (org_id, id, service_id, source_revision, source_conflict_key,
     ingest_revision, is_deleted, started_at, resolved_at, normalized_status,
     last_synced)
VALUES ('00000000-0000-4000-8000-000000000009', 'incident-1', 'service-1',
        toDateTime64('2026-08-25 01:00:00', 6, 'UTC'), 'source-1', 1, 0,
        toDateTime64('2026-08-25 01:00:00', 3, 'UTC'),
        toDateTime64('2026-08-25 02:00:00', 3, 'UTC'), 'resolved',
        toDateTime64('2026-08-25 02:00:00', 3, 'UTC'))`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO operational_service_repository_mappings
VALUES ('00000000-0000-4000-8000-000000000009', 'mapping-1', 'service-1',
        toUUID('00000000-0000-4000-8000-000000000108'),
        toDateTime64('2026-08-25 01:00:00', 6, 'UTC'), 'source-1', 1, 0, 1,
        toDateTime64('2026-08-24 00:00:00', 6, 'UTC'), NULL)`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO repos VALUES
('00000000-0000-4000-8000-000000000009', toUUID('00000000-0000-4000-8000-000000000108'))`); err != nil {
		t.Fatal(err)
	}
	mappedID, err := uuid.Parse(mappedRepo)
	if err != nil {
		t.Fatal(err)
	}
	unmappedID, err := uuid.Parse(unmappedRepo)
	if err != nil {
		t.Fatal(err)
	}
	checker := &ClickHouseSourceDataChecker{conn: conn}
	start := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)
	for _, testCase := range []struct {
		name string
		repo uuid.UUID
		want bool
	}{
		{name: "mapped", repo: mappedID, want: true},
		{name: "unmapped", repo: unmappedID, want: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := checker.existsIncident(ctx, orgID, start, end, []uuid.UUID{testCase.repo})
			if err != nil {
				t.Fatal(err)
			}
			if got != testCase.want {
				t.Fatalf("incident source for %s = %t, want %t", testCase.name, got, testCase.want)
			}
		})
	}
}

type recordingZeroRowsObserver struct {
	families []string
}

func (observer *recordingZeroRowsObserver) ObserveDailyMetricsFamilyZeroRowsWithSource(family string) error {
	observer.families = append(observer.families, family)
	return nil
}

func partitionExecutionFor(partitionID, runID, orgID string) *jobruntime.Execution[jobruntime.DailyMetricsPartitionArgs] {
	return &jobruntime.Execution[jobruntime.DailyMetricsPartitionArgs]{
		OrganizationID: &orgID,
		Envelope: jobcontract.Envelope{
			OrganizationID: &orgID,
			Domain:         jobcontract.DomainLink{Type: "daily_metrics_partition", ID: partitionID},
		},
		Args: jobruntime.DailyMetricsPartitionArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsPartitionPayload]{
			OrganizationID: &orgID,
			Domain:         jobcontract.DomainLink{Type: "daily_metrics_partition", ID: partitionID},
			Payload:        jobcontract.DailyMetricsPartitionPayload{PartitionID: partitionID},
		}},
	}
}
