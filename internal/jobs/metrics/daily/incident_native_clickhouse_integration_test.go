//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestIncidentValidFromNullGuardRecoversRepositoryDerivedMappings is
// CHAOS-4269/CHAOS-4295's live-ClickHouse proof, run through the real
// production entry point (IncidentExecutor.ComputeFamily), against the
// EXACT bug shape confirmed with executed evidence on the shared local
// stack 2026-09-01 (CHAOS-4269 comment): a mapping_kind="repository_derived"
// row with valid_from=NULL (the only shape map_issue_incidents ever writes)
// joined to a real incident.
//
//  1. RED ON THE PYTHON-EQUIVALENT PREDICATE: the exact WHERE clause
//     active_incidents_query used before this fix (`valid_from <= as_of`,
//     no NULL-OK guard) is run directly against this seeded data and MUST
//     return zero matched rows -- this is the CHAOS-4269 bug, reproduced,
//     not assumed.
//  2. GREEN ON THE FIX: IncidentExecutor.ComputeFamily -- which adds
//     `valid_from IS NULL OR valid_from <= as_of` -- must write exactly one
//     incident_metrics_daily row for this repo/day, with the correct
//     incidents_count and MTTR (started_at to resolved_at).
func TestIncidentValidFromNullGuardRecoversRepositoryDerivedMappings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range []string{
		// Exact live production schema (verified via `SHOW CREATE TABLE` on
		// the shared local stack, 2026-09-01) for the three columns this
		// test's join and predicate care about; every other NOT NULL column
		// is still declared so the seed INSERTs below match production
		// shape, not a trimmed lookalike.
		`CREATE TABLE operational_incidents (
    org_id String, provider String, provider_instance_id String, source_entity_type String,
    external_id String, source_version_at DateTime64(6, 'UTC'), source_revision UInt128,
    source_conflict_key String, ingest_revision UInt128, ordering_contract UInt8,
    id String, observed_at DateTime64(6, 'UTC'), last_synced DateTime64(6, 'UTC'),
    normalized_status Nullable(String), service_id Nullable(String), title String,
    started_at Nullable(DateTime64(6, 'UTC')), resolved_at Nullable(DateTime64(6, 'UTC')),
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(ingest_revision) ORDER BY (org_id, id, source_revision, source_conflict_key)`,
		`CREATE TABLE operational_service_repository_mappings (
    org_id String, provider String, provider_instance_id String, source_entity_type String,
    external_id String, source_version_at DateTime64(6, 'UTC'), source_revision UInt128,
    source_conflict_key String, ingest_revision UInt128, ordering_contract UInt8,
    id String, observed_at DateTime64(6, 'UTC'), last_synced DateTime64(6, 'UTC'),
    service_id String, repo_id Nullable(UUID), mapping_kind Nullable(String),
    valid_from Nullable(DateTime64(6, 'UTC')), valid_to Nullable(DateTime64(6, 'UTC')),
    is_active UInt8
) ENGINE = ReplacingMergeTree(ingest_revision) ORDER BY (org_id, id, source_revision, source_conflict_key)`,
		`CREATE TABLE repos (
    id UUID, repo String, org_id String, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, id)`,
		// Exact production shape: migration 004_quality_delivery_metrics.sql
		// + 024_add_org_id.sql / 027_add_org_id_to_sorting_keys.py.
		`CREATE TABLE incident_metrics_daily (
    repo_id UUID, day Date, incidents_count UInt32, mttr_p50_hours Nullable(Float64),
    mttr_p90_hours Nullable(Float64), computed_at DateTime('UTC'), org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const orgID = "00000000-0000-4000-8000-0000000000e0"
	repoID := "00000000-0000-4000-8000-0000000000e1"
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	startedAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	resolvedAt := time.Date(2026, 8, 24, 16, 0, 0, 0, time.UTC) // 6h MTTR

	if err := conn.Exec(ctx, `
INSERT INTO repos (id, repo, org_id, last_synced) VALUES (toUUID(?), 'org/repo-e', ?, now64(3))`,
		repoID, orgID); err != nil {
		t.Fatal(err)
	}

	// The CHAOS-4269 bug shape: mapping_kind="repository_derived",
	// valid_from=NULL -- the only shape map_issue_incidents
	// (providers/operational_migration.py:196-210) ever writes, confirmed
	// with executed evidence 2026-09-01: 0/20 repository_derived mapping
	// rows in the shared local stack have valid_from set.
	if err := conn.Exec(ctx, `
INSERT INTO operational_service_repository_mappings
    (org_id, provider, provider_instance_id, source_entity_type, external_id,
     source_version_at, source_revision, source_conflict_key, ingest_revision, ordering_contract,
     id, observed_at, last_synced, service_id, repo_id, mapping_kind, valid_from, valid_to, is_active)
VALUES (?, 'github', 'github', 'repository_mapping', 'org/repo-e:`+repoID+`',
        now64(6), 1, '', 1, 2,
        'mapping-e1', now64(6), now64(6), 'svc-e1', toUUID(?), 'repository_derived', NULL, NULL, 1)`,
		orgID, repoID); err != nil {
		t.Fatal(err)
	}

	if err := conn.Exec(ctx, `
INSERT INTO operational_incidents
    (org_id, provider, provider_instance_id, source_entity_type, external_id,
     source_version_at, source_revision, source_conflict_key, ingest_revision, ordering_contract,
     id, observed_at, last_synced, normalized_status, service_id, title, started_at, resolved_at, is_deleted)
VALUES (?, 'github', 'github', 'incident', 'org/repo-e#1',
        now64(6), 1, '', 1, 2,
        'incident-e1', now64(6), now64(6), 'resolved', 'svc-e1', 'test incident', ?, ?, 0)`,
		orgID, startedAt, resolvedAt); err != nil {
		t.Fatal(err)
	}

	// Point 1: RED on the exact pre-fix Python predicate -- no NULL-OK
	// guard on valid_from. Must match zero rows: this IS the CHAOS-4269 bug.
	row := conn.QueryRow(ctx, `
SELECT count()
FROM operational_incidents AS incident
INNER JOIN operational_service_repository_mappings AS mapping
    ON incident.org_id = mapping.org_id AND incident.service_id = mapping.service_id
WHERE mapping.org_id = ? AND mapping.repo_id IS NOT NULL AND mapping.is_active = 1
  AND mapping.valid_from <= now64(6, 'UTC')
  AND (mapping.valid_to IS NULL OR mapping.valid_to > now64(6, 'UTC'))`, orgID)
	var preFixMatches uint64
	if err := row.Scan(&preFixMatches); err != nil {
		t.Fatalf("pre-fix predicate query: %v", err)
	}
	if preFixMatches != 0 {
		t.Fatalf("pre-fix (no NULL-OK guard) predicate matched %d rows, want 0 -- this test's seed data no longer reproduces CHAOS-4269's bug shape", preFixMatches)
	}

	// Point 2: GREEN via the fix -- IncidentExecutor's NULL-OK guard.
	executor, err := NewIncidentExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: orgID, TargetDay: day}
	partition := Partition{
		ID: "00000000-0000-4000-8000-0000000000e2", RunID: "00000000-0000-4000-8000-0000000000e3",
		RepoIDs: []RepositoryID{RepositoryID(repoID)},
	}
	written, err := executor.ComputeFamily(ctx, run, partition)
	if err != nil {
		t.Fatalf("ComputeFamily: %v", err)
	}
	if written != 1 {
		t.Fatalf("ComputeFamily wrote %d rows, want 1", written)
	}

	metricsRow := conn.QueryRow(ctx, `
SELECT incidents_count, mttr_p50_hours, mttr_p90_hours
FROM incident_metrics_daily WHERE org_id = ? AND repo_id = toUUID(?) AND day = ?`,
		orgID, repoID, day)
	var (
		incidentsCount uint32
		mttrP50        float64
		mttrP90        float64
	)
	if err := metricsRow.Scan(&incidentsCount, &mttrP50, &mttrP90); err != nil {
		t.Fatalf("scan incident_metrics_daily: %v", err)
	}
	if incidentsCount != 1 {
		t.Fatalf("incidents_count = %d, want 1", incidentsCount)
	}
	if mttrP50 != 6.0 || mttrP90 != 6.0 {
		t.Fatalf("mttr_p50/p90 = %v/%v, want 6.0/6.0 (10:00->16:00 UTC)", mttrP50, mttrP90)
	}
}
