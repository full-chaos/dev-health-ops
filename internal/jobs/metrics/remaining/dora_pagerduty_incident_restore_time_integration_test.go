//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestDORAExecutorDerivesRestoreTimeFromAMappedPagerDutyIncident is the Go
// replacement for the deleted
// test_mapped_canonical_pagerduty_incident_drives_dora_restore_time
// (tests/test_pagerduty_clickhouse_live.py, CHAOS-5336): a resolved PagerDuty
// incident, mapped to a repository via a real
// operational_service_repository_mappings row, must drive
// time_to_restore_service through the SAME canonical-incidents path every
// other provider uses.
//
// DORAExecutor never branches on provider (parity.go:49's ComputeDORA is
// documented "provider-neutral"; IncidentProjectionQuery joins
// operational_incidents to operational_service_repository_mappings on
// service_id alone, with no provider predicate) -- this test exists to prove
// that neutrality end-to-end against a real ClickHouse, not just read it off
// the source, and to give the family a live regression guard once the
// Python oracle (compute_dora.py) is gone.
//
// Mirrors TestDORAExecutorComputesThroughTheRealClaimPath's executor/claim/
// readback shape; the seed data below (provider="pagerduty" on both the
// incident and the mapping row) is the one thing that test doesn't cover.
func TestDORAExecutorDerivesRestoreTimeFromAMappedPagerDutyIncident(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pgInstance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pgInstance.Close(context.Background())
	pool, err := pgxpool.New(ctx, pgInstance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createRemainingTables(t, ctx, pool)

	// Revision (contract=2), not Legacy: 067_operational_ordering_contract.py
	// pins operational_incidents/operational_service_repository_mappings with
	// `CONSTRAINT ordering_contract_v2 CHECK ordering_contract = 2` -- a
	// contract=1 seed row would be rejected by the schema outright, and
	// production runs on contract=2 (OPERATIONAL_ORDERING_CONTRACT=2, see
	// ci/local_validate.sh's metrics_readback stage).
	conn := migratedClickHouse(t, ctx, OperationalOrderingRevision)
	// migratedClickHouse sets OPERATIONAL_ORDERING_CONTRACT only for the
	// duration of the migration chain, then restores whatever the process
	// had before -- NewDORAExecutor reads it fresh at construction time
	// (configuredOperationalOrderingContract), defaulting to Legacy when
	// unset. Set it again here so the executor actually reads contract=2,
	// matching the schema this test just seeded.
	t.Setenv(operationalOrderingContractEnv, "2")

	const orgID = "00000000-0000-4000-8000-000000005336"
	repoID := "00000000-0000-4000-8000-000000005337"
	const serviceID = "pagerduty-service-1"
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	startedAt := day.Add(12 * time.Hour)
	resolvedAt := day.Add(16 * time.Hour) // 4h restore time, matching the
	// deleted Python test's own fixture (SOURCE_TIME + 4h).
	observedAt := resolvedAt

	if err := conn.Exec(ctx, `
INSERT INTO repos (id, repo, org_id, last_synced) VALUES (toUUID(?), 'full-chaos/payments-api', ?, now64(3))`,
		repoID, orgID); err != nil {
		t.Fatalf("seed repos: %v", err)
	}

	incidentBatch, err := conn.PrepareBatch(ctx, `
        INSERT INTO operational_incidents (
            org_id, provider, provider_instance_id, source_entity_type, external_id,
            source_version_at, id, observed_at, last_synced, normalized_status,
            service_id, title, is_deleted, started_at, resolved_at,
            source_revision, source_conflict_key, ingest_revision, ordering_contract
        )`)
	if err != nil {
		t.Fatalf("prepare incident batch: %v", err)
	}
	if err := incidentBatch.Append(
		orgID, "pagerduty", "instance-1", "incident", "incident-1",
		observedAt, "incident-1", observedAt, observedAt, "resolved",
		serviceID, "Payments latency", uint8(0), &startedAt, &resolvedAt,
		big.NewInt(1), "", big.NewInt(1), uint8(2),
	); err != nil {
		t.Fatalf("append incident: %v", err)
	}
	if err := incidentBatch.Send(); err != nil {
		t.Fatalf("send incident batch: %v", err)
	}

	mappingBatch, err := conn.PrepareBatch(ctx, `
        INSERT INTO operational_service_repository_mappings (
            org_id, provider, provider_instance_id, source_entity_type, external_id,
            source_version_at, id, observed_at, last_synced,
            service_id, repo_id, repo_full_name, repo_provider, mapping_kind, rule_id,
            valid_from, valid_to, is_active,
            source_revision, source_conflict_key, ingest_revision, ordering_contract
        )`)
	if err != nil {
		t.Fatalf("prepare mapping batch: %v", err)
	}
	repoUUID, err := uuid.Parse(repoID)
	if err != nil {
		t.Fatalf("parse repoID: %v", err)
	}
	validFrom := day.Add(-24 * time.Hour)
	if err := mappingBatch.Append(
		orgID, "pagerduty", "instance-1", "service_repository_mapping",
		"service-1:github:full-chaos/payments-api",
		observedAt, "mapping-1", observedAt, observedAt,
		serviceID, repoUUID, "full-chaos/payments-api", "github",
		"admin", "service_repository_mapping.admin.v1",
		&validFrom, (*time.Time)(nil), uint8(1),
		big.NewInt(1), "", big.NewInt(1), uint8(2),
	); err != nil {
		t.Fatalf("append mapping: %v", err)
	}
	if err := mappingBatch.Send(); err != nil {
		t.Fatalf("send mapping batch: %v", err)
	}

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	scope := json.RawMessage(`{"version":1,"day":"2026-08-24","sink":"auto","interval":"daily","backfill_days":1}`)
	run, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "dora",
		Generation:     "dora-v1",
		ScopeKey:       "all-repos",
		Scopes:         []json.RawMessage{scope},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitionID := deterministicPartitionID(run.ID, 1)

	executor, err := NewDORAExecutor(ctx, conn, nil, nil)
	if err != nil {
		t.Fatalf("NewDORAExecutor: %v", err)
	}
	handler, err := NewPartitionHandler[jobruntime.RemainingDORAArgs](store, executor, "dora")
	if err != nil {
		t.Fatalf("NewPartitionHandler: %v", err)
	}

	args := jobruntime.RemainingDORAArgs{
		EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.RemainingMetricsPartitionPayload]{
			ContractVersion: jobcontract.ContractVersionV1,
			OrganizationID:  strPtr(orgID),
			CorrelationID:   "chaos-5336-pagerduty-restore-time",
			IdempotencyKey:  "chaos-5336-pagerduty-restore-time-" + partitionID,
			Domain: jobcontract.DomainLink{
				Type: "remaining_metric_partition",
				ID:   partitionID,
			},
			Payload: jobcontract.NewRemainingMetricsPartitionPayload(
				jobcontract.KindRemainingDORA, partitionID),
		},
	}
	execution := &jobruntime.Execution[jobruntime.RemainingDORAArgs]{
		JobID:          1,
		Attempt:        1,
		Args:           args,
		Envelope:       args.ContractEnvelope(),
		CorrelationID:  "chaos-5336-pagerduty-restore-time",
		OrganizationID: strPtr(orgID),
	}

	if err := handler.Work(ctx, execution); err != nil {
		t.Fatalf("PartitionHandler.Work: %v", err)
	}

	var gotValue float64
	row := conn.QueryRow(ctx, `
		SELECT value FROM dora_metrics_daily
		WHERE org_id = {org_id:String} AND day = {day:Date} AND metric_name = 'time_to_restore_service'`,
		clickhouse.Named("org_id", orgID), clickhouse.Named("day", day.Format("2006-01-02")),
	)
	if err := row.Scan(&gotValue); err != nil {
		t.Fatalf(
			"readback dora_metrics_daily time_to_restore_service: %v -- "+
				"the mapped PagerDuty incident never reached the DORA "+
				"projection, or the projection joined on the wrong key",
			err,
		)
	}
	const wantSeconds = float64(4 * 60 * 60)
	if gotValue != wantSeconds {
		t.Fatalf("time_to_restore_service = %v, want %v (4h, resolvedAt-startedAt)", gotValue, wantSeconds)
	}
}
