//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// doraValidFromGuardStub is a minimal DORAObserver that also satisfies
// jobruntime.IncidentValidFromGuardObserver, the same way the production
// MetricsCollector satisfies both -- proving DORAExecutor's guard-reporting
// reaches an observer through the exact type assertion production uses,
// not a bespoke test-only seam.
type doraValidFromGuardStub struct {
	mu    sync.Mutex
	guard map[jobruntime.IncidentValidFromGuardReason]int
}

func (s *doraValidFromGuardStub) ObserveDORAPartition(int, int, int) error { return nil }

func (s *doraValidFromGuardStub) ObserveIncidentValidFromGuardRows(reason jobruntime.IncidentValidFromGuardReason, count int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.guard == nil {
		s.guard = map[jobruntime.IncidentValidFromGuardReason]int{}
	}
	s.guard[reason] += count
	return nil
}

func (s *doraValidFromGuardStub) count(reason jobruntime.IncidentValidFromGuardReason) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.guard[reason]
}

// TestDORAExecutorAdmitsANullValidFromMapping is a red-first regression: an
// operational_service_repository_mappings row with NULL valid_from means
// "valid since before records began" and must satisfy every as-of filter,
// including the DORA incident projection. IncidentProjectionQuery's
// mapping-join predicate admits it via a NULL-OK guard -- without that
// guard, ClickHouse's three-valued logic makes `NULL <= x` evaluate to NULL
// (falsy), so this exact seed would produce ZERO rows for
// time_to_restore_service. This test seeds ONLY a NULL-valid_from mapping
// (no set-valid_from sibling), so it cannot pass by accident the way a mixed
// fixture could.
func TestDORAExecutorAdmitsANullValidFromMapping(t *testing.T) {
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

	conn := migratedClickHouse(t, ctx, OperationalOrderingRevision)
	t.Setenv(operationalOrderingContractEnv, "2")

	const orgID = "00000000-0000-4000-8000-0000000000a1"
	repoID := "00000000-0000-4000-8000-0000000000a2"
	const serviceID = "pagerduty-service-nvf"
	day := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	startedAt := day.Add(10 * time.Hour)
	resolvedAt := day.Add(11 * time.Hour) // 1h restore time
	observedAt := resolvedAt

	if err := conn.Exec(ctx, `
INSERT INTO repos (id, repo, org_id, last_synced) VALUES (toUUID(?), 'full-chaos/nullvalidfrom-repo', ?, now64(3))`,
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
		orgID, "pagerduty", "instance-nvf", "incident", "incident-nvf",
		observedAt, "incident-nvf", observedAt, observedAt, "resolved",
		serviceID, "null valid_from repro", uint8(0), &startedAt, &resolvedAt,
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
	// The one thing this fixture is FOR: valid_from is NULL, exactly the
	// shape a repository_derived mapping (Go's internal_ingest producer, or
	// a backfilled pre-history row) can carry. No sibling mapping with a set
	// valid_from exists in this fixture.
	if err := mappingBatch.Append(
		orgID, "pagerduty", "instance-nvf", "service_repository_mapping",
		"service-nvf:github:full-chaos/nullvalidfrom-repo",
		observedAt, "mapping-nvf", observedAt, observedAt,
		serviceID, repoUUID, "full-chaos/nullvalidfrom-repo", "github",
		"repository_derived", "service_repository_mapping.repository_derived.v1",
		(*time.Time)(nil), (*time.Time)(nil), uint8(1),
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
	scope := json.RawMessage(`{"version":1,"day":"2026-06-01","sink":"auto","interval":"daily","backfill_days":1}`)
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

	guard := &doraValidFromGuardStub{}
	executor, err := NewDORAExecutor(ctx, conn, guard, nil)
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
			CorrelationID:   "null-valid-from-repro",
			IdempotencyKey:  "null-valid-from-repro-" + partitionID,
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
		CorrelationID:  "null-valid-from-repro",
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
				"a NULL valid_from on the mapping row silently dropped the "+
				"incident from the as-of projection",
			err,
		)
	}
	const wantSeconds = float64(1 * 60 * 60)
	if gotValue != wantSeconds {
		t.Fatalf("time_to_restore_service = %v, want %v (1h, resolvedAt-startedAt)", gotValue, wantSeconds)
	}

	if got := guard.count(jobruntime.IncidentValidFromGuardReasonNullRecovered); got != 1 {
		t.Fatalf("valid_from_null_recovered guard count = %d, want 1", got)
	}
	if got := guard.count(jobruntime.IncidentValidFromGuardReasonSet); got != 0 {
		t.Fatalf("valid_from_set guard count = %d, want 0 (this fixture has no set-valid_from mapping)", got)
	}
}
