//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const gitLabIncidentsServiceDDL = `
CREATE TABLE operational_services (
  org_id String, provider LowCardinality(String), provider_instance_id String,
  source_entity_type String, external_id String, source_version_at DateTime64(6, 'UTC'),
  source_revision UInt128, source_conflict_key String, ingest_revision UInt128,
  ordering_contract UInt8, id String, source_id Nullable(UUID), source_url Nullable(String),
  source_event_at Nullable(DateTime64(6, 'UTC')), source_event_id Nullable(String),
  observed_at DateTime64(6, 'UTC'), last_synced DateTime64(6, 'UTC'),
  raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
  normalized_status Nullable(String), normalized_severity Nullable(String),
  normalized_priority Nullable(String), relationship_provenance Nullable(String),
  relationship_confidence Nullable(Float64), name String, description Nullable(String),
  service_type Nullable(String), owning_team_id Nullable(String),
  escalation_policy_id Nullable(String), is_deleted Bool,
  deleted_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_revision) ORDER BY (org_id, id)`

const gitLabIncidentsMappingDDL = `
CREATE TABLE operational_service_repository_mappings (
  org_id String, provider LowCardinality(String), provider_instance_id String,
  source_entity_type String, external_id String, source_version_at DateTime64(6, 'UTC'),
  source_revision UInt128, source_conflict_key String, ingest_revision UInt128,
  ordering_contract UInt8, id String, source_id Nullable(UUID), source_url Nullable(String),
  source_event_at Nullable(DateTime64(6, 'UTC')), source_event_id Nullable(String),
  observed_at DateTime64(6, 'UTC'), last_synced DateTime64(6, 'UTC'),
  raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
  normalized_status Nullable(String), normalized_severity Nullable(String),
  normalized_priority Nullable(String), relationship_provenance Nullable(String),
  relationship_confidence Nullable(Float64), service_id String, repo_id Nullable(UUID),
  repo_full_name Nullable(String), repo_provider Nullable(String), mapping_kind Nullable(String),
  rule_id Nullable(String), valid_from Nullable(DateTime64(6, 'UTC')),
  valid_to Nullable(DateTime64(6, 'UTC')), is_active Bool
) ENGINE = ReplacingMergeTree(source_revision) ORDER BY (org_id, id)`

func TestGitLabIncidentsThreeDestinationsHaveExactRetryReadback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := postgres.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `
UPDATE public.integration_sources
SET external_id = '123', full_name = 'Acme/API'
WHERE id = $1`, firstSourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET provider = 'gitlab', dataset_key = 'incidents', cost_class = 'light',
    processor_flags = '{"sync_incidents": true}',
    since_at = '2026-07-01T00:00:00Z', before_at = '2026-07-31T23:59:59Z'
WHERE id = $1`, firstUnitID); err != nil {
		t.Fatal(err)
	}
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	claimNow := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: claimNow,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, gitLabIncidentsServiceDDL); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, gitLabIncidentsMappingDDL); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, jiraIncidentsDDL); err != nil {
		t.Fatal(err)
	}

	doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
		{body: gitLabRepositoryFixture},
		{body: `[{"id":9001,"iid":7,"issue_type":"incident","state":"opened","title":"API unavailable","created_at":"2026-07-20T10:00:00Z","updated_at":"2026-07-21T11:00:00Z","severity":"sev1"}]`},
	}}
	normalizedAt := claimNow.Add(123456 * time.Microsecond)
	batch, err := (GitLabIncidentsRouteHandler{PerPage: 2}).Collect(
		ctx, claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"),
		normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	sink := GitLabIncidentsClickHouseEffects{
		Conn:  conn,
		Lease: leaseGuardAt(repository, claim, claimNow),
	}
	for _, effect := range batch.Effects {
		duplicate, err := BuildEffectBatch(
			effect.Destination, effect.Recovery,
			[]json.RawMessage{effect.Rows[0], effect.Rows[0]},
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.WriteEffect(ctx, claim, duplicate); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("%s duplicate write error=%v", effect.Destination, err)
		}
		writeGitLabIncidentForeignCollision(t, ctx, sink, effect)
		inspection, err := sink.InspectEffect(ctx, claim, effect)
		if err != nil || inspection != EffectAbsent {
			t.Fatalf("%s before write inspection=%s error=%v", effect.Destination, inspection, err)
		}
	}

	crash := errors.New("simulated crash after durable mapping write")
	_, err = (EffectCommitter{
		Ledger: repository,
		Sink: crashAfterGitLabIncidentWrite{
			sink: sink, destination: "operational_service_repository_mappings", failure: crash,
		},
		Now: func() time.Time { return claimNow.Add(10 * time.Second) },
	}).Commit(ctx, claim, batch.Effects, normalizedAt)
	if !errors.Is(err, crash) {
		t.Fatalf("first commit error=%v", err)
	}
	persisted, err := repository.LoadEffects(ctx, claim, claimNow.Add(11*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	committed, writing, pending := 0, 0, 0
	for _, effect := range persisted.Effects {
		switch effect.Status {
		case GenerationBlockCommitted:
			committed++
		case GenerationBlockWriting:
			writing++
		case GenerationBlockPending:
			pending++
		}
	}
	if committed != 1 || writing != 1 || pending != 1 {
		t.Fatalf("persisted crash ledger=%+v", persisted.Effects)
	}

	recoveryNow := claimNow.Add(61 * time.Second)
	freshRepository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	recoveredClaim, err := freshRepository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: recoveryNow,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	persisted, err = freshRepository.LoadEffects(ctx, recoveredClaim, recoveryNow)
	if err != nil || !persisted.CreatedAt.Equal(normalizedAt) {
		t.Fatalf("reloaded ledger=%+v error=%v", persisted, err)
	}
	recoveryDoer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
		{body: gitLabRepositoryFixture},
		{body: `[{"id":9001,"iid":7,"issue_type":"incident","state":"opened","title":"API unavailable","created_at":"2026-07-20T10:00:00Z","updated_at":"2026-07-21T11:00:00Z","severity":"sev1"}]`},
	}}
	recoveredBatch, err := (GitLabIncidentsRouteHandler{PerPage: 2}).Collect(
		ctx, recoveredClaim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, recoveryDoer, "https://gitlab.example"),
		persisted.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	for index := range batch.Effects {
		if recoveredBatch.Effects[index].ContentDigest != batch.Effects[index].ContentDigest {
			t.Fatalf("effect[%d] recovered digest=%s want=%s", index, recoveredBatch.Effects[index].ContentDigest, batch.Effects[index].ContentDigest)
		}
	}
	freshSink := GitLabIncidentsClickHouseEffects{
		Conn: conn, Lease: leaseGuardAt(freshRepository, recoveredClaim, recoveryNow),
	}
	recoveryResult, err := (EffectCommitter{
		Ledger: freshRepository, Sink: freshSink, Readback: freshSink,
		Now: func() time.Time { return recoveryNow },
	}).Commit(ctx, recoveredClaim, recoveredBatch.Effects, persisted.CreatedAt)
	if err != nil || recoveryResult != (EffectCommitResult{Written: 1, Skipped: 1, MarkedCommitted: 1}) {
		t.Fatalf("recovery result=%+v error=%v", recoveryResult, err)
	}

	for _, effect := range recoveredBatch.Effects {
		for attempt := 1; attempt <= 2; attempt++ {
			if err := freshSink.WriteEffect(ctx, recoveredClaim, effect); err != nil {
				t.Fatalf("%s attempt %d write: %v", effect.Destination, attempt, err)
			}
			inspection, inspectErr := freshSink.InspectEffect(ctx, recoveredClaim, effect)
			if inspectErr != nil || inspection != EffectExact {
				t.Fatalf("%s attempt %d inspection=%s error=%v", effect.Destination, attempt, inspection, inspectErr)
			}
		}
	}
}

type crashAfterGitLabIncidentWrite struct {
	sink        GitLabIncidentsClickHouseEffects
	destination string
	failure     error
}

func (writer crashAfterGitLabIncidentWrite) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if err := writer.sink.WriteEffect(ctx, claim, effect); err != nil {
		return err
	}
	if effect.Destination == writer.destination {
		return writer.failure
	}
	return nil
}

func writeGitLabIncidentForeignCollision(
	t *testing.T,
	ctx context.Context,
	sink GitLabIncidentsClickHouseEffects,
	effect EffectBatch,
) {
	t.Helper()
	switch effect.Destination {
	case "operational_services":
		var row gitLabOperationalServiceRow
		if err := json.Unmarshal(effect.Rows[0], &row); err != nil {
			t.Fatal(err)
		}
		row.OrgID = "zz-foreign-org"
		if err := sink.writeServices(ctx, []gitLabOperationalServiceRow{row}); err != nil {
			t.Fatal(err)
		}
	case "operational_service_repository_mappings":
		var row gitLabServiceRepositoryMappingRow
		if err := json.Unmarshal(effect.Rows[0], &row); err != nil {
			t.Fatal(err)
		}
		row.OrgID = "zz-foreign-org"
		if err := sink.writeMappings(ctx, []gitLabServiceRepositoryMappingRow{row}); err != nil {
			t.Fatal(err)
		}
	case "operational_incidents":
		var row jiraIncidentRow
		if err := json.Unmarshal(effect.Rows[0], &row); err != nil {
			t.Fatal(err)
		}
		row.OrgID = "zz-foreign-org"
		if err := sink.writeIncidents(ctx, []jiraIncidentRow{row}); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unexpected destination %s", effect.Destination)
	}
}
