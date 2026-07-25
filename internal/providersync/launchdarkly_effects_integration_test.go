//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLaunchDarklyPlainMergeTreeEventCrashUsesExactReadback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := postgres.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := clickhouseInstance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET provider = 'launchdarkly', dataset_key = 'feature-flags',
    cost_class = 'medium', processor_flags = '{}'
WHERE id = $1`, firstUnitID); err != nil {
		t.Fatal(err)
	}
	conn, err := clickhousestore.Open(
		ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, `
CREATE TABLE feature_flag_event (
  org_id String,
  event_type String,
  flag_key String,
  environment String,
  repo_id String,
  actor_type String,
  prev_state String,
  next_state String,
  event_ts DateTime64(3, 'UTC'),
  ingested_at DateTime64(3, 'UTC'),
  source_event_id String,
  dedupe_key String
) ENGINE = MergeTree
ORDER BY (org_id, flag_key, environment, event_ts)`); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	firstClaim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	eventEffect, err := effectBatchFromValues(
		"feature_flag_event",
		EffectReadbackRequired,
		[]launchDarklyEventRow{{
			OrgID: "org-acme", EventType: "toggle", FlagKey: "checkout",
			Environment: "production", EventAt: now, IngestedAt: now,
			SourceEventID: "event-1", DedupeKey: "event-1",
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewEffectLedgerState(
		firstClaim, []EffectBatch{eventEffect}, now.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.PrepareEffects(
		ctx, firstClaim, state, now.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if err := repository.BeginEffect(
		ctx, firstClaim, 0, eventEffect.ContentDigest, now.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	firstSink := LaunchDarklyClickHouseEffects{
		Conn: conn, Lease: leaseGuardAt(repository, firstClaim, now.Add(2*time.Second)),
	}
	if err := firstSink.WriteEffect(ctx, firstClaim, eventEffect); err != nil {
		t.Fatal(err)
	}
	// Kill window: ClickHouse accepted the plain-MergeTree event, while the
	// Postgres effect remains writing.
	recoveryNow := now.Add(61 * time.Second)
	freshRepository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := freshRepository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: recoveryNow,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	freshSink := LaunchDarklyClickHouseEffects{
		Conn: conn, Lease: leaseGuardAt(freshRepository, recovered, recoveryNow),
	}
	result, err := (EffectCommitter{
		Ledger: freshRepository, Sink: freshSink, Readback: freshSink,
		Now: func() time.Time { return recoveryNow },
	}).Commit(ctx, recovered, []EffectBatch{eventEffect})
	if err != nil || result.MarkedCommitted != 1 || result.Written != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	var rows uint64
	if err := conn.QueryRow(ctx, `
SELECT count()
FROM feature_flag_event
WHERE org_id = 'org-acme' AND dedupe_key = 'event-1'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("plain MergeTree event rows=%d", rows)
	}
}

func leaseGuardAt(
	repository *PostgresRepository,
	claim Claim,
	now time.Time,
) leaseGuard {
	return leaseGuard{assert: func(ctx context.Context) error {
		return repository.Assert(ctx, claim, now)
	}}
}

type leaseGuard struct {
	assert func(context.Context) error
}

func (guard leaseGuard) Assert(ctx context.Context) error {
	return guard.assert(ctx)
}
