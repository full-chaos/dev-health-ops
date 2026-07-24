//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// reposDDL mirrors the production table after migrations 024 (org_id), 027
// (sorting key), 028 (provider), and 065 (source_id). The engine and version
// column are the point of this suite: the readback must resolve the winning
// ReplacingMergeTree version rather than every physical row.
const reposDDL = `
CREATE TABLE repos (
  id UUID,
  org_id String DEFAULT 'default',
  repo String,
  ref Nullable(String),
  created_at DateTime64(3, 'UTC'),
  settings Nullable(String),
  tags Nullable(String),
  provider String DEFAULT 'unknown',
  source_id Nullable(UUID) DEFAULT NULL,
  last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, id)`

type repositoryReadbackHarness struct {
	conn       driver.Conn
	repository *PostgresRepository
	claim      Claim
	sink       GitHubRepositoryClickHouseEffects
	now        time.Time
}

func startRepositoryReadbackHarness(
	t *testing.T,
	ctx context.Context,
) *repositoryReadbackHarness {
	t.Helper()
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := postgres.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := clickhouseInstance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	// repo-metadata is the CUT-09 native slice: light cost class, no watermark.
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET provider = 'github', dataset_key = 'repo-metadata',
    cost_class = 'light', processor_flags = '{}'
WHERE id = $1`, firstUnitID); err != nil {
		t.Fatal(err)
	}
	conn, err := clickhousestore.Open(
		ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, reposDDL); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &repositoryReadbackHarness{
		conn:       conn,
		repository: repository,
		claim:      claim,
		now:        now,
		sink: GitHubRepositoryClickHouseEffects{
			Conn: conn, Lease: leaseGuardAt(repository, claim, now),
		},
	}
}

func repositoryEffect(t *testing.T, claim Claim, row repositoryRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues(
		"repos", EffectReadbackRequired, []repositoryRow{row},
	)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

func repositoryFixtureRow(orgID string, at time.Time) repositoryRow {
	identity, err := repositoryIdentity("Acme/API")
	if err != nil {
		panic(err)
	}
	return repositoryRow{
		ID: identity, OrgID: orgID, Repo: "Acme/API",
		CreatedAt: at, LastSynced: at, Provider: "github",
		Settings: `{"source":"github","github_instance_url":"github.com",` +
			`"repo_id":4567,"url":"https://github.com/Acme/API","default_branch":"main"}`,
		Tags: `["github","Go"]`,
	}
}

// TestGitHubRepositoryReadbackResolvesWinningReplacingMergeTreeVersion proves
// the readback SQL against a real ReplacingMergeTree table. The unit tests
// cover the decision table; only a live engine proves the argMax query shape
// actually resolves the winning version and that NULL ref/source_id survive
// the ifNull scan contract.
func TestGitHubRepositoryReadbackResolvesWinningReplacingMergeTreeVersion(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startRepositoryReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now
	current := repositoryFixtureRow(claim.OrgID, now)
	effect := repositoryEffect(t, claim, current)

	// 1. Nothing written yet.
	inspection, err := sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("empty table inspection=%s error=%v", inspection, err)
	}

	// 2. Only an earlier occurrence's version exists. Pre-merge history must
	//    not be mistaken for this effect.
	previous := repositoryFixtureRow(claim.OrgID, now.Add(-24*time.Hour))
	previous.Settings = `{"source":"github","default_branch":"master"}`
	if err := sink.WriteEffect(
		ctx, claim, repositoryEffect(t, claim, previous),
	); err != nil {
		t.Fatal(err)
	}
	inspection, err = sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("stale-only inspection=%s error=%v", inspection, err)
	}

	// 3. This occurrence lands on top of that unmerged history. The readback
	//    must report exact even though two physical versions coexist.
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	assertRepositoryVersionCount(t, ctx, harness, current.ID, 2)
	inspection, err = sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectExact {
		t.Fatalf("pre-merge history inspection=%s error=%v", inspection, err)
	}

	// 4. A duplicate reinsert of the identical row is still exactly this
	//    effect; ReplacingMergeTree collapses the copies on merge.
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	assertRepositoryVersionCount(t, ctx, harness, current.ID, 3)
	inspection, err = sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectExact {
		t.Fatalf("duplicate inspection=%s error=%v", inspection, err)
	}
}

// TestGitHubRepositoryReadbackReportsConflictingWinningVersions covers the
// cases where recovery must stop instead of reinserting.
func TestGitHubRepositoryReadbackReportsConflictingWinningVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Run("same version, different content", func(t *testing.T) {
		harness := startRepositoryReadbackHarness(t, ctx)
		claim, sink, now := harness.claim, harness.sink, harness.now
		expected := repositoryFixtureRow(claim.OrgID, now)
		// Another writer owns the key at this exact version.
		other := expected
		other.Settings = `{"source":"external_ingest"}`
		if err := sink.WriteEffect(
			ctx, claim, repositoryEffect(t, claim, other),
		); err != nil {
			t.Fatal(err)
		}
		inspection, err := sink.InspectEffect(
			ctx, claim, repositoryEffect(t, claim, expected),
		)
		if err != nil || inspection != EffectConflict {
			t.Fatalf("inspection=%s error=%v", inspection, err)
		}
	})

	t.Run("newer occurrence superseded the key", func(t *testing.T) {
		harness := startRepositoryReadbackHarness(t, ctx)
		claim, sink, now := harness.claim, harness.sink, harness.now
		expected := repositoryFixtureRow(claim.OrgID, now)
		newer := repositoryFixtureRow(claim.OrgID, now.Add(time.Hour))
		if err := sink.WriteEffect(
			ctx, claim, repositoryEffect(t, claim, newer),
		); err != nil {
			t.Fatal(err)
		}
		inspection, err := sink.InspectEffect(
			ctx, claim, repositoryEffect(t, claim, expected),
		)
		if err != nil || inspection != EffectConflict {
			t.Fatalf("inspection=%s error=%v", inspection, err)
		}
	})

	t.Run("external ingest stamped source_id", func(t *testing.T) {
		harness := startRepositoryReadbackHarness(t, ctx)
		claim, sink, now := harness.claim, harness.sink, harness.now
		expected := repositoryFixtureRow(claim.OrgID, now)
		// The native sink never writes source_id, so a stamped row means the
		// external-ingest path owns this key. Written directly because the
		// bounded effect row has no source_id field at all.
		if err := harness.conn.Exec(ctx, `
INSERT INTO repos (
  id, org_id, repo, ref, created_at, settings, tags, provider, source_id,
  last_synced
) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)`,
			expected.ID, expected.OrgID, expected.Repo, expected.CreatedAt,
			expected.Settings, expected.Tags, expected.Provider,
			"22222222-2222-4222-8222-222222222222", expected.LastSynced,
		); err != nil {
			t.Fatal(err)
		}
		inspection, err := sink.InspectEffect(
			ctx, claim, repositoryEffect(t, claim, expected),
		)
		if err != nil || inspection != EffectConflict {
			t.Fatalf("inspection=%s error=%v", inspection, err)
		}
	})
}

// TestGitHubRepositoryCrashWindowRecoversWithoutDuplicateVersion is the
// end-to-end fence: ClickHouse accepted the insert, the process died before
// CommitEffect, and recovery must mark the effect committed from the readback
// instead of writing a second physical version.
func TestGitHubRepositoryCrashWindowRecoversWithoutDuplicateVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startRepositoryReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now
	row := repositoryFixtureRow(claim.OrgID, now)
	effect := repositoryEffect(t, claim, row)

	state, err := NewEffectLedgerState(
		claim, []EffectBatch{effect}, now.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.repository.PrepareEffects(
		ctx, claim, state, now.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if err := harness.repository.BeginEffect(
		ctx, claim, 0, effect.ContentDigest, now.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	// Kill window: ClickHouse holds the row, Postgres still says writing.
	recoveryNow := now.Add(61 * time.Second)
	freshRepository, err := NewPostgresRepository(harness.repository.Pool)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := freshRepository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
		Now: recoveryNow, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	freshSink := GitHubRepositoryClickHouseEffects{
		Conn:  harness.conn,
		Lease: leaseGuardAt(freshRepository, recovered, recoveryNow),
	}
	result, err := (EffectCommitter{
		Ledger: freshRepository, Sink: freshSink, Readback: freshSink,
		Now: func() time.Time { return recoveryNow },
	}).Commit(ctx, recovered, []EffectBatch{effect})
	if err != nil || result.MarkedCommitted != 1 || result.Written != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	// The readback fence is what keeps this at one physical version; a blind
	// replay-safe reinsert would leave two visible to raw readers until merge.
	assertRepositoryVersionCount(t, ctx, harness, row.ID, 1)
}

func assertRepositoryVersionCount(
	t *testing.T,
	ctx context.Context,
	harness *repositoryReadbackHarness,
	id string,
	want uint64,
) {
	t.Helper()
	var rows uint64
	if err := harness.conn.QueryRow(ctx, `
SELECT count() FROM repos WHERE org_id = 'org-acme' AND id = ?`,
		id,
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != want {
		t.Fatalf("physical repos versions=%d want %d", rows, want)
	}
}
