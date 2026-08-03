//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const gitBlameDDL = `
CREATE TABLE git_blame (
  repo_id UUID, path String, line_no UInt32,
  author_email Nullable(String), author_name Nullable(String),
  author_when Nullable(DateTime64(3, 'UTC')), commit_hash Nullable(String),
  line Nullable(String), last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, repo_id, path, line_no)`

type blameReadbackHarness struct {
	conn       driver.Conn
	repository *PostgresRepository
	claim      Claim
	sink       GitHubBlameClickHouseEffects
	now        time.Time
}

func startBlameReadbackHarness(t *testing.T, ctx context.Context) *blameReadbackHarness {
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
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET provider = 'github', dataset_key = 'blame',
    cost_class = 'heavy', processor_flags = '{"sync_blame": true}'
WHERE id = $1`, firstUnitID); err != nil {
		t.Fatal(err)
	}
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, gitBlameDDL); err != nil {
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
	return &blameReadbackHarness{
		conn: conn, repository: repository, claim: claim, now: now,
		sink: GitHubBlameClickHouseEffects{
			Conn: conn, Lease: leaseGuardAt(repository, claim, now),
		},
	}
}

func TestGitHubBlameReadbackSelectsBothOwningTenantsFromNaturalKeyCollision(t *testing.T) {
	ctx, sink := newGitHubBlameIntegrationSink(t)
	claim := nativeTestClaim("github", "blame")
	claim.OrgID = "org-a"
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	emailA, nameA, hashA := "a@example.com", "Tenant A", "abc123"
	row := gitBlameRow{
		RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Path: "src/main.go", LineNo: 1,
		AuthorEmail: &emailA, AuthorName: &nameA, CommitHash: &hashA,
		LastSynced: now, OrgID: claim.OrgID,
	}
	otherClaim := claim
	otherClaim.OrgID = "org-b"
	emailB, nameB, hashB := "b@example.com", "Tenant B", "def456"
	otherRow := row
	otherRow.OrgID, otherRow.AuthorEmail, otherRow.AuthorName, otherRow.CommitHash =
		otherClaim.OrgID, &emailB, &nameB, &hashB
	if err := sink.WriteEffect(ctx, otherClaim, gitBlameEffect(t, otherRow)); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, gitBlameEffect(t, row)); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		claim Claim
		row   gitBlameRow
		name  string
	}{
		{claim: claim, row: row, name: nameA},
		{claim: otherClaim, row: otherRow, name: nameB},
	} {
		var count uint64
		var minOrg, maxOrg, minName, maxName string
		if err := sink.Conn.QueryRow(ctx, `
SELECT count(), min(org_id), max(org_id), min(ifNull(author_name, '')), max(ifNull(author_name, ''))
FROM git_blame FINAL
WHERE org_id = ? AND repo_id = ? AND path = ? AND line_no = ?`,
			test.row.OrgID, test.row.RepoID, test.row.Path, test.row.LineNo,
		).Scan(&count, &minOrg, &maxOrg, &minName, &maxName); err != nil {
			t.Fatal(err)
		}
		if count != 1 || minOrg != test.row.OrgID || maxOrg != test.row.OrgID || minName != test.name || maxName != test.name {
			t.Fatalf("tenant=%s count=%d orgs=(%q,%q) names=(%q,%q)", test.row.OrgID, count, minOrg, maxOrg, minName, maxName)
		}
		inspection, err := sink.InspectEffect(ctx, test.claim, gitBlameEffect(t, test.row))
		if err != nil || inspection != EffectExact {
			t.Fatalf("tenant=%s inspection=%s error=%v", test.row.OrgID, inspection, err)
		}
	}
}

func TestGitHubBlameCoverageReadsOnlyTheOwningTenant(t *testing.T) {
	ctx, sink := newGitHubBlameIntegrationSink(t)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claimA := nativeTestClaim("github", "blame")
	claimA.OrgID = "org-a"
	claimB := claimA
	claimB.OrgID = "org-b"
	email, name, hash := "a@example.com", "Ada", "abc123"
	row := func(orgID, path string) gitBlameRow {
		return gitBlameRow{
			RepoID: repoID, Path: path, LineNo: 1,
			AuthorEmail: &email, AuthorName: &name, CommitHash: &hash,
			LastSynced: now, OrgID: orgID,
		}
	}
	for _, write := range []struct {
		claim Claim
		path  string
	}{
		{claim: claimA, path: "src/a-only.go"},
		{claim: claimA, path: "src/shared.go"},
		{claim: claimA, path: "src/third.go"},
		{claim: claimB, path: "src/b-only.go"},
		{claim: claimB, path: "src/shared.go"},
	} {
		if err := sink.WriteEffect(ctx, write.claim, gitBlameEffect(t, row(write.claim.OrgID, write.path))); err != nil {
			t.Fatal(err)
		}
	}
	coverage := GitHubBlameClickHouseCoverage{Conn: sink.Conn, Lease: sink.Lease}
	pathsA, err := coverage.BlamedPaths(ctx, claimA, repoID)
	if err != nil {
		t.Fatal(err)
	}
	pathsB, err := coverage.BlamedPaths(ctx, claimB, repoID)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(pathsA)
	slices.Sort(pathsB)
	if !slices.Equal(pathsA, []string{"src/a-only.go", "src/shared.go", "src/third.go"}) {
		t.Fatalf("org-a paths=%v", pathsA)
	}
	if !slices.Equal(pathsB, []string{"src/b-only.go", "src/shared.go"}) {
		t.Fatalf("org-b paths=%v", pathsB)
	}
	bounded := GitHubBlameClickHouseCoverage{Conn: sink.Conn, Lease: sink.Lease, MaxPaths: 2}
	if _, err := bounded.BlamedPaths(ctx, claimA, repoID); !errors.Is(err, ErrGitHubBlameProgressUnavailable) {
		t.Fatalf("over-bound coverage error=%v, want ErrGitHubBlameProgressUnavailable", err)
	}
}

func TestGitHubBlameReadbackResolvesWinningReplacingMergeTreeVersion(t *testing.T) {
	ctx, sink := newGitHubBlameIntegrationSink(t)
	claim := nativeTestClaim("github", "blame")
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	email, name, hash := "a@example.com", "Current", "abc123"
	current := gitBlameRow{
		RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Path: "src/main.go", LineNo: 1,
		AuthorEmail: &email, AuthorName: &name, CommitHash: &hash,
		LastSynced: now, OrgID: claim.OrgID,
	}
	previous := current
	previousName := "Previous"
	previous.AuthorName, previous.LastSynced = &previousName, now.Add(-time.Hour)
	if err := sink.WriteEffect(ctx, claim, gitBlameEffect(t, previous)); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, gitBlameEffect(t, current)); err != nil || inspection != EffectAbsent {
		t.Fatalf("stale inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, gitBlameEffect(t, current)); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, gitBlameEffect(t, current)); err != nil || inspection != EffectExact {
		t.Fatalf("winning inspection=%s error=%v", inspection, err)
	}
}

// TestGitHubBlameCrashWindowRecoversWithoutDuplicateVersion proves the full
// crash boundary: ClickHouse accepted the rows, the process died before the
// PostgreSQL ledger commit, and the recovered owner reconciles exact rows
// instead of appending a second physical ReplacingMergeTree version.
func TestGitHubBlameCrashWindowRecoversWithoutDuplicateVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startBlameReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now
	client := gitHubRepositoryClient(t, gitHubBlameDoer{t: t, fileCount: 1}, "https://api.github.com")
	firstBatch, err := collectGitHubBlameFoundation(
		ctx, claim, client, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	effect := firstBatch.Effects[0]
	var firstRow gitBlameRow
	if err := json.Unmarshal(effect.Rows[0], &firstRow); err != nil {
		t.Fatal(err)
	}
	state, err := NewEffectLedgerState(claim, []EffectBatch{effect}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.repository.PrepareEffects(ctx, claim, state, now); err != nil {
		t.Fatal(err)
	}
	if err := harness.repository.BeginEffect(ctx, claim, 0, effect.ContentDigest, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}

	recoveryNow := now.Add(61 * time.Second)
	freshRepository, err := NewPostgresRepository(harness.repository.Pool)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := freshRepository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: claim.OrgID, Owner: uuid.NewString(), Now: recoveryNow,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := freshRepository.LoadEffects(ctx, recovered, recoveryNow)
	if err != nil {
		t.Fatal(err)
	}
	if !persisted.CreatedAt.UTC().Equal(now.UTC()) {
		t.Fatalf("persisted ledger CreatedAt=%s want=%s", persisted.CreatedAt, now)
	}
	recoveredBatch, err := collectGitHubBlameFoundation(
		ctx, recovered, client, persisted.CreatedAt.UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredBatch.Effects[0].ContentDigest != effect.ContentDigest {
		t.Fatalf("regenerated digest=%s want=%s", recoveredBatch.Effects[0].ContentDigest, effect.ContentDigest)
	}
	freshSink := GitHubBlameClickHouseEffects{
		Conn: harness.conn, Lease: leaseGuardAt(freshRepository, recovered, recoveryNow),
	}
	result, err := (EffectCommitter{
		Ledger: freshRepository, Sink: freshSink, Readback: freshSink,
		Now: func() time.Time { return recoveryNow },
	}).Commit(ctx, recovered, recoveredBatch.Effects, persisted.CreatedAt.UTC())
	if err != nil || result.MarkedCommitted != 1 || result.Written != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	var physicalRows uint64
	if err := harness.conn.QueryRow(ctx, `
SELECT count() FROM git_blame
WHERE org_id = ? AND repo_id = ? AND path = ?`,
		claim.OrgID, firstRow.RepoID, firstRow.Path,
	).Scan(&physicalRows); err != nil {
		t.Fatal(err)
	}
	if physicalRows != uint64(len(effect.Rows)) {
		t.Fatalf("physical git_blame rows=%d want=%d", physicalRows, len(effect.Rows))
	}
}

func newGitHubBlameIntegrationSink(t *testing.T) (context.Context, GitHubBlameClickHouseEffects) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, gitBlameDDL); err != nil {
		t.Fatal(err)
	}
	return ctx, GitHubBlameClickHouseEffects{
		Conn:  conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
}

func gitBlameEffect(t *testing.T, row gitBlameRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("git_blame", EffectReadbackRequired, []gitBlameRow{row})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
