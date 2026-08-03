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
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
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

const gitHubBlamePathProgressDDL = `
CREATE TABLE github_blame_path_progress (
  org_id LowCardinality(String), repo_id UUID, tree_ref String, path String,
  generation String, outcome LowCardinality(String),
  attempted_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(attempted_at)
ORDER BY (org_id, repo_id, tree_ref, path, generation)`

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
	if err := conn.Exec(ctx, gitHubBlamePathProgressDDL); err != nil {
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
	stateA, err := coverage.Progress(ctx, claimA, repoID, "tree-sha", "")
	if err != nil {
		t.Fatal(err)
	}
	stateB, err := coverage.Progress(ctx, claimB, repoID, "tree-sha", "")
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(stateA.BlamedPaths)
	slices.Sort(stateB.BlamedPaths)
	if !slices.Equal(stateA.BlamedPaths, []string{"src/a-only.go", "src/shared.go", "src/third.go"}) {
		t.Fatalf("org-a paths=%v", stateA.BlamedPaths)
	}
	if !slices.Equal(stateB.BlamedPaths, []string{"src/b-only.go", "src/shared.go"}) {
		t.Fatalf("org-b paths=%v", stateB.BlamedPaths)
	}
	bounded := GitHubBlameClickHouseCoverage{Conn: sink.Conn, Lease: sink.Lease, MaxPaths: 2}
	if _, err := bounded.Progress(ctx, claimA, repoID, "tree-sha", ""); !errors.Is(err, ErrGitHubBlameProgressUnavailable) {
		t.Fatalf("over-bound coverage error=%v, want ErrGitHubBlameProgressUnavailable", err)
	}
}

func TestGitHubBlameProgressIsolatedAcrossRepositoriesInOneOrganization(t *testing.T) {
	ctx, sink := newGitHubBlameIntegrationSink(t)
	claim := nativeTestClaim("github", "blame")
	repoA := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	repoB := "65dcfc23-a1e3-e490-d16a-afbfcec54939"
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	for _, marker := range []gitHubBlamePathProgressRow{
		newGitHubBlamePathProgressRow(claim, repoA, "tree-sha", "src/a.go", gitHubBlameOutcomeEmpty, now),
		newGitHubBlamePathProgressRow(claim, repoB, "tree-sha", "src/b.go", gitHubBlameOutcomeRetryableError, now),
	} {
		effect, err := effectBatchFromValues(
			"github_blame_path_progress", EffectReadbackRequired,
			[]gitHubBlamePathProgressRow{marker},
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatal(err)
		}
	}
	coverage := GitHubBlameClickHouseCoverage{Conn: sink.Conn, Lease: sink.Lease}
	stateA, err := coverage.Progress(ctx, claim, repoA, "tree-sha", claim.GenerationKey())
	if err != nil {
		t.Fatal(err)
	}
	stateB, err := coverage.Progress(ctx, claim, repoB, "tree-sha", claim.GenerationKey())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(stateA.EmptyPaths, []string{"src/a.go"}) ||
		len(stateA.FailedAttempts) != 0 || stateA.InFlightOutcomes["src/a.go"] != gitHubBlameOutcomeEmpty {
		t.Fatalf("repo-a progress=%+v", stateA)
	}
	if len(stateB.EmptyPaths) != 0 || stateB.FailedAttempts["src/b.go"] != 1 ||
		stateB.InFlightOutcomes["src/b.go"] != gitHubBlameOutcomeRetryableError {
		t.Fatalf("repo-b progress=%+v", stateB)
	}
}

func TestGitHubBlameZeroRangeProgressPreventsReselection(t *testing.T) {
	ctx, sink := newGitHubBlameIntegrationSink(t)
	claim := nativeTestClaim("github", "blame")
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	marker := newGitHubBlamePathProgressRow(
		claim, repoID, "tree-sha", "src/file-000.go", gitHubBlameOutcomeEmpty, now,
	)
	effect, err := effectBatchFromValues(
		"github_blame_path_progress", EffectReadbackRequired,
		[]gitHubBlamePathProgressRow{marker},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	attempted := []string{}
	client := gitHubRepositoryClient(t, gitHubBlameDoer{
		t: t, fileCount: 1, blamePaths: &attempted,
	}, "https://api.github.com")
	batch, err := (GitHubBlameRouteHandler{
		Coverage: GitHubBlameClickHouseCoverage{Conn: sink.Conn, Lease: sink.Lease},
	}).Collect(ctx, claim, providerfoundation.Credential{}, client, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(attempted) != 0 || batch.Result["inventory_status"] != "empty" ||
		len(batch.Effects[0].Rows) != 0 || len(batch.Effects[1].Rows) != 0 {
		t.Fatalf("attempted=%v result=%v effects=%+v", attempted, batch.Result, batch.Effects)
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
// production retry path. The ordered progress effect is durable first,
// ClickHouse then accepts blame rows, the process dies before the PostgreSQL
// blame-effect commit, and CompleteRouteExecutor reconstructs the exact
// in-flight selection before coverage can choose a new manifest.
func TestGitHubBlameCrashWindowRecoversWithoutDuplicateVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startBlameReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now
	client := gitHubRepositoryClient(t, gitHubBlameDoer{t: t, fileCount: 2}, "https://api.github.com")
	handler := GitHubBlameRouteHandler{
		Coverage: GitHubBlameClickHouseCoverage{Conn: harness.conn, Lease: sink.Lease},
		MaxFiles: 1,
	}
	firstBatch, err := handler.Collect(
		ctx, claim, providerfoundation.Credential{}, client, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if firstBatch.Effects[0].Destination != "github_blame_path_progress" ||
		firstBatch.Effects[1].Destination != "git_blame" {
		t.Fatalf("effect order=%v,%v", firstBatch.Effects[0].Destination, firstBatch.Effects[1].Destination)
	}
	progressEffect, effect := firstBatch.Effects[0], firstBatch.Effects[1]
	var firstRow gitBlameRow
	if err := json.Unmarshal(effect.Rows[0], &firstRow); err != nil {
		t.Fatal(err)
	}
	state, err := NewEffectLedgerState(claim, firstBatch.Effects, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.repository.PrepareEffects(ctx, claim, state, now); err != nil {
		t.Fatal(err)
	}
	if err := harness.repository.BeginEffect(ctx, claim, 0, progressEffect.ContentDigest, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, progressEffect); err != nil {
		t.Fatal(err)
	}
	if err := harness.repository.CommitEffect(ctx, claim, 0, progressEffect.ContentDigest, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := harness.repository.BeginEffect(ctx, claim, 1, effect.ContentDigest, now.Add(3*time.Second)); err != nil {
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
	freshSink := GitHubBlameClickHouseEffects{
		Conn: harness.conn, Lease: leaseGuardAt(freshRepository, recovered, recoveryNow),
	}
	session := &LeaseSession{
		Repository: freshRepository, Claim: recovered, LeaseDuration: time.Minute,
		Deadline: recoveryNow.Add(5 * time.Minute), Now: func() time.Time { return recoveryNow },
	}
	descriptor, _ := (CompleteRouteSwitches{GithubBlame: true}).Descriptor("github", "blame")
	executor := CompleteRouteExecutor{
		Credentials: providerfoundation.CredentialResolver{
			Repository: githubBlameIntegrationCredentialRepository{},
			Decryptor:  githubBlameIntegrationCredentialDecryptor{},
		},
		Doer: gitHubBlameDoer{t: t, fileCount: 2},
		Retry: providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		Budget:       executorBudgetStore{},
		BudgetLimits: map[CostClass]int{CostHeavy: 1},
		BudgetTTL:    time.Minute,
		Gate: func(Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
			return executorBackoffGate{}
		},
		Handler: GitHubBlameRouteHandler{
			Coverage: GitHubBlameClickHouseCoverage{Conn: harness.conn, Lease: freshSink.Lease},
			MaxFiles: 1,
		},
		Comparator: ProductionContractComparator{},
		Committer: EffectCommitter{
			Ledger: freshRepository, Sink: freshSink, Readback: freshSink,
			Now: func() time.Time { return recoveryNow },
		},
		HeartbeatInterval: 30 * time.Second,
		Now:               func() time.Time { return recoveryNow },
	}
	result, err := executor.Execute(ctx, session, descriptor)
	if err != nil || result.Effects.MarkedCommitted != 1 || result.Effects.Skipped != 1 || result.Effects.Written != 0 {
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

type githubBlameIntegrationCredentialRepository struct{}

func (githubBlameIntegrationCredentialRepository) ResolveEncrypted(
	context.Context,
	providerfoundation.TenantScope,
) (providerfoundation.EncryptedCredential, error) {
	return providerfoundation.EncryptedCredential{
		ID: firstCredentialID, Provider: "github", Name: "fixture", Active: true,
		Ciphertext: secrets.NewValue("opaque"),
		Config:     map[string]string{"base_url": "https://api.github.com"},
	}, nil
}

type githubBlameIntegrationCredentialDecryptor struct{}

func (githubBlameIntegrationCredentialDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return []byte(`{"token":"fixture-token"}`), nil
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
	if err := conn.Exec(ctx, gitHubBlamePathProgressDDL); err != nil {
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
