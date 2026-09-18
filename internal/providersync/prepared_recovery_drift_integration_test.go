//go:build integration

package providersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

var errSimulatedCrash = errors.New("simulated crash after the sink accepted the write")

// crashAfterFirstWriteSink lets the first effect land in ClickHouse and then
// fails before the ledger can record the commit: the crash window recovery
// exists for.
type crashAfterFirstWriteSink struct {
	inner   EffectSink
	crashed bool
}

func (sink *crashAfterFirstWriteSink) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if err := sink.inner.WriteEffect(ctx, claim, effect); err != nil {
		return err
	}
	if !sink.crashed {
		sink.crashed = true
		return errSimulatedCrash
	}
	return nil
}

type driftCredentialRepository struct{ provider, baseURL string }

func (repository driftCredentialRepository) ResolveEncrypted(
	context.Context, providerfoundation.TenantScope,
) (providerfoundation.EncryptedCredential, error) {
	return providerfoundation.EncryptedCredential{
		ID: firstCredentialID, Provider: repository.provider, Name: "fixture", Active: true,
		Ciphertext: secrets.NewValue("opaque"),
		Config:     map[string]string{"base_url": repository.baseURL},
	}, nil
}

// countingDoer records how many provider requests a pass made.
type countingDoer struct {
	mu       sync.Mutex
	delegate providerfoundation.HTTPDoer
	requests int
}

func (doer *countingDoer) Do(request *http.Request) (*http.Response, error) {
	doer.mu.Lock()
	doer.requests++
	doer.mu.Unlock()
	return doer.delegate.Do(request)
}

type driftRoute struct {
	provider, dataset, flags, baseURL string
	handler                           CompleteRouteHandler
	sink                              func(driver.Conn, providerfoundation.LeaseGuard) interface {
		EffectSink
		EffectReadback
	}
}

type driftOutcome struct {
	harness         *carryForwardLedgerHarness
	firstErr        error
	result          CompleteRouteExecutionResult
	err             error
	recoveryFetches int
}

func driftExecutor(
	route driftRoute, harness *carryForwardLedgerHarness, repository *PostgresRepository,
	claim Claim, at time.Time, doer providerfoundation.HTTPDoer, sink EffectSink, readback EffectReadback,
) CompleteRouteExecutor {
	return CompleteRouteExecutor{
		Credentials: providerfoundation.CredentialResolver{
			Repository: driftCredentialRepository{provider: route.provider, baseURL: route.baseURL},
			Decryptor:  githubBlameIntegrationCredentialDecryptor{},
		},
		Doer: doer,
		Retry: providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		Budget:       executorBudgetStore{},
		BudgetLimits: map[CostClass]int{CostMedium: 1},
		BudgetTTL:    time.Minute,
		Gate: func(Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
			return executorBackoffGate{}
		},
		Handler:    route.handler,
		Comparator: ProductionContractComparator{},
		Committer: EffectCommitter{
			Ledger: repository, Sink: sink, Readback: readback,
			Now: func() time.Time { return at },
		},
		HeartbeatInterval: 30 * time.Second,
		Now:               func() time.Time { return at },
	}
}

// runDriftCell executes one unit the way the worker does: the first pass
// crashes after its first effect landed, the provider then answers
// differently, and the unit is re-claimed and executed again. prepared runs the
// route as registered; !prepared runs it without snapshot recovery, so the same
// cell runs with and without the prepared snapshot.
func runDriftCell(
	t *testing.T, ctx context.Context, route driftRoute,
	before, after providerfoundation.HTTPDoer, prepared bool,
) driftOutcome {
	t.Helper()
	harness := startProviderLedgerHarness(t, ctx, route.provider, route.dataset, route.flags)
	descriptor, ok := Descriptor(route.provider, route.dataset)
	if !ok {
		t.Fatalf("no descriptor for %s/%s", route.provider, route.dataset)
	}
	// The snapshot arm runs the route exactly as registered; the other arm
	// is the same route on a binary that does not recover it from a snapshot.
	if prepared && !descriptor.PreparedManifestRecovery {
		t.Fatalf("%s/%s is not registered for prepared recovery", route.provider, route.dataset)
	}
	if !prepared {
		descriptor.PreparedManifestRecovery = false
	}

	firstSinks := route.sink(harness.conn, leaseGuardAt(harness.repository, harness.claim, harness.now))
	crashing := &crashAfterFirstWriteSink{inner: firstSinks}
	firstSession := &LeaseSession{
		Repository: harness.repository, Claim: harness.claim, LeaseDuration: time.Minute,
		Deadline: harness.now.Add(5 * time.Minute), Now: func() time.Time { return harness.now },
	}
	_, firstErr := driftExecutor(route, harness, harness.repository, harness.claim, harness.now,
		before, crashing, firstSinks).Execute(ctx, firstSession, descriptor)

	recoveryNow := harness.now.Add(61 * time.Second)
	fresh, err := NewPostgresRepository(harness.repository.Pool)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := fresh.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: harness.claim.OrgID, Owner: uuid.NewString(),
		Now: recoveryNow, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &LeaseSession{
		Repository: fresh, Claim: recovered, LeaseDuration: time.Minute,
		Deadline: recoveryNow.Add(5 * time.Minute), Now: func() time.Time { return recoveryNow },
	}
	counting := &countingDoer{delegate: after}
	sinks := route.sink(harness.conn, leaseGuardAt(fresh, recovered, recoveryNow))
	result, err := driftExecutor(route, harness, fresh, recovered, recoveryNow,
		counting, sinks, sinks).Execute(ctx, session, descriptor)
	return driftOutcome{harness: harness, firstErr: firstErr, result: result, err: err, recoveryFetches: counting.requests}
}

func githubDeploymentsDriftRoute() driftRoute {
	return driftRoute{
		provider: "github", dataset: "deployments", flags: `{"sync_deployments": true}`,
		baseURL: "https://api.github.com", handler: GitHubDeploymentsRouteHandler{},
		sink: func(conn driver.Conn, lease providerfoundation.LeaseGuard) interface {
			EffectSink
			EffectReadback
		} {
			return GitHubDeploymentsClickHouseEffects{Conn: conn, Lease: lease}
		},
	}
}

// TestPreparedRecoveryReplaysTheCrashedPassAcrossProviderDrift is the drift
// table: every cell changes what the provider answers between the crash and
// the retry. With the prepared snapshot the retry replays the crashed pass's
// own rows, makes no provider request, and commits from the readback; the
// same cell without the snapshot re-collects different rows and the ledger
// refuses them.
func TestPreparedRecoveryReplaysTheCrashedPassAcrossProviderDrift(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	found := `[{"number":5,"merged_at":"2026-07-22T09:00:00Z","merge_commit_sha":"abc123"}]`
	notFound := `{"message":"Not Found"}`
	pullRequestFound := &githubDeploymentPullLookupDoer{pullStatus: http.StatusOK, pullBody: found}
	pullRequestFailed := &githubDeploymentPullLookupDoer{pullStatus: http.StatusNotFound, pullBody: notFound}
	pullRequestEmpty := &githubDeploymentPullLookupDoer{pullStatus: http.StatusOK, pullBody: `[]`}

	reviewReply := `{"data":{"repository":{"pr0":{"number":42,"reviews":{"nodes":[` +
		`{"databaseId":7001,"state":"APPROVED","submittedAt":"2026-07-11T10:30:00Z","author":{"login":"octocat"}}` +
		`],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`
	reviewsOK := func() providerfoundation.HTTPDoer {
		return &gitHubPullRequestReviewRouteDoer{t: t, restBodies: defaultGitHubPullRequestFixtures(), graphQLReply: reviewReply}
	}
	reviewsFailed := func() providerfoundation.HTTPDoer {
		return &gitHubPullRequestReviewRouteDoer{
			t: t, restBodies: defaultGitHubPullRequestFixtures(),
			graphQLStatus: http.StatusServiceUnavailable, graphQLReply: `{"message":"temporarily unavailable"}`,
		}
	}
	githubPRs := driftRoute{
		provider: "github", dataset: "prs", flags: `{"sync_prs": true}`,
		baseURL: "https://api.github.com", handler: GitHubPullRequestSocialRouteHandler{},
		sink: func(conn driver.Conn, lease providerfoundation.LeaseGuard) interface {
			EffectSink
			EffectReadback
		} {
			return GitHubPullRequestSocialClickHouseEffects{Conn: conn, Lease: lease}
		},
	}

	gitlabDeployment := `[{"id":501,"iid":7,"status":"success","environment":{"name":"production"},"created_at":"2026-07-22T10:00:00Z","sha":"abc"}]`
	gitlabDeploymentsDoer := func(mergeRequest gitLabDeploymentsResponse) providerfoundation.HTTPDoer {
		return &gitLabDeploymentsDoer{t: t, responses: []gitLabDeploymentsResponse{
			{body: gitLabRepositoryFixture}, {body: `[]`}, {body: gitlabDeployment}, mergeRequest,
		}}
	}
	gitlabDeployments := driftRoute{
		provider: "gitlab", dataset: "deployments", flags: `{"sync_deployments": true}`,
		baseURL: "https://gitlab.example", handler: GitLabDeploymentsRouteHandler{},
		sink: func(conn driver.Conn, lease providerfoundation.LeaseGuard) interface {
			EffectSink
			EffectReadback
		} {
			return GitLabDeploymentsClickHouseEffects{Conn: conn, Lease: lease}
		},
	}

	gitlabPRsBefore := func() providerfoundation.HTTPDoer {
		return &gitLabPullRequestDoer{t: t, responses: gitLabPullRequestFixtureResponses()}
	}
	gitlabPRsAfter := func() providerfoundation.HTTPDoer {
		responses := gitLabPullRequestFixtureResponses()
		responses[1].body = strings.Replace(responses[1].body, `"title":"Add API"`, `"title":"Add API, retitled after the crash"`, 1)
		return &gitLabPullRequestDoer{t: t, responses: responses}
	}
	gitlabPRs := driftRoute{
		provider: "gitlab", dataset: "prs", flags: `{"sync_prs": true}`,
		baseURL: "https://gitlab.example", handler: GitLabPullRequestRouteHandler{PerPage: 2},
		sink: func(conn driver.Conn, lease providerfoundation.LeaseGuard) interface {
			EffectSink
			EffectReadback
		} {
			return GitLabPullRequestSocialClickHouseEffects{Conn: conn, Lease: lease}
		},
	}

	for _, cell := range []struct {
		name          string
		route         driftRoute
		before, after func() providerfoundation.HTTPDoer
	}{
		{"github deployments: pull request lookup fails, then finds the pull request", githubDeploymentsDriftRoute(),
			func() providerfoundation.HTTPDoer { return pullRequestFailed }, func() providerfoundation.HTTPDoer { return pullRequestFound }},
		{"github deployments: pull request lookup fails, then succeeds empty", githubDeploymentsDriftRoute(),
			func() providerfoundation.HTTPDoer { return pullRequestFailed }, func() providerfoundation.HTTPDoer { return pullRequestEmpty }},
		{"github deployments: pull request lookup finds the pull request, then fails", githubDeploymentsDriftRoute(),
			func() providerfoundation.HTTPDoer { return pullRequestFound }, func() providerfoundation.HTTPDoer { return pullRequestFailed }},
		{"github prs: review enrichment fails, then succeeds", githubPRs, reviewsFailed, reviewsOK},
		{"github prs: review enrichment succeeds, then fails", githubPRs, reviewsOK, reviewsFailed},
		{"gitlab deployments: merge request lookup fails, then succeeds", gitlabDeployments,
			func() providerfoundation.HTTPDoer {
				return gitlabDeploymentsDoer(gitLabDeploymentsResponse{status: http.StatusServiceUnavailable, body: `{"message":"temporary"}`})
			},
			func() providerfoundation.HTTPDoer {
				return gitlabDeploymentsDoer(gitLabDeploymentsResponse{body: `[{"iid":45,"state":"merged","merged_at":"2026-07-21T10:00:00Z"}]`})
			}},
		{"gitlab prs: a merge request is retitled between the crash and the retry", gitlabPRs, gitlabPRsBefore, gitlabPRsAfter},
	} {
		t.Run(cell.name, func(t *testing.T) {
			withSnapshot := runDriftCell(t, ctx, cell.route, cell.before(), cell.after(), true)
			if !errors.Is(withSnapshot.firstErr, errSimulatedCrash) {
				t.Fatalf("first pass error=%v, want the simulated crash", withSnapshot.firstErr)
			}
			// The crashed effect is settled by its readback: exact when it
			// landed rows (marked committed), absent when it carried none
			// (replayed as the same no-op write).
			settled := withSnapshot.result.Effects.MarkedCommitted + withSnapshot.result.Effects.ResetForReplay
			if withSnapshot.err != nil || settled != 1 || withSnapshot.recoveryFetches != 0 {
				t.Fatalf("prepared recovery err=%v effects=%+v provider_requests=%d, want the crashed pass replayed from its snapshot with no provider request",
					withSnapshot.err, withSnapshot.result.Effects, withSnapshot.recoveryFetches)
			}
			withoutSnapshot := runDriftCell(t, ctx, cell.route, cell.before(), cell.after(), false)
			if !errors.Is(withoutSnapshot.err, ErrEffectLedgerConflict) {
				t.Fatalf("re-collect recovery err=%v, want ErrEffectLedgerConflict: the cell must drift", withoutSnapshot.err)
			}
		})
	}
}

// TestSupersededSnapshotIsDiscardedAndTheRoutePreparedAgain runs the manifest
// change a deploy can bring on real Postgres and ClickHouse: a unit's stored
// snapshot and ledger describe a destination set the route no longer emits
// (here: git_pull_requests only, for pull request 7, which the retry no
// longer returns). The invariant: a discard never erases the evidence of a
// write that may have landed, and a read error is never treated as absent.
func TestSupersededSnapshotIsDiscardedAndTheRoutePreparedAgain(t *testing.T) {
	for _, cell := range []string{"pending", "missing snapshot row", "writing and landed", "writing and absent"} {
		t.Run(cell, func(t *testing.T) { runSupersededSnapshotCell(t, cell) })
	}
}

func runSupersededSnapshotCell(t *testing.T, cell string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	log := captureSlog(t)
	harness := startProviderLedgerHarness(t, ctx, "github", "prs", `{"sync_prs": true}`)
	reviewReply := `{"data":{"repository":{"pr0":{"number":42,"reviews":{"nodes":[` +
		`{"databaseId":7001,"state":"APPROVED","submittedAt":"2026-07-11T10:30:00Z","author":{"login":"octocat"}}` +
		`],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`
	newDoer := func() providerfoundation.HTTPDoer {
		return &gitHubPullRequestReviewRouteDoer{t: t, restBodies: defaultGitHubPullRequestFixtures(), graphQLReply: reviewReply}
	}
	route := driftRoute{
		provider: "github", dataset: "prs", flags: `{"sync_prs": true}`,
		baseURL: "https://api.github.com", handler: GitHubPullRequestSocialRouteHandler{},
		sink: func(conn driver.Conn, lease providerfoundation.LeaseGuard) interface {
			EffectSink
			EffectReadback
		} {
			return GitHubPullRequestSocialClickHouseEffects{Conn: conn, Lease: lease}
		},
	}

	// The superseded document: one git_pull_requests effect for pull request
	// 7, as an older binary with a smaller destination set stored it.
	oldRow := pullRequestReadbackFixture(harness.now)
	oldRow.OrgID, oldRow.Number = harness.claim.OrgID, 7
	oldEffect := pullRequestEffect(t, oldRow)
	superseded := CompleteRouteBatch{
		Effects: []EffectBatch{oldEffect}, Result: map[string]any{"prs_synced": 1},
		Watermark: harness.claim.BeforeAt,
		Evidence:  FetchEvidence{Provider: "github", Dataset: "prs", Records: 1},
	}
	result, err := json.Marshal(superseded.Result)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(storedPreparedRouteSnapshot{
		SchemaVersion: preparedRouteSnapshotSchemaVersion, Generation: harness.claim.GenerationKey(),
		OrgID: harness.claim.OrgID, Provider: "github", Dataset: "prs", NormalizedAt: harness.now,
		Effects: []storedPreparedEffect{{
			Destination: oldEffect.Destination, ContentDigest: oldEffect.ContentDigest,
			Recovery: oldEffect.Recovery, Rows: oldEffect.Rows, PayloadBytes: oldEffect.PayloadBytes,
		}},
		Result: result, Watermark: superseded.Watermark, Evidence: superseded.Evidence,
		Comparison: ShadowComparison{Match: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	reference := PreparedRouteSnapshotReference{
		SchemaVersion: preparedRouteSnapshotSchemaVersion,
		ContentDigest: hex.EncodeToString(digest[:]), PayloadBytes: len(payload),
	}
	desired, err := NewEffectLedgerState(harness.claim, superseded.Effects, harness.now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.repository.prepareRouteSnapshotPayload(ctx, harness.claim, desired, payload, reference, harness.now); err != nil {
		t.Fatal(err)
	}
	if cell == "writing and landed" || cell == "writing and absent" {
		if err := harness.repository.BeginEffect(ctx, harness.claim, 0, oldEffect.ContentDigest, harness.now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if cell == "writing and landed" {
		sink := route.sink(harness.conn, leaseGuardAt(harness.repository, harness.claim, harness.now))
		if err := sink.WriteEffect(ctx, harness.claim, oldEffect); err != nil {
			t.Fatal(err)
		}
	}
	if cell == "missing snapshot row" {
		if _, err := harness.repository.Pool.Exec(ctx, deletePreparedRouteSnapshotSQL,
			harness.claim.OrgID, harness.claim.ID, harness.claim.GenerationKey()); err != nil {
			t.Fatal(err)
		}
		state, err := harness.repository.LoadEffects(ctx, harness.claim, harness.now)
		if err != nil {
			t.Fatal(err)
		}
		if err := harness.repository.ResetPreparedEffectsForReplan(ctx, harness.claim, state, harness.now); !errors.Is(err, ErrEffectLedgerConflict) {
			t.Fatalf("reset of a snapshot ledger without its row err=%v, want ErrEffectLedgerConflict", err)
		}
		if _, err := harness.repository.LoadEffects(ctx, harness.claim, harness.now); err != nil {
			t.Fatalf("the refused reset lost the ledger: %v", err)
		}
		return
	}

	recoveryNow := harness.now.Add(61 * time.Second)
	fresh, err := NewPostgresRepository(harness.repository.Pool)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := fresh.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: harness.claim.OrgID, Owner: uuid.NewString(),
		Now: recoveryNow, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &LeaseSession{
		Repository: fresh, Claim: recovered, LeaseDuration: time.Minute,
		Deadline: recoveryNow.Add(5 * time.Minute), Now: func() time.Time { return recoveryNow },
	}
	descriptor, _ := Descriptor("github", "prs")
	sinks := route.sink(harness.conn, leaseGuardAt(fresh, recovered, recoveryNow))
	counting := &countingDoer{delegate: newDoer()}
	executed, execErr := driftExecutor(route, harness, fresh, recovered, recoveryNow, counting, sinks, sinks).
		Execute(ctx, session, descriptor)

	pullRequestRows := func(number int) uint64 {
		var rows uint64
		if err := harness.conn.QueryRow(ctx,
			`SELECT count() FROM git_pull_requests FINAL WHERE org_id = ? AND repo_id = ? AND number = ?`,
			recovered.OrgID, oldRow.RepoID, number).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	var snapshotRows int
	var snapshotDigest string
	if err := fresh.Pool.QueryRow(ctx, `
SELECT count(*), coalesce(max(content_digest), '') FROM public.sync_run_unit_effect_snapshots
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`,
		recovered.OrgID, recovered.ID, recovered.GenerationKey(),
	).Scan(&snapshotRows, &snapshotDigest); err != nil {
		t.Fatal(err)
	}

	if cell == "writing and landed" {
		if !errors.Is(execErr, ErrEffectRecoveryUnsafe) || counting.requests != 0 ||
			snapshotRows != 1 || snapshotDigest != reference.ContentDigest || pullRequestRows(7) != 1 || pullRequestRows(42) != 0 {
			t.Fatalf("landed write err=%v provider_requests=%d snapshot_rows=%d kept=%v pr7=%d pr42=%d, want the discard refused with the ledger, snapshot and landed row kept",
				execErr, counting.requests, snapshotRows, snapshotDigest == reference.ContentDigest, pullRequestRows(7), pullRequestRows(42))
		}
		if _, err := fresh.LoadEffects(ctx, recovered, recoveryNow); err != nil {
			t.Fatalf("the refused discard lost the ledger: %v", err)
		}
		if !strings.Contains(log.String(), "reason=manifest_mismatch_write_landed") {
			t.Fatalf("log lacks reason=manifest_mismatch_write_landed: %s", log.String())
		}
		return
	}
	if execErr != nil || executed.Effects.Written != 2 || snapshotRows != 1 || snapshotDigest == reference.ContentDigest ||
		pullRequestRows(7) != 0 || pullRequestRows(42) != 1 {
		t.Fatalf("%s: err=%v effects=%+v snapshot_rows=%d replaced=%v pr7=%d pr42=%d, want the superseded document discarded, the route re-collected and nothing of pull request 7 left behind",
			cell, execErr, executed.Effects, snapshotRows, snapshotDigest != reference.ContentDigest, pullRequestRows(7), pullRequestRows(42))
	}
	for _, want := range []string{"reason=manifest_mismatch ", "recovery=snapshot_discarded"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log lacks %q: %s", want, log.String())
		}
	}
}
