package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-4190: prod cicd/tests units terminalize after 5 attempts with the
// bare, unwrapped ErrInvalidConfiguration ("provider sync configuration is
// invalid"), category provider_unit_exhausted. Three mechanisms were already
// eliminated by test on that ticket (completion comparator, batch/comparator
// watermark agreement, Gate factory nil-per-claim).
//
// Root cause pinned here: SuiteID/CaseID/coverage SnapshotID are derived from
// (run_id, name[, path]) ONLY -- github_tests_reports.go:684 (`hashTestIdentifier(runID,
// name, "")`), :761 (`hashTestIdentifier(suite.SuiteID, name)`), :591/:877 for
// coverage -- with no artifact scoping. The chunked route accumulates
// suites/cases/coverage across EVERY artifact of one workflow run into a
// SINGLE effects batch (github_tests_chunked_route.go:634-789, cumulative
// append at ~745-747) with no cross-artifact dedup. A run whose artifacts
// both contribute a suite/case with the same name -- plausible for a matrix
// build where every job uploads an identically-named JUnit report --
// therefore reaches TestOpsClickHouseEffects.WriteEffect
// (github_tests_effects_clickhouse.go) with two rows sharing one natural
// key. recordGitHubTestsKey (same file, ~line 73/196) is a WORKING-AS-DESIGNED
// guard against exactly that (see
// TestGitHubTestsMultiRowEffectsAreAtomicAndRejectDuplicateNaturalKeys in
// github_tests_effects_integration_test.go) -- it correctly refuses the
// batch, bare, every time, because the content is deterministic. That is
// what burns all 5 attempts.
//
// The fixture below is NOT new: TestGitHubTestsPerRunArtifactsUnderCapIsUntouched
// already drives 2 artifacts each contributing 1 suite through this exact
// path and asserts the unit finalizes complete with an advanced watermark.
// These tests assert the CORRECT behavior for that same fixture -- distinct
// natural keys per artifact, and a batch WriteEffect actually accepts -- so
// they fail red against the unfixed producer and must pass green after it is
// fixed to scope every hashed id to the artifact it came from.

func TestGitHubTestsCrossArtifactSameNameSuiteGetsDistinctNaturalKeys(t *testing.T) {
	doer := &githubTestsOversizedRunDoer{t: t, jobs: 1, artifacts: 2, reportSuitesPerArtifact: 1}
	claim := nativeTestClaim("github", "cicd")
	final, effects := walkGitHubTestsChunksCapturingEffects(t, GitHubTestsRouteHandler{}, claim, githubTestsClient(t, doer), 4)

	// Anti-vacuity: the route must genuinely report this unit as a normal,
	// complete success -- otherwise a duplicate key inside a withheld/failed
	// unit would be an unsurprising, unrelated finding.
	if final.Watermark == nil {
		t.Fatal("fixture unit did not advance its watermark; not the success case under test")
	}
	if doer.artifacts != 2 || doer.archiveRequests != 2 {
		t.Fatalf("fixture must download exactly 2 artifacts for 1 run, got artifacts=%d downloaded=%d",
			doer.artifacts, doer.archiveRequests)
	}

	suiteRows := decodeGitHubTestsEffectRows[testSuiteResultRow](t, effects, "test_suite_results")
	if len(suiteRows) != 2 {
		t.Fatalf("test_suite_results rows=%d, want 2 (one per artifact)", len(suiteRows))
	}
	if suiteRows[0].SuiteID == "" || suiteRows[0].SuiteID == suiteRows[1].SuiteID {
		t.Fatalf("CHAOS-4190: suite ids=%q,%q from two DISTINCT artifacts of the same run collided; "+
			"WriteEffect's recordGitHubTestsKey rejects the second as a duplicate",
			suiteRows[0].SuiteID, suiteRows[1].SuiteID)
	}

	caseRows := decodeGitHubTestsEffectRows[testCaseResultRow](t, effects, "test_case_results")
	if len(caseRows) != 2 {
		t.Fatalf("test_case_results rows=%d, want 2 (one per artifact)", len(caseRows))
	}
	if caseRows[0].CaseID == "" || caseRows[0].CaseID == caseRows[1].CaseID {
		t.Fatalf("CHAOS-4190: case ids=%q,%q from two DISTINCT artifacts of the same run collided",
			caseRows[0].CaseID, caseRows[1].CaseID)
	}
}

func TestGitHubTestsCrossArtifactBatchCommitsBothArtifactsWithoutError(t *testing.T) {
	doer := &githubTestsOversizedRunDoer{t: t, jobs: 1, artifacts: 2, reportSuitesPerArtifact: 1}
	claim := nativeTestClaim("github", "cicd")
	_, effects := walkGitHubTestsChunksCapturingEffects(t, GitHubTestsRouteHandler{}, claim, githubTestsClient(t, doer), 4)

	suiteEffect, ok := findGitHubTestsEffect(effects, "test_suite_results")
	if !ok {
		t.Fatal("no non-empty test_suite_results effect emitted")
	}
	if len(suiteEffect.Rows) != 2 {
		t.Fatalf("test_suite_results effect rows=%d, want 2 (one per artifact)", len(suiteEffect.Rows))
	}

	batchConn := &githubTestsDuplicateKeyConn{}
	sink := TestOpsClickHouseEffects{
		Conn:  batchConn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	err := sink.WriteEffect(context.Background(), claim, suiteEffect)
	if errors.Is(err, ErrInvalidConfiguration) {
		// This is CHAOS-4190's exact prod log signature: the bare, unwrapped
		// sentinel text ("provider sync configuration is invalid", no
		// wrapping) that the cicd unit terminalizes on after 5 attempts. Two
		// genuinely distinct artifacts of one run must not collide into a
		// rejected duplicate.
		t.Fatalf("CHAOS-4190: WriteEffect rejected two genuinely distinct artifacts' rows as duplicates: %v "+
			"(err.Error()=%q)", err, err.Error())
	}
	if err != nil {
		t.Fatalf("WriteEffect unexpected error=%v", err)
	}
	if batchConn.batch == nil || batchConn.batch.appends != 2 {
		t.Fatalf("committed appends=%v, want both artifacts' rows written", batchConn.batch)
	}
}

func findGitHubTestsEffect(effects []EffectBatch, destination string) (EffectBatch, bool) {
	for _, effect := range effects {
		if effect.Destination == destination && len(effect.Rows) > 0 {
			return effect, true
		}
	}
	return EffectBatch{}, false
}

func decodeGitHubTestsEffectRows[T any](t *testing.T, effects []EffectBatch, destination string) []T {
	t.Helper()
	effect, ok := findGitHubTestsEffect(effects, destination)
	if !ok {
		t.Fatalf("no non-empty %s effect emitted", destination)
	}
	rows, err := decodeEffectRows[T](effect)
	if err != nil {
		t.Fatalf("decode %s: %v", destination, err)
	}
	return rows
}

// walkGitHubTestsChunksCapturingEffects drives CollectChunks exactly like
// walkGitHubTestsChunks (github_tests_page_budget_test.go), but additionally
// captures every emitted CompleteRouteBatch's Effects. walkGitHubTestsChunks
// only keeps the terminal metadata batch, which carries no data rows, so it
// cannot see the per-run effect batches this test needs to inspect.
func walkGitHubTestsChunksCapturingEffects(
	t *testing.T,
	handler GitHubTestsRouteHandler,
	claim Claim,
	client *providerfoundation.HTTPClient,
	maxChunks int,
) (CompleteRouteBatch, []EffectBatch) {
	t.Helper()
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	var final CompleteRouteBatch
	var effects []EffectBatch
	resume := ""
	for pass := 0; ; pass++ {
		if pass > 500 {
			t.Fatal("continuation walk never reached a final emission")
		}
		emitted := 0
		last := resume
		finalSeen := false
		err := handler.CollectChunks(
			context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				last = emission.CursorAfter
				if emission.Final {
					final = emission.Batch
					finalSeen = true
					return nil
				}
				effects = append(effects, emission.Batch.Effects...)
				emitted++
				if emitted >= maxChunks {
					return errGitHubTestsWalkContinuation
				}
				return nil
			},
		)
		if finalSeen {
			if err != nil {
				t.Fatalf("final emission returned err=%v", err)
			}
			return final, effects
		}
		if !errors.Is(err, errGitHubTestsWalkContinuation) {
			t.Fatalf("pass %d err=%v, want a continuation yield", pass, err)
		}
		resume = last
	}
}

// githubTestsDuplicateKeyConn is a driver.Conn whose PrepareBatch returns a
// batch that accepts every Append, so WriteEffect reaches its
// recordGitHubTestsKey natural-key check instead of stopping at the
// `sink.Conn == nil` wiring guard.
type githubTestsDuplicateKeyConn struct {
	driver.Conn
	batch *githubTestsDuplicateKeyBatch
}

func (c *githubTestsDuplicateKeyConn) PrepareBatch(
	context.Context, string, ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	c.batch = &githubTestsDuplicateKeyBatch{}
	return c.batch, nil
}

type githubTestsDuplicateKeyBatch struct {
	driver.Batch
	appends int
}

func (b *githubTestsDuplicateKeyBatch) Append(...any) error { b.appends++; return nil }
func (b *githubTestsDuplicateKeyBatch) Send() error         { return nil }
func (b *githubTestsDuplicateKeyBatch) Abort() error        { return nil }
