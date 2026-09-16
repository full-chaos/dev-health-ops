//go:build integration

package goapiproof

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const repointRunningBuild = "ffd9e5d5dc8ee21de5befa1bae47ba9195be135e"

// seedRow puts one row at the shape the live compose stack had on
// 2026-09-09: a registered candidate build plus a routing row naming it.
func seedRow(t *testing.T, ctx context.Context, operation, documentDigest, mode, build string, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, registerCandidateBuildSQL, testSchemaDigest, documentDigest, operation, build); err != nil {
		t.Fatalf("register %s: %v", operation, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build,
			 owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', $5, 100, 'seeded', 'test')`,
		testSchemaDigest, documentDigest, operation, build, mode); err != nil {
		t.Fatalf("seed %s: %v", operation, err)
	}
}

// THE DEFECT THIS VERB EXISTS FOR. A shadow row could not be re-pointed
// by any supported verb: enable --mode takes only canary|primary, and
// disable never writes current_candidate_build. Measured on compose
// 2026-09-09 with three shadow rows stuck at an old build while the proof
// runner refused on all fifteen. Here the shadow row moves to the running
// build AND STAYS SHADOW.
func TestRepointMovesAShadowRowWithoutMakingItReachable(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "hotspots", testDocumentDigest, "shadow", testCandidateBuild, pool)

	outcomes, err := Repoint(ctx, pool, RepointRequest{
		PrincipalID:    testPrincipalID,
		SchemaDigest:   testSchemaDigest,
		RunningBuild:   repointRunningBuild,
		RecordedBy:     "lane-stack-owner",
		ReviewEvidence: "CHAOS-5486 re-point to the running build",
	})
	if err != nil {
		t.Fatalf("Repoint: %v", err)
	}
	if len(outcomes) != 1 {
		t.Fatalf("outcomes = %d, want 1", len(outcomes))
	}
	if got := outcomes[0]; got.ModeBefore != "shadow" || got.ModeAfter != "shadow" || !got.Changed {
		t.Fatalf("outcome = %+v, want shadow->shadow and Changed", got)
	}

	var mode, build, recordedBy, evidence string
	if err := pool.QueryRow(ctx, `
		SELECT mode, current_candidate_build, recorded_by, review_evidence
		  FROM go_api_routing_state WHERE selected_operation = 'hotspots'`).
		Scan(&mode, &build, &recordedBy, &evidence); err != nil {
		t.Fatal(err)
	}
	if mode != "shadow" {
		t.Fatalf("mode = %q after a re-point, want shadow -- a provenance write must never change reachability", mode)
	}
	if build != repointRunningBuild {
		t.Fatalf("current_candidate_build = %q, want the running build", build)
	}
	if recordedBy != "lane-stack-owner" || evidence == "seeded" {
		t.Fatalf("provenance not recorded: recorded_by=%q review_evidence=%q", recordedBy, evidence)
	}
}

// Every reachability column survives, not just mode.
func TestRepointLeavesOwnerAndRolloutUntouched(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "flowMatrix", testDocumentDigest, "canary", testCandidateBuild, pool)
	if _, err := pool.Exec(ctx, `UPDATE go_api_routing_state SET rollout_percentage = 42 WHERE selected_operation = 'flowMatrix'`); err != nil {
		t.Fatal(err)
	}

	if _, err := Repoint(ctx, pool, RepointRequest{
		PrincipalID:  testPrincipalID,
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		RecordedBy: "t", ReviewEvidence: "e",
	}); err != nil {
		t.Fatalf("Repoint: %v", err)
	}

	var owner, mode string
	var rollout int
	if err := pool.QueryRow(ctx, `SELECT owner, mode, rollout_percentage FROM go_api_routing_state WHERE selected_operation = 'flowMatrix'`).
		Scan(&owner, &mode, &rollout); err != nil {
		t.Fatal(err)
	}
	if owner != "go" || mode != "canary" || rollout != 42 {
		t.Fatalf("owner=%q mode=%q rollout=%d -- want go/canary/42 untouched", owner, mode, rollout)
	}
}

// The FK is the reason ordering matters: register first, or the update is
// rejected. Re-pointing to a build nothing has registered must still work,
// because registration happens inside the same transaction.
func TestRepointRegistersTheBuildItPointsAt(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "cognitiveLoad", testDocumentDigest, "canary", testCandidateBuild, pool)

	var before int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_candidate_build WHERE candidate_build = $1`, repointRunningBuild).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before != 0 {
		t.Fatalf("precondition: running build already registered (%d rows)", before)
	}
	if _, err := Repoint(ctx, pool, RepointRequest{
		PrincipalID:  testPrincipalID,
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		RecordedBy: "t", ReviewEvidence: "e",
	}); err != nil {
		t.Fatalf("Repoint: %v", err)
	}
	var after int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_candidate_build WHERE candidate_build = $1`, repointRunningBuild).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != 1 {
		t.Fatalf("candidate build rows for the running build = %d, want 1", after)
	}
}

// Re-running is a no-op in EFFECT and says so, rather than reporting a
// write that changed nothing.
func TestRepointIsIdempotentAndReportsUnchanged(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "reviewEdges", testDocumentDigest, "canary", testCandidateBuild, pool)

	request := RepointRequest{
		PrincipalID:  testPrincipalID,
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		RecordedBy: "t", ReviewEvidence: "e",
	}
	first, err := Repoint(ctx, pool, request)
	if err != nil {
		t.Fatalf("first Repoint: %v", err)
	}
	if !first[0].Changed {
		t.Fatal("first run must report Changed")
	}
	second, err := Repoint(ctx, pool, request)
	if err != nil {
		t.Fatalf("second Repoint: %v", err)
	}
	if second[0].Changed {
		t.Fatal("second run must report Unchanged: the row already names the running build")
	}
	if summary := Summarize(second); summary.Unchanged != 1 || summary.Changed != 0 {
		t.Fatalf("summary = %+v, want Unchanged 1 / Changed 0", summary)
	}
}

// A dry run reports exactly what a real run would do and writes nothing.
func TestRepointDryRunWritesNothing(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "workGraphEdges", testDocumentDigest, "shadow", testCandidateBuild, pool)

	outcomes, err := Repoint(ctx, pool, RepointRequest{
		PrincipalID:  testPrincipalID,
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		RecordedBy: "t", ReviewEvidence: "e", DryRun: true,
	})
	if err != nil {
		t.Fatalf("Repoint dry-run: %v", err)
	}
	if !outcomes[0].Changed {
		t.Fatal("dry run must still report that the row WOULD change")
	}
	var build string
	if err := pool.QueryRow(ctx, `SELECT current_candidate_build FROM go_api_routing_state WHERE selected_operation = 'workGraphEdges'`).Scan(&build); err != nil {
		t.Fatal(err)
	}
	if build != testCandidateBuild {
		t.Fatalf("dry run wrote: build = %q, want the original %q", build, testCandidateBuild)
	}
}

// Mixed modes in one call: the live shape is 12 canary + 3 shadow, and
// every one must keep its own mode.
func TestRepointPreservesEachRowsOwnMode(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "canaryOp", "aa"+testDocumentDigest[2:], "canary", testCandidateBuild, pool)
	seedRow(t, ctx, "shadowOp", "bb"+testDocumentDigest[2:], "shadow", testCandidateBuild, pool)
	seedRow(t, ctx, "pythonOp", "cc"+testDocumentDigest[2:], "python", testCandidateBuild, pool)

	outcomes, err := Repoint(ctx, pool, RepointRequest{
		PrincipalID:  testPrincipalID,
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		RecordedBy: "t", ReviewEvidence: "e",
	})
	if err != nil {
		t.Fatalf("Repoint: %v", err)
	}
	if len(outcomes) != 3 {
		t.Fatalf("outcomes = %d, want 3", len(outcomes))
	}
	want := map[string]string{"canaryOp": "canary", "shadowOp": "shadow", "pythonOp": "python"}
	rows, err := pool.Query(ctx, `SELECT selected_operation, mode, current_candidate_build FROM go_api_routing_state`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var operation, mode, build string
		if err := rows.Scan(&operation, &mode, &build); err != nil {
			t.Fatal(err)
		}
		if mode != want[operation] {
			t.Fatalf("%s mode = %q, want %q", operation, mode, want[operation])
		}
		if build != repointRunningBuild {
			t.Fatalf("%s build = %q, want the running build", operation, build)
		}
		seen++
	}
	if seen != 3 {
		t.Fatalf("saw %d rows, want 3", seen)
	}
}

// Selecting a subset must leave every other row alone.
func TestRepointHonoursTheOperationFilter(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "chosen", "aa"+testDocumentDigest[2:], "canary", testCandidateBuild, pool)
	seedRow(t, ctx, "untouched", "bb"+testDocumentDigest[2:], "shadow", testCandidateBuild, pool)

	if _, err := Repoint(ctx, pool, RepointRequest{
		PrincipalID:  testPrincipalID,
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		Operations: []string{"chosen"}, RecordedBy: "t", ReviewEvidence: "e",
	}); err != nil {
		t.Fatalf("Repoint: %v", err)
	}
	var build string
	if err := pool.QueryRow(ctx, `SELECT current_candidate_build FROM go_api_routing_state WHERE selected_operation = 'untouched'`).Scan(&build); err != nil {
		t.Fatal(err)
	}
	if build != testCandidateBuild {
		t.Fatalf("unselected row moved to %q", build)
	}
}

// an operation with a catalog-matching row AND a
// DOCUMENT_DRIFT row (as `status` names it) has BOTH re-pointed together
// by the operation filter alone, since Repoint has no catalog of its
// own to prefer one over the other. SelectDocumentDigest is the only way
// to move just one of them, leaving the sibling row's build untouched.
func TestRepointSelectDocumentDigestMovesOnlyTheNamedRow(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	const driftedDigest = "5555555555555555555555555555555555555555555555555555555555555555"
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", testCandidateBuild, pool)
	seedRow(t, ctx, "featureFlags", driftedDigest, "shadow", testCandidateBuild, pool)

	outcomes, err := Repoint(ctx, pool, RepointRequest{
		PrincipalID:          testPrincipalID,
		SchemaDigest:         testSchemaDigest,
		RunningBuild:         repointRunningBuild,
		Operations:           []string{"featureFlags"},
		SelectDocumentDigest: driftedDigest,
		RecordedBy:           "t", ReviewEvidence: "e",
	})
	if err != nil {
		t.Fatalf("Repoint -document: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].DocumentDigest != driftedDigest || !outcomes[0].Changed {
		t.Fatalf("outcomes = %+v, want the drifted row alone, changed", outcomes)
	}

	rows, err := pool.Query(ctx, `SELECT document_digest, current_candidate_build FROM go_api_routing_state WHERE selected_operation = 'featureFlags' ORDER BY document_digest`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var digest, build string
		if err := rows.Scan(&digest, &build); err != nil {
			t.Fatal(err)
		}
		got[digest] = build
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if got[testDocumentDigest] != testCandidateBuild {
		t.Fatalf("the catalog-matching row moved: build = %q, want it UNTOUCHED at %q", got[testDocumentDigest], testCandidateBuild)
	}
	if got[driftedDigest] != repointRunningBuild {
		t.Fatalf("the named row = %q, want it re-pointed to %q", got[driftedDigest], repointRunningBuild)
	}
}

// A digest no row for the named operation carries is refused BY NAME.
func TestRepointSelectDocumentDigestRefusesWhenNoRowCarriesIt(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", testCandidateBuild, pool)

	_, err := Repoint(ctx, pool, RepointRequest{
		PrincipalID:          testPrincipalID,
		SchemaDigest:         testSchemaDigest,
		RunningBuild:         repointRunningBuild,
		Operations:           []string{"featureFlags"},
		SelectDocumentDigest: "6666666666666666666666666666666666666666666666666666666666666666",
		RecordedBy:           "t", ReviewEvidence: "e",
	})
	if !errors.Is(err, ErrRepointDocumentNotLive) {
		t.Fatalf("Repoint = %v, want ErrRepointDocumentNotLive", err)
	}
	var build string
	if err := pool.QueryRow(ctx, `SELECT current_candidate_build FROM go_api_routing_state WHERE selected_operation = 'featureFlags'`).Scan(&build); err != nil {
		t.Fatal(err)
	}
	if build != testCandidateBuild {
		t.Fatalf("a refused repoint changed the row: build = %q", build)
	}
}

// "No rows" is an error, not an empty success: an unreachable or empty
// registry and a fully-correct one must never read alike.
func TestRepointRefusesWhenNothingMatches(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	if _, err := Repoint(ctx, pool, RepointRequest{
		PrincipalID:  testPrincipalID,
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		RecordedBy: "t", ReviewEvidence: "e",
	}); !errors.Is(err, ErrRepointNoRows) {
		t.Fatalf("Repoint on an empty registry = %v, want ErrRepointNoRows", err)
	}
}

// r2 mutation ledger (M29, SURVIVED): the post-write re-read exists to
// PROVE Repoint never touches reachability, because the UPDATE's own SET
// list carries no `mode` column -- but the package comment names the
// residual this defends against: "a trigger, a rule or a later edit to
// [the] statement could" still move it. Nothing in either suite ever put
// one there. This does, standing in for "anything other than this verb's
// own UPDATE": a trigger that flips mode on any UPDATE to the row,
// firing precisely because Repoint's own (provenance-only) UPDATE still
// touches the row.
func TestRepointRefusesWhenSomethingElseDriftsModeDuringTheWrite(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "flowMatrix", testDocumentDigest, "canary", testCandidateBuild, pool)

	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION test_drift_mode_on_update() RETURNS trigger AS $$
		BEGIN
			NEW.mode := CASE WHEN OLD.mode = 'canary' THEN 'shadow' ELSE 'canary' END;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER test_drift_mode BEFORE UPDATE ON go_api_routing_state
			FOR EACH ROW EXECUTE FUNCTION test_drift_mode_on_update();
	`); err != nil {
		t.Fatal(err)
	}

	_, err := Repoint(ctx, pool, RepointRequest{
		SchemaDigest:   testSchemaDigest,
		RunningBuild:   repointRunningBuild,
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "r2 M29 killer",
		PrincipalID:    testPrincipalID,
	})
	if err == nil {
		t.Fatal("repoint must refuse when mode moved during the write -- its whole contract is that it never touches reachability")
	}
	if !strings.Contains(err.Error(), "mode changed") {
		t.Fatalf("refused for a different reason, so the mode-drift assertion is not what caught it: %v", err)
	}
}

// repoint's "affected 0 rows" guard carries the identical comment
// enable's sibling guard does -- CHAOS-5416's silent-success shape -- but
// only enable's had a trigger fixture forcing it to actually fail. Same
// technique here: a BEFORE UPDATE trigger returning NULL skips the write
// entirely, and the guard must catch it rather than report success.
func TestRepointRefusesWhenTheRoutingRowWriteIsSwallowed(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", testCandidateBuild, pool)

	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION test_swallow_repoint_update() RETURNS trigger AS $$
		BEGIN
			RETURN NULL; -- BEFORE UPDATE returning NULL skips the row entirely
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER test_swallow_repoint_update BEFORE UPDATE ON go_api_routing_state
			FOR EACH ROW EXECUTE FUNCTION test_swallow_repoint_update();
	`); err != nil {
		t.Fatal(err)
	}

	_, err := Repoint(ctx, pool, RepointRequest{
		PrincipalID:    testPrincipalID,
		SchemaDigest:   testSchemaDigest,
		RunningBuild:   repointRunningBuild,
		Operations:     []string{"featureFlags"},
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "affected-0 swallow guard",
	})
	if err == nil {
		t.Fatal("Repoint must refuse when the routing-row write affected 0 rows -- CHAOS-5416's exact silent-success shape")
	}
	if !strings.Contains(err.Error(), "affected 0 rows") {
		t.Fatalf("refused for a different reason, so the RowsAffected() check is not what caught it: %v", err)
	}

	var build string
	if err := pool.QueryRow(ctx, `SELECT current_candidate_build FROM go_api_routing_state WHERE selected_operation = 'featureFlags'`).Scan(&build); err != nil {
		t.Fatal(err)
	}
	if build != testCandidateBuild {
		t.Fatalf("the row's build = %q despite the write being swallowed -- want it UNCHANGED at %q", build, testCandidateBuild)
	}
}

// `enable` and `repoint` acquire the routing-state
// row lock and the candidate-build row lock in OPPOSITE orders (see
// routing_enable.go's package-level comment, corrected by this same
// change) -- CB then RS in `enable`, RS (a FOR UPDATE pre-lock, up
// front) then CB in `repoint`. Two transactions locking the same pair of
// resources in opposite orders is the textbook shape of a deadlock, not
// something that avoids one.
//
// This drives the EXACT statements Enable/Repoint execute (the same SQL
// constants, same argument shapes), stepped through an explicit,
// channel-synchronised interleaving -- deterministic, not a race against
// real HTTP/subprocess timing the way two live binaries racing each
// other would be. It exists to PIN the current, documented, deferred
// defect (CHAOS-5507's actual fix is a separate PR) so a regression that
// makes this WORSE -- data corruption instead of a clean rollback -- has
// something to fail.
//
// WHICH SIDE SURVIVES is picked by configuration, not by wait order.
// Postgres's deadlock detector aborts whichever waiter's own
// deadlock_timeout check happens to run once the cycle exists, and each
// backend only ever runs that check once, on its own clock, starting from
// when IT joined the wait queue. Forcing the loser by making it wait
// queue first does not work: if the second side joins more than one
// deadlock_timeout after the first, the first side's one-shot check ran
// before the cycle existed and never fires again, so the SECOND side's
// check finds the cycle and it becomes the victim instead. Below, the
// side forceFirstToWait names gets a short deadlock_timeout so its check
// fires soon after it blocks with the cycle present; the other side gets
// a deadlock_timeout far longer than the test can run, so its check never
// fires first. The short value has to stay ABOVE the largest gap this
// process can plausibly put between the two sides joining the wait queue
// -- a value too close to zero misses the same way the old wait-order
// approach did, only for the opposite reason: the one-shot check fires
// before the cycle even exists, instead of after the other side's check
// already claimed it. That makes the victim a property of the setting,
// not of which backend happened to join the wait queue first. Only repoint's
// write ever lands in current_candidate_build (repointRoutingRowSQL) --
// the exact statement this test fires at repoint (registerCandidateBuildSQL)
// never touches that column, so when repoint is the survivor the column
// is untouched, not "repoint's write". Both orderings are exercised below
// via forceFirstToWait, so both are covered every run.
func TestEnableAndRepointLockOrderInversionDeadlocks(t *testing.T) {
	t.Run("RepointEntersTheCycleFirst", func(t *testing.T) {
		runLockOrderInversionDeadlock(t, "repoint")
	})
	t.Run("EnableEntersTheCycleFirst", func(t *testing.T) {
		runLockOrderInversionDeadlock(t, "enable")
	})
}

// backendPID reports tx's server-side backend process id, so a second
// connection can look tx up in pg_stat_activity while tx sits mid-wait.
func backendPID(t *testing.T, ctx context.Context, tx pgx.Tx) int {
	t.Helper()
	var pid int
	if err := tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatalf("read backend pid: %v", err)
	}
	return pid
}

// waitUntilBlockedOnLock polls pg_stat_activity until pid is actually
// waiting on a heavyweight lock. This is the difference between "the Go
// goroutine that issues the Exec has been scheduled" and "the statement
// has reached Postgres and joined the wait queue" -- only the latter
// starts that backend's deadlock_timeout clock, which is the clock this
// test relies on to force who wins.
func waitUntilBlockedOnLock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := pool.QueryRow(ctx,
			`SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = $1`, pid).Scan(&waiting); err != nil {
			t.Fatalf("poll pg_stat_activity for pid %d: %v", pid, err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("pid %d never entered a lock wait within 5s", pid)
}

// runLockOrderInversionDeadlock drives the enable/repoint deadlock with
// forceFirstToWait ("repoint" or "enable") named as the side that must be
// the deadlock victim. Its connection is given a short deadlock_timeout,
// so its one-shot check runs and finds the cycle soon after it blocks; the
// other connection's deadlock_timeout is set far longer than this test can
// run, so its check never gets a chance to fire first. That is what pins
// the outcome -- not which side's Exec happens to reach Postgres's wait
// queue first, which network scheduling does not guarantee. Both
// directions are exercised (see the two subtests above) so both victims
// are covered every run.
func runLockOrderInversionDeadlock(t *testing.T, forceFirstToWait string) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	const operation = "flowMatrix"
	const buildA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const buildB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	seedRow(t, ctx, operation, testDocumentDigest, "canary", buildA, pool)

	repointTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin repoint side: %v", err)
	}
	defer func() { _ = repointTx.Rollback(ctx) }()
	enableTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin enable side: %v", err)
	}
	defer func() { _ = enableTx.Rollback(ctx) }()

	// Pin the victim by configuration, not by wait order (see the
	// package comment above TestEnableAndRepointLockOrderInversionDeadlocks).
	// The side named by forceFirstToWait gets a short deadlock_timeout so
	// its one-shot deadlock check fires soon after the cycle forms; the
	// other side's is set far longer than this test can run, so its check
	// never gets a chance to run first.
	var victimTx, survivorTx pgx.Tx
	switch forceFirstToWait {
	case "repoint":
		victimTx, survivorTx = repointTx, enableTx
	case "enable":
		victimTx, survivorTx = enableTx, repointTx
	default:
		t.Fatalf("forceFirstToWait = %q, want %q or %q", forceFirstToWait, "repoint", "enable")
	}
	if _, err := survivorTx.Exec(ctx, "SET deadlock_timeout = '60s'"); err != nil {
		t.Fatalf("set deadlock_timeout on the side that must survive: %v", err)
	}
	// 2s, not sub-second: it must comfortably outlast the gap between the
	// two sides actually joining Postgres's wait queue (goroutine and
	// network scheduling, not just the Go-level ordering below), or its
	// one-shot check runs before the cycle exists and never gets a second
	// try -- confirmed by temporarily forcing that gap past 100ms, which
	// reintroduced a miss (see the comment above this function).
	if _, err := victimTx.Exec(ctx, "SET deadlock_timeout = '2s'"); err != nil {
		t.Fatalf("set deadlock_timeout on the side that must deadlock: %v", err)
	}

	// Step 1: repoint's RS pre-lock lands FIRST -- exactly
	// selectRepointCandidatesSQL, `FOR UPDATE`, iterated to completion so
	// the lock is actually held server-side, not merely queued.
	rows, err := repointTx.Query(ctx, selectRepointCandidatesSQL, testSchemaDigest)
	if err != nil {
		t.Fatalf("repoint FOR UPDATE select: %v", err)
	}
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("repoint FOR UPDATE select: %v", err)
	}
	if rowCount == 0 {
		t.Fatal("the seeded row must be visible to repoint's own read")
	}

	// Step 2: enable's CB insert lands SECOND, for the SAME build both
	// sides "read from /buildinfo" -- succeeds immediately (a different
	// table, no lock held on it yet).
	if _, err := enableTx.Exec(ctx, registerCandidateBuildSQL, testSchemaDigest, testDocumentDigest, operation, buildB); err != nil {
		t.Fatalf("enable's candidate-build insert: %v", err)
	}

	// Step 3: BOTH sides now reach for the resource the OTHER already
	// holds -- repoint wants the CB row enable just locked, enable wants
	// the RS row repoint has held since step 1. This is the cycle.
	// forceFirstToWait's Exec is sent, and CONFIRMED blocked server-side,
	// before the other side's Exec is sent at all -- this fixes the wait
	// ORDER so the pg_stat_activity check above has something to confirm,
	// but it is the deadlock_timeout values set earlier, not this order,
	// that decide which side Postgres cancels.
	type outcome struct {
		side string
		err  error
	}
	results := make(chan outcome, 2)
	fireRepoint := func() {
		_, err := repointTx.Exec(ctx, registerCandidateBuildSQL, testSchemaDigest, testDocumentDigest, operation, buildB)
		results <- outcome{"repoint", err}
	}
	fireEnable := func() {
		now := time.Now().UTC()
		_, err := enableTx.Exec(ctx, upsertRoutingStateSQL,
			testSchemaDigest, testDocumentDigest, operation, buildB,
			"canary", 100, "r6 F1 killer", "lane-routing-verbs", now)
		results <- outcome{"enable", err}
	}

	var firstPID int
	switch forceFirstToWait {
	case "repoint":
		firstPID = backendPID(t, ctx, repointTx)
		go fireRepoint()
	case "enable":
		firstPID = backendPID(t, ctx, enableTx)
		go fireEnable()
	default:
		t.Fatalf("forceFirstToWait = %q, want %q or %q", forceFirstToWait, "repoint", "enable")
	}
	waitUntilBlockedOnLock(t, ctx, pool, firstPID)
	if forceFirstToWait == "repoint" {
		go fireEnable()
	} else {
		go fireRepoint()
	}

	var first, second outcome
	select {
	case first = <-results:
	case <-time.After(15 * time.Second):
		t.Fatal("neither side returned within 15s -- Postgres's deadlock_timeout (default 1s) should have resolved this")
	}
	select {
	case second = <-results:
	case <-time.After(15 * time.Second):
		t.Fatal("only one side returned within 15s of the first")
	}

	var deadlocked, survived outcome
	switch {
	case first.err != nil && second.err == nil:
		deadlocked, survived = first, second
	case second.err != nil && first.err == nil:
		deadlocked, survived = second, first
	default:
		t.Fatalf("want exactly one side to fail with a deadlock and the other to succeed, got %s.err=%v %s.err=%v", first.side, first.err, second.side, second.err)
	}

	var pgErr *pgconn.PgError
	if !errors.As(deadlocked.err, &pgErr) || pgErr.Code != "40P01" {
		t.Fatalf("%s failed, but not with SQLSTATE 40P01 (deadlock_detected): %v", deadlocked.side, deadlocked.err)
	}
	if deadlocked.side != forceFirstToWait {
		t.Fatalf("forced %s to enter the cycle first, but %s deadlocked instead of %s -- the forcing mechanism did not produce the intended ordering", forceFirstToWait, deadlocked.side, forceFirstToWait)
	}
	t.Logf("%s deadlocked (SQLSTATE 40P01) as expected; %s survived", deadlocked.side, survived.side)

	// Roll back the deadlocked side (Postgres already aborted it -- this
	// just releases pgx's client-side tracking) and commit the survivor,
	// then verify the DB is left CONSISTENT, not half-written: exactly
	// the survivor's write landed, nothing orphaned.
	if deadlocked.side == "repoint" {
		_ = repointTx.Rollback(ctx)
		if err := enableTx.Commit(ctx); err != nil {
			t.Fatalf("commit the survivor (enable): %v", err)
		}
	} else {
		_ = enableTx.Rollback(ctx)
		if err := repointTx.Commit(ctx); err != nil {
			t.Fatalf("commit the survivor (repoint): %v", err)
		}
	}

	var mode, build string
	if err := pool.QueryRow(ctx, `SELECT mode, current_candidate_build FROM go_api_routing_state WHERE selected_operation = $1`, operation).Scan(&mode, &build); err != nil {
		t.Fatalf("read back the row: %v", err)
	}
	if mode != "canary" {
		t.Fatalf("mode = %q, want canary (neither side changes mode) -- the deadlock must not have corrupted an UNRELATED column", mode)
	}

	// The statement fired at repoint here (registerCandidateBuildSQL) is a
	// CB-table insert, not repointRoutingRowSQL -- it never touches
	// current_candidate_build. So the column reflects enable's write when
	// enable survives, and is UNCHANGED (still buildA) when repoint
	// survives, since the only write ever aimed at the column -- enable's
	// -- is the one that deadlocked and rolled back.
	switch deadlocked.side {
	case "repoint":
		if build != buildB {
			t.Fatalf("current_candidate_build = %q, want %q -- enable survived and its write must be visible", build, buildB)
		}
	case "enable":
		if build != buildA {
			t.Fatalf("current_candidate_build = %q, want %q -- enable deadlocked, so its write to buildB must be ABSENT and the column must be left exactly as seeded", build, buildA)
		}
	}
}
