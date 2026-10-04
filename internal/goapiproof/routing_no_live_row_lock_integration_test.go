//go:build integration

package goapiproof

// CHAOS-8586 defect 3: the answer `carry` and `repoint` give when their survey
// finds no row at the live digest is decided under a table lock.
//
// The race, as the review found it: the check was a plain read, so a writer
// whose row at the live digest was written but not yet committed was invisible
// to it. The verb answered "nothing here" (a no-op, exit 0), the writer
// committed a moment later, and the row was never carried -- after the roll
// its operation was dark. Each test below races the real `enable` against the
// real verb on real Postgres: `enable` is held after its row is written and
// before it commits, the verb runs beside it and must WAIT for it rather than
// answer around it, and the test then checks the state the run reaches.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// awaitLockWaiters returns once at least n backends are waiting on a lock. It
// FAILS if a caller returns first: a caller that answers before it has queued
// behind the writer answered from a read that could not see the writer's row.
func awaitLockWaiters(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int, done ...<-chan error) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, ch := range done {
			select {
			case err := <-ch:
				t.Fatalf("a caller answered (err = %v) before %d backend(s) were waiting on a lock: it did not wait for the writer in flight", err, n)
			default:
			}
		}
		var waiting int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			 WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fewer than %d backend(s) blocked on a lock within 20s: the interleaving this test needs did not happen, so it proves nothing", n)
}

// holdEnableBeforeCommit runs the real Enable in a goroutine and holds it after
// its routing row is written and before it commits: a third session holds a
// SHARE lock on go_api_routing_audits, and Enable's audit insert -- its last
// write before COMMIT -- waits on it. release lets Enable finish.
func holdEnableBeforeCommit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, request EnableRequest) (release func(), result <-chan error) {
	t.Helper()
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Rollback(context.Background()) })
	if _, err := holder.Exec(ctx, `LOCK TABLE go_api_routing_audits IN SHARE MODE`); err != nil {
		t.Fatalf("hold the audit table: %v", err)
	}
	results := make(chan error, 1)
	go func() {
		_, err := Enable(ctx, pool, request)
		results <- err
	}()
	awaitLockWaiters(t, ctx, pool, 1, results)
	var inFlight int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_state`).Scan(&inFlight); err != nil || inFlight != 0 {
		t.Fatalf("enable's row is visible before it committed (%d row(s), err %v): the setup is not the race", inFlight, err)
	}
	return func() {
		if err := holder.Commit(ctx); err != nil {
			t.Fatalf("release the audit table: %v", err)
		}
	}, results
}

func awaitResult(t *testing.T, what string, results <-chan error) error {
	t.Helper()
	select {
	case err := <-results:
		return err
	case <-time.After(20 * time.Second):
		t.Fatalf("%s did not answer within 20s", what)
		return nil
	}
}

func TestCarryRacingEnableWaitsForItsRowAndThenCarriesIt(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	release, enabled := holdEnableBeforeCommit(t, ctx, pool, enableRequest("featureFlags"))

	carried := make(chan error, 1)
	go func() {
		_, err := Carry(ctx, pool, carryIntegrationRequest())
		carried <- err
	}()
	awaitLockWaiters(t, ctx, pool, 2, carried, enabled)
	release()

	if err := awaitResult(t, "enable", enabled); err != nil {
		t.Fatalf("enable: %v", err)
	}
	err := awaitResult(t, "carry", carried)
	if !errors.Is(err, ErrCarrySourceRowChanged) || errors.Is(err, ErrRoutingTableEmpty) || errors.Is(err, ErrRoutingRowsOnlyDark) {
		t.Fatalf("carry = %v, want ErrCarrySourceRowChanged: the row it waited for is at the live digest now", err)
	}
	if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 0 {
		t.Fatalf("the refused run wrote %d row(s) at the target digest", len(rows))
	}

	// The state the operator reaches by doing what the refusal says: run again,
	// and the enabled row is carried, so the roll does not leave it dark.
	outcomes, err := Carry(ctx, pool, carryIntegrationRequest())
	if err != nil {
		t.Fatalf("carry re-run: %v", err)
	}
	if summary := SummarizeCarry(outcomes); summary.Carried != 1 {
		t.Fatalf("carry re-run summary = %+v, want the enabled row carried", summary)
	}
	row, ok := carryRowByOperation(rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest), "featureFlags")
	if !ok || row.Mode != "canary" || row.Build != verbsRunningBuild {
		t.Fatalf("target row = %+v (present=%t), want the enabled canary row carried", row, ok)
	}
}

func TestRepointRacingEnableWaitsForItsRowAndRepointsIt(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	// enable at an older build, so the row the re-point waits for has a build to move.
	request := enableRequest("featureFlags")
	request.RunningBuild = testCandidateBuild
	release, enabled := holdEnableBeforeCommit(t, ctx, pool, request)

	type result struct {
		outcomes []RepointOutcome
		err      error
	}
	results := make(chan result, 1)
	repointed := make(chan error, 1)
	go func() {
		outcomes, err := Repoint(ctx, pool, RepointRequest{
			PrincipalID:  testPrincipalID,
			SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
			RecordedBy: "t", ReviewEvidence: "e",
		})
		results <- result{outcomes, err}
		repointed <- err
	}()
	awaitLockWaiters(t, ctx, pool, 2, repointed, enabled)
	release()

	if err := awaitResult(t, "enable", enabled); err != nil {
		t.Fatalf("enable: %v", err)
	}
	var got result
	select {
	case got = <-results:
	case <-time.After(20 * time.Second):
		t.Fatal("repoint did not answer within 20s of enable's commit")
	}
	if got.err != nil {
		t.Fatalf("repoint = %v, want success: it starts over once the row it waited for is committed", got.err)
	}
	if len(got.outcomes) != 1 || !got.outcomes[0].Changed || got.outcomes[0].BuildFrom != testCandidateBuild || got.outcomes[0].BuildTo != repointRunningBuild {
		t.Fatalf("outcomes = %+v, want the enabled row re-pointed %s -> %s", got.outcomes, testCandidateBuild, repointRunningBuild)
	}
	var mode, build string
	if err := pool.QueryRow(ctx, `SELECT mode, current_candidate_build FROM go_api_routing_state WHERE selected_operation = 'featureFlags'`).Scan(&mode, &build); err != nil || mode != "canary" || build != repointRunningBuild {
		t.Fatalf("row = mode %q build %q (err %v), want canary at %s", mode, build, err, repointRunningBuild)
	}
}
