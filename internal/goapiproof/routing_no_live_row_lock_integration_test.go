//go:build integration

package goapiproof

// CHAOS-8586 defect 3: the answer `carry` and `repoint` give when their survey
// finds no row at the live digest is decided under a table lock; so is the
// answer of a `carry` that skipped every live row, and a carry re-checks the
// rows it skipped as well as the rows it copied.
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
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_state WHERE mode IN ('canary', 'primary') AND selected_operation = $1`,
		request.Operations[0]).Scan(&inFlight); err != nil || inFlight != 0 {
		t.Fatalf("enable's row is visible as served before it committed (%d row(s), err %v): the setup is not the race", inFlight, err)
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
