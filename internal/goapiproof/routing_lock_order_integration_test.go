//go:build integration

package goapiproof

// CHAOS-5507: the two writers must take their locks in the SAME order.
//
// THE DEFECT, as filed from CHAOS-5486's r1 review round (finding F2):
// `repoint` locked the routing rows (SELECT ... FOR UPDATE) and THEN
// inserted the candidate build; `enable` -- and the Python verb it is a
// port of -- inserted the candidate build and THEN wrote the routing row.
// Two transactions running those concurrently on the same
// (schema_digest, document_digest, selected_operation) key each hold what
// the other is waiting for, and Postgres breaks the cycle by aborting
// one: SQLSTATE 40P01.
//
// The abort is CLEAN -- no partial write, each verb stays all-or-nothing
// -- which is exactly why this was never noticed. It does not corrupt
// anything; it just makes a rollout operation fail for a reason the
// operator cannot act on, at the moment they are least able to
// investigate.
//
// This file is the red-first proof and the regression guard in one:
// `TestConcurrentRepointAndEnableNeverDeadlock` reproduced 40P01 against
// the pre-fix Repoint and must stay at zero aborts after it.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// deadlockSQLState is Postgres's "deadlock detected". Matched on the
// SQLSTATE and not on the message text, which is localised and carries a
// process id.
const deadlockSQLState = "40P01"

func isDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == deadlockSQLState
	}
	// pgx wraps some paths without preserving the typed error; the
	// SQLSTATE still appears in the rendered text, and a false negative
	// here would turn a red test green.
	return err != nil && strings.Contains(err.Error(), deadlockSQLState)
}

// lockOrderRounds is how many times the two writers race. Postgres's
// deadlock detector fires after deadlock_timeout (1s by default), so a
// reproduction is not guaranteed on any single round -- it needs enough
// attempts that the interleaving lands. Twelve reproduced it reliably on
// bigboy against the pre-fix code while keeping the whole test inside a
// few seconds once fixed (the fixed path never waits).
const lockOrderRounds = 20

// enablersPerRound is how many `enable` transactions race one `repoint`.
// See the comment in TestConcurrentRepointAndEnableNeverDeadlock: the
// contended window is the gap between two statements, so widening the
// number of contenders is what makes the reproduction reliable.
const enablersPerRound = 6

// racingOperations is deliberately more than one: a single-row race can
// only deadlock on the ONE key, while several rows also exercise the
// row-ordering half of the convention -- two multi-row writers that visit
// rows in different orders deadlock on the routing table alone, without
// the candidate-build table being involved at all.
var racingOperations = []struct{ operation, documentDigest string }{
	{"featureFlags", testDocumentDigest},
	{"hotspots", testDocumentDigest2},
	{"flowMatrix", "3333333333333333333333333333333333333333333333333333333333333333"},
}

func seedRacingRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, build string) {
	t.Helper()
	for _, row := range racingOperations {
		seedRow(t, ctx, row.operation, row.documentDigest, "canary", build, pool)
	}
}

func racingEnableRequest(build string) EnableRequest {
	digests := map[string]string{}
	operations := make([]string, 0, len(racingOperations))
	for _, row := range racingOperations {
		digests[row.operation] = row.documentDigest
		operations = append(operations, row.operation)
	}
	return EnableRequest{
		PrincipalID:         testPrincipalID,
		SchemaDigest:        testSchemaDigest,
		RunningBuild:        build,
		Operations:          operations,
		DocumentDigest:      digests,
		Mode:                "canary",
		RolloutPercentage:   100,
		RecordedBy:          "lane-routing-verbs",
		ReviewEvidence:      "CHAOS-5507 concurrency",
		AcknowledgeUnproven: true,
	}
}

// THE REGRESSION GUARD. Zero deadlock aborts across every round.
//
// BOTH WRITERS TARGET THE SAME BUILD, and that is what makes the race
// real rather than decorative. The candidate-build table's key is
// (schema_digest, document_digest, selected_operation, candidate_build):
// two writers pointing rows at DIFFERENT builds insert different keys and
// never contend there at all -- a first version of this test did exactly
// that and passed against the known-broken code, proving nothing. In
// production they always target the same build, because both read it from
// the same running process's /buildinfo.
//
// The build alternates BETWEEN rounds so each round has real work for
// both writers: a re-point whose rows already name the target build skips
// its insert entirely, and a round where one side is a no-op is a round
// that cannot deadlock.
//
// Reproduced RED first against Repoint's pre-fix ordering (FOR UPDATE on
// the routing rows, then the candidate-build insert): deadlock aborts,
// SQLSTATE 40P01. With the CHAOS-5507 ordering -- register the candidate
// build BEFORE taking any routing-row lock -- zero.
func TestConcurrentRepointAndEnableNeverDeadlock(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)

	builds := [2]string{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	seedRacingRows(t, ctx, pool, testCandidateBuild)

	var mutex sync.Mutex
	var deadlocks []string
	var otherErrors []string
	record := func(verb string, err error) {
		if err == nil {
			return
		}
		mutex.Lock()
		defer mutex.Unlock()
		if isDeadlock(err) {
			deadlocks = append(deadlocks, verb+": "+err.Error())
			return
		}
		otherErrors = append(otherErrors, verb+": "+err.Error())
	}

	for round := 0; round < lockOrderRounds; round++ {
		build := builds[round%2]
		var wait sync.WaitGroup
		// SEVERAL enables against ONE re-point per round, not one each.
		// The window that has to be hit is the gap between the re-point's
		// FOR UPDATE and its candidate-build insert -- microseconds of Go
		// between two statements. A single enable lands in it rarely
		// enough that the whole test passed against the broken code on one
		// run in three; several concurrent enables per round turn "rarely"
		// into "reliably", without changing what is being proved.
		for enabler := 0; enabler < enablersPerRound; enabler++ {
			wait.Add(1)
			go func() {
				defer wait.Done()
				_, err := Enable(ctx, pool, racingEnableRequest(build))
				record("enable", err)
			}()
		}
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := Repoint(ctx, pool, RepointRequest{
				PrincipalID:    testPrincipalID,
				SchemaDigest:   testSchemaDigest,
				RunningBuild:   build,
				RecordedBy:     "lane-routing-verbs",
				ReviewEvidence: "CHAOS-5507 concurrency",
			})
			record("repoint", err)
		}()
		wait.Wait()
	}

	if len(deadlocks) > 0 {
		t.Fatalf("%d deadlock abort(s) across %d rounds -- the two writers are taking their locks in opposite orders (CHAOS-5507):\n  %s",
			len(deadlocks), lockOrderRounds, strings.Join(deadlocks, "\n  "))
	}
	// Any OTHER error is reported separately rather than folded in: a
	// test that passed because both goroutines failed early for an
	// unrelated reason would be a green no-op.
	if len(otherErrors) > 0 {
		t.Fatalf("the race produced %d non-deadlock error(s), so this run proves nothing about lock order:\n  %s",
			len(otherErrors), strings.Join(otherErrors, "\n  "))
	}

	// Non-vacuity: every row must have ended at the LAST round's build,
	// which means both writers actually ran to completion on contended
	// rows rather than the race quietly not happening.
	want := builds[(lockOrderRounds-1)%2]
	for _, row := range racingOperations {
		var build string
		if err := pool.QueryRow(ctx,
			`SELECT current_candidate_build FROM go_api_routing_state
			  WHERE schema_digest = $1 AND selected_operation = $2`,
			testSchemaDigest, row.operation).Scan(&build); err != nil {
			t.Fatal(err)
		}
		if build != want {
			t.Fatalf("%s ended at %q, want %q -- the race did not run to completion", row.operation, build, want)
		}
	}
}

// Two REPOINTS racing each other must not deadlock either, and that is a
// different mechanism: it has nothing to do with the candidate-build
// table and everything to do with visiting the routing rows in the same
// order. A writer that iterated a Go map would visit them in a random
// order on every run.
func TestConcurrentRepointsNeverDeadlock(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRacingRows(t, ctx, pool, testCandidateBuild)

	var mutex sync.Mutex
	var failures []string
	for round := 0; round < lockOrderRounds; round++ {
		var wait sync.WaitGroup
		for index, build := range []string{
			"cccccccccccccccccccccccccccccccccccccccc",
			"dddddddddddddddddddddddddddddddddddddddd",
		} {
			wait.Add(1)
			go func(index int, build string) {
				defer wait.Done()
				_, err := Repoint(ctx, pool, RepointRequest{
					PrincipalID:    testPrincipalID,
					SchemaDigest:   testSchemaDigest,
					RunningBuild:   build,
					RecordedBy:     "lane-routing-verbs",
					ReviewEvidence: "CHAOS-5507 concurrency",
				})
				if err == nil {
					return
				}
				mutex.Lock()
				defer mutex.Unlock()
				failures = append(failures, err.Error())
			}(index, build)
		}
		wait.Wait()
	}
	if len(failures) > 0 {
		t.Fatalf("%d failure(s) across %d rounds of two concurrent re-points:\n  %s",
			len(failures), lockOrderRounds, strings.Join(failures, "\n  "))
	}
}

// Two ENABLES racing each other, same reasoning from the other side.
func TestConcurrentEnablesNeverDeadlock(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRacingRows(t, ctx, pool, testCandidateBuild)

	var mutex sync.Mutex
	var failures []string
	for round := 0; round < lockOrderRounds; round++ {
		var wait sync.WaitGroup
		for _, build := range []string{
			"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			"ffffffffffffffffffffffffffffffffffffffff",
		} {
			wait.Add(1)
			go func(build string) {
				defer wait.Done()
				_, err := Enable(ctx, pool, racingEnableRequest(build))
				if err == nil {
					return
				}
				mutex.Lock()
				defer mutex.Unlock()
				failures = append(failures, err.Error())
			}(build)
		}
		wait.Wait()
	}
	if len(failures) > 0 {
		t.Fatalf("%d failure(s) across %d rounds of two concurrent enables:\n  %s",
			len(failures), lockOrderRounds, strings.Join(failures, "\n  "))
	}
}

// THE TOCTOU THE FIX OPENS, closed and proved.
//
// Registering the candidate build before taking any routing-row lock
// means the document digest it is registered under comes from an
// UNLOCKED read. Another writer can move that digest in between. This
// drives exactly that interleaving -- deterministically, by holding the
// rows locked until the re-point is provably blocked -- and asserts the
// re-point notices, starts over, and lands correctly on the NEW digest
// rather than writing against a registration that no longer matches.
func TestRepointDetectsADocumentDigestThatMovedUnderItAndRetries(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)

	const movedDigest = "4444444444444444444444444444444444444444444444444444444444444444"
	const runningBuild = "1111111111111111111111111111111111111111"
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "shadow", testCandidateBuild, pool)

	// The row's new (digest, build) pair must be registered before the
	// racer can move it: go_api_routing_state carries a 4-column foreign
	// key, so an UPDATE to an unregistered pair is refused by the
	// database, not by this test.
	if _, err := pool.Exec(ctx, registerCandidateBuildSQL,
		testSchemaDigest, movedDigest, "featureFlags", testCandidateBuild); err != nil {
		t.Fatal(err)
	}

	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Trap #110: a t.Fatal between here and the explicit commit below must
	// not leak this transaction's pooled connection.
	defer func() { _ = blocker.Rollback(ctx) }()
	if _, err := blocker.Exec(ctx,
		`SELECT 1 FROM go_api_routing_state WHERE schema_digest = $1 FOR UPDATE`, testSchemaDigest); err != nil {
		t.Fatal(err)
	}

	type result struct {
		outcomes []RepointOutcome
		err      error
	}
	done := make(chan result, 1)
	go func() {
		outcomes, err := Repoint(ctx, pool, RepointRequest{
			PrincipalID:    testPrincipalID,
			SchemaDigest:   testSchemaDigest,
			RunningBuild:   runningBuild,
			RecordedBy:     "lane-routing-verbs",
			ReviewEvidence: "CHAOS-5507 toctou",
		})
		done <- result{outcomes, err}
	}()

	// The re-point has now surveyed (unlocked, seeing testDocumentDigest),
	// registered the build under it, and is blocked on the locking read.
	waitForALockWaiter(t, ctx, pool)

	if _, err := blocker.Exec(ctx, `
		UPDATE go_api_routing_state SET document_digest = $1
		 WHERE schema_digest = $2 AND selected_operation = 'featureFlags'`,
		movedDigest, testSchemaDigest); err != nil {
		t.Fatal(err)
	}
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	got := <-done
	if got.err != nil {
		t.Fatalf("Repoint = %v; a ONE-SHOT race must be retried transparently, not surfaced to the operator", got.err)
	}
	if len(got.outcomes) != 1 || !got.outcomes[0].Changed {
		t.Fatalf("outcomes = %+v, want one changed row", got.outcomes)
	}

	var documentDigest, build, mode string
	if err := pool.QueryRow(ctx, `
		SELECT document_digest, current_candidate_build, mode FROM go_api_routing_state
		 WHERE schema_digest = $1 AND selected_operation = 'featureFlags'`, testSchemaDigest).
		Scan(&documentDigest, &build, &mode); err != nil {
		t.Fatal(err)
	}
	if documentDigest != movedDigest {
		t.Fatalf("document_digest = %s, want the racer's %s -- the retry must work from the NEW shape, not the surveyed one", documentDigest, movedDigest)
	}
	if build != runningBuild {
		t.Fatalf("current_candidate_build = %s, want %s", build, runningBuild)
	}
	if mode != "shadow" {
		t.Fatalf("mode = %q: a re-point must never touch reachability, race or no race", mode)
	}
	// And the retry registered the build under the NEW digest, which is
	// what the row's foreign key now depends on.
	var registered int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM go_api_candidate_build
		 WHERE schema_digest = $1 AND document_digest = $2
		   AND selected_operation = 'featureFlags' AND candidate_build = $3`,
		testSchemaDigest, movedDigest, runningBuild).Scan(&registered); err != nil {
		t.Fatal(err)
	}
	if registered != 1 {
		t.Fatalf("the running build is registered %d time(s) under the moved document digest, want 1", registered)
	}
}
