//go:build integration

package issueprlinks_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestIssuePRProvenanceCollisionSurvivesMerge is the acceptance test for
// CHAOS-4769's fix, and it currently asserts the DEFECT.
//
// # What it measures, and why the measurement decided the fix
//
// `work_graph_issue_pr` is `ReplacingMergeTree(last_synced)` keyed on
// `(org_id, repo_id, work_item_id, pr_number)` (migrations 014 + 024 + 027).
// Provenance is in NEITHER the sorting key nor the version column. Three
// producers write the same key with different provenance:
//
//	builder.py:466  _derive_issue_pr_links_from_dependencies  native
//	builder.py:470  _build_issue_pr_edges_from_fast_path      THE READER
//	builder.py:474  _build_issue_pr_edges                     explicit_text
//	builder.py:482  _build_heuristic_issue_pr_edges           heuristic
//
// Note where the reader sits: INSIDE the write sequence, not after it. Within
// one build it cannot see that build's fallback rows, so the collision it
// observes is always cross-build. The ticket's original writer table omitted
// the reader entirely and therefore encoded a model the code does not have.
//
// The proposed fix was reader-side precedence: rank native > explicit_text >
// heuristic, then `last_synced`. This test exists because that fix DOES NOT
// SURVIVE A BACKGROUND MERGE, which was measured rather than argued:
//
//	step 4: 3 rows -> 1 row after OPTIMIZE ... FINAL, and the survivor is the
//	        fallback. The native row is not out-ranked, it is PHYSICALLY GONE.
//
// A ReplacingMergeTree merge does not hide losing rows; it discards them. No
// reader-side ordering can recover a row that no longer exists. So precedence
// has to live in the VERSION COLUMN (a schema migration), and this test is the
// acceptance criterion for that migration.
//
// # Two regimes, both measured, both fatal
//
//	(a) native carries a REAL dependency stamp (builder.py:769) which is older
//	    than the fallbacks' `self._now`, so native has the lowest version and
//	    the merge deletes it.
//	(b) native falls back to `self._now` (three paths: falsy, ValueError from
//	    fromisoformat, non-datetime), tying the fallbacks EXACTLY, because
//	    `self._now` is captured once per build at builder.py:159.
//
// Regime (b) was the contested one. Two predictions were recorded BEFORE the
// run: "survivor is not reliably either" (lane-pathb-go) and "the LAST-INSERTED
// part wins" (lane-4752-go). Measured over K=10 trials with a fresh table each:
// last-inserted won 10/10. Five trials inverted the insertion order and
// produced a NATIVE survivor 5/5 -- which is what makes it a test of the
// mechanism rather than a pile of confirmations that heuristic wins.
//
// The first prediction was REFUTED. That matters for the fixture rather than
// just the record: because the tie is deterministic, and production inserts
// heuristic LAST, the red is STABLE. There is no flaky-golden risk here.
//
// # THE FIX HAS LANDED: migration 084
//
// These assertions were FLIPPED with `084_issue_pr_provenance_version_precedence.py`.
// They previously asserted the fallback survived; they now assert native
// survives on Key A and explicit_text on Key B, and the tie subtest asserts
// native regardless of insertion order.
//
// Red-before-green is recorded in the PR: run against a tree WITHOUT 084 and
// every flipped assertion fails. A green flipped test on its own is equally
// consistent with a migration that did nothing.
//
// The row COUNTS are deliberately unchanged (Key A 3 -> 1, Key B 2 -> 1). The
// merge must still collapse to one row; the fix is that it is now the right
// one. A migration that moved those counts did something other than this fix.
func TestIssuePRProvenanceCollisionSurvivesMerge(t *testing.T) {
	ctx := context.Background()
	conn := connect(ctx, t)

	// STOP MERGES while seeding, and this is load-bearing rather than tidiness.
	//
	// Step 3 asserts every seeded row is still visible without FINAL. That is
	// only true until a BACKGROUND merge fires, and nothing schedules those
	// predictably -- so the assertion was passing on timing. It passed twice,
	// then adding Key C's two extra inserts changed the part count and it began
	// reporting "1 rows without FINAL, want 3": the merge had already collapsed
	// them. The test was measuring merge scheduling, not the engine's ranking.
	//
	// Merges are restarted before step 4, which needs a real merge to run.
	mustExec(ctx, t, conn, "SYSTEM STOP MERGES work_graph_issue_pr")

	const (
		org   = "00000000-4769-0000-0000-000000000001"
		repo  = "00000000-4769-0000-0000-000000000002"
		keyA  = "00000000-4769-0000-0000-000000000003"
		keyB  = "00000000-4769-0000-0000-000000000004"
		keyC  = "00000000-4769-0000-0000-000000000006"
		prA   = 4769
		prB   = 4770
		prC   = 4772
		tieID = "00000000-4769-0000-0000-000000000005"
	)

	// Key A has native present; Key B does NOT, and that is the point: with
	// native present, native wins regardless of how the other two are ordered,
	// so Key A alone cannot distinguish a three-way order from the boolean
	// `native ? 0 : 1`. Key B is where explicit_text > heuristic is decided.
	//
	// Key B's confidence is INVERTED (heuristic 0.99 > explicit_text 0.90) so a
	// mutant that ranks by confidence fails. Confidence is carried by the fast
	// path, never ranked on; a fixture whose confidence descends in step with
	// the correct answer would pass such a mutant.
	seed(ctx, t, conn, org, repo, keyA, prA, "native", 1.00, "2026-01-01 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyA, prA, "explicit_text", 0.90, "2026-01-02 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyA, prA, "heuristic", 0.50, "2026-01-03 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyB, prB, "explicit_text", 0.90, "2026-01-01 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyB, prB, "heuristic", 0.99, "2026-01-02 00:00:00.000")
	// Key C exists to test THE MULTIPLIER, which Keys A and B cannot.
	//
	// A and B span 2026-01-01..2026-01-03 -- 172,800,000 ms. Any multiplier
	// larger than that orders them correctly, so they pass under 2**45, under
	// 2**40, and under 2**28 alike: they are structurally blind to the constant
	// whose correctness the whole design rests on. Found by lane-4752-go while
	// reviewing their own container run, which had reported "the design works"
	// on exactly this blind fixture.
	//
	// A far-future sentinel makes the constant load-bearing. Under 2**40 the
	// recency term crosses a rank step and the FALLBACK wins -- the fix
	// inverting into the defect it removes:
	//     heuristic@2100 = 1*2**40 + 4,102,444,800,000 = 5,201,956,427,776
	//     native@2026    = 3*2**40 + 1,788,307,200,000 = 5,086,842,083,328  LOSES
	// Under 2**45 native wins by 2*2**45, which no plausible stamp can cross.
	//
	// A Key D (epoch-0 stamp on the NATIVE row) was considered and left out: it
	// passes under both 2**40 and 2**45, so it would not discriminate and would
	// be a test that cannot fail on the thing it appears to check.
	seed(ctx, t, conn, org, repo, keyC, prC, "native", 1.00, "2026-01-01 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyC, prC, "heuristic", 0.50, "2100-01-01 00:00:00.000")
	for _, pr := range []int{prA, prB, prC, 4773, 4774, 4775, 4776} {
		mustExec(ctx, t, conn, fmt.Sprintf(
			`INSERT INTO git_pull_requests (repo_id, number, org_id, created_at, last_synced)
			 VALUES ('%s', %d, '%s', '2025-12-01 00:00:00.000', '2025-12-01 00:00:00.000')`,
			repo, pr, org))
	}

	// STEP 1. The fast path INNER JOINs git_pull_requests, so an unseeded join
	// partner makes a REAL collision return zero rows -- and "cannot reproduce"
	// is indistinguishable from "no defect". Assert non-empty BEFORE asserting
	// anything about which row survived, or the whole test is vacuous.
	visible := scanReader(ctx, t, conn, org, repo)
	if len(visible) == 0 {
		t.Fatal("FIXTURE BROKEN: the fast-path join returned zero rows, so nothing " +
			"below tests the defect; seed git_pull_requests for both keys")
	}

	// STEP 2. FLIP: after the fix these become "native" and "explicit_text".
	assertSurvivor(t, visible, prA, "native")
	assertSurvivor(t, visible, prB, "explicit_text")
	// The multiplier's own assertion: rank must beat a 74-year recency gap.
	assertSurvivor(t, visible, prC, "native")

	// STEP 3. Without FINAL every seeded row is still visible. This is also the
	// precondition for the Go port's approach (drop FINAL, dedup with
	// LIMIT 1 BY over the sorting key): FINAL collapses to the max-version row
	// BEFORE any ranking can see the alternatives.
	assertRowCount(ctx, t, conn, keyA, 3)
	assertRowCount(ctx, t, conn, keyB, 2)
	assertRowCount(ctx, t, conn, keyC, 2)

	// STEP 4. The measurement that decided the fix. Merges must be running
	// again for OPTIMIZE ... FINAL to do anything.
	mustExec(ctx, t, conn, "SYSTEM START MERGES work_graph_issue_pr")
	mustExec(ctx, t, conn, "OPTIMIZE TABLE work_graph_issue_pr FINAL")
	assertRowCount(ctx, t, conn, keyA, 1)
	assertRowCount(ctx, t, conn, keyB, 1)
	// FLIP: after the fix, native (A) and explicit_text (B) must be what remain.
	assertOnlyProvenance(ctx, t, conn, keyA, "native")
	assertOnlyProvenance(ctx, t, conn, keyB, "explicit_text")
	assertRowCount(ctx, t, conn, keyC, 1)
	assertOnlyProvenance(ctx, t, conn, keyC, "native")
	// EVERY carried column must round-trip, not just provenance.
	//
	// The migration copies a hardcoded column list. If that list ever stops
	// matching the table, the shadow still HAS the missing column (it is built
	// from SHOW CREATE TABLE) but the copy leaves it at its DEFAULT, EXCHANGE
	// swaps it in and the original is dropped -- silent, total loss with no
	// error. A test that reads back only `provenance` is green through exactly
	// that. Found on review by lane-4752-go; the migration also fails closed on
	// it, so the two defences are independent.
	assertSurvivingRowIntact(ctx, t, conn, keyA, org, repo, prA,
		1.00, "native", "seed-native", "2026-01-01 00:00:00.000")

	// STEP 5. Regime (b): an EXACT version tie, resolved by part recency.
	// A fresh table per trial -- not merely a fresh key -- because an earlier
	// trial's part structure would otherwise carry into the next one and
	// produce a confident, specific, wrong answer.
	t.Run("exact version tie resolves by part recency", func(t *testing.T) {
		const tie = "2026-02-02 12:00:00.000"
		for trial := 1; trial <= 10; trial++ {
			order := []string{"native", "heuristic"}
			if trial > 5 {
				// Inverted. If "last-inserted wins" holds, native must survive
				// here -- this is the half that can FALSIFY the mechanism
				// rather than accumulate confirmations of it.
				order = []string{"heuristic", "native"}
			}
			table := fmt.Sprintf("chaos4769_tie_%d", trial)
			mustExec(ctx, t, conn, "DROP TABLE IF EXISTS "+table)
			// `AS work_graph_issue_pr` copies the MIGRATED structure and engine,
			// version column included, rather than restating them here. That is
			// load-bearing: an earlier version of this subtest wrote its own DDL
			// with ENGINE = ReplacingMergeTree(last_synced), so the trial tables
			// kept the pre-084 behaviour and the subtest failed against a
			// working migration. A hand-written fixture schema can drift from
			// the thing it is meant to be testing; a copied one cannot.
			mustExec(ctx, t, conn, fmt.Sprintf("CREATE TABLE %s AS work_graph_issue_pr", table))
			for _, prov := range order {
				// One INSERT is one PART, which is what distinct writer calls
				// produce. The part boundary IS the mechanism here.
				mustExec(ctx, t, conn, fmt.Sprintf(
					`INSERT INTO %s (repo_id, work_item_id, pr_number, confidence,
					 provenance, evidence, last_synced, org_id)
					 VALUES ('%s','%s',4771,0.5,'%s','tie','%s','%s')`,
					table, repo, tieID, prov, tie, org))
			}
			mustExec(ctx, t, conn, "OPTIMIZE TABLE "+table+" FINAL")
			var survivor string
			if err := conn.QueryRow(ctx,
				"SELECT provenance FROM "+table+" LIMIT 1").Scan(&survivor); err != nil {
				t.Fatalf("trial %d: read survivor: %v", trial, err)
			}
			// Migration 084 REMOVES this tie rather than resolving it: with
			// version_rank = rank*2**45 + millis, native (rank 3) and heuristic
			// (rank 1) carrying identical last_synced differ by 2*2**45, so rank
			// decides no matter which part was written last.
			//
			// Before 084 this asserted the OPPOSITE -- "the last-inserted part
			// wins", measured 10/10 -- so the inverted-order half expected a
			// heuristic survivor. Those five trials are what make this a test of
			// the MECHANISM: they now prove rank beats insertion order, where
			// before they proved insertion order was all there was.
			if survivor != "native" {
				t.Errorf("trial %d (insertion order %v): survivor = %q, want \"native\" -- "+
					"provenance rank must beat part recency", trial, order, survivor)
			}
			mustExec(ctx, t, conn, "DROP TABLE "+table)
		}
	})

	writeProof(t, "issue-pr-provenance-collision")
}

func connect(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()
	instance := startClickHouse(ctx, t)
	chschema.Apply(ctx, t, instance)
	return openConn(ctx, t, instance)
}

func startClickHouse(ctx context.Context, t *testing.T) *containers.Instance {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	return instance
}

func openConn(ctx context.Context, t *testing.T, instance *containers.Instance) driver.Conn {
	t.Helper()
	opts, err := stdclickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func mustExec(ctx context.Context, t *testing.T, conn driver.Conn, sql string) {
	t.Helper()
	if err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("exec %.90s: %v", sql, err)
	}
}

func seed(ctx context.Context, t *testing.T, conn driver.Conn,
	org, repo, workItem string, pr int, provenance string, confidence float64, lastSynced string) {
	t.Helper()
	mustExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence,
		 provenance, evidence, last_synced, org_id)
		 VALUES ('%s','%s',%d,%f,'%s','%s','%s','%s')`,
		repo, workItem, pr, confidence, provenance, "seed-"+provenance, lastSynced, org))
}

type link struct {
	prNumber   uint32
	provenance string
}

// scanReader reproduces the fast path's own query, INNER JOIN and FINAL
// included, rather than reading the table directly -- the JOIN is load-bearing
// and a direct read would not exercise it.
func scanReader(ctx context.Context, t *testing.T, conn driver.Conn, org, repo string) []link {
	t.Helper()
	rows, err := conn.Query(ctx, fmt.Sprintf(`
		SELECT p.pr_number, p.provenance
		FROM work_graph_issue_pr AS p FINAL
		INNER JOIN git_pull_requests AS pr FINAL ON (
			toString(p.repo_id) = toString(pr.repo_id)
			AND p.pr_number = pr.number
			AND toString(p.org_id) = toString(pr.org_id))
		WHERE toString(p.org_id) = '%s' AND toString(p.repo_id) = '%s'
		  AND pr.created_at >= '2025-01-01 00:00:00'
		  AND pr.created_at <= '2027-01-01 00:00:00'
		ORDER BY p.pr_number`, org, repo))
	if err != nil {
		t.Fatalf("fast-path read: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []link
	for rows.Next() {
		var l link
		if err := rows.Scan(&l.prNumber, &l.provenance); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, l)
	}
	return out
}

func assertSurvivor(t *testing.T, rows []link, pr int, want string) {
	t.Helper()
	for _, r := range rows {
		if r.prNumber == uint32(pr) {
			if r.provenance != want {
				t.Errorf("pr %d: fast path returned provenance %q, want %q", pr, r.provenance, want)
			}
			return
		}
	}
	t.Errorf("pr %d: absent from the fast-path result entirely", pr)
}

func assertRowCount(ctx context.Context, t *testing.T, conn driver.Conn, workItem string, want uint64) {
	t.Helper()
	var got uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM work_graph_issue_pr WHERE work_item_id = ?", workItem).Scan(&got); err != nil {
		t.Fatalf("count rows for %s: %v", workItem, err)
	}
	if got != want {
		t.Errorf("work_item %s: %d rows without FINAL, want %d", workItem, got, want)
	}
}

func assertOnlyProvenance(ctx context.Context, t *testing.T, conn driver.Conn, workItem, want string) {
	t.Helper()
	var got string
	if err := conn.QueryRow(ctx,
		"SELECT provenance FROM work_graph_issue_pr WHERE work_item_id = ? LIMIT 1",
		workItem).Scan(&got); err != nil {
		t.Fatalf("read surviving provenance for %s: %v", workItem, err)
	}
	if got != want {
		t.Errorf("work_item %s: surviving provenance %q, want %q", workItem, got, want)
	}
}

// writeProof makes "it ran" checkable rather than assumed. A guard nothing
// invokes is indistinguishable from a guard that passes (CHAOS-4837).
func writeProof(t *testing.T, marker string) {
	t.Helper()
	dir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if dir == "" {
		// Optional here, unlike the live-python oracles. This test runs in the
		// INTEGRATION stage, whose registration proof is its row in
		// ci/go_integration_shards.tsv rather than a marker file: the shard
		// planner fails on an unlisted package, so an unregistered test cannot
		// silently not-run. The marker is still written when a proof dir is
		// supplied, so a live-oracle-style invocation keeps working.
		return
	}
	if err := os.WriteFile(filepath.Join(dir, marker), []byte("executed"), 0o644); err != nil {
		t.Fatalf("write proof marker: %v", err)
	}
}

// assertSurvivingRowIntact reads back every carried column for a key.
func assertSurvivingRowIntact(
	ctx context.Context, t *testing.T, conn driver.Conn,
	workItem, org, repo string, pr int,
	confidence float64, provenance, evidence, lastSynced string,
) {
	t.Helper()
	var (
		gotRepo, gotItem, gotProv, gotEvidence, gotOrg, gotSynced string
		gotPR                                                     uint32
		gotConfidence                                             float32
	)
	if err := conn.QueryRow(ctx, `
		SELECT toString(repo_id), work_item_id, pr_number, confidence, provenance,
		       evidence, toString(last_synced), toString(org_id)
		FROM work_graph_issue_pr WHERE work_item_id = ? LIMIT 1`, workItem,
	).Scan(&gotRepo, &gotItem, &gotPR, &gotConfidence, &gotProv,
		&gotEvidence, &gotSynced, &gotOrg); err != nil {
		t.Fatalf("read back the surviving row for %s: %v", workItem, err)
	}
	for _, c := range []struct{ field, got, want string }{
		{"repo_id", gotRepo, repo},
		{"work_item_id", gotItem, workItem},
		{"provenance", gotProv, provenance},
		{"evidence", gotEvidence, evidence},
		{"org_id", gotOrg, org},
	} {
		if c.got != c.want {
			t.Errorf("%s: %s = %q, want %q", workItem, c.field, c.got, c.want)
		}
	}
	if gotPR != uint32(pr) {
		t.Errorf("%s: pr_number = %d, want %d", workItem, gotPR, pr)
	}
	if float64(gotConfidence) != confidence {
		t.Errorf("%s: confidence = %v, want %v", workItem, gotConfidence, confidence)
	}
	if !strings.HasPrefix(gotSynced, lastSynced[:19]) {
		t.Errorf("%s: last_synced = %q, want prefix %q", workItem, gotSynced, lastSynced[:19])
	}
}

// TestMigration084CarriesExistingRows is the ONLY test that exercises the
// migration's copy. Everything else in this file runs against a table the
// chain already migrated while it was EMPTY.
//
// That gap was invisible and total: made `_copy` return without copying, the
// whole rest of the suite still passed (codex round 2, P1). A `_copy`
// regression would EXCHANGE in an empty shadow and lose every pre-existing
// key in production, silently, with a green suite.
//
// So this test does what production does: it puts rows in a PRE-084 table and
// then migrates them.
func TestMigration084CarriesExistingRows(t *testing.T) {
	ctx := context.Background()
	instance := startClickHouse(ctx, t)
	chschema.Apply(ctx, t, instance)
	conn := openConn(ctx, t, instance)

	// Rewind to the pre-084 shape by DERIVING it from the live chain, not by
	// hand-writing it.
	//
	// A hand-written rewind is the trial-table defect one level up: if the real
	// 014+024 leave a setting, codec or default this test does not reproduce,
	// the migration is exercised against something EASIER than production and
	// stays green on a shape it was never tested against.
	//
	// So the shape comes from `SHOW CREATE TABLE` on the chain-migrated table,
	// with only what 084 itself changed inverted: drop the version column,
	// restore ReplacingMergeTree(last_synced). Everything else -- columns,
	// types, ORDER BY, settings -- comes from the chain.
	rewindToPre084(ctx, t, conn)

	// Merges are stopped only while the FIXTURE is built, so the seeded rows are
	// all present when the migration starts. They are restarted immediately
	// before the migration call below -- see the note there.
	mustExec(ctx, t, conn, "SYSTEM STOP MERGES work_graph_issue_pr")

	// NOTE: merges are deliberately RUNNING across the migration call.
	// An earlier version did, which manufactured the protection the migration
	// lacked and hid a P1: a background merge inside the migration's own copy
	// window removes the older (native) row while the key-count conservation
	// check still passes. The migration now stops merges itself; this test must
	// exercise that guard rather than substitute for it.

	const (
		org  = "00000000-4769-0000-0000-000000000001"
		repo = "00000000-4769-0000-0000-000000000002"
		keyA = "00000000-4769-0000-0000-000000000003"
		keyB = "00000000-4769-0000-0000-000000000004"
		keyC = "00000000-4769-0000-0000-000000000006"
		keyE = "00000000-4769-0000-0000-000000000007"
		keyF = "00000000-4769-0000-0000-000000000008"
		keyG = "00000000-4769-0000-0000-000000000009"
		keyH = "00000000-4769-0000-0000-00000000000a"
	)
	seed(ctx, t, conn, org, repo, keyA, 4769, "native", 1.00, "2026-01-01 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyA, 4769, "heuristic", 0.50, "2026-01-03 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyB, 4770, "explicit_text", 0.90, "2026-01-01 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyB, 4770, "heuristic", 0.99, "2026-01-02 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyC, 4772, "native", 1.00, "2026-01-01 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyC, 4772, "heuristic", 0.50, "2100-01-01 00:00:00.000")

	// Key E: an unsupported provenance with a PRE-EPOCH stamp. Under a bare
	// `rank * M` this wraps UInt64 to ~1.8e19 and DELETES the native row during
	// the copy -- the migration destroying the data it exists to protect.
	seed(ctx, t, conn, org, repo, keyE, 4773, "native", 1.00, "2026-01-01 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyE, 4773, "unknown", 0.10, "1900-01-01 00:00:00.000")

	// Key F: same provenance, differing ONLY in milliseconds, and the LATER one
	// is inserted FIRST. Under `toUnixTimestamp` (seconds) the two tie and the
	// last-inserted part wins, so the .000 row survives and this fails. Under
	// milliseconds the .500 row survives. That makes the precision load-bearing
	// rather than merely documented.
	seed(ctx, t, conn, org, repo, keyF, 4774, "heuristic", 0.50, "2026-01-04 00:00:00.500")
	seed(ctx, t, conn, org, repo, keyF, 4774, "heuristic", 0.50, "2026-01-04 00:00:00.000")

	// !! KEY G IS BLIND ON THE CI IMAGE PIN. Measured, both engines:
	//
	//     26.6.1.1193 (ci/go pin)  '9999-12-31' stores as 2299-12-31, millis
	//                              10,413,791,999,999  -> 2**45 margin x3.38
	//     26.7.6.57   (prod line)  '9999-12-31' stores as written, millis
	//                              253,402,300,799,000 -> 2**45 margin x0.14
	//
	// DateTime64(3) SATURATES at 2299 on the pinned image and does not on the
	// prod line. So the 2**45 defect is UNREACHABLE in CI and live in
	// production, and the mutant proof for this key is only valid on 26.7:
	//     26.6.1 + 2**45 mutant -> PASSES (clamped, cannot see it)
	//     26.7   + 2**45 mutant -> FAILS, "surviving provenance heuristic, want native"
	// Do not read a green here as certifying the multiplier. That is recorded
	// in the PR body as a CI-vs-prod gap wider than this migration.
	//
	// Keys G and H sit at DateTime64(3)'s REPRESENTABLE extremes, not at a
	// plausible-looking one. Key C used year 2100 -- an imagined extreme -- and
	// that is exactly why the corpus could not see that 2**45 was too small:
	// year 9999's milliseconds (253,402,300,799,000) exceed two rank steps at
	// 2**45, so a heuristic row there outranked native and the copy discarded
	// it. The constant is now certified against what the TYPE can hold.
	seed(ctx, t, conn, org, repo, keyG, 4775, "native", 1.00, "2026-01-01 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyG, 4775, "heuristic", 0.50, "9999-12-31 23:59:59.999")
	seed(ctx, t, conn, org, repo, keyH, 4776, "native", 1.00, "1900-01-01 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyH, 4776, "heuristic", 0.50, "2026-01-01 00:00:00.000")

	before := countRows(ctx, t, conn)
	if before != 14 {
		t.Fatalf("seeded %d rows, want 14 -- fixture broken before the migration runs", before)
	}

	// Merges stay STOPPED across this call, deliberately, and that is not the
	// masking this test was criticised for.
	//
	// Re-enabling them here was tried and is worse than useless: between the
	// START and the migration's own STOP there is a window nothing controls,
	// and a background merge collapses the contested keys by `last_synced`
	// BEFORE the migration begins. Measured: 10 rows present immediately
	// before the call, and native already gone by the time the copy ran. That
	// is the ledger's accepted pre-migration limitation, not the P1, and it
	// makes the test flaky about the wrong thing.
	//
	// The hook below reaches the real window deterministically instead.
	t.Logf("rows immediately before the migration: %d", countRows(ctx, t, conn))

	// Force a merge INSIDE the migration's snapshot->copy window.
	//
	// WHAT THIS PROVES: the migration's own SYSTEM STOP MERGES is load-bearing.
	// Guarded, ClickHouse ABORTS this OPTIMIZE (Code 236 "Cancelled merging
	// parts" -- measured on 26.6.1.1193 and 26.7.6.57) and every native row
	// survives. With the guard removed the same statement collapses the
	// contested keys by `last_synced` and the fallback wins, which is the
	// migration destroying the rows it exists to promote.
	//
	// WHAT THIS DOES NOT PROVE: that the unguarded window is reachable by a
	// BACKGROUND merge in a live race. That state cannot be held open from
	// outside -- a pre-084 table with merges enabled may collapse a contested
	// key at any instant -- so this FORCES the window rather than waiting for
	// it. The guard is shown necessary and sufficient against a merge landing
	// there; how likely one is to land there is not measured.
	applyMigration084(ctx, t, instance, "OPTIMIZE TABLE work_graph_issue_pr FINAL")

	// The copy carried every key. A no-op copy leaves zero.
	if got := countRows(ctx, t, conn); got == 0 {
		t.Fatal("the migrated table is EMPTY: the copy carried nothing and EXCHANGE " +
			"swapped in an empty shadow -- this is the total-data-loss case")
	}
	mustExec(ctx, t, conn, "OPTIMIZE TABLE work_graph_issue_pr FINAL")

	for _, c := range []struct{ key, want, why string }{
		{keyA, "native", "native must outrank a later heuristic"},
		{keyB, "explicit_text", "explicit_text must outrank a later, higher-confidence heuristic"},
		{keyC, "native", "rank must beat a 74-year recency gap"},
		{keyE, "native", "an unknown provenance with a pre-epoch stamp must not wrap and win"},
		{keyF, "heuristic", "the later MILLISECOND must win among equal provenance"},
		{keyG, "native", "rank must beat DateTime64(3)'s MAXIMUM representable stamp"},
		{keyH, "native", "rank must hold at DateTime64(3)'s MINIMUM representable stamp"},
	} {
		assertOnlyProvenance(ctx, t, conn, c.key, c.want)
	}
	// Full precision, not truncated to seconds: this is what fails under a
	// toUnixTimestamp mutant.
	assertLastSynced(ctx, t, conn, keyF, "2026-01-04 00:00:00.500")
	assertLastSynced(ctx, t, conn, keyA, "2026-01-01 00:00:00.000")

	// EVERY key survived, not merely "some rows are present". A copy that
	// carried one key and dropped four would satisfy a non-zero count.
	var keys uint64
	if err := conn.QueryRow(ctx,
		"SELECT uniqExact(work_item_id) FROM work_graph_issue_pr").Scan(&keys); err != nil {
		t.Fatalf("count distinct keys: %v", err)
	}
	if keys != 7 {
		t.Errorf("%d distinct keys survived the migration, want 7 -- the copy lost keys", keys)
	}

	// EVERY CARRIED COLUMN, with values distinct per row, on a row that was
	// written BEFORE the migration and carried through it. The other test
	// checks this on rows written after; only here does it prove the copy
	// preserves column values rather than merely row identity.
	assertSurvivingRowIntact(ctx, t, conn, keyA, org, repo, 4769,
		1.00, "native", "seed-native", "2026-01-01 00:00:00.000")
}

// applyMigration084 runs ONE migration against the live container, the way the
// production runner does, so the copy is exercised on rows that already exist.
func applyMigration084(
	ctx context.Context, t *testing.T, instance *containers.Instance, afterSnapshotSQL string,
) {
	t.Helper()
	if err := runMigration084(ctx, t, instance, afterSnapshotSQL); err != nil {
		t.Fatalf("apply migration 084: %v", err)
	}
}

func runMigration084(
	ctx context.Context, t *testing.T, instance *containers.Instance, afterSnapshotSQL string,
) error {
	t.Helper()
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("http dsn: %v", err)
	}
	root := repoRootForMigration(t)
	script := `
import sys, importlib.util
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
sink = ClickHouseMetricsSink(dsn=sys.argv[1])
try:
    # importlib, NOT runpy.run_path: run_path returns a COPY of the namespace,
    # so assigning the hook on it would never reach the module's functions and
    # the seam would be silently inert -- a control that cannot fail. Verified.
    spec = importlib.util.spec_from_file_location("m084", sys.argv[2])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if len(sys.argv) > 3 and sys.argv[3]:
        def _hook(sql=sys.argv[3], client=sink.client):
            # Split on ";": the driver's command() takes ONE statement, so a
            # multi-statement hook (START MERGES then OPTIMIZE) must be issued
            # as separate calls. Sending both in one string is rejected, which
            # reads as HOOK_REFUSED and silently turns the red into a green.
            print("HOOK_RAN")
            outcome = "HOOK_SUCCEEDED"
            for statement in [part.strip() for part in sql.split(";") if part.strip()]:
                try:
                    client.command(statement)
                except Exception as error:
                    outcome = "HOOK_REFUSED"
                    print("HOOK_STATEMENT_REFUSED", statement[:60],
                          type(error).__name__, error)
            print(outcome)
        module.after_snapshot_hook = _hook
    module.upgrade(sink.client)
finally:
    sink.close()
print("MIGRATION_084_APPLIED")
`
	python := os.Getenv("DEV_HEALTH_PYTHON")
	if python == "" {
		python = "python3"
	}
	migration := filepath.Join(root, "src", "dev_health_ops", "migrations",
		"clickhouse", "084_issue_pr_provenance_version_precedence.py")
	command := exec.CommandContext(ctx, python, "-c", script, dsn, migration, afterSnapshotSQL)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	out, err := command.CombinedOutput()
	if afterSnapshotSQL != "" && !strings.Contains(string(out), "HOOK_RAN") {
		t.Fatalf("the after-snapshot hook never fired, so nothing was forced into "+
			"the copy window and this run proves nothing:\n%s", out)
	}
	if afterSnapshotSQL != "" {
		t.Logf("after-snapshot hook outcome: %s", hookOutcome(string(out)))
	}
	if err != nil || !strings.Contains(string(out), "MIGRATION_084_APPLIED") {
		return fmt.Errorf("%w\n%s", err, out)
	}
	return nil
}

func repoRootForMigration(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for range 8 {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("could not locate the repository root")
	return ""
}

func countRows(ctx context.Context, t *testing.T, conn driver.Conn) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM work_graph_issue_pr").Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

func assertLastSynced(ctx context.Context, t *testing.T, conn driver.Conn, workItem, want string) {
	t.Helper()
	var got string
	if err := conn.QueryRow(ctx,
		"SELECT toString(last_synced) FROM work_graph_issue_pr WHERE work_item_id = ? LIMIT 1",
		workItem).Scan(&got); err != nil {
		t.Fatalf("read last_synced for %s: %v", workItem, err)
	}
	if got != want {
		t.Errorf("%s: last_synced = %q, want %q (full millisecond precision)", workItem, got, want)
	}
}

// rewindToPre084 recreates work_graph_issue_pr as the chain left it BEFORE
// migration 084, deriving the shape rather than restating it.
//
// It also asserts the derived shape matches the DDL this test used to
// hand-write. That assertion is the red-first for the change: if the chain
// ever leaves a shape the hand-written version did not reproduce, this fails
// and names the difference instead of silently testing an easier table.
func rewindToPre084(ctx context.Context, t *testing.T, conn driver.Conn) {
	t.Helper()
	var live string
	if err := conn.QueryRow(ctx, "SHOW CREATE TABLE work_graph_issue_pr").Scan(&live); err != nil {
		t.Fatalf("SHOW CREATE TABLE: %v", err)
	}

	// Invert exactly what 084 did, and nothing else.
	var kept []string
	for _, line := range strings.Split(live, "\n") {
		if strings.Contains(line, "version_rank") && strings.Contains(line, "MATERIALIZED") {
			continue // the column 084 added
		}
		kept = append(kept, line)
	}
	derived := strings.Join(kept, "\n")
	derived = strings.Replace(derived, "ReplacingMergeTree(version_rank)",
		"ReplacingMergeTree(last_synced)", 1)
	// A trailing comma is left behind when the dropped column was last.
	derived = strings.Replace(derived, ",\n)", "\n)", 1)
	if strings.Contains(derived, "version_rank") {
		t.Fatalf("derived pre-084 DDL still mentions version_rank:\n%s", derived)
	}

	mustExec(ctx, t, conn, "DROP TABLE IF EXISTS work_graph_issue_pr")
	mustExec(ctx, t, conn, derived)

	// The former hand-written shape, built in a scratch table, must match.
	mustExec(ctx, t, conn, "DROP TABLE IF EXISTS pre084_handwritten_probe")
	mustExec(ctx, t, conn, `CREATE TABLE pre084_handwritten_probe (
		repo_id UUID, work_item_id String, pr_number UInt32, confidence Float32,
		provenance String, evidence String, last_synced DateTime64(3,'UTC'),
		org_id String DEFAULT 'default'
	) ENGINE = ReplacingMergeTree(last_synced)
	ORDER BY (org_id, repo_id, work_item_id, pr_number)`)
	derivedShape := columnShape(ctx, t, conn, "work_graph_issue_pr")
	handShape := columnShape(ctx, t, conn, "pre084_handwritten_probe")
	if derivedShape != handShape {
		t.Errorf("the pre-084 shape DERIVED from the chain differs from the "+
			"hand-written one this test used to rely on:\n derived: %s\n hand:    %s",
			derivedShape, handShape)
	}
	if a, b := sortingKey(ctx, t, conn, "work_graph_issue_pr"),
		sortingKey(ctx, t, conn, "pre084_handwritten_probe"); a != b {
		t.Errorf("derived sorting key %q != hand-written %q", a, b)
	}
	mustExec(ctx, t, conn, "DROP TABLE pre084_handwritten_probe")
}

func columnShape(ctx context.Context, t *testing.T, conn driver.Conn, table string) string {
	t.Helper()
	rows, err := conn.Query(ctx,
		"SELECT name, type, default_kind FROM system.columns "+
			"WHERE database = currentDatabase() AND table = ? ORDER BY position", table)
	if err != nil {
		t.Fatalf("column shape for %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	var parts []string
	for rows.Next() {
		var name, typ, kind string
		if err := rows.Scan(&name, &typ, &kind); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, name+" "+typ+" "+kind)
	}
	return strings.Join(parts, " | ")
}

func sortingKey(ctx context.Context, t *testing.T, conn driver.Conn, table string) string {
	t.Helper()
	var key string
	if err := conn.QueryRow(ctx,
		"SELECT sorting_key FROM system.tables "+
			"WHERE database = currentDatabase() AND name = ?", table).Scan(&key); err != nil {
		t.Fatalf("sorting key for %s: %v", table, err)
	}
	return key
}

// hookOutcome reports WHAT happened to the forced statement, never WHY.
//
// It used to say "REFUSED — the migration's SYSTEM STOP MERGES held", which
// asserts a cause it cannot observe. A refusal for ANY reason took that label:
// lane-4752-go demonstrated it with `OPTIMIZE TABLE no_such_table_at_all FINAL`,
// which is refused because the table does not exist and was reported as the
// guard holding. That is the same misreading the per-statement split was added
// to remove -- fixed in the behaviour, left standing in the label.
//
// It cannot fake a green (a test failing for the wrong reason still fails), so
// it is a false EXPLANATION on a red rather than a false pass. The accompanying
// HOOK_STATEMENT_REFUSED line names the statement and the exception, which is
// where the actual reason lives.
func hookOutcome(out string) string {
	switch {
	case strings.Contains(out, "HOOK_REFUSED"):
		return "REFUSED — a hook statement was rejected; this does NOT establish " +
			"that the guard held (see HOOK_STATEMENT_REFUSED for which and why)"
	case strings.Contains(out, "HOOK_SUCCEEDED"):
		return "SUCCEEDED — every hook statement ran"
	default:
		return "unknown"
	}
}

// TestMigration084RefusesWhenAMergeLandsMidCopy is the deterministic red for
// the merge-landed detection.
//
// WHY DETECTION EXISTS ALONGSIDE THE GUARD: `SYSTEM STOP MERGES` is not a
// counter. Measured on 26.7.6.57 -- two STOPs followed by ONE START re-enable
// merges. So any concurrent START (an operator, or another migration's own
// `finally`) silently re-arms merges underneath the copy, and the guard alone
// cannot be trusted to have held for the whole window.
//
// The hook here does exactly that: START MERGES, then OPTIMIZE FINAL. With
// detection the migration compares the source's part set and row count against
// the snapshot and REFUSES before EXCHANGE, leaving the original untouched.
// Without detection it swaps in a copy that is missing the native rows.
func TestMigration084RefusesWhenAMergeLandsMidCopy(t *testing.T) {
	ctx := context.Background()
	instance := startClickHouse(ctx, t)
	chschema.Apply(ctx, t, instance)
	conn := openConn(ctx, t, instance)

	rewindToPre084(ctx, t, conn)
	mustExec(ctx, t, conn, "SYSTEM STOP MERGES work_graph_issue_pr")

	const (
		org  = "00000000-4769-0000-0000-000000000001"
		repo = "00000000-4769-0000-0000-000000000002"
		keyA = "00000000-4769-0000-0000-000000000003"
	)
	seed(ctx, t, conn, org, repo, keyA, 4769, "native", 1.00, "2026-01-01 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyA, 4769, "heuristic", 0.50, "2026-01-03 00:00:00.000")
	before := countRows(ctx, t, conn)
	if before != 2 {
		t.Fatalf("seeded %d rows, want 2", before)
	}

	err := runMigration084(ctx, t, instance,
		"SYSTEM START MERGES work_graph_issue_pr; OPTIMIZE TABLE work_graph_issue_pr FINAL")
	if err == nil {
		t.Fatal("the migration COMPLETED while a merge ran under its copy: it would " +
			"have swapped in a table missing the native rows it exists to promote")
	}
	if !strings.Contains(err.Error(), "changed between the snapshot and the copy") {
		t.Errorf("migration failed for the wrong reason: %v", err)
	}

	// Refusal must leave the ORIGINAL table usable, not half-migrated.
	if got := engineOf(ctx, t, conn, "work_graph_issue_pr"); !strings.Contains(got, "last_synced") {
		t.Errorf("after refusing, the source engine is %q -- expected the "+
			"pre-084 ReplacingMergeTree(last_synced), untouched", got)
	}
}

func engineOf(ctx context.Context, t *testing.T, conn driver.Conn, table string) string {
	t.Helper()
	var engine string
	if err := conn.QueryRow(ctx,
		"SELECT engine_full FROM system.tables "+
			"WHERE database = currentDatabase() AND name = ?", table).Scan(&engine); err != nil {
		t.Fatalf("engine for %s: %v", table, err)
	}
	return engine
}

// TestMigration084ConvergesFromAPartialRun covers the state codex round 4 found
// unrecoverable: the version COLUMN already exists but the ENGINE was never
// swapped, which a run interrupted between the two leaves behind.
//
// `_assert_shadow_matches` used to build its expected shape as "source columns
// + version_rank". With the column already present that named it TWICE, the
// check aborted, and the rerun dropped the shadow and failed identically
// forever -- the precedence defect left in place permanently by the very check
// meant to protect it.
func TestMigration084ConvergesFromAPartialRun(t *testing.T) {
	ctx := context.Background()
	instance := startClickHouse(ctx, t)
	chschema.Apply(ctx, t, instance)
	conn := openConn(ctx, t, instance)

	rewindToPre084(ctx, t, conn)
	// The partial state: column added, engine still the old one.
	mustExec(ctx, t, conn, "ALTER TABLE work_graph_issue_pr ADD COLUMN `version_rank` UInt64 "+
		"MATERIALIZED (multiIf(provenance = 'native', 3, provenance = 'explicit_text', 2, "+
		"provenance = 'heuristic', 1, 0) + 1) * 1125899906842624 + toUnixTimestamp64Milli(last_synced)")
	if got := engineOf(ctx, t, conn, "work_graph_issue_pr"); !strings.Contains(got, "last_synced") {
		t.Fatalf("fixture wrong: engine is %q, expected the pre-084 one", got)
	}

	const (
		org  = "00000000-4769-0000-0000-000000000001"
		repo = "00000000-4769-0000-0000-000000000002"
		keyA = "00000000-4769-0000-0000-000000000003"
	)
	mustExec(ctx, t, conn, "SYSTEM STOP MERGES work_graph_issue_pr")
	seed(ctx, t, conn, org, repo, keyA, 4769, "native", 1.00, "2026-01-01 00:00:00.000")
	seed(ctx, t, conn, org, repo, keyA, 4769, "heuristic", 0.50, "2026-01-03 00:00:00.000")

	applyMigration084(ctx, t, instance, "")

	if got := engineOf(ctx, t, conn, "work_graph_issue_pr"); !strings.Contains(got, "version_rank") {
		t.Errorf("after a partial-run rerun the engine is %q, want ReplacingMergeTree(version_rank)", got)
	}
	mustExec(ctx, t, conn, "OPTIMIZE TABLE work_graph_issue_pr FINAL")
	assertOnlyProvenance(ctx, t, conn, keyA, "native")
}

// TestMigration084RefusesADefaultVersionColumn covers round 4's second P2.
//
// A DEFAULT column canonicalises to the same expression as a MATERIALIZED one,
// so an expression-only check accepted it and recorded 084 as applied. DEFAULT
// stays explicitly WRITABLE, so a client can supply its own version -- the
// reviewer showed UInt64 max letting heuristic beat native after a merge. The
// skip path must therefore check the column KIND, not only its expression.
func TestMigration084RefusesADefaultVersionColumn(t *testing.T) {
	ctx := context.Background()
	instance := startClickHouse(ctx, t)
	chschema.Apply(ctx, t, instance)
	conn := openConn(ctx, t, instance)

	mustExec(ctx, t, conn, "DROP TABLE IF EXISTS work_graph_issue_pr")
	mustExec(ctx, t, conn, "CREATE TABLE work_graph_issue_pr ("+
		"repo_id UUID, work_item_id String, pr_number UInt32, confidence Float32, "+
		"provenance String, evidence String, last_synced DateTime64(3,'UTC'), "+
		"org_id String DEFAULT 'default', "+
		"`version_rank` UInt64 DEFAULT (multiIf(provenance = 'native', 3, "+
		"provenance = 'explicit_text', 2, provenance = 'heuristic', 1, 0) + 1) "+
		"* 1125899906842624 + toUnixTimestamp64Milli(last_synced)) "+
		"ENGINE = ReplacingMergeTree(version_rank) "+
		"ORDER BY (org_id, repo_id, work_item_id, pr_number)")

	err := runMigration084(ctx, t, instance, "")
	if err == nil {
		t.Fatal("the migration ACCEPTED a DEFAULT version column and recorded itself " +
			"as applied: the column stays writable, so a caller can supply its own " +
			"version and outrank native")
	}
	if !strings.Contains(err.Error(), "not MATERIALIZED") {
		t.Errorf("refused for the wrong reason: %v", err)
	}
}
