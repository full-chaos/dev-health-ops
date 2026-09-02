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
	for _, pr := range []int{prA, prB, prC} {
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

	// Rewind to the pre-084 shape: the chain has already migrated the table,
	// so recreate it as migration 014+024 left it and let 084 do the work.
	mustExec(ctx, t, conn, "DROP TABLE IF EXISTS work_graph_issue_pr")
	mustExec(ctx, t, conn, `CREATE TABLE work_graph_issue_pr (
		repo_id UUID, work_item_id String, pr_number UInt32, confidence Float32,
		provenance String, evidence String, last_synced DateTime64(3,'UTC'),
		org_id String DEFAULT 'default'
	) ENGINE = ReplacingMergeTree(last_synced)
	ORDER BY (org_id, repo_id, work_item_id, pr_number)`)
	mustExec(ctx, t, conn, "SYSTEM STOP MERGES work_graph_issue_pr")

	const (
		org  = "00000000-4769-0000-0000-000000000001"
		repo = "00000000-4769-0000-0000-000000000002"
		keyA = "00000000-4769-0000-0000-000000000003"
		keyB = "00000000-4769-0000-0000-000000000004"
		keyC = "00000000-4769-0000-0000-000000000006"
		keyE = "00000000-4769-0000-0000-000000000007"
		keyF = "00000000-4769-0000-0000-000000000008"
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

	before := countRows(ctx, t, conn)
	if before != 10 {
		t.Fatalf("seeded %d rows, want 10 -- fixture broken before the migration runs", before)
	}

	applyMigration084(ctx, t, instance)

	// The copy carried every key. A no-op copy leaves zero.
	if got := countRows(ctx, t, conn); got == 0 {
		t.Fatal("the migrated table is EMPTY: the copy carried nothing and EXCHANGE " +
			"swapped in an empty shadow -- this is the total-data-loss case")
	}
	mustExec(ctx, t, conn, "SYSTEM START MERGES work_graph_issue_pr")
	mustExec(ctx, t, conn, "OPTIMIZE TABLE work_graph_issue_pr FINAL")

	for _, c := range []struct{ key, want, why string }{
		{keyA, "native", "native must outrank a later heuristic"},
		{keyB, "explicit_text", "explicit_text must outrank a later, higher-confidence heuristic"},
		{keyC, "native", "rank must beat a 74-year recency gap"},
		{keyE, "native", "an unknown provenance with a pre-epoch stamp must not wrap and win"},
		{keyF, "heuristic", "the later MILLISECOND must win among equal provenance"},
	} {
		assertOnlyProvenance(ctx, t, conn, c.key, c.want)
	}
	// Full precision, not truncated to seconds: this is what fails under a
	// toUnixTimestamp mutant.
	assertLastSynced(ctx, t, conn, keyF, "2026-01-04 00:00:00.500")
	assertLastSynced(ctx, t, conn, keyA, "2026-01-01 00:00:00.000")
}

// applyMigration084 runs ONE migration against the live container, the way the
// production runner does, so the copy is exercised on rows that already exist.
func applyMigration084(ctx context.Context, t *testing.T, instance *containers.Instance) {
	t.Helper()
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("http dsn: %v", err)
	}
	root := repoRootForMigration(t)
	script := `
import sys, runpy
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
sink = ClickHouseMetricsSink(dsn=sys.argv[1])
try:
    runpy.run_path(sys.argv[2])["upgrade"](sink.client)
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
	command := exec.CommandContext(ctx, python, "-c", script, dsn, migration)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	out, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "MIGRATION_084_APPLIED") {
		t.Fatalf("apply migration 084: %v\n%s", err, out)
	}
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
