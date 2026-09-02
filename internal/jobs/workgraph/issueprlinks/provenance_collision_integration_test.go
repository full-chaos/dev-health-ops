//go:build integration

package issueprlinks_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

	const (
		org   = "00000000-4769-0000-0000-000000000001"
		repo  = "00000000-4769-0000-0000-000000000002"
		keyA  = "00000000-4769-0000-0000-000000000003"
		keyB  = "00000000-4769-0000-0000-000000000004"
		prA   = 4769
		prB   = 4770
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
	for _, pr := range []int{prA, prB} {
		exec(ctx, t, conn, fmt.Sprintf(
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

	// STEP 3. Without FINAL every seeded row is still visible. This is also the
	// precondition for the Go port's approach (drop FINAL, dedup with
	// LIMIT 1 BY over the sorting key): FINAL collapses to the max-version row
	// BEFORE any ranking can see the alternatives.
	assertRowCount(ctx, t, conn, keyA, 3)
	assertRowCount(ctx, t, conn, keyB, 2)

	// STEP 4. The measurement that decided the fix.
	exec(ctx, t, conn, "OPTIMIZE TABLE work_graph_issue_pr FINAL")
	assertRowCount(ctx, t, conn, keyA, 1)
	assertRowCount(ctx, t, conn, keyB, 1)
	// FLIP: after the fix, native (A) and explicit_text (B) must be what remain.
	assertOnlyProvenance(ctx, t, conn, keyA, "native")
	assertOnlyProvenance(ctx, t, conn, keyB, "explicit_text")

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
			exec(ctx, t, conn, "DROP TABLE IF EXISTS "+table)
			exec(ctx, t, conn, fmt.Sprintf(`CREATE TABLE %s (
				repo_id UUID, work_item_id String, pr_number UInt32, confidence Float32,
				provenance String, evidence String, last_synced DateTime64(3,'UTC'),
				org_id String DEFAULT 'default'
			) ENGINE = ReplacingMergeTree(last_synced)
			ORDER BY (org_id, repo_id, work_item_id, pr_number)`, table))
			for _, prov := range order {
				// One INSERT is one PART, which is what distinct writer calls
				// produce. The part boundary IS the mechanism here.
				exec(ctx, t, conn, fmt.Sprintf(
					`INSERT INTO %s (repo_id, work_item_id, pr_number, confidence,
					 provenance, evidence, last_synced, org_id)
					 VALUES ('%s','%s',4771,0.5,'%s','tie','%s','%s')`,
					table, repo, tieID, prov, tie, org))
			}
			exec(ctx, t, conn, "OPTIMIZE TABLE "+table+" FINAL")
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
			exec(ctx, t, conn, "DROP TABLE "+table)
		}
	})

	writeProof(t, "issue-pr-provenance-collision")
}

func connect(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

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

func exec(ctx context.Context, t *testing.T, conn driver.Conn, sql string) {
	t.Helper()
	if err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("exec %.90s: %v", sql, err)
	}
}

func seed(ctx context.Context, t *testing.T, conn driver.Conn,
	org, repo, workItem string, pr int, provenance string, confidence float64, lastSynced string) {
	t.Helper()
	exec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence,
		 provenance, evidence, last_synced, org_id)
		 VALUES ('%s','%s',%d,%f,'%s','seed','%s','%s')`,
		repo, workItem, pr, confidence, provenance, lastSynced, org))
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
