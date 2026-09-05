//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/reviewedges"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// reviewEdgesSchema is the production shape of the three tables this family
// touches: the two raw sync sources from 000_raw_tables.sql (both
// ReplacingMergeTree(last_synced), with org_id added by migration 024) and the
// plain-MergeTree output from 004_quality_delivery_metrics.sql.
// Sorting keys are migration 027's, org_id FIRST on all three
// (027_add_org_id_to_sorting_keys.py:63,64,46) -- NOT the pre-027 keys from
// 000_raw_tables.sql / 004_quality_delivery_metrics.sql.
//
// This is load-bearing, not cosmetic. The first version of this fixture used
// the pre-027 keys, and under ReplacingMergeTree(last_synced) with
// ORDER BY (repo_id, number) two orgs sharing a repo_id are the SAME ROW: org
// B's later last_synced silently replaced org A's PR before any query ran, so
// TestReviewEdgesDedupIsTenantScopedWhenTwoOrgsShareARepoID computed 0 edges.
//
// The failure was a coin flip away from being invisible. Had org B's
// last_synced been the EARLIER one, org A's row would have survived, the test
// would have passed, and it would have proven nothing about the loader's
// org_id filter -- which is the thing it exists to pin. A fixture whose
// sorting key does not match production does not test production.
var reviewEdgesSchema = []string{
	`CREATE TABLE git_pull_requests (
    repo_id UUID, number UInt32, title Nullable(String), body Nullable(String),
    state Nullable(String), author_name Nullable(String), author_email Nullable(String),
    created_at DateTime64(3, 'UTC'), merged_at Nullable(DateTime64(3, 'UTC')),
    closed_at Nullable(DateTime64(3, 'UTC')),
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, number)`,
	`CREATE TABLE git_pull_request_reviews (
    repo_id UUID, number UInt32, review_id String, reviewer String, state String,
    submitted_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, number, review_id)`,
	`CREATE TABLE review_edges_daily (
    repo_id UUID, day Date, reviewer String, author String,
    reviews_count UInt32, computed_at DateTime('UTC'), org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day)
  ORDER BY (org_id, repo_id, reviewer, author, day)`,
}

// TestReviewEdgesComputeFamilyDeduplicatesResyncedRows is the loader-level
// proof, and it is the ONLY layer that can see this family's most important
// behaviour change.
//
// Python queries both ReplacingMergeTree sources RAW -- no FINAL, no argMax
// (loaders/clickhouse.py:283-320) -- so a re-synced review row is COUNTED
// TWICE and inflates reviews_count, and a re-synced PR row makes the author
// last-write-wins over an unordered result set. The native loader reads
// FINAL (clickhouse.go:91, :163), so:
//
//   - the duplicated review is counted ONCE (native count is LOWER than
//     Python's here, and correct), and
//   - the PR author is deterministically the latest-synced value.
//
// The frozen golden cannot see either: it feeds identical rows to both sides,
// which is exactly the point -- the compute is unchanged and the divergence
// lives entirely in WHICH rows reach it.
func TestReviewEdgesComputeFamilyDeduplicatesResyncedRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range reviewEdgesSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		orgA  = "00000000-0000-4000-8000-0000000000a0"
		orgB  = "00000000-0000-4000-8000-0000000000b0"
		repoA = "00000000-0000-4000-8000-0000000000a1"
		repoB = "00000000-0000-4000-8000-0000000000b1"
	)

	// PR 1: two rows, same key, different last_synced. FINAL must take the
	// LATER author (ann@…), never the earlier decoy.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests
(repo_id, number, author_name, author_email, created_at, merged_at, last_synced, org_id) VALUES
(toUUID('`+repoA+`'), 1, 'Stale', 'stale@example.com', '2026-08-24 08:00:00.000', NULL, '2026-08-24 08:00:00.000', '`+orgA+`'),
(toUUID('`+repoA+`'), 1, 'Ann',   'ann@example.com',   '2026-08-24 08:00:00.000', NULL, '2026-08-24 09:00:00.000', '`+orgA+`'),
(toUUID('`+repoB+`'), 1, 'Dee',   'dee@example.com',   '2026-08-24 08:00:00.000', NULL, '2026-08-24 08:00:00.000', '`+orgB+`')
`); err != nil {
		t.Fatal(err)
	}

	// review r1 is INGESTED TWICE (the CHAOS-5045 re-ingestion shape). Python
	// would count it twice; the native loader counts it once.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_request_reviews
(repo_id, number, review_id, reviewer, state, submitted_at, last_synced, org_id) VALUES
(toUUID('`+repoA+`'), 1, 'r1', 'Bob', 'APPROVED',  '2026-08-24 10:00:00.000', '2026-08-24 10:00:00.000', '`+orgA+`'),
(toUUID('`+repoA+`'), 1, 'r1', 'Bob', 'APPROVED',  '2026-08-24 10:00:00.000', '2026-08-24 11:00:00.000', '`+orgA+`'),
(toUUID('`+repoA+`'), 1, 'r2', 'Cal', 'COMMENTED', '2026-08-24 10:30:00.000', '2026-08-24 10:30:00.000', '`+orgA+`'),
(toUUID('`+repoB+`'), 1, 'r9', 'Eve', 'APPROVED',  '2026-08-24 10:00:00.000', '2026-08-24 10:00:00.000', '`+orgB+`')
`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewReviewEdgesExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	rowsWritten, err := executor.ComputeFamily(ctx,
		Run{ID: "run-a", OrganizationID: orgA, TargetDay: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)},
		Partition{ID: "partition-a", RunID: "run-a", RepoIDs: []RepositoryID{RepositoryID(repoA)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if rowsWritten != 2 {
		t.Fatalf("wrote %d rows, want 2 (Bob->ann and Cal->ann)", rowsWritten)
	}

	type edge struct {
		reviewer string
		author   string
		count    uint32
	}
	rows, err := conn.Query(ctx, `
SELECT reviewer, author, reviews_count FROM review_edges_daily
WHERE org_id = ? ORDER BY reviewer`, orgA)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []edge
	for rows.Next() {
		var e edge
		if err := rows.Scan(&e.reviewer, &e.author, &e.count); err != nil {
			t.Fatal(err)
		}
		got = append(got, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	want := []edge{
		// count 1, NOT 2: the duplicated r1 is deduplicated by last_synced.
		{"Bob", "ann@example.com", 1},
		{"Cal", "ann@example.com", 1},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d edges %+v, want %d %+v", len(got), got, len(want), want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Errorf("edge %d: got %+v, want %+v", index, got[index], want[index])
		}
	}
	// The author proves FINAL took the LATER PR row: 'stale@example.com'
	// would mean the dedup picked by insertion order instead of last_synced.
	for _, e := range got {
		if e.author == "stale@example.com" {
			t.Error("author resolved to the earlier-synced PR row -- FINAL is not resolving by last_synced")
		}
	}

	// Cross-tenant: org B's repo is in the table but not in this partition,
	// and its rows must not appear under org A.
	var strayRows uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM review_edges_daily WHERE org_id != ?`, orgA,
	).Scan(&strayRows); err != nil {
		t.Fatal(err)
	}
	if strayRows != 0 {
		t.Errorf("%d row(s) written outside org A", strayRows)
	}
}

// TestReviewEdgesDropsAReviewWhoseParentPullRequestIsOutsideTheDayWindow is the
// end-to-end proof of the dropped-edge quirk, through real SQL rather than a
// hand-built row list: the PR loader's window is `created_at` OR `merged_at` in
// the day, so a review submitted today on a PR created LAST week and not merged
// today finds no author and its edge vanishes.
//
// Mirrored deliberately from reviews.py:52-54. If this ever starts producing an
// edge, the Python producer's behaviour changed and the port must follow
// through this test, not silently.
func TestReviewEdgesDropsAReviewWhoseParentPullRequestIsOutsideTheDayWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range reviewEdgesSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		org  = "00000000-0000-4000-8000-0000000000c0"
		repo = "00000000-0000-4000-8000-0000000000c1"
	)
	// PR 1 was created a week before the target day and never merged: outside
	// BOTH arms of the window predicate. PR 2 was created that day.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests
(repo_id, number, author_name, author_email, created_at, merged_at, last_synced, org_id) VALUES
(toUUID('`+repo+`'), 1, 'Old', 'old@example.com', '2026-08-17 08:00:00.000', NULL, '2026-08-17 08:00:00.000', '`+org+`'),
(toUUID('`+repo+`'), 2, 'New', 'new@example.com', '2026-08-24 08:00:00.000', NULL, '2026-08-24 08:00:00.000', '`+org+`')
`); err != nil {
		t.Fatal(err)
	}
	// Both reviews are submitted ON the target day.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_request_reviews
(repo_id, number, review_id, reviewer, state, submitted_at, last_synced, org_id) VALUES
(toUUID('`+repo+`'), 1, 'r1', 'Bob', 'APPROVED', '2026-08-24 10:00:00.000', '2026-08-24 10:00:00.000', '`+org+`'),
(toUUID('`+repo+`'), 2, 'r2', 'Bob', 'APPROVED', '2026-08-24 10:30:00.000', '2026-08-24 10:30:00.000', '`+org+`')
`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewReviewEdgesExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	rowsWritten, err := executor.ComputeFamily(ctx,
		Run{ID: "run-c", OrganizationID: org, TargetDay: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)},
		Partition{ID: "partition-c", RunID: "run-c", RepoIDs: []RepositoryID{RepositoryID(repo)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if rowsWritten != 1 {
		t.Fatalf("wrote %d rows, want 1 -- the review of the out-of-window PR must be dropped", rowsWritten)
	}
	var author string
	if err := conn.QueryRow(ctx,
		`SELECT author FROM review_edges_daily WHERE org_id = ?`, org,
	).Scan(&author); err != nil {
		t.Fatal(err)
	}
	if author != "new@example.com" {
		t.Errorf("author = %q, want new@example.com (the in-window PR)", author)
	}
}

// TestReviewEdgesTupleDedupTakesTheWholeWinningRowIncludingItsNulls pins
// whole-row NULL behaviour: the half of the dedup no unit test can reach and no
// frozen golden can see.
//
// NAME AND HISTORY. This test was written when the loader used
// argMax(tuple(...)) and is named for it. The loader now reads FINAL
// (clickhouse.go:91, :163) -- so the mechanism named in the test's name is gone
// while what it PINS is unchanged, and deliberately so. The name is kept
// because it is what the CHAOS-4547 ledger rows cite; renaming it would orphan
// that history for a cosmetic gain.
//
// # What is actually being defended
//
// ClickHouse aggregate functions SKIP rows whose argument is NULL. With one
// argMax per column, a PR whose NEWEST row has a NULL author_email but whose
// OLDER row has a value returns the STALE value -- the dedup surfaces a
// composite that never existed, while claiming to surface "the latest".
// CHAOS-4547 verified that mechanism against a live ClickHouse.
//
// FINAL cannot do that: it resolves a whole row, NULLs included, which is what
// a ReplacingMergeTree actually holds -- per-column latest-non-NULL is not a
// state the table can ever be in. argMax(tuple(...)) was chosen earlier for the
// same reason, as an EMULATION of FINAL; reading FINAL directly is the same
// property obtained from the engine rather than reconstructed.
//
// So the assertion is unchanged and still discriminating: the newest row for
// PR 1 has author_email NULL and author_name 'Newest', so the author must
// resolve from the NAME, not from the older row's email. A regression to
// per-column argMax makes the author 'stale@example.com' and this test says so
// -- which is why it is still worth running under FINAL.
func TestReviewEdgesTupleDedupTakesTheWholeWinningRowIncludingItsNulls(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range reviewEdgesSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		org  = "00000000-0000-4000-8000-0000000000d0"
		repo = "00000000-0000-4000-8000-0000000000d1"
	)

	// OLDER row: author_email present. NEWER row: author_email NULL, name set.
	// Independent argMax(author_email, last_synced) would skip the NULL and
	// return 'stale@example.com'; FINAL returns the newer row whole.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests
(repo_id, number, author_name, author_email, created_at, merged_at, last_synced, org_id) VALUES
(toUUID('`+repo+`'), 1, 'Stale',  'stale@example.com', '2026-08-24 08:00:00.000', NULL, '2026-08-24 08:00:00.000', '`+org+`'),
(toUUID('`+repo+`'), 1, 'Newest', NULL,                '2026-08-24 08:00:00.000', NULL, '2026-08-24 10:00:00.000', '`+org+`')
`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_request_reviews
(repo_id, number, review_id, reviewer, state, submitted_at, last_synced, org_id) VALUES
(toUUID('`+repo+`'), 1, 'r1', 'Bob', 'APPROVED', '2026-08-24 11:00:00.000', '2026-08-24 11:00:00.000', '`+org+`')
`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewReviewEdgesExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ComputeFamily(ctx,
		Run{ID: "run-d", OrganizationID: org, TargetDay: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)},
		Partition{ID: "partition-d", RunID: "run-d", RepoIDs: []RepositoryID{RepositoryID(repo)}},
	); err != nil {
		t.Fatal(err)
	}

	var author string
	if err := conn.QueryRow(ctx,
		`SELECT author FROM review_edges_daily WHERE org_id = ?`, org,
	).Scan(&author); err != nil {
		t.Fatal(err)
	}
	if author == "stale@example.com" {
		t.Fatal(
			"author resolved to the OLDER row's email -- the dedup is null-skipping per column, " +
				"which means it has been split back into independent argMax calls (CHAOS-4547)",
		)
	}
	if author != "Newest" {
		t.Errorf(
			"author = %q, want \"Newest\": the winning row has a NULL email, so the identity "+
				"must fall through to that same row's NAME, not to another row's email",
			author,
		)
	}
}

// TestReviewEdgesDedupIsTenantScopedWhenTwoOrgsShareARepoID is the cross-tenant
// guard for the dedup, on the one shape that can actually expose it: TWO ORGS
// CARRYING THE SAME repo_id.
//
// That is not hypothetical. repo ids are derived deterministically from the
// repo slug (uuid5 over the URL), so two orgs syncing the same public repo get
// the SAME repo_id -- clickhouse_dedup.py:115's own comment records this as the
// reason review_edges_daily's natural key had to gain org_id.
//
// # What this defends
//
// Migrations 027/042 rekeyed git_pull_requests to (org_id, repo_id, number),
// and FINAL resolves rows BY that sorting key -- so two tenants sharing a
// repo_id are separate rows to it, not one. Under the earlier GROUP BY dedup a
// key omitting org_id collapsed BOTH tenants' rows into one group and the
// newest won across the tenant boundary; that shape is what this test was
// written against and it remains the regression to guard.
//
// Today this loader is safe regardless, because its org filter is
// PRE-aggregation -- in the same SELECT as the GROUP BY, so WHERE runs first
// and the aggregate never sees the other tenant. The GROUP BY now leads with
// org_id anyway, which makes the boundary STRUCTURAL: it holds even if a later
// refactor hoists that filter into an outer query to reuse the inner one.
//
// STATUS OF THAT CLAIM, stated exactly: the mechanism is derived from the
// table's sorting key (migrations 027/042) plus SQL evaluation order -- WHERE
// before GROUP BY -- and this test is compile-verified only. The red/green
// demonstration (hoist the filter outward; RED without org_id in the GROUP BY,
// GREEN with it) requires a real ClickHouse and therefore the bigboy
// integration turn. It has NOT been executed on the Mac, because integration
// tests do not run there.
//
// Said plainly so nobody reads a reasoned argument as a measurement: this
// assertion is a guard whose own failure mode is currently unproven. The
// standing rule is to state what code DOES only after executing it, and the
// executed half of this is still owed.
func TestReviewEdgesDedupIsTenantScopedWhenTwoOrgsShareARepoID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range reviewEdgesSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		orgA = "00000000-0000-4000-8000-0000000000e0"
		orgB = "00000000-0000-4000-8000-0000000000e1"
		// THE SAME repo_id under both orgs -- the collision the deterministic
		// slug-derived id actually produces.
		shared = "00000000-0000-4000-8000-0000000000ee"
	)

	// Org B's row is NEWER. A dedup that ignores org_id would let it win for an
	// org A read.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests
(repo_id, number, author_name, author_email, created_at, merged_at, last_synced, org_id) VALUES
(toUUID('`+shared+`'), 1, 'AnnOrgA', 'ann@org-a.example',  '2026-08-24 08:00:00.000', NULL, '2026-08-24 08:00:00.000', '`+orgA+`'),
(toUUID('`+shared+`'), 1, 'BobOrgB', 'bob@org-b.example',  '2026-08-24 08:00:00.000', NULL, '2026-08-24 23:00:00.000', '`+orgB+`')
`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_request_reviews
(repo_id, number, review_id, reviewer, state, submitted_at, last_synced, org_id) VALUES
(toUUID('`+shared+`'), 1, 'rA', 'ReviewerA', 'APPROVED', '2026-08-24 10:00:00.000', '2026-08-24 10:00:00.000', '`+orgA+`'),
(toUUID('`+shared+`'), 1, 'rB', 'ReviewerB', 'APPROVED', '2026-08-24 11:00:00.000', '2026-08-24 23:00:00.000', '`+orgB+`')
`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewReviewEdgesExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

	// Compute for org A ONLY.
	if _, err := executor.ComputeFamily(ctx,
		Run{ID: "run-e", OrganizationID: orgA, TargetDay: targetDay},
		Partition{ID: "partition-e", RunID: "run-e", RepoIDs: []RepositoryID{RepositoryID(shared)}},
	); err != nil {
		t.Fatal(err)
	}

	rows, err := conn.Query(ctx, `
SELECT reviewer, author, org_id FROM review_edges_daily ORDER BY reviewer`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type edge struct{ reviewer, author, org string }
	var got []edge
	for rows.Next() {
		var e edge
		if err := rows.Scan(&e.reviewer, &e.author, &e.org); err != nil {
			t.Fatal(err)
		}
		got = append(got, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if len(got) != 1 {
		t.Fatalf("got %d edges %+v, want exactly 1 (only org A was computed)", len(got), got)
	}
	if got[0].org != orgA {
		t.Errorf("edge carries org_id %q, want org A %q", got[0].org, orgA)
	}
	// The decisive assertion: org B's row is NEWER by last_synced, so a dedup
	// that collapsed both tenants would attribute org A's edge to org B's author.
	if got[0].author == "bob@org-b.example" {
		t.Fatal(
			"CROSS-TENANT LEAK: org A's edge resolved to org B's author. The dedup " +
				"collapsed two tenants sharing a repo_id -- its GROUP BY no longer matches " +
				"git_pull_requests' sorting key (org_id, repo_id, number) from migrations 027/042",
		)
	}
	if got[0].author != "ann@org-a.example" {
		t.Errorf("author = %q, want org A's own %q", got[0].author, "ann@org-a.example")
	}
	if got[0].reviewer != "ReviewerA" {
		t.Errorf("reviewer = %q, want org A's own \"ReviewerA\"", got[0].reviewer)
	}
}

// TestReviewEdgesFinalIsDeterministicOnALastSyncedTie pins the reason this
// family reads FINAL rather than argMax.
//
// Python reads these tables raw, so the dedup is something this port ADDS; an
// added dedup on a ReplacingMergeTree uses FINAL. The property that matters is
// behaviour on a version TIE, which is not an exotic case here -- a re-sync
// writing several columns in one batch stamps them with the same last_synced by
// construction. argMax is nondeterministic on a tie; FINAL keeps the
// last-inserted row.
//
// The POSITIVE CONTROL is the point of this test, not decoration. A test that
// merely observes "FINAL returned one row" proves nothing unless the same
// fixture could have returned two -- a negative result on a setup that cannot
// exhibit the effect is meaningless. So the control reads the SAME rows WITHOUT
// FINAL first and asserts it sees BOTH, establishing that the collapse below is
// doing work.
func TestReviewEdgesFinalIsDeterministicOnALastSyncedTie(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range reviewEdgesSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		orgA = "00000000-0000-4000-8000-0000000000f0"
		repo = "00000000-0000-4000-8000-0000000000f1"
		tie  = "2026-08-24 09:00:00.000"
	)

	// Two snapshots of ONE PR with an IDENTICAL last_synced, inserted as two
	// SEPARATE statements so "last inserted" is unambiguous -- within a single
	// multi-row INSERT the order is an implementation detail this test must not
	// depend on.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests
(repo_id, number, author_name, author_email, created_at, merged_at, last_synced, org_id) VALUES
(toUUID('`+repo+`'), 1, 'First', 'first@example.com', '2026-08-24 08:00:00.000', NULL, '`+tie+`', '`+orgA+`')
`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests
(repo_id, number, author_name, author_email, created_at, merged_at, last_synced, org_id) VALUES
(toUUID('`+repo+`'), 1, 'Second', 'second@example.com', '2026-08-24 08:00:00.000', NULL, '`+tie+`', '`+orgA+`')
`); err != nil {
		t.Fatal(err)
	}

	// POSITIVE CONTROL: without FINAL the same fixture yields BOTH rows. If this
	// ever reports 1, the fixture stopped being able to exhibit a tie at all and
	// every assertion below became vacuous -- fail loudly rather than pass.
	var raw uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM git_pull_requests WHERE org_id = ? AND repo_id = ?`,
		orgA, repo,
	).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw != 2 {
		t.Fatalf(
			"positive control: raw read returned %d row(s), want 2 -- the fixture can no "+
				"longer exhibit a last_synced tie, so this test proves nothing", raw,
		)
	}

	// FINAL collapses the tie to exactly one row, and keeps the LAST inserted.
	loader, err := reviewedges.NewClickHouseLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	prs, err := loader.LoadPullRequests(ctx, orgA, []uuid.UUID{uuid.MustParse(repo)}, start, start.AddDate(0, 0, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(prs) != 1 {
		t.Fatalf("FINAL returned %d row(s) %+v, want exactly 1", len(prs), prs)
	}
	if prs[0].AuthorEmail != "second@example.com" {
		t.Errorf(
			"FINAL kept author_email %q, want %q (the LAST-inserted row on an equal-version tie)",
			prs[0].AuthorEmail, "second@example.com",
		)
	}
}
