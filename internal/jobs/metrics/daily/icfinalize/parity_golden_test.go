//go:build integration

package icfinalize

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestICFinalizeMatchesTheFrozenPythonGolden is CHAOS-4290's parity proof
// (PR3, parity-ic-finalize-oracle): compute_ic.py's compute_ic_metrics_daily
// and compute_ic_landscape_rolling are deleted in this same PR (CHAOS-3092
// no-straddle: the native Go executor has been the sole writer since #2241's
// finalize policy landed, so a still-present Python compute path was already
// dead weight, not a live fallback -- see this PR's body for the
// no-fail-open citation).
//
// This is a FROZEN-golden test, not a live dual-execution one (unlike
// internal/testsupport/computeparity's former capacity/dora pattern -- that
// package is itself retired now, CHAOS-5336, along with the Python it
// compared against): there is no
// live Python left to execute at test time once this PR merges. The golden
// file (testdata/parity_golden.json) was captured ONCE, via a throwaway
// script run against the still-live compute_ic.py before its deletion in
// this same commit -- see the PR body for the exact capture recipe; the
// script itself was never committed.
//
// The corpus (three identities) is chosen to exercise the divergence-prone
// branches directly:
//   - alice: git-backed AND has work items (base=git, WI-aggregation branch,
//     a team_map override winning over her git-derived team).
//   - bob: git-backed ONLY, no work items at all (base=git, WI-absent
//     branch, no team override).
//   - carol: work-item-only, NO git record (base=synthesized branch) --
//     exercises SynthesizedRepoID and the "no team resolvable" shape.
//
// alice and bob share a landscape team on purpose: compute_ic.py's
// _percentile_rank returns 0.5 for a ONE-member team unconditionally, which
// would make x_norm/y_norm trivially 0.5 for every row and pin nothing.
// Two members with distinct values give a real 0.25/0.75 split to check.
func TestICFinalizeMatchesTheFrozenPythonGolden(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	golden := loadParityGolden(t)

	const orgID = "00000000-0000-4000-8000-0000000ec101"
	day := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	// Fixed clock: computed_at is a write timestamp, not a compute output --
	// excluded from the golden comparison on both sides (Python's capture
	// used its own wall-clock value at capture time; this fixes Go's to a
	// stable, assertable one instead of chasing an inherently unequal field).
	fixedNow := time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)
	// CHAOS-5274: the seed rows below and the executor's native write share
	// user_metrics_daily's dedup key (org_id, repo_id, author_email, day) for
	// alice and bob -- git-backed identities keep their OWN repo_id, per
	// writeUserMetrics' comment. If both writes carried the SAME computed_at,
	// `ORDER BY computed_at DESC LIMIT 1 BY` would have no defined winner
	// between them, and this test observed exactly that nondeterminism on a
	// real CI run: it read back the stale seed generation instead of the
	// native one, with every merge-only field (PRsOpened, WorkItemsCompleted,
	// DeliveryUnits, CycleP50/90Hours, the team_map-overridden TeamID) coming
	// back as the seed row's own zero/literal value. seedNow is strictly
	// EARLIER than the executor's clock so the native write is always the
	// later, winning generation -- see the assertComputedAtIsExecutorGeneration
	// checks below, which fail loudly if a future change reintroduces a tie
	// instead of silently reading whichever generation ClickHouse happens to
	// keep on top.
	seedNow := fixedNow.Add(-1 * time.Hour)

	repoAlice := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	repoBob := uuid.MustParse("22222222-2222-4222-8222-222222222222")

	// alice: git-backed AND has work items. Values match
	// capture_ic_finalize_golden.py's git_alice/wi_alice literally.
	if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
        (repo_id, day, author_email, identity_id, team_id, team_name, loc_added, loc_deleted,
         loc_touched, prs_authored, prs_merged, avg_commit_size_loc, commits_count,
         files_changed, large_commits_count, avg_pr_cycle_hours, median_pr_cycle_hours,
         pr_cycle_p75_hours, pr_cycle_p90_hours, prs_with_first_review,
         pr_first_review_p50_hours, pr_first_review_p90_hours, pr_review_time_p50_hours,
         pr_pickup_time_p50_hours, reviews_given, changes_requested_given, reviews_received,
         review_reciprocity, pr_interruption_load, context_spread_count, review_request_load,
         active_hours, weekend_days, computed_at, org_id)
        VALUES (?, ?, 'alice@example.com', 'alice@example.com', 'team-git-a', 'Team Git A',
                400, 150, 550, 4, 3, 45.8, 12, 9, 1, 18.5, 15.0, 22.0, 30.0, 3,
                2.5, 6.0, 1.2, 0.8, 5, 1, 4, 1.25, 2, 3, 2, 6.5, 0, ?, ?)`,
		repoAlice, day, seedNow, orgID); err != nil {
		t.Fatal(err)
	}
	// bob: git-backed ONLY, zero-valued review/collaboration/burnout fields
	// (matching git_bob's field defaults in the capture script).
	if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
        (repo_id, day, author_email, identity_id, team_id, team_name, loc_added, loc_deleted,
         loc_touched, prs_authored, prs_merged, avg_commit_size_loc, commits_count,
         files_changed, large_commits_count, avg_pr_cycle_hours, median_pr_cycle_hours,
         computed_at, org_id)
        VALUES (?, ?, 'bob@example.com', 'bob@example.com', 'team-git-b', 'Team Git B',
                90, 20, 110, 1, 1, 22.0, 5, 3, 0, 9.0, 9.0, ?, ?)`,
		repoBob, day, seedNow, orgID); err != nil {
		t.Fatal(err)
	}

	// Work-item rows: alice (aggregation branch) and carol (work-item-only,
	// no git row exists for her at all).
	if err := conn.Exec(ctx, `INSERT INTO work_item_user_metrics_daily
        (day, provider, work_scope_id, user_identity, team_id, team_name,
         items_started, items_completed, wip_count_end_of_day,
         cycle_time_p50_hours, cycle_time_p90_hours, computed_at, org_id)
        VALUES (?, 'jira', 'scope-alice', 'alice@example.com', 'team-git-a', 'Team Git A',
                6, 4, 2, 10.0, 20.0, ?, ?)`,
		day, seedNow, orgID); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO work_item_user_metrics_daily
        (day, provider, work_scope_id, user_identity, team_id, team_name,
         items_started, items_completed, wip_count_end_of_day,
         cycle_time_p50_hours, cycle_time_p90_hours, computed_at, org_id)
        VALUES (?, 'linear', 'scope-carol', 'carol@example.com', 'team-wi-c', 'Team WI C',
                3, 2, 1, 14.0, 28.0, ?, ?)`,
		day, seedNow, orgID); err != nil {
		t.Fatal(err)
	}

	executor := NewExecutor(conn)
	executor.now = func() time.Time { return fixedNow }
	// Mirrors capture_ic_finalize_golden.py's TEAM_MAP exactly -- ONE map for
	// all three identities, used for both the metrics-merge override and the
	// landscape fallback, matching computeForDay's single-TeamResolver
	// design (there is no way in production for these two steps to see
	// different team assignments for the same identity).
	teamMap := map[string]string{
		"alice@example.com": "team-shared-ab",
		"bob@example.com":   "team-shared-ab",
		"carol@example.com": "team-wi-c",
	}
	resolveTeam := TeamResolver(func(identity string) (string, bool) {
		mapped, ok := teamMap[identity]
		return mapped, ok
	})

	if _, err := executor.computeForDay(ctx, orgID, day, resolveTeam); err != nil {
		t.Fatalf("computeForDay: %v", err)
	}

	gotUserMetrics := readBackUserMetrics(ctx, t, conn, orgID, day)
	gotLandscape := readBackLandscape(ctx, t, conn, orgID, day)

	// CHAOS-5274: assert the dedup read actually resolved to the NATIVE
	// generation (computed_at == fixedNow, the executor's clock) rather than
	// the stale seed generation (computed_at == seedNow) surviving a tied
	// ORDER BY. This must fail LOUDLY if a future change reintroduces a tie,
	// instead of silently reading whichever generation ClickHouse happens to
	// keep on top -- exactly the failure mode this test hit on a real CI run.
	assertComputedAtIsExecutorGeneration(t, gotUserMetrics, fixedNow)

	assertUserMetricsMatchGolden(t, golden.UserMetrics, gotUserMetrics, orgID)
	assertLandscapeMatchesGolden(t, golden.LandscapeRolling, gotLandscape)
}

// assertComputedAtIsExecutorGeneration is CHAOS-5274's regression guard: it
// fails loudly, with the exact wrong timestamp, if the dedup read ever
// resolves to the seed generation instead of the native write -- rather than
// letting that show up only as a confusing cascade of zero-valued/reverted
// fields in assertUserMetricsMatchGolden below.
func assertComputedAtIsExecutorGeneration(
	t *testing.T, got map[string]gotUserMetricRow, wantComputedAt time.Time,
) {
	t.Helper()
	for email, row := range got {
		if !row.ComputedAt.Equal(wantComputedAt) {
			t.Errorf(
				"%s: ComputedAt = %s, want %s (native executor generation) -- "+
					"the dedup read resolved to a DIFFERENT generation than the one "+
					"computeForDay just wrote, which means ORDER BY computed_at DESC "+
					"LIMIT 1 BY had no defined winner (a tie) or picked the seed row",
				email, row.ComputedAt, wantComputedAt,
			)
		}
	}
}

// --- golden file shape (mirrors capture_ic_finalize_golden.py's dump) ---

type goldenUserMetric struct {
	RepoID             string  `json:"repo_id"`
	AuthorEmail        string  `json:"author_email"`
	CommitsCount       int64   `json:"commits_count"`
	LOCAdded           int64   `json:"loc_added"`
	LOCDeleted         int64   `json:"loc_deleted"`
	FilesChanged       int64   `json:"files_changed"`
	LargeCommitsCount  int64   `json:"large_commits_count"`
	AvgCommitSizeLOC   float64 `json:"avg_commit_size_loc"`
	PRsAuthored        int64   `json:"prs_authored"`
	PRsMerged          int64   `json:"prs_merged"`
	AvgPRCycleHours    float64 `json:"avg_pr_cycle_hours"`
	MedianPRCycleHours float64 `json:"median_pr_cycle_hours"`
	TeamID             *string `json:"team_id"`
	TeamName           *string `json:"team_name"`
	IdentityID         string  `json:"identity_id"`
	LOCTouched         int64   `json:"loc_touched"`
	PRsOpened          int64   `json:"prs_opened"`
	WorkItemsCompleted int64   `json:"work_items_completed"`
	WorkItemsActive    int64   `json:"work_items_active"`
	DeliveryUnits      int64   `json:"delivery_units"`
	CycleP50Hours      float64 `json:"cycle_p50_hours"`
	CycleP90Hours      float64 `json:"cycle_p90_hours"`
}

type goldenLandscapeRow struct {
	IdentityID       string  `json:"identity_id"`
	TeamID           *string `json:"team_id"`
	MapName          string  `json:"map_name"`
	XRaw             float64 `json:"x_raw"`
	YRaw             float64 `json:"y_raw"`
	XNorm            float64 `json:"x_norm"`
	YNorm            float64 `json:"y_norm"`
	ChurnLOC30d      int64   `json:"churn_loc_30d"`
	DeliveryUnits30d int64   `json:"delivery_units_30d"`
	CycleP5030dHours float64 `json:"cycle_p50_30d_hours"`
	WIPMax30d        int64   `json:"wip_max_30d"`
}

type parityGolden struct {
	UserMetrics      []goldenUserMetric   `json:"user_metrics"`
	LandscapeRolling []goldenLandscapeRow `json:"landscape_rolling"`
}

func loadParityGolden(t *testing.T) parityGolden {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "parity_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden parityGolden
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	return golden
}

// --- Go-side readback ---

type gotUserMetricRow struct {
	RepoID             uuid.UUID
	AuthorEmail        string
	CommitsCount       uint32
	LOCAdded           uint32
	LOCDeleted         uint32
	FilesChanged       uint32
	LargeCommitsCount  uint32
	AvgCommitSizeLOC   float64
	PRsAuthored        uint32
	PRsMerged          uint32
	AvgPRCycleHours    float64
	MedianPRCycleHours float64
	TeamID             *string
	TeamName           *string
	IdentityID         string
	LOCTouched         uint32
	PRsOpened          uint32
	WorkItemsCompleted uint32
	WorkItemsActive    uint32
	DeliveryUnits      uint32
	CycleP50Hours      float64
	CycleP90Hours      float64
	ComputedAt         time.Time
}

func readBackUserMetrics(
	ctx context.Context, t *testing.T, conn Conn, orgID string, day time.Time,
) map[string]gotUserMetricRow {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT author_email, repo_id, identity_id, team_id, team_name,
       loc_added, loc_deleted, loc_touched, commits_count, files_changed,
       large_commits_count, avg_commit_size_loc, prs_authored, prs_merged,
       avg_pr_cycle_hours, median_pr_cycle_hours, prs_opened,
       work_items_completed, work_items_active, delivery_units,
       cycle_p50_hours, cycle_p90_hours, computed_at
FROM (
    SELECT * FROM user_metrics_daily
    ORDER BY computed_at DESC
    LIMIT 1 BY org_id, repo_id, author_email, day
) AS user_metrics_daily
WHERE day = ? AND org_id = ?`, day.Format("2006-01-02"), orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]gotUserMetricRow{}
	for rows.Next() {
		var row gotUserMetricRow
		var teamID, teamName *string
		if err := rows.Scan(&row.AuthorEmail, &row.RepoID, &row.IdentityID, &teamID, &teamName,
			&row.LOCAdded, &row.LOCDeleted, &row.LOCTouched, &row.CommitsCount, &row.FilesChanged,
			&row.LargeCommitsCount, &row.AvgCommitSizeLOC, &row.PRsAuthored, &row.PRsMerged,
			&row.AvgPRCycleHours, &row.MedianPRCycleHours, &row.PRsOpened,
			&row.WorkItemsCompleted, &row.WorkItemsActive, &row.DeliveryUnits,
			&row.CycleP50Hours, &row.CycleP90Hours, &row.ComputedAt,
		); err != nil {
			t.Fatal(err)
		}
		row.TeamID, row.TeamName = teamID, teamName
		out[row.AuthorEmail] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

type gotLandscapeRow struct {
	IdentityID       string
	TeamID           *string
	MapName          string
	XRaw             float64
	YRaw             float64
	XNorm            float64
	YNorm            float64
	ChurnLOC30d      uint64
	DeliveryUnits30d uint32
	CycleP5030dHours float64
	WIPMax30d        uint32
}

func readBackLandscape(
	ctx context.Context, t *testing.T, conn Conn, orgID string, day time.Time,
) map[string]gotLandscapeRow {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT identity_id, team_id, map_name, x_raw, y_raw, x_norm, y_norm,
       churn_loc_30d, delivery_units_30d, cycle_p50_30d_hours, wip_max_30d
FROM ic_landscape_rolling_30d
WHERE as_of_day = ? AND org_id = ?`, day.Format("2006-01-02"), orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]gotLandscapeRow{}
	for rows.Next() {
		var row gotLandscapeRow
		var teamID *string
		if err := rows.Scan(&row.IdentityID, &teamID, &row.MapName, &row.XRaw, &row.YRaw,
			&row.XNorm, &row.YNorm, &row.ChurnLOC30d, &row.DeliveryUnits30d,
			&row.CycleP5030dHours, &row.WIPMax30d,
		); err != nil {
			t.Fatal(err)
		}
		row.TeamID = teamID
		out[row.IdentityID+"\x1f"+row.MapName] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// --- comparisons ---

// assertUserMetricsMatchGolden compares every field EXCEPT one documented,
// pre-existing divergence (team-lead's Q1 ruling, #2241/#2243): repo_id for
// carol (work-item-only). Python's `uuid.uuid4()` is random per capture,
// Go's SynthesizedRepoID is a deterministic UUIDv5 -- the two CANNOT be
// equal to each other by construction. Asserted separately against
// SynthesizedRepoID's own known value instead of the golden's (meaningless,
// unreproducible) literal.
//
// team_id compares normally for all three identities (the corpus gives
// carol a real team_map override, "team-wi-c", precisely so this field
// isn't a second divergence to special-case, unlike team_name below).
//
// team_name for carol is a SECOND documented divergence: compute_ic.py's
// team override touches team_id only, never team_name, so her synthesized
// base record's team_name stays Python's None on that side -- but Go's
// GitUserMetric zero value (no override mechanism exists for TeamName at
// all) writes as a literal empty string, not SQL NULL, on this side.
// Neither side is wrong; a work-item-only identity simply has no
// git-sourced team_name to carry on either implementation, expressed in
// each language's own idiom for "absent".
func assertUserMetricsMatchGolden(
	t *testing.T, golden []goldenUserMetric, got map[string]gotUserMetricRow, orgID string,
) {
	t.Helper()
	if len(golden) != len(got) {
		t.Fatalf("row count: golden=%d got=%d (got=%+v)", len(golden), len(got), got)
	}
	for _, want := range golden {
		row, ok := got[want.AuthorEmail]
		if !ok {
			t.Fatalf("%s: missing from Go's output entirely", want.AuthorEmail)
		}
		if want.AuthorEmail != "carol@example.com" {
			if row.RepoID.String() != want.RepoID {
				t.Errorf("%s: RepoID = %s, want %s", want.AuthorEmail, row.RepoID, want.RepoID)
			}
		} else {
			// Carol: SynthesizedRepoID divergence documented above.
			wantSynthesized := SynthesizedRepoID(orgID, "carol@example.com")
			if row.RepoID != wantSynthesized {
				t.Errorf("carol@example.com: RepoID = %s, want SynthesizedRepoID(...)=%s",
					row.RepoID, wantSynthesized)
			}
		}
		assertOptionalStringEqual(t, want.AuthorEmail, "TeamID", row.TeamID, want.TeamID)
		if want.AuthorEmail != "carol@example.com" {
			assertOptionalStringEqual(t, want.AuthorEmail, "TeamName", row.TeamName, want.TeamName)
		} else if row.TeamName != nil && *row.TeamName != "" {
			// team_name null-vs-empty-string divergence documented above.
			t.Errorf("carol@example.com: TeamName = %v, want nil or empty (Python's None)", *row.TeamName)
		}
		if int64(row.CommitsCount) != want.CommitsCount {
			t.Errorf("%s: CommitsCount = %d, want %d", want.AuthorEmail, row.CommitsCount, want.CommitsCount)
		}
		if int64(row.LOCAdded) != want.LOCAdded {
			t.Errorf("%s: LOCAdded = %d, want %d", want.AuthorEmail, row.LOCAdded, want.LOCAdded)
		}
		if int64(row.LOCDeleted) != want.LOCDeleted {
			t.Errorf("%s: LOCDeleted = %d, want %d", want.AuthorEmail, row.LOCDeleted, want.LOCDeleted)
		}
		if int64(row.LOCTouched) != want.LOCTouched {
			t.Errorf("%s: LOCTouched = %d, want %d", want.AuthorEmail, row.LOCTouched, want.LOCTouched)
		}
		if int64(row.FilesChanged) != want.FilesChanged {
			t.Errorf("%s: FilesChanged = %d, want %d", want.AuthorEmail, row.FilesChanged, want.FilesChanged)
		}
		if int64(row.LargeCommitsCount) != want.LargeCommitsCount {
			t.Errorf("%s: LargeCommitsCount = %d, want %d", want.AuthorEmail, row.LargeCommitsCount, want.LargeCommitsCount)
		}
		assertFloatEqual(t, want.AuthorEmail, "AvgCommitSizeLOC", row.AvgCommitSizeLOC, want.AvgCommitSizeLOC)
		if int64(row.PRsAuthored) != want.PRsAuthored {
			t.Errorf("%s: PRsAuthored = %d, want %d", want.AuthorEmail, row.PRsAuthored, want.PRsAuthored)
		}
		if int64(row.PRsMerged) != want.PRsMerged {
			t.Errorf("%s: PRsMerged = %d, want %d", want.AuthorEmail, row.PRsMerged, want.PRsMerged)
		}
		assertFloatEqual(t, want.AuthorEmail, "AvgPRCycleHours", row.AvgPRCycleHours, want.AvgPRCycleHours)
		assertFloatEqual(t, want.AuthorEmail, "MedianPRCycleHours", row.MedianPRCycleHours, want.MedianPRCycleHours)
		if row.IdentityID != want.IdentityID {
			t.Errorf("%s: IdentityID = %s, want %s", want.AuthorEmail, row.IdentityID, want.IdentityID)
		}
		if int64(row.PRsOpened) != want.PRsOpened {
			t.Errorf("%s: PRsOpened = %d, want %d", want.AuthorEmail, row.PRsOpened, want.PRsOpened)
		}
		if int64(row.WorkItemsCompleted) != want.WorkItemsCompleted {
			t.Errorf("%s: WorkItemsCompleted = %d, want %d", want.AuthorEmail, row.WorkItemsCompleted, want.WorkItemsCompleted)
		}
		if int64(row.WorkItemsActive) != want.WorkItemsActive {
			t.Errorf("%s: WorkItemsActive = %d, want %d", want.AuthorEmail, row.WorkItemsActive, want.WorkItemsActive)
		}
		if int64(row.DeliveryUnits) != want.DeliveryUnits {
			t.Errorf("%s: DeliveryUnits = %d, want %d", want.AuthorEmail, row.DeliveryUnits, want.DeliveryUnits)
		}
		assertFloatEqual(t, want.AuthorEmail, "CycleP50Hours", row.CycleP50Hours, want.CycleP50Hours)
		assertFloatEqual(t, want.AuthorEmail, "CycleP90Hours", row.CycleP90Hours, want.CycleP90Hours)
	}
}

func assertLandscapeMatchesGolden(
	t *testing.T, golden []goldenLandscapeRow, got map[string]gotLandscapeRow,
) {
	t.Helper()
	if len(golden) != len(got) {
		gotKeys := make([]string, 0, len(got))
		for key := range got {
			gotKeys = append(gotKeys, key)
		}
		sort.Strings(gotKeys)
		t.Fatalf("row count: golden=%d got=%d (got keys=%v)", len(golden), len(got), gotKeys)
	}
	for _, want := range golden {
		key := want.IdentityID + "\x1f" + want.MapName
		row, ok := got[key]
		if !ok {
			t.Fatalf("%s: missing from Go's output entirely", key)
		}
		assertOptionalStringEqual(t, key, "TeamID", row.TeamID, want.TeamID)
		assertFloatEqual(t, key, "XRaw", row.XRaw, want.XRaw)
		assertFloatEqual(t, key, "YRaw", row.YRaw, want.YRaw)
		assertFloatEqual(t, key, "XNorm", row.XNorm, want.XNorm)
		assertFloatEqual(t, key, "YNorm", row.YNorm, want.YNorm)
		if int64(row.ChurnLOC30d) != want.ChurnLOC30d {
			t.Errorf("%s: ChurnLOC30d = %d, want %d", key, row.ChurnLOC30d, want.ChurnLOC30d)
		}
		if int64(row.DeliveryUnits30d) != want.DeliveryUnits30d {
			t.Errorf("%s: DeliveryUnits30d = %d, want %d", key, row.DeliveryUnits30d, want.DeliveryUnits30d)
		}
		assertFloatEqual(t, key, "CycleP5030dHours", row.CycleP5030dHours, want.CycleP5030dHours)
		if int64(row.WIPMax30d) != want.WIPMax30d {
			t.Errorf("%s: WIPMax30d = %d, want %d", key, row.WIPMax30d, want.WIPMax30d)
		}
	}
}

func assertFloatEqual(t *testing.T, subject, field string, got, want float64) {
	t.Helper()
	const epsilon = 1e-9
	if math.Abs(got-want) > epsilon {
		t.Errorf("%s: %s = %v, want %v", subject, field, got, want)
	}
}

func assertOptionalStringEqual(t *testing.T, subject, field string, got, want *string) {
	t.Helper()
	switch {
	case got == nil && want == nil:
		return
	case got == nil || want == nil:
		t.Errorf("%s: %s = %v, want %v", subject, field, derefOrNil(got), derefOrNil(want))
	case *got != *want:
		t.Errorf("%s: %s = %q, want %q", subject, field, *got, *want)
	}
}

func derefOrNil(value *string) string {
	if value == nil {
		return "<nil>"
	}
	return *value
}
