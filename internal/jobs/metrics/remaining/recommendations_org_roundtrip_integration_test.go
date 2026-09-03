//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestOneOrgSurvivesDiscoveryComputeAndWrite exercises the whole org-level path
// against a real ClickHouse: discover the teams, evaluate every rule for each,
// write the batch, and read it back.
//
// The loader parity test upstream proves the SNAPSHOT matches Python. This
// proves the three steps built on top of it -- discovery, the per-team loop and
// the stamped write -- which the snapshot comparison cannot see at all: a
// correct snapshot that is then written under the wrong stamp, or for only one
// of two teams, passes every assertion in that test.
func TestOneOrgSurvivesDiscoveryComputeAndWrite(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)

	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	conn := openLoaderClickHouse(t, ctx, dsn)
	seedLoaderFixture(t, ctx, conn)

	executor, err := NewRecommendationsExecutor(ctx, conn, loaderOrgID)
	if err != nil {
		t.Fatalf("construct executor: %v", err)
	}

	// Discovery first, on its own, because a silently empty result would make
	// every later assertion vacuous -- ComputeOrg treats "no teams" as a
	// successful no-op, so a broken query reads as a healthy quiet org.
	teamIDs, err := executor.DiscoverTeamIDs(ctx, loaderOrgID)
	if err != nil {
		t.Fatalf("discover teams: %v", err)
	}
	if len(teamIDs) == 0 {
		t.Fatal("discovery returned no teams for a seeded org — every assertion " +
			"below would then pass against an empty run")
	}
	foundAlpha, foundBeta := false, false
	for _, teamID := range teamIDs {
		switch teamID {
		case loaderTeamA:
			foundAlpha = true
		case loaderTeamB:
			foundBeta = true
		}
	}
	if !foundAlpha || !foundBeta {
		t.Fatalf("discovery returned %v, missing one of the two seeded teams "+
			"(%s, %s)", teamIDs, loaderTeamA, loaderTeamB)
	}

	// The as_of path: `now` is a pure function of the finalized day, so this
	// exact value would be written as computed_at on every re-run if the sink
	// did not re-stamp it.
	asOf := mustDate(t, "2026-08-31")
	now, _ := EvaluationInstant(&asOf, nil)

	before := time.Now().UTC().Add(-time.Second)
	outcome, err := executor.ComputeOrg(ctx, loaderOrgID, now, 30, "v1", "")
	if err != nil {
		t.Fatalf("compute org: %v", err)
	}
	after := time.Now().UTC().Add(time.Second)

	if outcome.FailedTeams != 0 {
		t.Errorf("%d team(s) failed against a clean fixture", outcome.FailedTeams)
	}
	if outcome.RowsWritten == 0 {
		t.Fatal("no rows written — a run that evaluates every rule for two teams " +
			"must emit tombstones even when nothing fires")
	}
	if outcome.Teams != len(teamIDs) {
		t.Errorf("outcome reports %d teams, discovery found %d",
			outcome.Teams, len(teamIDs))
	}

	// Read back what actually landed, rather than trusting the returned count.
	// EVERY column, not the five the assertions below happen to need. Reading a
	// subset makes this test blind to exactly the corruption it is best placed
	// to catch: five of the thirteen columns are adjacent strings, so a
	// crossed title/rationale is invisible unless the text itself comes back.
	rows, err := conn.Query(ctx, `
        SELECT team_id, org_id, rule_id, rule_version,
               window_start, window_end, fired, severity,
               title, rationale, success_criterion, evidence_json, computed_at
        FROM recommendations_daily
        WHERE org_id = ?`, loaderOrgID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer func() { _ = rows.Close() }()

	stamps := map[time.Time]int{}
	windowEnds := map[time.Time]int{}
	perTeam := map[string]int{}
	tombstonesChecked := 0
	total := 0
	for rows.Next() {
		var teamID, orgID, ruleID, ruleVersion string
		var severity, title, rationale, successCriterion, evidenceJSON string
		var fired bool
		var windowStart, windowEnd, computedAt time.Time
		if err := rows.Scan(
			&teamID, &orgID, &ruleID, &ruleVersion,
			&windowStart, &windowEnd, &fired, &severity,
			&title, &rationale, &successCriterion, &evidenceJSON, &computedAt,
		); err != nil {
			t.Fatalf("scan: %v", err)
		}

		// Cross-column corruption is silent by construction: ClickHouse binds
		// by position, and these five are all String. The registry is the
		// oracle -- a row must carry ITS OWN rule's text, not a neighbour's.
		if definition, ok := expectedTombstoneText(ruleID); ok && !fired {
			tombstonesChecked++
			if title != definition.title {
				t.Errorf("tombstone for %s came back with title %q, want %q — a "+
					"crossed append writes another column's text with no error",
					ruleID, title, definition.title)
			}
			if severity != definition.severity {
				t.Errorf("tombstone for %s came back with severity %q, want %q",
					ruleID, severity, definition.severity)
			}
			if successCriterion != definition.successCriterion {
				t.Errorf("tombstone for %s came back with success_criterion %q, want %q",
					ruleID, successCriterion, definition.successCriterion)
			}
			if title == rationale || title == successCriterion {
				t.Errorf("tombstone for %s has identical text in two columns "+
					"(title=%q rationale=%q success_criterion=%q) — the signature "+
					"of a swapped append among the adjacent strings",
					ruleID, title, rationale, successCriterion)
			}
		}
		if orgID != loaderOrgID {
			t.Errorf("row carries org_id %q, want %q", orgID, loaderOrgID)
		}
		if evidenceJSON == "" {
			t.Errorf("rule %s wrote an empty evidence_json; a tombstone writes "+
				"\"[]\", never the empty string", ruleID)
		}
		if !windowStart.Before(windowEnd) {
			t.Errorf("window_start %s is not before window_end %s",
				windowStart, windowEnd)
		}

		stamps[computedAt]++
		windowEnds[windowEnd]++
		perTeam[teamID]++
		total++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	// The tombstone assertions above are guarded by !fired, so a writer that
	// hard-coded fired=true would skip every one of them and leave this test
	// green having checked nothing. Requiring at least one observed tombstone
	// is what makes the guard's own satisfaction an assertion.
	if tombstonesChecked == 0 {
		t.Error("no tombstone rows were observed, so the registry-text assertions " +
			"never ran — a writer that always reports fired=true would pass this " +
			"test having checked nothing")
	}

	if total != outcome.RowsWritten {
		t.Errorf("read back %d rows, the run reported writing %d", total, outcome.RowsWritten)
	}
	if len(perTeam) != len(teamIDs) {
		t.Errorf("rows landed for %d teams, want %d — a per-team failure that is "+
			"tolerated must still be visible as missing rows", len(perTeam), len(teamIDs))
	}

	// ONE generation for the whole run. Per-row stamps would let a reader doing
	// argMax per rule observe one rule's new state beside another's old one.
	if len(stamps) != 1 {
		t.Errorf("the run wrote %d distinct computed_at values; a scheduled run "+
			"must replace the org's rule state as a single generation", len(stamps))
	}

	// And that generation is the WRITE time, not the engine instant. This is
	// the assertion the loader parity test structurally cannot make.
	for stamp := range stamps {
		if stamp.Equal(now) {
			t.Errorf("computed_at is the engine instant %s. On the as_of path that "+
				"value is constant across re-runs of the same finalized day, so two "+
				"runs would be indistinguishable to argMax(fired, computed_at) and "+
				"to ReplacingMergeTree(computed_at) — a recovered signal might never "+
				"clear (CHAOS-2398)", now)
		}
		if stamp.Before(before) || stamp.After(after) {
			t.Errorf("computed_at %s is outside the run's wall-clock window [%s, %s]",
				stamp, before, after)
		}
	}

	// window_end, by contrast, MUST stay a pure function of as_of — it is the
	// key readers group by, and a wall-clock value there would scatter one
	// day's state across several windows.
	if len(windowEnds) != 1 {
		t.Errorf("the run wrote %d distinct window_end values; it must be one "+
			"function of as_of", len(windowEnds))
	}
	for windowEnd := range windowEnds {
		if !windowEnd.Equal(now) {
			t.Errorf("window_end is %s, want the anchored instant %s", windowEnd, now)
		}
	}
}

// TestASecondRunSupersedesTheFirstOnUnchangedData is the property CHAOS-2398
// exists for, and it cannot be observed in a single run.
//
// Two runs of the SAME finalized day carry an identical engine instant. If that
// value were persisted, the two generations would be indistinguishable and the
// winner undefined. Running twice and requiring the second to be strictly newer
// is the only assertion that reaches it.
func TestASecondRunSupersedesTheFirstOnUnchangedData(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)

	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	conn := openLoaderClickHouse(t, ctx, dsn)
	seedLoaderFixture(t, ctx, conn)

	executor, err := NewRecommendationsExecutor(ctx, conn, loaderOrgID)
	if err != nil {
		t.Fatalf("construct executor: %v", err)
	}

	asOf := mustDate(t, "2026-08-31")
	now, _ := EvaluationInstant(&asOf, nil)

	if _, err := executor.ComputeOrg(ctx, loaderOrgID, now, 30, "v1", ""); err != nil {
		t.Fatalf("first run: %v", err)
	}
	firstStamps := distinctStamps(t, ctx, conn)
	if len(firstStamps) != 1 {
		t.Fatalf("first run wrote %d generations, want 1", len(firstStamps))
	}

	// CORRECTION: an earlier version of this comment said ClickHouse DateTime is
	// second-granular. The column is DateTime64(3, 'UTC') (migration 039), i.e.
	// MILLISECOND. The sleep is still needed and still far larger than it has to
	// be, but the reason matters: anyone trimming it to "just over a second"
	// would be reasoning from the wrong unit.
	//
	// AND THE LIMIT THIS TEST DOES NOT REACH: at millisecond resolution, two
	// retries inside the same millisecond store EQUAL versions, and neither
	// argMax(fired, computed_at) nor ReplacingMergeTree(computed_at) can then
	// pick the later one. So "the most recent write always wins" holds only for
	// runs separated by >=1ms. That limit is INHERITED FROM THE REFERENCE, which
	// stamps the same way, so it is not a parity defect and is not fixed here --
	// but this test deliberately steps over it rather than probing it, and
	// saying so is the difference between a known limit and an unnoticed one.
	time.Sleep(2 * time.Second)

	if _, err := executor.ComputeOrg(ctx, loaderOrgID, now, 30, "v1", ""); err != nil {
		t.Fatalf("second run: %v", err)
	}
	secondStamps := distinctStamps(t, ctx, conn)
	if len(secondStamps) != 2 {
		t.Fatalf("after two runs the table holds %d distinct computed_at values, "+
			"want 2 — identical stamps leave the winner undefined for both "+
			"argMax and ReplacingMergeTree", len(secondStamps))
	}

	// The later run must be strictly newer, so "most recent write wins" is
	// decidable rather than arbitrary.
	if !secondStamps[1].After(secondStamps[0]) {
		t.Errorf("the second run's generation %s is not strictly newer than the "+
			"first's %s", secondStamps[1], secondStamps[0])
	}
}

// distinctStamps returns every computed_at generation currently in the table,
// ascending. Sorted server-side so "the later run" is a fact about the data
// rather than about row arrival order.
func distinctStamps(t *testing.T, ctx context.Context, conn driver.Conn) []time.Time {
	t.Helper()
	rows, err := conn.Query(ctx, `
        SELECT DISTINCT computed_at
        FROM recommendations_daily
        WHERE org_id = ?
        ORDER BY computed_at ASC`, loaderOrgID)
	if err != nil {
		t.Fatalf("read generations: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var stamps []time.Time
	for rows.Next() {
		var stamp time.Time
		if err := rows.Scan(&stamp); err != nil {
			t.Fatalf("scan generation: %v", err)
		}
		stamps = append(stamps, stamp)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate generations: %v", err)
	}
	return stamps
}

// expectedTombstoneText reads the REGISTRY rather than restating its strings.
//
// Restating them would create a second copy free to drift from the one under
// test, and a test that agrees with its own copy of the answer proves nothing.
//
// Only fired=false rows are checked here. That is the tombstone asymmetry, not
// an omission: a tombstone takes title/severity/success_criterion from the
// registry, while a FIRED row takes them from the evaluator, which carries
// different strings by design.
func expectedTombstoneText(ruleID string) (ruleDefinition, bool) {
	definition, ok := ruleRegistry[ruleID]
	return definition, ok
}
