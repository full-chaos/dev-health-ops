//go:build integration

package providersync

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// A second walk of the same run (a re-walk by a backfill row) over a report the planner drops as an aggregate of its parts: the
// route is collected and its effects written twice, at two normalizedAt, into a migrated ClickHouse. The newest rows after the
// second walk hold the same keys, the dropped report's rows stay retracted with zero measures, no key is added, and every
// served column is equal to the state after the first walk; only last_synced moves to the second walk.

func rewalkTwiceEffects(t *testing.T, normalizedAt time.Time, artifacts ...rowsArtifact) []EffectBatch {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(&githubTestsRowsDoer{t: t, runs: 1, artifacts: artifacts}))
	client.Metrics = providerfoundation.NewMetrics()
	claim := nativeTestClaim("github", "tests")
	var effects []EffectBatch
	resume := ""
	for pass := 0; ; pass++ {
		if pass > 500 {
			t.Fatal("continuation walk never reached a final emission")
		}
		emitted, last, finalSeen := 0, resume, false
		err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				last = emission.CursorAfter
				effects = append(effects, emission.Batch.Effects...)
				if emission.Final {
					finalSeen = true
					return nil
				}
				emitted++
				if emitted >= 8 {
					return errGitHubTestsWalkContinuation
				}
				return nil
			})
		if finalSeen {
			if err != nil {
				t.Fatalf("final emission returned err=%v", err)
			}
			return effects
		}
		if !errors.Is(err, errGitHubTestsWalkContinuation) {
			t.Fatalf("pass %d err=%v, want a continuation yield", pass, err)
		}
		resume = last
	}
}

type rewalkTwiceState struct {
	suites, cases   map[string]string
	retractedSuites int
	syncedAt        map[string]int
}

// rewalkTwiceRead reads the newest rows (FINAL) of the run: every served column per key, and the last_synced values apart.
func rewalkTwiceRead(t *testing.T, ctx context.Context, sink TestOpsClickHouseEffects) rewalkTwiceState {
	t.Helper()
	state := rewalkTwiceState{suites: map[string]string{}, cases: map[string]string{}, syncedAt: map[string]int{}}
	rows, err := sink.Conn.Query(ctx, `SELECT run_id, suite_id, suite_name, toString(report_slot), toUInt32(run_attempt), toUInt8(retracted),
  toUInt32(total_count), toUInt32(passed_count), toUInt32(failed_count), toUInt32(skipped_count), toUInt32(error_count),
  toUInt32(quarantined_count), toUInt32(retried_count), ifNull(toString(duration_seconds), ''), ifNull(toString(started_at), ''),
  arrayStringConcat(legacy_suite_ids, ','), toString(last_synced)
FROM test_suite_results FINAL WHERE org_id = 'org-acme' ORDER BY run_id, suite_id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var runID, suiteID, name, slot, duration, started, legacy, synced string
		var attempt uint32
		var retracted uint8
		var total, passed, failed, skipped, errored, quarantined, retried uint32
		if err := rows.Scan(&runID, &suiteID, &name, &slot, &attempt, &retracted, &total, &passed, &failed, &skipped, &errored, &quarantined,
			&retried, &duration, &started, &legacy, &synced); err != nil {
			t.Fatal(err)
		}
		key := runID + "|" + suiteID
		state.suites[key] = fmt.Sprint(name, slot, attempt, retracted, total, passed, failed, skipped, errored, quarantined, retried, duration, started, legacy)
		if retracted == 1 {
			state.retractedSuites++
			if total+passed+failed+skipped+errored+quarantined+retried != 0 || duration != "" {
				t.Errorf("retracted suite row %s holds measures %s", key, state.suites[key])
			}
		}
		state.syncedAt[synced]++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	caseRows, err := sink.Conn.Query(ctx, `SELECT run_id, suite_id, case_id, toString(case_name), toString(status), toUInt8(retracted),
  ifNull(toString(duration_seconds), ''), toString(last_synced)
FROM test_case_results FINAL WHERE org_id = 'org-acme' ORDER BY run_id, suite_id, case_id`)
	if err != nil {
		t.Fatal(err)
	}
	for caseRows.Next() {
		var runID, suiteID, caseID, name, status, duration, synced string
		var retracted uint8
		if err := caseRows.Scan(&runID, &suiteID, &caseID, &name, &status, &retracted, &duration, &synced); err != nil {
			t.Fatal(err)
		}
		state.cases[runID+"|"+suiteID+"|"+caseID] = fmt.Sprint(name, status, retracted, duration)
		state.syncedAt[synced]++
	}
	if err := caseRows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = caseRows.Close()
	return state
}

func TestGitHubTestsRouteReWalkTwiceKeepsTheNewestRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	sink := newTestOpsMigratedSink(t, ctx)
	claim := nativeTestClaim("github", "tests")
	artifacts := retiredFxListing("3", "4", "6")
	first, second := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), time.Date(2026, 8, 15, 9, 30, 0, 0, time.UTC)

	write := func(label string, effects []EffectBatch) {
		t.Helper()
		destinations := map[string]int{}
		for _, effect := range effects {
			if err := sink.WriteEffect(ctx, claim, effect); err != nil {
				t.Fatalf("%s: write %s: %v", label, effect.Destination, err)
			}
			destinations[effect.Destination] += len(effect.Rows)
		}
		t.Logf("REWALK %s wrote %v", label, destinations)
	}
	write("walk 1", rewalkTwiceEffects(t, first, artifacts...))
	one := rewalkTwiceRead(t, ctx, sink)
	write("walk 2", rewalkTwiceEffects(t, second, artifacts...))
	two := rewalkTwiceRead(t, ctx, sink)

	if len(one.suites) == 0 || len(one.cases) == 0 || one.retractedSuites != 2 {
		t.Fatalf("walk 1: %d suite keys (%d retracted), %d case keys; want the dropped aggregate's 2 suites retracted next to its parts",
			len(one.suites), one.retractedSuites, len(one.cases))
	}
	compare := func(kind string, before, after map[string]string) {
		t.Helper()
		if len(before) != len(after) {
			t.Errorf("%s keys: %d after walk 1, %d after walk 2, want the same keys (no new key)", kind, len(before), len(after))
		}
		for key, value := range before {
			got, ok := after[key]
			switch {
			case !ok:
				t.Errorf("%s key %s is gone after walk 2", kind, key)
			case got != value:
				t.Errorf("%s key %s: served columns %q after walk 1, %q after walk 2, want equal", kind, key, value, got)
			}
		}
		for key := range after {
			if _, ok := before[key]; !ok {
				t.Errorf("%s key %s is new after walk 2", kind, key)
			}
		}
	}
	compare("suite", one.suites, two.suites)
	compare("case", one.cases, two.cases)
	if two.retractedSuites != one.retractedSuites {
		t.Errorf("retracted suite rows: %d after walk 1, %d after walk 2, want equal", one.retractedSuites, two.retractedSuites)
	}
	stamp := func(at time.Time) string { return at.Format("2006-01-02 15:04:05.000") }
	if len(one.syncedAt) != 1 || one.syncedAt[stamp(first)] == 0 || len(two.syncedAt) != 1 || two.syncedAt[stamp(second)] == 0 {
		t.Errorf("last_synced after walk 1 %v, after walk 2 %v; want every newest row at walk 1's then walk 2's normalizedAt", one.syncedAt, two.syncedAt)
	}
	t.Logf("REWALK-TWICE suites %d (retracted %d) cases %d: same keys, same served columns; last_synced %v -> %v",
		len(two.suites), two.retractedSuites, len(two.cases), one.syncedAt, two.syncedAt)
}
