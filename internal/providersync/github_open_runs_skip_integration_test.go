//go:build integration

package providersync

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

type openSkipUnit struct {
	walk                        openSkipWalk
	backlogBefore, backlogAfter int
	reads                       []int
}

// 13966: the open phase on the real ClickHouse reader (ci_pipeline_runs from the chmigrate baseline) and the real
// CollectChunks, one hourly unit after another, each a new generation. The rows a unit writes go into ClickHouse after
// the unit, as the sink does. Cells: C1 a 410 run drains 0/0/0 on main and 299/299/99 after; C2 the skipped run is
// named in the WARN line and the counter; C3 it is read again on a later unit and drains when GitHub answers; C4, C4b,
// C4c a rate limit or an auth error still fails the unit; C5 the healthy drain keeps its rows and order; C6 three
// transient skips in a row fail the unit, two and a good run do not.
func TestGitHubOpenRunsSkipOnTheRealReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, ciPipelineRunsDDL(t)); err != nil {
		t.Fatal(err)
	}
	repoID := listingRepoID(t)
	reader := GitHubOpenRunsClickHouseReader{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	claim := openWindowClaim()
	startedBefore := claim.SinceAt.UTC().Add(-githubTestsRefreshLookbackDefault)
	stale := listingSince.Add(-60 * 24 * time.Hour)
	const first = 500000

	// seed stores count open rows from id first, 30 minutes apart; GitHub reports the oldest still in progress when
	// headOpen, and every other run completed.
	seed := func(t *testing.T, count int, headOpen bool) *listingFake {
		t.Helper()
		if err := conn.Exec(ctx, `TRUNCATE TABLE ci_pipeline_runs`); err != nil {
			t.Fatal(err)
		}
		fake := newListingFake(t, makeListingRuns(1, 1, listingSince.Add(time.Minute), time.Minute))
		values := make([]string, 0, count)
		for index := 0; index < count; index++ {
			started := stale.Add(time.Duration(index) * 30 * time.Minute)
			id := first + index
			status := "running"
			if index%2 == 1 {
				status = "queued"
			}
			values = append(values, fmt.Sprintf("('%s','%s','%d','github_actions','%s','%s','%s')", claim.OrgID, repoID, id, status,
				started.Format("2006-01-02 15:04:05"), started.Format("2006-01-02 15:04:05")))
			if index == 0 && headOpen {
				fake.byID[id] = listingRun{id: id, created: started, updated: listingSince, status: "in_progress"}
			} else {
				fake.byID[id] = listingRun{id: id, created: started, updated: started.Add(time.Hour), status: "completed", conclusion: "success"}
			}
		}
		if err := conn.Exec(ctx, `INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, provider, status, started_at, last_synced) VALUES `+strings.Join(values, ",")); err != nil {
			t.Fatal(err)
		}
		return fake
	}
	backlog := func(t *testing.T) int {
		t.Helper()
		count, err := reader.OpenRunBacklog(ctx, claim, repoID, startedBefore, 1000000)
		if err != nil {
			t.Fatal(err)
		}
		return count
	}
	statusOf := func(t *testing.T, runID int) string {
		t.Helper()
		var status *string
		if err := conn.QueryRow(ctx, `SELECT status FROM ci_pipeline_runs FINAL WHERE org_id = ? AND repo_id = ? AND run_id = ?`,
			claim.OrgID, repoID, strconv.Itoa(runID)).Scan(&status); err != nil {
			t.Fatal(err)
		}
		return stringPtrValue(status)
	}
	units := func(t *testing.T, fake *listingFake, doer *openSkipDoer, count int) []openSkipUnit {
		t.Helper()
		var out []openSkipUnit
		for unit := 1; unit <= count; unit++ {
			result := openSkipUnit{backlogBefore: backlog(t)}
			reads := len(fake.runGets)
			result.walk = walkOpenSkip(t, GitHubTestsRouteHandler{OpenRuns: reader}, claim, doer, 0)
			result.reads = append([]int(nil), fake.runGets[reads:]...)
			if written := result.walk.pipelines; len(written) > 0 {
				rows := make([]string, 0, len(written))
				for _, row := range written {
					rows = append(rows, fmt.Sprintf("('%s','%s','%s','github_actions','%s','%s','%s')", row.OrgID, row.RepoID, row.RunID,
						stringPtrValue(row.Status), row.StartedAt.UTC().Format("2006-01-02 15:04:05.000"), row.LastSynced.UTC().Format("2006-01-02 15:04:05.000")))
				}
				if err := conn.Exec(ctx, `INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, provider, status, started_at, last_synced) VALUES `+strings.Join(rows, ",")); err != nil {
					t.Fatal(err)
				}
			}
			result.backlogAfter = backlog(t)
			t.Logf("unit %d err=%v backlog %d -> %d (drained %d) result reread=%s skipped=%s by-id reads answered by GitHub=%d",
				unit, result.walk.err, result.backlogBefore, result.backlogAfter, result.backlogBefore-result.backlogAfter,
				result.walk.result("open_runs_reread"), result.walk.result("open_runs_skipped"), len(result.reads))
			out = append(out, result)
		}
		return out
	}
	drained := func(got []openSkipUnit) string {
		var out []int
		for _, unit := range got {
			out = append(out, unit.backlogBefore-unit.backlogAfter)
		}
		return fmt.Sprint(out)
	}
	const poison = first + 1

	var healthyFirstUnitReads []int
	t.Run("C5 healthy drain", func(t *testing.T) {
		captureMembershipLogs(t)
		fake := seed(t, 1500, true)
		got := units(t, fake, newOpenSkipDoer(fake), 3)
		for index, unit := range got {
			if unit.walk.err != nil {
				t.Fatalf("unit %d err=%v", index+1, unit.walk.err)
			}
		}
		if drained(got) != "[299 299 99]" {
			t.Fatalf("healthy drain per unit = %s, want [299 299 99] (cap 300, 300, 100; the still-open run uses a slot)", drained(got))
		}
		for index, id := range got[0].reads {
			if id != first+index {
				t.Fatalf("unit 1 read %d at position %d, want %d: oldest first by (started_at, run_id)", id, index, first+index)
			}
		}
		if len(got[0].reads) != 300 {
			t.Fatalf("unit 1 made %d by-id reads, want 300", len(got[0].reads))
		}
		healthyFirstUnitReads = got[0].reads
	})

	t.Run("C1 C2 one 410 run at the head", func(t *testing.T) {
		records := captureMembershipLogs(t)
		fake := seed(t, 1500, true)
		doer := newOpenSkipDoer(fake)
		doer.runs[poison] = []openSkipReply{{status: 410, body: openSkipPoisonBody}}
		got := units(t, fake, doer, 3)
		for index, unit := range got {
			if unit.walk.err != nil {
				t.Fatalf("RED: unit %d err=%v; the walk stops at the failing run every unit", index+1, unit.walk.err)
			}
			if got := unit.walk.result("open_runs_skipped"); got != "1" {
				t.Fatalf("unit %d open_runs_skipped = %s, want 1", index+1, got)
			}
			if want := openSkipCounterLine("gone", 1); !strings.Contains(unit.walk.metrics, want) {
				t.Fatalf("unit %d: missing %q in:\n%s", index+1, want, unit.walk.metrics)
			}
		}
		if drained(got) != "[299 299 99]" {
			t.Fatalf("drain per unit with one 410 run = %s, want [299 299 99] like the healthy drain", drained(got))
		}
		if status := statusOf(t, poison); status != "running" && status != "queued" {
			t.Fatalf("the skipped run's row is %q, want it left open (a skip writes nothing)", status)
		}
		if doer.hitsOf(poison) != 3 {
			t.Fatalf("the failing run was read %d times over 3 units, want once a unit", doer.hitsOf(poison))
		}
		lines := openSkipLines(*records)
		if len(lines) != 3 {
			t.Fatalf("got %d skip lines over 3 units, want 3: %v", len(lines), lines)
		}
		for _, line := range lines {
			if line["run"] != strconv.Itoa(poison) || line["reason"] != "gone" || fmt.Sprint(line["status"]) != "410" {
				t.Fatalf("skip line = %v, want run %d reason gone status 410", line, poison)
			}
		}
		assertNoBodyInLogs(t, *records)
		if healthyFirstUnitReads != nil {
			var want []int
			for _, id := range healthyFirstUnitReads {
				if id != poison {
					want = append(want, id)
				}
			}
			want = append(want, first+300)
			if fmt.Sprint(got[0].reads) != fmt.Sprint(want) {
				t.Fatalf("unit 1 read order differs from the healthy order without the failing run:\n got %v\nwant %v", got[0].reads, want)
			}
		}
	})

	t.Run("C3 the skipped run drains on a later unit", func(t *testing.T) {
		captureMembershipLogs(t)
		fake := seed(t, 1500, true)
		doer := newOpenSkipDoer(fake)
		// Units 1 and 2 get a 410; on unit 3 GitHub answers the run as completed.
		doer.runs[poison] = []openSkipReply{{status: 410, body: openSkipPoisonBody}, {status: 410, body: openSkipPoisonBody}, {}}
		got := units(t, fake, doer, 3)
		for index, unit := range got {
			if unit.walk.err != nil {
				t.Fatalf("RED: unit %d err=%v", index+1, unit.walk.err)
			}
		}
		if status := statusOf(t, poison); status != "success" {
			t.Fatalf("after unit 3 the run's row is %q, want success", status)
		}
		if got[2].walk.result("open_runs_skipped") != "0" {
			t.Fatalf("unit 3 open_runs_skipped = %s, want 0", got[2].walk.result("open_runs_skipped"))
		}
		if doer.hitsOf(poison) != 3 {
			t.Fatalf("the run was read %d times over 3 units, want once a unit", doer.hitsOf(poison))
		}
	})

	for _, tc := range []struct {
		name      string
		reply     openSkipReply
		wantClass providerfoundation.ErrorClass
	}{
		{"C4 429", openSkipReply{status: 429, body: openSkipPoisonBody}, providerfoundation.ErrorRateLimited},
		{"C4 403 primary limit remaining 0", openSkipReply{status: 403, header: map[string]string{"X-Ratelimit-Remaining": "0"}, body: openSkipPoisonBody}, providerfoundation.ErrorRateLimited},
		{"C4b 403 secondary limit", openSkipReply{status: 403, body: `{"message":"You have exceeded a secondary rate limit"}`}, providerfoundation.ErrorRateLimited},
		{"C4c 401", openSkipReply{status: 401, body: openSkipPoisonBody}, providerfoundation.ErrorAuthentication},
	} {
		t.Run(tc.name+" still fails the unit", func(t *testing.T) {
			records := captureMembershipLogs(t)
			fake := seed(t, 6, false)
			doer := newOpenSkipDoer(fake)
			doer.runs[poison] = []openSkipReply{tc.reply}
			got := units(t, fake, doer, 1)
			var providerErr *providerfoundation.ProviderError
			if !errors.As(got[0].walk.err, &providerErr) || providerErr.Class != tc.wantClass {
				t.Fatalf("unit err=%v, want a %s error", got[0].walk.err, tc.wantClass)
			}
			if got[0].backlogBefore-got[0].backlogAfter != 1 || doer.hitsOf(poison+1) != 0 {
				t.Fatalf("drained %d, read of the next run %d; want 1 and 0: the walk stops at the failing run",
					got[0].backlogBefore-got[0].backlogAfter, doer.hitsOf(poison+1))
			}
			if lines := openSkipLines(*records); len(lines) != 0 {
				t.Fatalf("logged as a skip: %v", lines)
			}
		})
	}

	t.Run("C6 three transient skips in a row fail the unit", func(t *testing.T) {
		captureMembershipLogs(t)
		fake := seed(t, 6, false)
		doer := newOpenSkipDoer(fake)
		bad := []openSkipReply{{status: 502, body: openSkipPoisonBody}}
		doer.runs[first+1], doer.runs[first+2], doer.runs[first+3] = bad, bad, bad
		got := units(t, fake, doer, 1)
		var providerErr *providerfoundation.ProviderError
		if !errors.As(got[0].walk.err, &providerErr) || providerErr.Class != providerfoundation.ErrorTransient {
			t.Fatalf("unit err=%v, want the third transient error", got[0].walk.err)
		}
		if doer.hitsOf(first+3) != 1 || doer.hitsOf(first+4) != 0 {
			t.Fatalf("by-id reads of the third and fourth run = %d, %d; want 1, 0", doer.hitsOf(first+3), doer.hitsOf(first+4))
		}
	})

	t.Run("C6 two transient skips then a good run do not", func(t *testing.T) {
		captureMembershipLogs(t)
		fake := seed(t, 6, false)
		doer := newOpenSkipDoer(fake)
		bad := []openSkipReply{{status: 502, body: openSkipPoisonBody}}
		doer.runs[first+1], doer.runs[first+2], doer.runs[first+4] = bad, bad, bad
		got := units(t, fake, doer, 1)
		if got[0].walk.err != nil {
			t.Fatalf("RED: unit err=%v, want success", got[0].walk.err)
		}
		if got[0].backlogBefore-got[0].backlogAfter != 3 {
			t.Fatalf("drained %d, want 3 (the runs at offsets 0, 3, 5)", got[0].backlogBefore-got[0].backlogAfter)
		}
		if want := openSkipCounterLine("transient", 3); !strings.Contains(got[0].walk.metrics, want) {
			t.Fatalf("missing %q in:\n%s", want, got[0].walk.metrics)
		}
	})
}
