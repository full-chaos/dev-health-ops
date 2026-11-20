package providersync

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// lateRerunRunsIntervalClaim is the midnight claim [07-04 23:00, 07-05 00:00] of these cells: runs range
// [07-04 17:00, 07-05 00:00], deep runs range [06-26 00:00, 07-04 16:59:59].
func lateRerunRunsIntervalClaim() (Claim, string, string) {
	m := time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC)
	claim := lateRerunClaim(m.Add(-time.Hour), m)
	regular := githubTestsRunsQuery(claim, githubTestsRefreshLookbackDefault, "").Get("created")
	deep := githubTestsLateRerunRunsQuery(claim, githubTestsRefreshLookbackDefault, githubTestsLateRerunLookbackDefault, "").Get("created")
	return claim, regular, deep
}

// lateRerunRunsIntervalRead runs one claim on one route and returns the pipeline rows per run id, the late_rerun_runs
// observations and whether the watermark advanced.
func lateRerunRunsIntervalRead(t *testing.T, route string, claim Claim, doer providerfoundation.HTTPDoer) (map[string]int, []GitHubTestsIncomplete, bool) {
	t.Helper()
	rows := map[string]int{}
	record := func(effects []EffectBatch) {
		for _, effect := range effects {
			if effect.Destination != "ci_pipeline_runs" {
				continue
			}
			decoded, decodeErr := decodeEffectRows[githubTestsPipelineRow](effect)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			for _, row := range decoded {
				rows[row.RunID]++
			}
		}
	}
	client := githubTestsClient(t, fakehttp.Client(doer))
	now := time.Date(2026, 7, 5, 1, 0, 0, 0, time.UTC)
	var batch CompleteRouteBatch
	if route == "oracle" {
		var err error
		batch, err = GitHubTestsRouteHandler{}.Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
		if err != nil {
			t.Fatal(err)
		}
		record(batch.Effects)
	} else if err := (GitHubTestsRouteHandler{}).CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, now, "",
		func(emission ChunkRouteEmission) error {
			record(emission.Batch.Effects)
			if emission.Final {
				batch = emission.Batch
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	if batch.Result == nil {
		t.Fatal("no final emission")
	}
	var incomplete []GitHubTestsIncomplete
	if decodeCompletionValue(batch.Result, "incomplete", &incomplete) != nil {
		t.Fatal("result incomplete does not decode")
	}
	return rows, lateRerunIncompleteOf(incomplete, githubTestsLateRerunRunsComponent), batch.Watermark != nil
}

// lateRerunRunsWiden returns a change that serves target with its start moved to lo.
func lateRerunRunsWiden(target, lo string) func(string) string {
	return func(created string) string {
		if created != target {
			return created
		}
		_, end, _ := strings.Cut(created, "..")
		return lo + ".." + end
	}
}

// Each runs range admits only the runs of its own created interval, whatever the listing answered: a loose deep listing
// does not read a run older than the deep interval, and a run that a loose runs listing and the deep listing both answer
// is read once, by the range whose interval holds it. Jobs reads and pipeline rows, both routes.
func TestLateRerunRunsIntervalAdmitsOnlyItsOwnRuns(t *testing.T) {
	claim, regular, deep := lateRerunRunsIntervalClaim()
	m := *claim.BeforeAt
	deepLo := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	regularLo := claim.SinceAt.Add(-githubTestsRefreshLookbackDefault)
	deepHiWiden := func(created string) string {
		if created != deep {
			return created
		}
		start, _, _ := strings.Cut(created, "..")
		return start + ".." + regularLo.Format(time.RFC3339)
	}
	for _, cell := range []struct {
		name    string
		change  func(string) string
		created time.Time
		want    int
	}{
		{"loose deep, one second below the deep interval", lateRerunRunsWiden(deep, "2026-06-25T00:00:00Z"), deepLo.Add(-time.Second), 0},
		{"loose deep, at the deep interval start", lateRerunRunsWiden(deep, "2026-06-25T00:00:00Z"), deepLo, 1},
		{"loose runs range, one second below its start (in both listings)", lateRerunRunsWiden(regular, "2026-07-04T16:00:00Z"), regularLo.Add(-time.Second), 1},
		{"loose runs range, at its start", lateRerunRunsWiden(regular, "2026-07-04T16:00:00Z"), regularLo, 1},
		{"loose deep end, at the runs range start (in both listings)", deepHiWiden, regularLo, 1},
	} {
		for _, route := range []string{"chunked", "oracle"} {
			t.Run(cell.name+"/"+route, func(t *testing.T) {
				run := listingRun{id: 881, created: cell.created, startedAt: m.Add(-30 * time.Minute), updated: m.Add(-10 * time.Minute),
					status: "completed", conclusion: "success", attempt: 2}
				fake := newListingFake(t, []listingRun{run})
				rows, _, _ := lateRerunRunsIntervalRead(t, route, claim, lateRerunReportsListDoer{listingFake: fake, change: cell.change})
				if fake.jobGets[run.id] != cell.want || rows["881"] != cell.want {
					t.Errorf("jobs reads=%d rows=%d, want %d and %d", fake.jobGets[run.id], rows["881"], cell.want, cell.want)
				}
			})
		}
	}
}

// A full page whose oldest run lies outside its range's interval (a loose listing) never moves the next range outside it:
// an oldest run below the interval ends the range (it reached past the start), and a deep page wholly above the interval is
// a stop with one advancing late_rerun_runs observation. No run outside the interval is read by that range.
func TestLateRerunRunsIntervalKeepsThePeelInside(t *testing.T) {
	claim, regular, deep := lateRerunRunsIntervalClaim()
	m := *claim.BeforeAt
	deepLo, deepHi, _ := strings.Cut(deep, "..")
	regularLo, _, _ := strings.Cut(regular, "..")
	deepLoTime, _ := time.Parse(time.RFC3339, deepLo)
	deepHiTime, _ := time.Parse(time.RFC3339, deepHi)
	regularLoTime, _ := time.Parse(time.RFC3339, regularLo)
	below := func(at time.Time, id int) listingRun {
		return listingRun{id: id, created: at.Add(-time.Second), updated: m.Add(-time.Minute), startedAt: m.Add(-2 * time.Minute), status: "completed", conclusion: "success", attempt: 2}
	}
	for _, cell := range []struct {
		name, target, replacement string
		runs                      []listingRun
		outside, wantReads        int
		stop                      bool
	}{
		{"deep, oldest below the interval", deep, "2026-06-25T00:00:00Z.." + deepHi,
			append(makeListingRuns(20, 999, m.AddDate(0, 0, -5), time.Second), below(deepLoTime, 3000)), 3000, 0, false},
		{"deep, page wholly above the interval", deep, "2026-07-05T01:00:00Z..2026-07-05T01:16:39Z",
			makeListingRuns(20, 1000, m.Add(time.Hour), time.Second), 20, 0, true},
		{"runs range, oldest below the interval (the deep range reads it)", regular, "2026-07-04T16:00:00Z.." + strings.SplitN(regular, "..", 2)[1],
			append(makeListingRuns(20, 999, regularLoTime.Add(time.Hour), time.Second), below(regularLoTime, 3000)), 3000, 1, false},
	} {
		for _, route := range []string{"chunked", "oracle"} {
			t.Run(cell.name+"/"+route, func(t *testing.T) {
				fake := newListingFake(t, cell.runs)
				var asked []string
				rows, stops, advanced := lateRerunRunsIntervalRead(t, route, claim, lateRerunReportsListDoer{listingFake: fake, asked: &asked, change: func(created string) string {
					if created == cell.target {
						return cell.replacement
					}
					return created
				}})
				var outside []string
				for _, created := range asked {
					start, end, _ := strings.Cut(created, "..")
					hi, err := time.Parse(time.RFC3339, end)
					switch {
					case created == deep || created == regular:
					case start == deepLo && (err != nil || hi.Before(deepLoTime) || hi.After(deepHiTime)):
						outside = append(outside, created)
					case start == regularLo && (err != nil || hi.Before(regularLoTime) || hi.After(m)):
						outside = append(outside, created)
					}
				}
				if len(outside) != 0 || fake.jobGets[cell.outside] != cell.wantReads || rows[fmt.Sprint(cell.outside)] != cell.wantReads || (len(stops) == 1) != cell.stop || !advanced {
					t.Errorf("follow-up ranges outside the interval=%v run %d jobs reads=%d rows=%d late_rerun_runs=%v watermark advanced=%v, want none, %d, %d, stop=%v, true",
						outside, cell.outside, fake.jobGets[cell.outside], rows[fmt.Sprint(cell.outside)], stops, advanced, cell.wantReads, cell.wantReads, cell.stop)
				}
			})
		}
	}
}

// A claim that lists no deep range (no UTC midnight, an open end, full_resync, backfill) keeps main's runs-range
// admission: a run a loose listing answers although it was created one second below the runs range, changed inside the
// claim, is read once, as on main, on both routes.
func TestLateRerunRunsNoDeepRangeKeepsMainAdmission(t *testing.T) {
	m := time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC)
	for _, cell := range []struct {
		name, mode    string
		since, before time.Time
	}{
		{"incremental without a midnight", "incremental", m.Add(-2 * time.Hour), m.Add(-time.Hour)},
		{"incremental with an open end", "incremental", m.Add(-2 * time.Hour), time.Time{}},
		{"full_resync", "full_resync", m.Add(-time.Hour), m},
		{"backfill", "backfill", m.Add(-time.Hour), m},
	} {
		for _, route := range []string{"chunked", "oracle"} {
			t.Run(cell.name+"/"+route, func(t *testing.T) {
				claim := lateRerunClaim(cell.since, cell.before)
				claim.Mode = cell.mode
				if cell.before.IsZero() {
					claim.BeforeAt = nil
				}
				regular := githubTestsRunsQuery(claim, githubTestsRefreshLookbackDefault, "").Get("created")
				lo := cell.since.Add(-githubTestsRefreshLookbackDefault)
				run := listingRun{id: 9101, created: lo.Add(-time.Second), startedAt: cell.since, updated: cell.since.Add(time.Minute),
					status: "completed", conclusion: "success", attempt: 2}
				fake := newListingFake(t, []listingRun{run})
				rows, _, _ := lateRerunRunsIntervalRead(t, route, claim, lateRerunReportsListDoer{listingFake: fake,
					change: lateRerunRunsWiden(regular, lo.Add(-time.Hour).Format(time.RFC3339))})
				if fake.jobGets[run.id] != 1 || rows["9101"] != 1 {
					t.Errorf("jobs reads=%d rows=%d, want main's 1 and 1", fake.jobGets[run.id], rows["9101"])
				}
			})
		}
	}
}

// After a capped page the range is peeled: the next query ends at the oldest created second seen. A loose narrowed page
// that answers again a run created above that ceiling (already read in the first range) does not read it again: the
// chunked route admits a run under the active query's ceiling (the cut second inclusive), as the oracle keeps one read
// per run id. Runs range and deep range, both routes.
func TestLateRerunRunsPeeledRangeAdmitsUnderItsCeiling(t *testing.T) {
	midnight, regular, deep := lateRerunRunsIntervalClaim()
	m := *midnight.BeforeAt
	noDeep := lateRerunClaim(m.Add(-2*time.Hour), m.Add(-time.Hour))
	for _, cell := range []struct {
		name   string
		claim  Claim
		isDeep bool
	}{{"runs range", midnight, false}, {"deep range", midnight, true}, {"runs range, no deep range", noDeep, false}} {
		for _, route := range []string{"chunked", "oracle"} {
			claim, isDeep := cell.claim, cell.isDeep
			t.Run(cell.name+"/"+route, func(t *testing.T) {
				first := regular
				if !githubTestsHoldsMidnight(claim) {
					first = githubTestsRunsQuery(claim, githubTestsRefreshLookbackDefault, "").Get("created")
				}
				at := claim.SinceAt.Add(-5 * time.Hour)
				if isDeep {
					first, at = deep, m.AddDate(0, 0, -5).Add(10*time.Hour)
				}
				lo, _, _ := strings.Cut(first, "..")
				run := listingRun{id: 9001, created: at.Add(2 * time.Hour), startedAt: m.Add(-time.Hour), updated: m.Add(-time.Minute),
					status: "completed", conclusion: "success", attempt: 2}
				doer := &lateRerunR2Doer{listingFake: newListingFake(t, append(makeListingRuns(100, 999, at, time.Second), run)), report: -1,
					extra: func(created string) *listingRun {
						if created != first && strings.HasPrefix(created, lo+"..") {
							return &run
						}
						return nil
					}}
				rows, _, _ := lateRerunRunsIntervalRead(t, route, claim, doer)
				if doer.jobGets[run.id] != 1 || rows["9001"] != 1 {
					t.Errorf("jobs reads=%d rows=%d, want 1 and 1 (the narrowed page's repeat above its ceiling is not read again)", doer.jobGets[run.id], rows["9001"])
				}
			})
		}
	}
}

// A run with no created_at listed by the runs range of a midnight claim keeps the runs range's own rule (it falls back to
// the start time): it is read once on both routes, as on main.
func TestLateRerunRunsIntervalKeepsANilCreatedRunOfTheRunsRange(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	updated := claim.SinceAt.Add(20 * time.Minute)
	other := []string{lateRerunRunPayload(41, nil, &updated, claim.SinceAt.Add(10*time.Minute))}
	rows, reads := lateRerunBothRoutes(t, claim, nil, other)
	for _, route := range []string{"oracle", "chunked"} {
		if rows[route]["41"] != 1 || reads[route]["41"] != 1 {
			t.Errorf("%s: rows=%d jobs reads=%d, want 1 and 1", route, rows[route]["41"], reads[route]["41"])
		}
	}
}

// lateRerunPeeledExtraDoer serves the fake GitHub and puts extra runs ahead of the first page of one created filter.
type lateRerunPeeledExtraDoer struct {
	*listingFake
	filter string
	extra  []listingRun
	served int
}

func (doer *lateRerunPeeledExtraDoer) Do(request *http.Request) (*http.Response, error) {
	if request.URL.Path != "/repos/acme/api/actions/runs" || request.URL.Query().Get("created") != doer.filter || request.URL.Query().Get("page") != "" {
		return doer.listingFake.Do(request)
	}
	doer.served++
	response, err := doer.listingFake.Do(request)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return nil, err
	}
	items := make([]string, 0, len(doer.extra))
	for _, run := range doer.extra {
		items = append(items, run.json())
	}
	remainder := strings.TrimPrefix(string(body), `{"workflow_runs":[`)
	if !strings.HasPrefix(remainder, "]") {
		remainder = "," + remainder
	}
	response.Body = io.NopCloser(strings.NewReader(`{"workflow_runs":[` + strings.Join(items, ",") + remainder))
	return response, nil
}

// A run that a peeled (narrowed) response supplies first, created above that response's ceiling, is not read by the
// oracle: it admits each listed run under the ceiling of the response that supplied it, as the chunked route does
// (TestLateRerunRunsPeeledRangeAdmitsUnderItsCeiling), for both ranges of a midnight claim. Runs inside the ceiling (a
// first-seen run at it, a listed run at it and below it) are read once. A claim with no deep range keeps main's oracle
// admission (the first-seen run above the ceiling is read there). One oracle walk per range.
func TestLateRerunRunsOracleAdmitsUnderTheSupplyingResponseCeiling(t *testing.T) {
	midnight, regular, deep := lateRerunRunsIntervalClaim()
	m := *midnight.BeforeAt
	noDeep := lateRerunClaim(m.Add(-2*time.Hour), m.Add(-time.Hour))
	for _, cell := range []struct {
		name, first string
		claim       Claim
		cut         time.Time
		bounded     bool
	}{
		{"runs range", regular, midnight, midnight.SinceAt.Add(-5 * time.Hour), true},
		{"deep range", deep, midnight, m.AddDate(0, 0, -5).Add(10 * time.Hour), true},
		{"runs range, no deep range", githubTestsRunsQuery(noDeep, githubTestsRefreshLookbackDefault, "").Get("created"), noDeep, noDeep.SinceAt.Add(-5 * time.Hour), false},
	} {
		t.Run(cell.name, func(t *testing.T) {
			lo, _, _ := strings.Cut(cell.first, "..")
			probe := func(id int, created time.Time) listingRun {
				return listingRun{id: id, created: created, startedAt: cell.claim.SinceAt.Add(time.Minute), updated: cell.claim.SinceAt.Add(2 * time.Minute),
					status: "completed", conclusion: "success", attempt: 2}
			}
			above, firstAt := probe(9001, cell.cut.Add(time.Second)), probe(9002, cell.cut)
			at, below := probe(1, cell.cut), probe(9003, cell.cut.Add(-time.Second))
			doer := &lateRerunPeeledExtraDoer{listingFake: newListingFake(t, append(makeListingRuns(100, 1000, cell.cut, time.Second), at, below)),
				filter: lo + ".." + cell.cut.Format(time.RFC3339), extra: []listingRun{above, firstAt}}
			rows, _, _ := lateRerunRunsIntervalRead(t, "oracle", cell.claim, doer)
			if doer.served != 1 {
				t.Fatalf("peeled query served %d times, want 1", doer.served)
			}
			wantAbove := 0
			if !cell.bounded {
				wantAbove = 1
			}
			for _, want := range []struct {
				run  listingRun
				want int
			}{{above, wantAbove}, {firstAt, 1}, {at, 1}, {below, 1}} {
				if doer.jobGets[want.run.id] != want.want || rows[fmt.Sprint(want.run.id)] != want.want {
					t.Errorf("run %d created %s: jobs reads=%d rows=%d, want %d and %d", want.run.id, want.run.created.Format(time.RFC3339),
						doer.jobGets[want.run.id], rows[fmt.Sprint(want.run.id)], want.want, want.want)
				}
			}
		})
	}
}
