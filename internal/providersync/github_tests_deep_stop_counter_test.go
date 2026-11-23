package providersync

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// githubTestsDeepStopLineWant is a fact about the listing only: the watermark outcome is decided at finalization (another
// component of the same walk can still hold it).
const githubTestsDeepStopLineWant = "deep listing stop: page budget reached"

// deepStopWalk is one whole CollectChunks walk (no yields) on a client whose metrics the cell reads, with every slog record
// the walk wrote.
type deepStopWalk struct {
	final    CompleteRouteBatch
	requests int
	metrics  string
	records  []slog.Record
}

func walkDeepStop(t *testing.T, handler GitHubTestsRouteHandler, claim Claim, runs []listingRun) deepStopWalk {
	t.Helper()
	return walkDeepStopReports(t, handler, claim, runs, false)
}

// walkDeepStopReports is walkDeepStop on a doer that serves one JUnit report artifact per run when withReports.
func walkDeepStopReports(t *testing.T, handler GitHubTestsRouteHandler, claim Claim, runs []listingRun, withReports bool) deepStopWalk {
	t.Helper()
	return walkDeepStopWith(t, handler, claim, runs, func(fake *listingFake) providerfoundation.HTTPDoer {
		return deletionShiftDoer(t, fake, withReports)
	})
}

// walkDeepStopWith is walkDeepStop on the doer wrap builds around the listing fake.
func walkDeepStopWith(
	t *testing.T, handler GitHubTestsRouteHandler, claim Claim, runs []listingRun, wrap func(*listingFake) providerfoundation.HTTPDoer,
) deepStopWalk {
	t.Helper()
	records := captureMembershipLogs(t)
	fake := newListingFake(t, runs)
	client := githubTestsClient(t, fakehttp.Client(wrap(fake)))
	client.Metrics = providerfoundation.NewMetrics()
	walk := deepStopWalk{}
	finalSeen := false
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := handler.CollectChunks(ctx, claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(emission ChunkRouteEmission) error {
			if emission.Final {
				walk.final, finalSeen = emission.Batch, true
			}
			return nil
		})
	if err != nil || !finalSeen {
		t.Fatalf("CollectChunks err=%v final=%v, want one whole walk", err, finalSeen)
	}
	var rendered bytes.Buffer
	if err := client.Metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	walk.requests, walk.metrics, walk.records = fake.requests, rendered.String(), append([]slog.Record(nil), *records...)
	return walk
}

// deepStopCount reads dev_health_provider_deep_stop_total for one listing: an absent series is 0.
func deepStopCount(t *testing.T, rendered, listing string) int {
	t.Helper()
	prefix := fmt.Sprintf(`dev_health_provider_deep_stop_total{provider="github",dataset="tests",listing=%q} `, listing)
	for line := range strings.SplitSeq(rendered, "\n") {
		if value, found := strings.CutPrefix(line, prefix); found {
			count, err := strconv.Atoi(value)
			if err != nil {
				t.Fatalf("series %q: %v", line, err)
			}
			return count
		}
	}
	return 0
}

// deepStopLines is the deep-stop Warn records of one listing. Each must carry the five fields (pages: exactly the pages the
// listing had entered at its stop) and no error operand (D4270).
func deepStopLines(t *testing.T, records []slog.Record, listing string, budget, pages int) int {
	t.Helper()
	count := 0
	for _, record := range records {
		if strings.HasPrefix(record.Message, "deep listing stop") && record.Message != githubTestsDeepStopLineWant {
			t.Errorf("deep-stop line %q, want %q (no claim about the watermark: finalization decides it)", record.Message, githubTestsDeepStopLineWant)
		}
		if record.Message != githubTestsDeepStopLineWant {
			continue
		}
		attrs := map[string]slog.Value{}
		record.Attrs(func(attr slog.Attr) bool {
			if _, isError := attr.Value.Any().(error); isError || attr.Key == "err" || attr.Key == "error" {
				t.Errorf("deep-stop line has an error operand %q=%v", attr.Key, attr.Value)
			}
			attrs[attr.Key] = attr.Value
			return true
		})
		if record.Level != slog.LevelWarn {
			t.Errorf("deep-stop line level=%v, want WARN", record.Level)
		}
		if attrs["listing"].String() != listing {
			continue
		}
		count++
		if attrs["provider"].String() != "github" || attrs["dataset"].String() != "tests" {
			t.Errorf("deep-stop line provider=%v dataset=%v, want github/tests", attrs["provider"], attrs["dataset"])
		}
		if got, ok := attrs["pages"]; !ok || got.Kind() != slog.KindInt64 || got.Int64() != int64(pages) {
			t.Errorf("deep-stop line pages=%v, want %d (the pages entered at the stop)", attrs["pages"], pages)
		}
		if got, ok := attrs["budget"]; !ok || got.Kind() != slog.KindInt64 || got.Int64() != int64(budget) {
			t.Errorf("deep-stop line budget=%v, want %d", attrs["budget"], budget)
		}
	}
	return count
}

// deepStopObservations is the page-budget observations main records for a deep listing (0 or 1).
func deepStopObservations(t *testing.T, final CompleteRouteBatch, component string) int {
	t.Helper()
	incomplete, ok := final.Result[GitHubTestsResultKeyIncomplete].([]GitHubTestsIncomplete)
	if !ok {
		t.Fatalf("result incomplete is %T", final.Result[GitHubTestsResultKeyIncomplete])
	}
	count := 0
	for _, observation := range incomplete {
		if observation.Component == component && observation.Cause == githubTestsPageBudgetCause {
			count += observation.Count
		}
	}
	return count
}

// A deep listing (the late re-run relist of the runs, or of the reports) that reaches its page budget keeps main's behavior
// (the watermark advances; the same requests) and now writes ONE Warn line and steps ONE counter per stopped listing. A deep
// walk that ends without a stop writes no line and steps no counter. The counter is the release event of a hold: it must
// equal the deep page-budget stops main records, per listing.
func TestGitHubTestsDeepStopIsCountedAndLogged(t *testing.T) {
	runsClaim := lateRerunMidnightClaim("incremental")
	inWindow := listingRun{id: 9000, created: runsClaim.SinceAt.Add(10 * time.Minute), updated: runsClaim.SinceAt.Add(15 * time.Minute),
		status: "completed", conclusion: "success"}

	reportsSince, reportsBefore := time.Date(2026, 7, 4, 23, 0, 0, 0, time.UTC), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC)
	reportsClaim := lateRerunClaim(reportsSince, reportsBefore)
	late := listingRun{id: 1, created: time.Date(2026, 7, 1, 0, 50, 0, 0, time.UTC), startedAt: time.Date(2026, 7, 4, 1, 0, 0, 0, time.UTC), status: "completed", conclusion: "success"}
	late.updated = late.startedAt.Add(time.Hour)
	old := time.Date(2026, 6, 28, 0, 0, 0, 0, time.UTC)

	for _, cell := range []struct {
		name     string
		handler  GitHubTestsRouteHandler
		claim    Claim
		runs     []listingRun
		budget   int
		pages    int
		stops    map[string]int
		requests int
	}{
		// The old runs are older than the runs range, so each fixture stops BOTH deep listings (as main records). Requests are
		// main's counts: the fix changes no request.
		// The runs deep range lists 150 late re-runs on a 1-page budget (the visit ends at the budget).
		{"runs", GitHubTestsRouteHandler{LateRerunMaxPages: 1}, runsClaim,
			append(lateRerunRuns(1, 150, runsClaim.SinceAt.UTC()), inWindow), 1, 1, map[string]int{"runs": 1, "reports": 1}, 207},
		// The reports deep range: a visit that ends at the budget, and a peeled range that finds the budget spent.
		{"reports", GitHubTestsRouteHandler{LateRerunMaxPages: 1}, reportsClaim,
			append([]listingRun{late}, makeListingRuns(2000, 150, old, time.Minute)...), 1, 1, map[string]int{"runs": 1, "reports": 1}, 7},
		{"reports at a peel", GitHubTestsRouteHandler{LateRerunMaxPages: 10}, reportsClaim,
			append([]listingRun{late}, makeListingRuns(3000, 1000, old, time.Minute)...), 10, 10, map[string]int{"runs": 1, "reports": 1}, 25},
		// Control: both deep ranges end on their own (150 late re-runs, the default budget): no stop, no line, no count.
		{"no stop", GitHubTestsRouteHandler{}, runsClaim,
			append(lateRerunRuns(1, 150, runsClaim.SinceAt.UTC()), inWindow), githubTestsLateRerunMaxPages, 0, map[string]int{"runs": 0, "reports": 0}, 309},
	} {
		t.Run(cell.name, func(t *testing.T) {
			walk := walkDeepStop(t, cell.handler, cell.claim, cell.runs)
			for listing, component := range map[string]string{"runs": githubTestsLateRerunRunsComponent, "reports": githubTestsLateRerunReportsComponent} {
				observed := deepStopObservations(t, walk.final, component)
				lines := deepStopLines(t, walk.records, listing, cell.budget, cell.pages)
				counted := deepStopCount(t, walk.metrics, listing)
				if want := cell.stops[listing]; observed != want {
					t.Errorf("%s: main recorded %d deep page-budget stop(s), want %d (the fixture)", listing, observed, want)
				}
				if lines != observed || counted != observed {
					t.Errorf("%s: deep-stop lines=%d counter=%d, want %d each (one per deep page-budget stop)", listing, lines, counted, observed)
				}
			}
			if walk.final.Watermark == nil || !walk.final.Watermark.Equal(*cell.claim.BeforeAt) {
				t.Errorf("watermark=%v, want the window end %s (main: a deep stop advances)", walk.final.Watermark, cell.claim.BeforeAt)
			}
			if walk.requests != cell.requests {
				t.Errorf("requests=%d, want %d (main)", walk.requests, cell.requests)
			}
		})
	}
}

// A walk record bound reached inside the runs deep range is not a page-budget stop: main records it as the same
// late_rerun_runs observation, but the deep-stop line and counter stay at 0 (they count the page budget only).
func TestGitHubTestsDeepWalkBoundIsNotCountedAsAPageBudgetStop(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	runs := append(lateRerunRuns(1, 3, claim.SinceAt.UTC()), makeListingRuns(9000, 1, claim.SinceAt.Add(5*time.Minute), time.Second)...)
	walk := walkDeepStop(t, GitHubTestsRouteHandler{MaxWalkRecords: 2, OpenRuns: &fakeOpenRuns{}}, claim, runs)
	if observed := deepStopObservations(t, walk.final, githubTestsLateRerunRunsComponent); observed != 1 {
		t.Fatalf("late_rerun_runs observations=%d, want 1 (the fixture: a walk bound inside the deep range)", observed)
	}
	for _, listing := range []string{"runs", "reports"} {
		if lines, counted := deepStopLines(t, walk.records, listing, 0, 0), deepStopCount(t, walk.metrics, listing); lines != 0 || counted != 0 {
			t.Errorf("%s: deep-stop lines=%d counter=%d at a walk bound, want 0", listing, lines, counted)
		}
	}
	if walk.final.Watermark == nil || !walk.final.Watermark.Equal(*claim.BeforeAt) {
		t.Errorf("watermark=%v, want the window end (main)", walk.final.Watermark)
	}
}

// A walk record bound reached inside the REPORTS deep range is not a page-budget stop either: main records it as the
// late_rerun_reports observation (the watermark advances), and the deep-stop line and counter stay at 0.
func TestGitHubTestsDeepReportsWalkBoundIsNotCountedAsAPageBudgetStop(t *testing.T) {
	chargedR1RaceSkip(t)
	listing := deletionShiftListings()[3]
	if listing.name != "deep_reports" {
		t.Fatalf("premise: listing %q, want deep_reports", listing.name)
	}
	// 140 runs of one report each, under the default 1,000-page budget: only the record bound can stop the deep reports range.
	bound := 100 * deletionShiftReportRecords(t, listing)
	walk := walkDeepStopReports(t, GitHubTestsRouteHandler{MaxWalkRecords: bound}, listing.claim, listing.runs(1, 140), true)
	if observed := deepStopObservations(t, walk.final, githubTestsLateRerunReportsComponent); observed != 1 {
		t.Fatalf("late_rerun_reports observations=%d, want 1 (the fixture: a walk bound inside the deep reports range)", observed)
	}
	if observed := deepStopObservations(t, walk.final, githubTestsLateRerunRunsComponent); observed != 0 {
		t.Fatalf("late_rerun_runs observations=%d, want 0 (the fixture: the runs deep range ends on its own)", observed)
	}
	for _, listing := range []string{"runs", "reports"} {
		if lines, counted := deepStopLines(t, walk.records, listing, 0, 0), deepStopCount(t, walk.metrics, listing); lines != 0 || counted != 0 {
			t.Errorf("%s: deep-stop lines=%d counter=%d at a walk bound, want 0", listing, lines, counted)
		}
	}
	if walk.final.Watermark == nil || !walk.final.Watermark.Equal(*listing.claim.BeforeAt) {
		t.Errorf("watermark=%v, want the window end (main)", walk.final.Watermark)
	}
}

// perRunArtifactPageDoer serves the listing fake, except that the artifacts of run blocked answer one non-report artifact and a
// next link: the per-run artifact listing (one page) ends at its page budget, a per_run_page_budget blocker.
type perRunArtifactPageDoer struct {
	fake    *listingFake
	blocked int
}

func (doer perRunArtifactPageDoer) Do(request *http.Request) (*http.Response, error) {
	path := "/repos/acme/api/actions/runs/" + strconv.Itoa(doer.blocked) + "/artifacts"
	if request.URL.Path != path {
		return doer.fake.Do(request)
	}
	if _, err := doer.fake.Do(request); err != nil {
		return nil, err
	}
	next := *request.URL
	query := next.Query()
	query.Set("page", "2")
	next.RawQuery = query.Encode()
	header := http.Header{"Content-Type": {"application/json"}, "Link": {"<" + next.String() + `>; rel="next"`}}
	return doer.fake.respond(request, header, `{"artifacts":[{"id":77,"name":"image.dockerbuild","size_in_bytes":10,"archive_download_url":"https://api.github.com/repos/acme/api/actions/artifacts/77/zip"}]}`, 200), nil
}

// A deep page-budget stop in the same walk as a per-run artifact page-budget blocker: the final watermark is held (main),
// and the deep stop still writes exactly one line and one count per listing. The line states only the listing fact; it
// must not claim that the watermark advances.
func TestGitHubTestsDeepStopLineMakesNoWatermarkClaim(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	inWindow := listingRun{id: 9000, created: claim.SinceAt.Add(10 * time.Minute), updated: claim.SinceAt.Add(15 * time.Minute),
		status: "completed", conclusion: "success"}
	runs := append(lateRerunRuns(1, 150, claim.SinceAt.UTC()), inWindow)
	walk := walkDeepStopWith(t, GitHubTestsRouteHandler{LateRerunMaxPages: 1}, claim, runs, func(fake *listingFake) providerfoundation.HTTPDoer {
		return perRunArtifactPageDoer{fake: fake, blocked: inWindow.id}
	})
	blocker := false
	incomplete, _ := walk.final.Result[GitHubTestsResultKeyIncomplete].([]GitHubTestsIncomplete)
	for _, observation := range incomplete {
		blocker = blocker || observation.Component == githubTestsRunArtifactsComponent && observation.Cause == githubTestsPerRunPageBudgetCause
	}
	if !blocker || walk.final.Watermark != nil {
		t.Fatalf("premise: incomplete=%v watermark=%v, want the run_artifacts per_run_page_budget blocker and a held watermark (main)",
			incomplete, walk.final.Watermark)
	}
	for listing, component := range map[string]string{"runs": githubTestsLateRerunRunsComponent, "reports": githubTestsLateRerunReportsComponent} {
		observed := deepStopObservations(t, walk.final, component)
		lines, counted := deepStopLines(t, walk.records, listing, 1, 1), deepStopCount(t, walk.metrics, listing)
		if observed != 1 || lines != 1 || counted != 1 {
			t.Errorf("%s: main stops=%d deep-stop lines=%d counter=%d, want 1 each", listing, observed, lines, counted)
		}
	}
	for _, record := range walk.records {
		if strings.HasPrefix(record.Message, "deep listing stop") && strings.Contains(record.Message, "watermark") {
			t.Errorf("deep-stop line %q claims a watermark outcome; the final watermark is %v", record.Message, walk.final.Watermark)
		}
	}
}
