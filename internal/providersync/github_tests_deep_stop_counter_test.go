package providersync

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

const githubTestsDeepStopLineWant = "deep listing stop: page budget reached; watermark advances"

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
	records := captureMembershipLogs(t)
	fake := newListingFake(t, runs)
	client := githubTestsClient(t, fakehttp.Client(fake))
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

// deepStopLines is the deep-stop Warn records of one listing. Each must carry the five fields and no error operand (D4270).
func deepStopLines(t *testing.T, records []slog.Record, listing string, budget int) int {
	t.Helper()
	count := 0
	for _, record := range records {
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
		if pages, ok := attrs["pages"]; !ok || pages.Kind() != slog.KindInt64 || pages.Int64() < int64(budget) {
			t.Errorf("deep-stop line pages=%v, want an int of at least the budget %d", attrs["pages"], budget)
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
		stops    map[string]int
		requests int
	}{
		// The old runs are older than the runs range, so each fixture stops BOTH deep listings (as main records). Requests are
		// main's counts: the fix changes no request.
		// The runs deep range lists 150 late re-runs on a 1-page budget (the visit ends at the budget).
		{"runs", GitHubTestsRouteHandler{LateRerunMaxPages: 1}, runsClaim,
			append(lateRerunRuns(1, 150, runsClaim.SinceAt.UTC()), inWindow), 1, map[string]int{"runs": 1, "reports": 1}, 207},
		// The reports deep range: a visit that ends at the budget, and a peeled range that finds the budget spent.
		{"reports", GitHubTestsRouteHandler{LateRerunMaxPages: 1}, reportsClaim,
			append([]listingRun{late}, makeListingRuns(2000, 150, old, time.Minute)...), 1, map[string]int{"runs": 1, "reports": 1}, 7},
		{"reports at a peel", GitHubTestsRouteHandler{LateRerunMaxPages: 10}, reportsClaim,
			append([]listingRun{late}, makeListingRuns(3000, 1000, old, time.Minute)...), 10, map[string]int{"runs": 1, "reports": 1}, 25},
		// Control: both deep ranges end on their own (150 late re-runs, the default budget): no stop, no line, no count.
		{"no stop", GitHubTestsRouteHandler{}, runsClaim,
			append(lateRerunRuns(1, 150, runsClaim.SinceAt.UTC()), inWindow), githubTestsLateRerunMaxPages, map[string]int{"runs": 0, "reports": 0}, 309},
	} {
		t.Run(cell.name, func(t *testing.T) {
			walk := walkDeepStop(t, cell.handler, cell.claim, cell.runs)
			for listing, component := range map[string]string{"runs": githubTestsLateRerunRunsComponent, "reports": githubTestsLateRerunReportsComponent} {
				observed := deepStopObservations(t, walk.final, component)
				lines := deepStopLines(t, walk.records, listing, cell.budget)
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
