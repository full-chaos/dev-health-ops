package providersync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// Cells of the charged-deletion credit's edge shapes (CHAOS-14434): a resume that starts AT the record bound, the deep
// reports average, a page GitHub packed differently between two attempts, and two runs of one created second listed in
// another order. Every shape runs with CoalesceRuns OFF and ON; the walks of 140-250 runs skip under -race (the plain test
// legs run them).

func chargedR1RaceSkip(t *testing.T) {
	t.Helper()
	if cutDifferentialRace {
		t.Skip("run in the plain test legs; -race keeps the guard cells")
	}
}

// chargedR1Unread is the survivors the uninterrupted walk of plain reads and the resumed walk (fake) does not.
func chargedR1Unread(listing deletionShiftListing, plain, fake *listingFake) []int {
	var unread []int
	for id := range listing.reads(plain) {
		if listing.reads(fake)[id] == 0 {
			unread = append(unread, id)
		}
	}
	slices.Sort(unread)
	return unread
}

// A checkpoint whose records are exactly at the record bound, then a deletion before its anchor: the resume checks the
// anchor before the bound stop and credits the deletion, so the walk reads what it reads when the checkpoint is one run
// before the bound (mid page on 70-run pages, at a page end, and at a page end with the page budget also spent).
func TestGitHubTestsResumeAtTheRecordBoundCreditsADeletion(t *testing.T) {
	chargedR1RaceSkip(t)
	for _, listing := range deletionShiftListings() {
		for _, coalesce := range []bool{false, true} {
			for _, position := range []string{"within_page", "page_end", "spent_page_end"} {
				t.Run(listing.name+"/"+deletionShiftFlag(coalesce)+"/"+position, func(t *testing.T) {
					const runs, at = 140, 100
					fixture := listing.runs(1, runs)
					bound := at
					if listing.reports {
						bound = at * deletionShiftReportRecords(t, listing)
						if !listing.deep {
							bound += runs
						}
					}
					handler := GitHubTestsRouteHandler{MaxWalkRecords: bound, CoalesceRuns: coalesce}
					if position == "spent_page_end" {
						if listing.deep {
							handler.LateRerunMaxPages = 1
						} else {
							handler.MaxRuns = 100
							if listing.reports {
								handler.MaxWalkRecords -= runs - at
								bound = handler.MaxWalkRecords
							}
						}
					}
					var cursor githubTestsChunkCursor
					walk := func(yieldAt int) deletionShiftOutcome {
						fake := newListingFake(t, slices.Clone(fixture))
						if position == "within_page" {
							fake.perPage = 70
						}
						deleted := false
						final := deletionShiftWalk(t, handler, listing.claim, deletionShiftDoer(t, fake, listing.reports),
							func(c githubTestsChunkCursor) bool { return listing.in(c) && c.RangeItems == yieldAt },
							func(c githubTestsChunkCursor) {
								cursor, deleted = c, true
								resumeDeletionDelete(fake, runs)
							}, nil)
						if !deleted {
							t.Fatal("premise: no deletion")
						}
						plain := newListingFake(t, slices.Clone(fake.runs))
						plain.perPage = fake.perPage
						deletionShiftPlainWalk(t, handler, listing.claim, deletionShiftDoer(t, plain, listing.reports))
						return deletionShiftOutcome{chargedR1Unread(listing, plain, fake), final.Watermark != nil, deletionShiftIncomplete(t, final.Result)}
					}
					before := walk(at - 1)
					outcome := walk(at)
					charged := githubTestsWalkRecords(cursor)
					if listing.reports {
						charged = githubTestsWindowRecords(cursor)
					}
					if charged != bound || (cursor.Index == 0) != (position != "within_page") {
						t.Fatalf("premise charged=%d bound=%d index=%d", charged, bound, cursor.Index)
					}
					t.Logf("CELL at_bound/%s/%s/%s before_bound_unread=%v at_bound_unread=%v watermark=%v incomplete=%v",
						listing.name, deletionShiftFlag(coalesce), position, before.unread, outcome.unread, outcome.watermark, outcome.incomplete)
					if !slices.Equal(outcome.unread, before.unread) {
						t.Errorf("a checkpoint at the record bound leaves %v unread, one run earlier %v", outcome.unread, before.unread)
					}
				})
			}
		}
	}
}

// The same at-bound checkpoint through the chunked executor (its attempt end is the yield): the first attempt end of the
// listing past its first page (mid page, or at a page end) is found on an undisturbed walk, and the bound is set to the
// records charged there. A cell whose walk has no attempt end at that position (with run groups on, none ends at a page end
// of these walks) is skipped; the route cells cover it.
func TestGitHubTestsResumeAtTheRecordBoundCreditsADeletionThroughTheExecutor(t *testing.T) {
	chargedR1RaceSkip(t)
	const runs = 512
	for _, listing := range deletionShiftListings() {
		for _, coalesce := range []bool{false, true} {
			for _, position := range []string{"within_page", "page_end"} {
				t.Run(listing.name+"/"+deletionShiftFlag(coalesce)+"/"+position, func(t *testing.T) {
					charged := func(c githubTestsChunkCursor) int {
						if listing.reports {
							return githubTestsWindowRecords(c)
						}
						return githubTestsWalkRecords(c)
					}
					probe := newListingFake(t, listing.runs(1, runs))
					var point githubTestsChunkCursor
					deletionShiftExecutorReached(t, GitHubTestsRouteHandler{CoalesceRuns: coalesce}, listing.claim, deletionShiftDoer(t, probe, listing.reports),
						func(c githubTestsChunkCursor) bool {
							if point.AnchorID == "" && listing.in(c) && c.RangeItems > 100 && (c.Index == 0) == (position == "page_end") {
								point = c
								return true
							}
							return false
						}, func() {})
					if point.AnchorID == "" {
						t.Skip("no attempt of this walk ends at this position")
					}
					handler := GitHubTestsRouteHandler{MaxWalkRecords: charged(point), CoalesceRuns: coalesce}
					fake := newListingFake(t, listing.runs(1, runs))
					var checkpoint githubTestsChunkCursor
					result, reached := deletionShiftExecutorReached(t, handler, listing.claim, deletionShiftDoer(t, fake, listing.reports),
						func(c githubTestsChunkCursor) bool {
							if !listing.in(c) || c.RangeItems != point.RangeItems {
								return false
							}
							checkpoint = c
							return true
						}, func() { resumeDeletionDelete(fake, runs) })
					if !reached {
						t.Skipf("with the bound at %d records no attempt ends at the probed point (items %d)", handler.MaxWalkRecords, point.RangeItems)
					}
					plain := newListingFake(t, slices.Clone(fake.runs))
					deletionShiftPlainWalk(t, handler, listing.claim, deletionShiftDoer(t, plain, listing.reports))
					unread := chargedR1Unread(listing, plain, fake)
					if charged(checkpoint) != handler.MaxWalkRecords {
						t.Fatalf("premise: charged=%d bound=%d", charged(checkpoint), handler.MaxWalkRecords)
					}
					incomplete := deletionShiftIncomplete(t, result.Result)
					t.Logf("CELL at_bound_executor/%s/%s/%s items=%d index=%d unread=%v watermark=%v incomplete=%v",
						listing.name, deletionShiftFlag(coalesce), position, checkpoint.RangeItems, checkpoint.Index, unread,
						result.Watermark != nil, incomplete)
					want := 0
					if listing.name == "artifacts" {
						want = 1
					}
					if len(unread) > want || len(unread) != 0 && (result.Watermark != nil || len(incomplete) == 0) {
						t.Errorf("%d survivor(s) unread (want at most %d, on a recorded stop holding the watermark)", len(unread), want)
					}
				})
			}
		}
	}
}

// Control: a checkpoint at the record bound with nothing deleted stops as main does: the same runs read, records, stop and
// watermark as the walk that never yielded, at the cost of at most one listing request (the anchor's page).
func TestGitHubTestsResumeAtTheRecordBoundWithoutADeletionStopsAsMain(t *testing.T) {
	chargedR1RaceSkip(t)
	for _, listing := range deletionShiftListings() {
		for _, coalesce := range []bool{false, true} {
			for _, position := range []string{"within_page", "page_end"} {
				t.Run(listing.name+"/"+deletionShiftFlag(coalesce)+"/"+position, func(t *testing.T) {
					const runs, at = 140, 100
					bound := at
					if listing.reports {
						bound = at * deletionShiftReportRecords(t, listing)
						if !listing.deep {
							bound += runs
						}
					}
					handler := GitHubTestsRouteHandler{MaxWalkRecords: bound, CoalesceRuns: coalesce}
					walk := func(yield bool) (githubTestsChunkCursor, *listingFake, CompleteRouteBatch) {
						fake := newListingFake(t, listing.runs(1, runs))
						if position == "within_page" {
							fake.perPage = 70
						}
						yielded := false
						last := chargedResumeWalk(t, handler, listing.claim, deletionShiftDoer(t, fake, listing.reports),
							func(c githubTestsChunkCursor) bool { return yield && !yielded && listing.in(c) && c.RangeItems == at },
							func(githubTestsChunkCursor) { yielded = true })
						if yield && !yielded {
							t.Fatal("premise: no yield at the bound")
						}
						batch, err := githubTestsFinalMetadataBatch(listing.claim, last)
						if err != nil {
							t.Fatal(err)
						}
						return last, fake, batch
					}
					resumed, resumedFake, resumedBatch := walk(true)
					plain, plainFake, plainBatch := walk(false)
					extra := resumedFake.listPages - plainFake.listPages
					t.Logf("CELL at_bound_control/%s/%s/%s records=%d runs=%d rc=%d extra_list_gets=%d watermark=%v incomplete=%v",
						listing.name, deletionShiftFlag(coalesce), position, githubTestsWalkRecords(resumed), len(listing.reads(resumedFake)),
						resumed.RecordCredit, extra, resumedBatch.Watermark != nil, deletionShiftIncomplete(t, resumedBatch.Result))
					if githubTestsWalkRecords(resumed) != githubTestsWalkRecords(plain) || resumed.RecordCredit != 0 ||
						!slices.Equal(chargedR1Unread(listing, plainFake, resumedFake), nil) ||
						len(listing.reads(resumedFake)) != len(listing.reads(plainFake)) ||
						(resumedBatch.Watermark != nil) != (plainBatch.Watermark != nil) ||
						!slices.Equal(deletionShiftIncomplete(t, resumedBatch.Result), deletionShiftIncomplete(t, plainBatch.Result)) {
						t.Errorf("the resumed walk differs from the walk that never yielded")
					}
					if extra < 0 || extra > 2 {
						t.Errorf("%d listing requests past the walk that never yielded, want at most the anchor's page (and the resume's own re-fetch)", extra)
					}
				})
			}
		}
	}
}

// chargedR1DeepReports is the deep reports listing with every run inside the relist only (no pipeline row: the runs pass
// reads none of them) when noPipelines, else the plain deep reports listing.
func chargedR1DeepReports(noPipelines bool) (deletionShiftListing, []listingRun) {
	listing := deletionShiftListings()[3]
	fixture := listing.runs(1, 250)
	if !noPipelines {
		return listing, fixture
	}
	since := time.Date(2026, 7, 20, 1, 0, 0, 0, time.UTC)
	before := since.Truncate(24 * time.Hour).Add(24 * time.Hour)
	listing.claim.SinceAt, listing.claim.BeforeAt = &since, &before
	reportEnd := githubTestsLateRerunFloor(listing.claim).Add(time.Hour)
	reportStart := githubTestsRelistFloor(listing.claim).Add(-time.Hour)
	for index := range fixture {
		fixture[index].created = reportStart.Add(time.Duration(index) * time.Second)
		fixture[index].startedAt = fixture[index].created
		fixture[index].updated = reportEnd
	}
	return listing, fixture
}

// The deep reports average (report rows over the runs pass's pipelines) under-credits a deleted run of 3 report rows, and
// a claim whose relist runs wrote no pipeline row earns no credit at all. Such a walk must not advance past the survivor it
// leaves: its deep stop at the record bound, in a walk that earned credit, holds the watermark on a recorded truncation.
// The next walk (credit 0, no new deletion) reads the survivor and advances.
func TestGitHubTestsResumeDeepReportsUnderCreditHoldsTheWatermark(t *testing.T) {
	chargedR1RaceSkip(t)
	for _, shape := range []struct {
		name        string
		noPipelines bool
		bounds      []int
	}{{"three_row_reports", false, []int{387, 388, 389}}, {"no_pipelines", true, []int{390}}} {
		listing, fixture := chargedR1DeepReports(shape.noPipelines)
		for _, coalesce := range []bool{false, true} {
			for _, pause := range []int{50, 100} {
				for _, bound := range shape.bounds {
					t.Run(fmt.Sprintf("%s/%s/pause_%d/bound_%d", shape.name, deletionShiftFlag(coalesce), pause, bound), func(t *testing.T) {
						handler := GitHubTestsRouteHandler{MaxWalkRecords: bound, CoalesceRuns: coalesce}
						fake := newListingFake(t, slices.Clone(fixture))
						var at githubTestsChunkCursor
						final := chargedResumeWalk(t, handler, listing.claim, deletionShiftDoer(t, fake, true),
							func(c githubTestsChunkCursor) bool {
								return at.AnchorID == "" && listing.in(c) && c.RangeItems == pause
							},
							func(c githubTestsChunkCursor) { at = c; resumeDeletionDelete(fake, 250) })
						if at.AnchorID == "" {
							t.Fatal("premise: no deletion")
						}
						batch, err := githubTestsFinalMetadataBatch(listing.claim, final)
						if err != nil {
							t.Fatal(err)
						}
						plain := newListingFake(t, slices.Clone(fake.runs))
						next := deletionShiftPlainWalk(t, handler, listing.claim, deletionShiftDoer(t, plain, true))
						unread := chargedR1Unread(listing, plain, fake)
						incomplete := deletionShiftIncomplete(t, batch.Result)
						t.Logf("CELL deep_reports_credit/%s/%s pause=%d bound=%d pipelines=%d rc=%d unread=%v watermark=%v incomplete=%v next_watermark=%v",
							shape.name, deletionShiftFlag(coalesce), pause, bound, at.Pipelines, final.RecordCredit, unread,
							batch.Watermark != nil, incomplete, next.Watermark != nil)
						held := batch.Watermark == nil && slices.Contains(incomplete, githubTestsArtifactInventoryComponent+"/"+githubTestsPageBudgetCause)
						if len(unread) != 0 && !held {
							t.Errorf("%v unread and the watermark advanced: a silent skip", unread)
						}
						if next.Watermark == nil {
							t.Errorf("the next walk (credit 0, no deletion) holds the watermark: a stall")
						}
						for _, id := range unread {
							if plain.artifactGets[id] == 0 {
								t.Errorf("the next walk does not read the held survivor %d", id)
							}
						}
					})
				}
			}
		}
	}
}

// Control: a deep stop at the record bound in a walk with no record credit advances the watermark exactly as main.
func TestGitHubTestsDeepStopAtTheRecordBoundWithoutCreditAdvances(t *testing.T) {
	chargedR1RaceSkip(t)
	for _, listing := range deletionShiftListings() {
		if !listing.deep {
			continue
		}
		for _, coalesce := range []bool{false, true} {
			t.Run(listing.name+"/"+deletionShiftFlag(coalesce), func(t *testing.T) {
				bound := 100
				if listing.reports {
					bound = 100 * deletionShiftReportRecords(t, listing)
				}
				fake := newListingFake(t, listing.runs(1, 140))
				final := deletionShiftPlainWalk(t, GitHubTestsRouteHandler{MaxWalkRecords: bound, CoalesceRuns: coalesce}, listing.claim,
					deletionShiftDoer(t, fake, listing.reports))
				incomplete := deletionShiftIncomplete(t, final.Result)
				t.Logf("CELL deep_bound_control/%s/%s watermark=%v incomplete=%v", listing.name, deletionShiftFlag(coalesce), final.Watermark != nil, incomplete)
				want := []string{githubTestsLateRerunRunsComponent + "/" + githubTestsPageBudgetCause}
				if listing.reports {
					want = []string{githubTestsLateRerunReportsComponent + "/" + githubTestsPageBudgetCause}
				}
				if final.Watermark == nil || !slices.Equal(incomplete, want) {
					t.Errorf("watermark advanced=%v incomplete=%v, want the advancing stop %v", final.Watermark != nil, incomplete, want)
				}
			})
		}
	}
}

// A page GitHub packs differently between two attempts (50 runs a page, then 55 or 65 or 100) moves the anchor with nothing
// deleted: no deletion credit, the record credit is the re-seek re-read of the anchor only, and the walk reads and stops as
// main (chargedR1PackingMain: the same cells on main).
func TestGitHubTestsResumePagePackingEarnsNoDeletionCredit(t *testing.T) {
	chargedR1RaceSkip(t)
	for _, listing := range deletionShiftListings() {
		for _, coalesce := range []bool{false, true} {
			for _, cell := range []struct {
				name                       string
				oldSize, newSize, consumed int
				pageBudget                 bool
			}{
				{"within_page_anchor_present", 50, 55, 60, false},
				{"within_page_anchor_absent", 50, 65, 60, false},
				{"page_end_anchor_present", 50, 55, 100, false},
				{"page_end_anchor_absent", 50, 100, 150, false},
				{"spent_page_end_anchor_present", 50, 55, 100, true},
				{"spent_page_end_anchor_absent", 50, 100, 150, true},
			} {
				name := listing.name + "/" + deletionShiftFlag(coalesce) + "/" + cell.name
				t.Run(name, func(t *testing.T) {
					fake := newListingFake(t, listing.runs(1, 250))
					fake.perPage = cell.oldSize
					handler := GitHubTestsRouteHandler{MaxWalkRecords: 4000, CoalesceRuns: coalesce}
					if cell.pageBudget {
						pages := cell.consumed / cell.oldSize
						if listing.deep {
							handler.LateRerunMaxPages = pages
						} else {
							handler.MaxRuns = pages * 100
						}
					}
					anchor := 251 - cell.consumed
					var checkpoint githubTestsChunkCursor
					final := chargedResumeWalk(t, handler, listing.claim, deletionShiftDoer(t, fake, listing.reports),
						func(c githubTestsChunkCursor) bool {
							return checkpoint.AnchorID == "" && listing.in(c) && c.RangeItems == cell.consumed
						},
						func(c githubTestsChunkCursor) { checkpoint = c; fake.perPage = cell.newSize })
					if checkpoint.AnchorID != fmt.Sprint(anchor) || len(fake.runs) != 250 {
						t.Fatalf("premise anchor=%s, want %d, runs=%d", checkpoint.AnchorID, anchor, len(fake.runs))
					}
					perRun := 1
					if listing.reports {
						perRun = deletionShiftReportRecords(t, listing)
					}
					chargedR1CheckMain(t, "packing/"+name, listing.claim, final, len(listing.reads(fake)), perRun)
				})
			}
		}
	}
}

// chargedR1TieDoer lists runs a and b (one created second) in the other order once reordered is set.
type chargedR1TieDoer struct {
	inner     providerfoundation.HTTPDoer
	reordered bool
	a, b      string
}

func (doer *chargedR1TieDoer) Do(request *http.Request) (*http.Response, error) {
	resp, err := doer.inner.Do(request)
	if err != nil || !doer.reordered || request.URL.Path != "/repos/acme/api/actions/runs" {
		return resp, err
	}
	payload, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	var page struct {
		Runs []json.RawMessage `json:"workflow_runs"`
	}
	if err := json.Unmarshal(payload, &page); err != nil {
		return nil, err
	}
	a, b := -1, -1
	for index, run := range page.Runs {
		switch githubTestsListedRunID(run) {
		case doer.a:
			a = index
		case doer.b:
			b = index
		}
	}
	if a >= 0 && b >= 0 {
		page.Runs[a], page.Runs[b] = page.Runs[b], page.Runs[a]
		if payload, err = json.Marshal(page); err != nil {
			return nil, err
		}
	}
	resp.Body = io.NopCloser(bytes.NewReader(payload))
	return resp, nil
}

// The anchor and the run listed before it share one created second, and GitHub lists them in the other order on the
// resume (the contract does not order inside a second): no deletion credit, the walk reads and stops as main.
func TestGitHubTestsResumeSameSecondSwapEarnsNoDeletionCredit(t *testing.T) {
	chargedR1RaceSkip(t)
	for _, listing := range deletionShiftListings() {
		for _, coalesce := range []bool{false, true} {
			for _, cell := range []struct {
				name     string
				consumed int
				spent    bool
			}{{"within_page", 60, false}, {"page_end", 100, false}, {"spent_page_end", 100, true}} {
				name := listing.name + "/" + deletionShiftFlag(coalesce) + "/" + cell.name
				t.Run(name, func(t *testing.T) {
					fixture := listing.runs(1, 250)
					anchor := 251 - cell.consumed
					fixture[anchor-1].created = fixture[anchor].created
					fake := newListingFake(t, fixture)
					doer := &chargedR1TieDoer{inner: deletionShiftDoer(t, fake, listing.reports), a: fmt.Sprint(anchor), b: fmt.Sprint(anchor + 1)}
					handler := GitHubTestsRouteHandler{MaxWalkRecords: 4000, CoalesceRuns: coalesce}
					if cell.spent {
						if listing.deep {
							handler.LateRerunMaxPages = 1
						} else {
							handler.MaxRuns = 100
						}
					}
					var checkpoint githubTestsChunkCursor
					final := chargedResumeWalk(t, handler, listing.claim, doer,
						func(c githubTestsChunkCursor) bool {
							return !doer.reordered && listing.in(c) && c.RangeItems == cell.consumed
						},
						func(c githubTestsChunkCursor) { checkpoint = c; doer.reordered = true })
					if checkpoint.AnchorID != doer.a || listing.reads(fake)[anchor] != 2 || listing.reads(fake)[anchor+1] != 2 {
						t.Fatalf("premise: anchor=%s reads=%d/%d", checkpoint.AnchorID, listing.reads(fake)[anchor], listing.reads(fake)[anchor+1])
					}
					perRun := 1
					if listing.reports {
						perRun = deletionShiftReportRecords(t, listing)
					}
					chargedR1CheckMain(t, "swap/"+name, listing.claim, final, len(listing.reads(fake)), 2*perRun)
				})
			}
		}
	}
}

// chargedR1CheckMain asserts a no-deletion move earns the re-seek re-read credit only (replay records) and reads and stops
// as main: the runs read, the stop and the watermark main's binary gives in the same cell (chargedR1Main, measured on main).
func chargedR1CheckMain(t *testing.T, name string, claim Claim, final githubTestsChunkCursor, read, replay int) {
	t.Helper()
	batch, err := githubTestsFinalMetadataBatch(claim, final)
	if err != nil {
		t.Fatal(err)
	}
	got := fmt.Sprintf("read=%d watermark=%v incomplete=%v", read, batch.Watermark != nil, deletionShiftIncomplete(t, batch.Result))
	t.Logf("CELL %s rc=%d replay=%d %s", name, final.RecordCredit, replay, got)
	if final.RecordCredit != replay {
		t.Errorf("record credit %d, want the re-seek re-read only %d (no deletion)", final.RecordCredit, replay)
	}
	if want, ok := chargedR1Main[name]; !ok || got != want {
		t.Errorf("%s, main gives %q", got, want)
	}
}

var chargedR1Main = map[string]string{
	"packing/runs/coalesce_off/within_page_anchor_present":            "read=250 watermark=true incomplete=[]",
	"packing/runs/coalesce_off/within_page_anchor_absent":             "read=250 watermark=true incomplete=[]",
	"packing/runs/coalesce_off/page_end_anchor_present":               "read=250 watermark=true incomplete=[]",
	"packing/runs/coalesce_off/page_end_anchor_absent":                "read=250 watermark=true incomplete=[]",
	"packing/runs/coalesce_off/spent_page_end_anchor_present":         "read=209 watermark=false incomplete=[run_inventory/page_budget_exhausted artifact_inventory/page_budget_exhausted]",
	"packing/runs/coalesce_off/spent_page_end_anchor_absent":          "read=250 watermark=true incomplete=[]",
	"packing/runs/coalesce_on/within_page_anchor_present":             "read=250 watermark=true incomplete=[]",
	"packing/runs/coalesce_on/within_page_anchor_absent":              "read=250 watermark=true incomplete=[]",
	"packing/runs/coalesce_on/page_end_anchor_present":                "read=250 watermark=true incomplete=[]",
	"packing/runs/coalesce_on/page_end_anchor_absent":                 "read=250 watermark=true incomplete=[]",
	"packing/runs/coalesce_on/spent_page_end_anchor_present":          "read=209 watermark=false incomplete=[run_inventory/page_budget_exhausted artifact_inventory/page_budget_exhausted]",
	"packing/runs/coalesce_on/spent_page_end_anchor_absent":           "read=250 watermark=true incomplete=[]",
	"packing/deep_runs/coalesce_off/within_page_anchor_present":       "read=250 watermark=true incomplete=[]",
	"packing/deep_runs/coalesce_off/within_page_anchor_absent":        "read=250 watermark=true incomplete=[]",
	"packing/deep_runs/coalesce_off/page_end_anchor_present":          "read=250 watermark=true incomplete=[]",
	"packing/deep_runs/coalesce_off/page_end_anchor_absent":           "read=250 watermark=true incomplete=[]",
	"packing/deep_runs/coalesce_off/spent_page_end_anchor_present":    "read=209 watermark=true incomplete=[late_rerun_runs/page_budget_exhausted late_rerun_reports/page_budget_exhausted]",
	"packing/deep_runs/coalesce_off/spent_page_end_anchor_absent":     "read=250 watermark=true incomplete=[]",
	"packing/deep_runs/coalesce_on/within_page_anchor_present":        "read=250 watermark=true incomplete=[]",
	"packing/deep_runs/coalesce_on/within_page_anchor_absent":         "read=250 watermark=true incomplete=[]",
	"packing/deep_runs/coalesce_on/page_end_anchor_present":           "read=250 watermark=true incomplete=[]",
	"packing/deep_runs/coalesce_on/page_end_anchor_absent":            "read=250 watermark=true incomplete=[]",
	"packing/deep_runs/coalesce_on/spent_page_end_anchor_present":     "read=209 watermark=true incomplete=[late_rerun_runs/page_budget_exhausted late_rerun_reports/page_budget_exhausted]",
	"packing/deep_runs/coalesce_on/spent_page_end_anchor_absent":      "read=250 watermark=true incomplete=[]",
	"packing/artifacts/coalesce_off/within_page_anchor_present":       "read=250 watermark=true incomplete=[]",
	"packing/artifacts/coalesce_off/within_page_anchor_absent":        "read=250 watermark=true incomplete=[]",
	"packing/artifacts/coalesce_off/page_end_anchor_present":          "read=250 watermark=true incomplete=[]",
	"packing/artifacts/coalesce_off/page_end_anchor_absent":           "read=250 watermark=true incomplete=[]",
	"packing/artifacts/coalesce_off/spent_page_end_anchor_present":    "read=209 watermark=false incomplete=[run_inventory/page_budget_exhausted artifact_inventory/page_budget_exhausted]",
	"packing/artifacts/coalesce_off/spent_page_end_anchor_absent":     "read=250 watermark=false incomplete=[run_inventory/page_budget_exhausted]",
	"packing/artifacts/coalesce_on/within_page_anchor_present":        "read=250 watermark=true incomplete=[]",
	"packing/artifacts/coalesce_on/within_page_anchor_absent":         "read=250 watermark=true incomplete=[]",
	"packing/artifacts/coalesce_on/page_end_anchor_present":           "read=250 watermark=true incomplete=[]",
	"packing/artifacts/coalesce_on/page_end_anchor_absent":            "read=250 watermark=true incomplete=[]",
	"packing/artifacts/coalesce_on/spent_page_end_anchor_present":     "read=209 watermark=false incomplete=[run_inventory/page_budget_exhausted artifact_inventory/page_budget_exhausted]",
	"packing/artifacts/coalesce_on/spent_page_end_anchor_absent":      "read=250 watermark=false incomplete=[run_inventory/page_budget_exhausted]",
	"packing/deep_reports/coalesce_off/within_page_anchor_present":    "read=250 watermark=true incomplete=[]",
	"packing/deep_reports/coalesce_off/within_page_anchor_absent":     "read=250 watermark=true incomplete=[]",
	"packing/deep_reports/coalesce_off/page_end_anchor_present":       "read=250 watermark=true incomplete=[]",
	"packing/deep_reports/coalesce_off/page_end_anchor_absent":        "read=250 watermark=true incomplete=[]",
	"packing/deep_reports/coalesce_off/spent_page_end_anchor_present": "read=209 watermark=true incomplete=[late_rerun_runs/page_budget_exhausted late_rerun_reports/page_budget_exhausted]",
	"packing/deep_reports/coalesce_off/spent_page_end_anchor_absent":  "read=250 watermark=true incomplete=[late_rerun_runs/page_budget_exhausted]",
	"packing/deep_reports/coalesce_on/within_page_anchor_present":     "read=250 watermark=true incomplete=[]",
	"packing/deep_reports/coalesce_on/within_page_anchor_absent":      "read=250 watermark=true incomplete=[]",
	"packing/deep_reports/coalesce_on/page_end_anchor_present":        "read=250 watermark=true incomplete=[]",
	"packing/deep_reports/coalesce_on/page_end_anchor_absent":         "read=250 watermark=true incomplete=[]",
	"packing/deep_reports/coalesce_on/spent_page_end_anchor_present":  "read=209 watermark=true incomplete=[late_rerun_runs/page_budget_exhausted late_rerun_reports/page_budget_exhausted]",
	"packing/deep_reports/coalesce_on/spent_page_end_anchor_absent":   "read=250 watermark=true incomplete=[late_rerun_runs/page_budget_exhausted]",
	"swap/runs/coalesce_off/within_page":                              "read=250 watermark=true incomplete=[]",
	"swap/runs/coalesce_off/page_end":                                 "read=250 watermark=true incomplete=[]",
	"swap/runs/coalesce_off/spent_page_end":                           "read=198 watermark=false incomplete=[run_inventory/page_budget_exhausted artifact_inventory/page_budget_exhausted]",
	"swap/runs/coalesce_on/within_page":                               "read=250 watermark=true incomplete=[]",
	"swap/runs/coalesce_on/page_end":                                  "read=250 watermark=true incomplete=[]",
	"swap/runs/coalesce_on/spent_page_end":                            "read=198 watermark=false incomplete=[run_inventory/page_budget_exhausted artifact_inventory/page_budget_exhausted]",
	"swap/deep_runs/coalesce_off/within_page":                         "read=250 watermark=true incomplete=[]",
	"swap/deep_runs/coalesce_off/page_end":                            "read=250 watermark=true incomplete=[]",
	"swap/deep_runs/coalesce_off/spent_page_end":                      "read=198 watermark=true incomplete=[late_rerun_runs/page_budget_exhausted late_rerun_reports/page_budget_exhausted]",
	"swap/deep_runs/coalesce_on/within_page":                          "read=250 watermark=true incomplete=[]",
	"swap/deep_runs/coalesce_on/page_end":                             "read=250 watermark=true incomplete=[]",
	"swap/deep_runs/coalesce_on/spent_page_end":                       "read=198 watermark=true incomplete=[late_rerun_runs/page_budget_exhausted late_rerun_reports/page_budget_exhausted]",
	"swap/artifacts/coalesce_off/within_page":                         "read=250 watermark=true incomplete=[]",
	"swap/artifacts/coalesce_off/page_end":                            "read=250 watermark=true incomplete=[]",
	"swap/artifacts/coalesce_off/spent_page_end":                      "read=198 watermark=false incomplete=[run_inventory/page_budget_exhausted artifact_inventory/page_budget_exhausted]",
	"swap/artifacts/coalesce_on/within_page":                          "read=250 watermark=true incomplete=[]",
	"swap/artifacts/coalesce_on/page_end":                             "read=250 watermark=true incomplete=[]",
	"swap/artifacts/coalesce_on/spent_page_end":                       "read=198 watermark=false incomplete=[run_inventory/page_budget_exhausted artifact_inventory/page_budget_exhausted]",
	"swap/deep_reports/coalesce_off/within_page":                      "read=250 watermark=true incomplete=[]",
	"swap/deep_reports/coalesce_off/page_end":                         "read=250 watermark=true incomplete=[]",
	"swap/deep_reports/coalesce_off/spent_page_end":                   "read=198 watermark=true incomplete=[late_rerun_runs/page_budget_exhausted late_rerun_reports/page_budget_exhausted]",
	"swap/deep_reports/coalesce_on/within_page":                       "read=250 watermark=true incomplete=[]",
	"swap/deep_reports/coalesce_on/page_end":                          "read=250 watermark=true incomplete=[]",
	"swap/deep_reports/coalesce_on/spent_page_end":                    "read=198 watermark=true incomplete=[late_rerun_runs/page_budget_exhausted late_rerun_reports/page_budget_exhausted]",
}

// A run created between two listed seconds appears ahead of the anchor (in its own second, so the anchor shares its
// second with no run): the anchor is found one place LATER than stored and nothing is proven deleted. The record credit
// stays the re-seek re-read of the anchor (1), as main.
func TestGitHubTestsResumeRunAppearingAheadEarnsNoDeletionCredit(t *testing.T) {
	chargedR1RaceSkip(t)
	window := listingClaim(listingSince, listingSince.Add(6*time.Hour))
	for _, coalesce := range []bool{false, true} {
		t.Run(deletionShiftFlag(coalesce), func(t *testing.T) {
			fake := newListingFake(t, makeListingRuns(1, 150, listingSince.Add(time.Minute), time.Minute))
			moved := false
			final := chargedResumeWalk(t, GitHubTestsRouteHandler{CoalesceRuns: coalesce}, window, fake,
				func(c githubTestsChunkCursor) bool {
					return !moved && c.Phase == githubTestsRunsPhase && c.Index > 0 && c.RangeItems == 30
				},
				func(c githubTestsChunkCursor) {
					moved = true
					second, err := time.Parse(time.RFC3339, c.RangeOldest)
					if err != nil {
						t.Fatal(err)
					}
					fake.runs = append(fake.runs, listingRun{id: 100_001, created: second.Add(30 * time.Second), updated: second.Add(time.Minute),
						status: "completed", conclusion: "success"})
				})
			t.Logf("CELL appearing/%s rc=%d", deletionShiftFlag(coalesce), final.RecordCredit)
			if !moved || final.RecordCredit != 1 {
				t.Errorf("moved=%v record credit %d, want the anchor re-read 1 (no deletion credit)", moved, final.RecordCredit)
			}
		})
	}
}
