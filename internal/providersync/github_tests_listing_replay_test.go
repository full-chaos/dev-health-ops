package providersync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// peelReplayDoer counts the listing replays. A replay starts each range of its listing again (a request with no page
// number) once the walk has spent its budget: a range start of a listing the walk began before it had served budget pages,
// requested again after that, is a replay's (the walk's own resumes inside a first page all come earlier). fail answers
// 500 to such a request (index: the range start's order of first appearance; hit: its replay count so far); afterList runs
// after every listing page.
type peelReplayDoer struct {
	delegate  providerfoundation.HTTPDoer
	budget    int
	mu        sync.Mutex
	served    int
	order     map[string]int
	early     map[string]bool
	hits      map[string]int
	fail      func(index, hit int) bool
	failPage2 bool
	afterList func()
	// replayAt is the listing pages served at each replay hit; emptyPage serves a page of a range start's family (by its
	// order of first appearance and page number) with no runs, its Link header kept.
	replayAt  []int
	emptyPage func(index, page int) bool
}

func newPeelReplayDoer(delegate providerfoundation.HTTPDoer, budget int) *peelReplayDoer {
	return &peelReplayDoer{delegate: delegate, budget: budget, order: map[string]int{}, early: map[string]bool{}, hits: map[string]int{}}
}

func (doer *peelReplayDoer) Do(request *http.Request) (*http.Response, error) {
	listing := request.URL.Path == "/repos/acme/api/actions/runs"
	if listing && request.URL.Query().Get("page") == "" {
		key := request.URL.Query().Encode()
		doer.mu.Lock()
		index, seen := doer.order[key]
		if !seen {
			index = len(doer.order)
			doer.order[key], doer.early[key] = index, doer.served < doer.budget
		}
		hit := 0
		if seen && doer.early[key] && doer.served >= doer.budget {
			doer.hits[key]++
			hit = doer.hits[key]
			doer.replayAt = append(doer.replayAt, doer.served)
		}
		doer.mu.Unlock()
		if hit > 0 && doer.fail != nil && doer.fail(index, hit) {
			return &http.Response{StatusCode: http.StatusInternalServerError, Header: http.Header{"Content-Type": {"application/json"}},
				Body: io.NopCloser(strings.NewReader(`{"message":"boom"}`)), Request: request}, nil
		}
	}
	if listing && doer.failPage2 && request.URL.Query().Get("page") == "2" {
		query := request.URL.Query()
		query.Del("page")
		doer.mu.Lock()
		replaying := doer.hits[query.Encode()] > 0
		doer.mu.Unlock()
		if replaying {
			return &http.Response{StatusCode: http.StatusInternalServerError, Header: http.Header{"Content-Type": {"application/json"}},
				Body: io.NopCloser(strings.NewReader(`{"message":"boom"}`)), Request: request}, nil
		}
	}
	response, err := doer.delegate.Do(request)
	if listing && err == nil && doer.emptyPage != nil {
		query := request.URL.Query()
		page, _ := strconv.Atoi(query.Get("page"))
		query.Del("page")
		doer.mu.Lock()
		index, known := doer.order[query.Encode()]
		doer.mu.Unlock()
		if known && doer.emptyPage(index, page) {
			response.Body.Close()
			response.Body = io.NopCloser(strings.NewReader(`{"total_count":0,"workflow_runs":[]}`))
			response.ContentLength = -1
			response.Header.Del("Content-Length")
		}
	}
	if listing {
		doer.mu.Lock()
		doer.served++
		doer.mu.Unlock()
		if doer.afterList != nil {
			doer.afterList()
		}
	}
	return response, err
}

// replays is the most replays of a listing: the hits of the first two range starts (a deep claim lists its regular runs
// range first). A walk that yields often resumes inside the first page of a later range after its budget; only the first
// ranges' resumes all come before it.
func (doer *peelReplayDoer) replays() int {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	most := 0
	for key, hits := range doer.hits {
		if doer.order[key] <= 1 {
			most = max(most, hits)
		}
	}
	return most
}

func peelReplayCounter(listing deletionShiftListing, c githubTestsChunkCursor) int {
	switch {
	case listing.reports:
		return c.ArtifactPages
	case listing.deep:
		return c.LateRunPages
	}
	return c.RunPages
}

func peelReplayBudget(listing deletionShiftListing, pages int) (budget, limit int) {
	return pages, pages + min((pages+9)/10, 10)
}

// peelReplayReplay walks the listing's replay (githubTestsReplayListing) against the fake, under the claim's queries.
func peelReplayReplay(t *testing.T, listing deletionShiftListing, handler GitHubTestsRouteHandler, doer providerfoundation.HTTPDoer, budget int) githubTestsFrontier {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(doer))
	owner, repository, err := splitGitHubRepository(listing.claim.SourceExternalID)
	if err != nil {
		t.Fatal(err)
	}
	claim, lookback, late := listing.claim, handler.refreshLookback(), handler.lateRerunLookback()
	query := func(hi string) url.Values { return githubTestsRunsQuery(claim, lookback, hi) }
	interval := githubTestsRunsInterval(claim, false, lookback, late)
	switch {
	case listing.reports && listing.deep:
		query = func(hi string) url.Values { return githubTestsRelistQueryAt(claim, late, hi) }
		interval = githubTestsArtifactInterval(claim, true, late)
	case listing.reports:
		query = func(hi string) url.Values { return githubTestsArtifactRunQueryAt(claim, hi) }
		interval = githubTestsArtifactInterval(claim, false, late)
	case listing.deep:
		query = func(hi string) url.Values { return githubTestsLateRerunRunsQuery(claim, lookback, late, hi) }
		interval = githubTestsRunsInterval(claim, true, lookback, late)
	}
	walk := githubTestsListingWalk{ctx: context.Background(), client: client,
		path: providerRelativePath(client, "repos", owner, repository) + "/actions/runs"}
	frontier, err := githubTestsReplayListing(walk, query, interval, !listing.reports, budget)
	if err != nil {
		t.Fatal(err)
	}
	return frontier
}

// The replay is the walk: on a static listing, githubTestsReplayListing enters the pages a fresh walk of the listing
// enters, lists the runs it lists (a re-listed cut second counted again) and, at a page-budget stop, ends at the walk's
// anchor. Shapes: peels, a stuck second at the listing's top, a saturated second after a peel, the listing's end.
func TestGitHubTestsListingReplayFrontierEqualsFreshWalk(t *testing.T) {
	peelReplaySkipRace(t)
	type shape struct {
		name            string
		count, from, to int
		pages           int
	}
	shapes := []shape{
		{"peel", 2500, 0, 0, 11},
		{"stuck", 1200, 1, 1200, 25},
		{"saturated", 2000, 901, 2000, 25},
		{"listing_end", 500, 0, 0, 11},
	}
	for _, listing := range deletionShiftListings() {
		for _, sh := range shapes {
			for _, coalesce := range []bool{false, true} {
				name := fmt.Sprintf("%s/%s/%s", listing.name, sh.name, deletionShiftFlag(coalesce))
				t.Run(name, func(t *testing.T) {
					runs := peelReplayRuns(listing, sh.count, sh.from, sh.to)
					handler := peelReplayHandler(listing, sh.pages, coalesce)
					handler.noListingReplay = true
					fake := newListingFake(t, slices.Clone(runs))
					listed, pages, anchor := 0, 0, ""
					err := handler.CollectChunks(context.Background(), listing.claim, providerfoundation.Credential{},
						githubTestsClient(t, fakehttp.Client(deletionShiftDoer(t, fake, listing.reports))),
						time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "", func(emission ChunkRouteEmission) error {
							if emission.Barrier || emission.Final {
								return nil
							}
							before, beforeErr := decodeGitHubTestsChunkCursor(emission.CursorBefore)
							after, afterErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
							if beforeErr != nil || afterErr != nil {
								t.Fatal(beforeErr, afterErr)
							}
							if !listing.in(before) {
								return nil
							}
							listed++
							pages = max(pages, peelReplayCounter(listing, before), peelReplayCounter(listing, after))
							if after.AnchorID != "" {
								anchor = after.AnchorID
							}
							return nil
						})
					if err != nil {
						t.Fatal(err)
					}
					frontier := peelReplayReplay(t, listing, handler, deletionShiftDoer(t, newListingFake(t, slices.Clone(runs)), listing.reports), sh.pages)
					order := make([]string, 0, len(frontier.runs))
					for id := range frontier.runs {
						order = append(order, id)
					}
					sort.Slice(order, func(i, j int) bool {
						a, b := frontier.runs[order[i]], frontier.runs[order[j]]
						return a.second > b.second || a.second == b.second && a.ordinal < b.ordinal
					})
					last, previous := order[len(order)-1], order[max(0, len(order)-2)]
					t.Logf("CELL differential/%s walk pages=%d listed=%d anchor=%s replay pages=%d listed=%d last=%s",
						name, pages, listed, anchor, frontier.pages, frontier.listed, last)
					if listed == 0 || frontier.pages != pages || frontier.listed != listed {
						t.Errorf("replay pages=%d listed=%d, fresh walk pages=%d listed=%d", frontier.pages, frontier.listed, pages, listed)
					}
					// At the page budget the walk's anchor is its last listed run; at a listing end (or a stuck range) the range
					// end clears it after the last run, so the anchor is the run before it.
					if want := previous; pages == sh.pages && last != anchor || pages != sh.pages && want != anchor {
						t.Errorf("the replay ends at run %s (before it %s), the fresh walk's anchor is %s", last, previous, anchor)
					}
				})
			}
		}
	}
}

// peelReplayRoute runs one route-mode shape through a peelReplayDoer (yield at the first cursor at, delete, resume) and the
// survivors' uninterrupted walk through a plain doer, both under handler.
func peelReplayRoute(
	t *testing.T, listing deletionShiftListing, handler GitHubTestsRouteHandler, budget int, runs []listingRun,
	at func(githubTestsChunkCursor) bool, setup func(*listingFake, *peelReplayDoer), deleted ...int,
) (deletionShiftOutcome, *listingFake, *peelReplayDoer) {
	t.Helper()
	fake := newListingFake(t, slices.Clone(runs))
	doer := newPeelReplayDoer(deletionShiftDoer(t, fake, listing.reports), budget)
	if setup != nil {
		setup(fake, doer)
	}
	in := func(c githubTestsChunkCursor) bool { return listing.in(c) && at(c) }
	final := deletionShiftWalk(t, handler, listing.claim, doer, in, func(githubTestsChunkCursor) { resumeDeletionDelete(fake, deleted...) }, nil)
	survivors := newListingFake(t, slices.Clone(fake.runs))
	plain := handler
	plain.noListingReplay = false
	deletionShiftPlainWalk(t, plain, listing.claim, deletionShiftDoer(t, survivors, listing.reports))
	var unread []int
	for id := range listing.reads(survivors) {
		if listing.reads(fake)[id] == 0 {
			unread = append(unread, id)
		}
	}
	sort.Ints(unread)
	return deletionShiftOutcome{unread: unread, watermark: final.Watermark != nil, incomplete: deletionShiftIncomplete(t, final.Result)}, fake, doer
}

func peelReplaySecondRangeAt(c githubTestsChunkCursor) bool {
	return peelReplaySecondRange(c) && c.RangeItems == 2 && c.Index > 0
}

// A replay that does not finish grants nothing and returns no error: the walk records the page-budget stop as the release
// before it (the same survivors unread, the same observation). The replay fails on its first request, on its second page,
// or does not start: the rate floor (remaining 1,005 of 5,000: floor 1,000 < 1,005 - 11 fails), no rate headers.
func TestGitHubTestsPeelReplayUnfinishedReplayRecordsTheStop(t *testing.T) {
	peelReplaySkipRace(t)
	listing := deletionShiftListings()[0]
	runs := peelReplayRuns(listing, 2150, 0, 0)
	for _, coalesce := range []bool{false, true} {
		handler := peelReplayHandler(listing, 12, coalesce)
		off := handler
		off.noListingReplay = true
		main, _, _ := peelReplayRoute(t, listing, off, 12, runs, peelReplaySecondRangeAt, nil, 2130)
		for _, cell := range []struct {
			name    string
			setup   func(*listingFake, *peelReplayDoer)
			replays int
		}{
			{"first_request", func(_ *listingFake, doer *peelReplayDoer) {
				doer.fail = func(index, _ int) bool { return index == 0 }
			}, 1},
			{"mid_replay", func(_ *listingFake, doer *peelReplayDoer) { doer.failPage2 = true }, 1},
			{"rate_floor", func(fake *listingFake, _ *peelReplayDoer) { fake.remaining = 1005 }, 0},
			{"no_rate_headers", func(fake *listingFake, _ *peelReplayDoer) { fake.noHeaders = true }, 0},
		} {
			name := fmt.Sprintf("unfinished/%s/%s", cell.name, deletionShiftFlag(coalesce))
			t.Run(name, func(t *testing.T) {
				outcome, _, doer := peelReplayRoute(t, listing, handler, 12, runs, peelReplaySecondRangeAt, cell.setup, 2130)
				t.Logf("CELL %s %s replays=%d main_unread=%d", name, outcome, doer.replays(), len(main.unread))
				if len(main.unread) == 0 {
					t.Fatal("premise: the release before leaves no survivor unread on this shape")
				}
				if !slices.Equal(outcome.unread, main.unread) || !slices.Equal(outcome.incomplete, main.incomplete) {
					t.Errorf("unread=%v incomplete=%v, the release before: unread=%v incomplete=%v", outcome.unread, outcome.incomplete, main.unread, main.incomplete)
				}
				if doer.replays() > cell.replays {
					t.Errorf("replays=%d, want at most %d", doer.replays(), cell.replays)
				}
			})
		}
	}
}

// peelReplayYieldingWalk walks the route to its final emission, yielding (a new invocation) after every `every` run
// emissions; observe sees each emitted cursor and may ask for a yield now (then afterYield runs before the resume).
func peelReplayYieldingWalk(
	t *testing.T, handler GitHubTestsRouteHandler, claim Claim, doer providerfoundation.HTTPDoer, every int,
	observe func(githubTestsChunkCursor) bool, afterYield func(),
) CompleteRouteBatch {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(doer))
	resume, stop := "", errors.New("yield")
	for pass := 1; pass < 2000; pass++ {
		var final *CompleteRouteBatch
		emitted, asked := 0, false
		err := handler.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
			time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume, func(emission ChunkRouteEmission) error {
				if emission.Barrier {
					return nil
				}
				resume = emission.CursorAfter
				if emission.Final {
					batch := emission.Batch
					final = &batch
					return nil
				}
				after, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				emitted++
				if observe(after) {
					asked = true
					return stop
				}
				if emitted >= every {
					return stop
				}
				return nil
			})
		if final != nil {
			return *final
		}
		if !errors.Is(err, stop) {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if asked && afterYield != nil {
			afterYield()
		}
	}
	t.Fatal("the walk never reached a final emission")
	return CompleteRouteBatch{}
}

// F1: deletions go on through the whole walk (once the walk is past its first range, 30 runs of that range go after every
// listing page: the surviving runs' walk reaches deeper and deeper, and the active range does not move) and the walk yields
// every 16 runs. The walk ends; its page counter never falls and stops at budget + X at most; a listing is replayed at most
// X + 1 times (each grant is followed by a charged page; at the cap no replay runs).
func TestGitHubTestsPeelReplayGrantsEndUnderOngoingDeletions(t *testing.T) {
	peelReplaySkipRace(t)
	for _, listing := range deletionShiftListings()[:2] {
		for _, coalesce := range []bool{false, true} {
			name := fmt.Sprintf("ongoing/%s/%s", listing.name, deletionShiftFlag(coalesce))
			t.Run(name, func(t *testing.T) {
				const pages = 21
				budget, limit := peelReplayBudget(listing, pages)
				fake := newListingFake(t, peelReplayRuns(listing, 2600, 0, 0))
				doer := newPeelReplayDoer(deletionShiftDoer(t, fake, listing.reports), budget)
				pastFirst, next := false, 2600
				doer.afterList = func() {
					for range 30 {
						if pastFirst && next > 1700 {
							resumeDeletionDelete(fake, next)
							next--
						}
					}
				}
				highest, falls, capped := 0, 0, -1
				peelReplayYieldingWalk(t, peelReplayHandler(listing, pages, coalesce), listing.claim, doer, 16, func(c githubTestsChunkCursor) bool {
					if listing.in(c) && peelReplayCounter(listing, c) >= limit && capped < 0 {
						doer.mu.Lock()
						capped = doer.served
						doer.mu.Unlock()
					}
					if listing.in(c) {
						pastFirst = pastFirst || c.RangeHi != ""
						counter := peelReplayCounter(listing, c)
						if counter < highest {
							falls++
						}
						highest = max(highest, counter)
					}
					return false
				}, nil)
				t.Logf("CELL %s counter=%d budget=%d limit=%d replays=%d falls=%d", name, highest, budget, limit, doer.replays(), falls)
				if highest <= budget {
					t.Fatalf("premise: no grant (counter %d, budget %d)", highest, budget)
				}
				if highest > limit || falls != 0 {
					t.Errorf("page counter reached %d (fell %d times), want at most %d and never falling", highest, falls, limit)
				}
				if doer.replays() > limit-budget+1 {
					t.Errorf("replays=%d, want at most X + 1 = %d", doer.replays(), limit-budget+1)
				}
				if capped < 0 {
					t.Fatalf("premise: the counter never reached the cap %d", limit)
				}
				for _, at := range doer.replayAt {
					if at > capped {
						t.Errorf("a replay ran after the counter reached the cap (listing page %d, cap at page %d)", at, capped)
					}
				}
			})
		}
	}
}

// F1: a grant needs the anchor to move. The two granted pages come back empty (a next link, no runs): the walk stops again
// with the anchor of the grant, and that stop grants nothing more (the pages charged stop at budget + 2, not at the cap).
func TestGitHubTestsPeelReplayGrantNeedsTheAnchorToMove(t *testing.T) {
	peelReplaySkipRace(t)
	listing := deletionShiftListings()[0]
	const pages = 21
	budget, limit := peelReplayBudget(listing, pages)
	for _, coalesce := range []bool{false, true} {
		t.Run(deletionShiftFlag(coalesce), func(t *testing.T) {
			fake := newListingFake(t, peelReplayRuns(listing, 2600, 0, 0))
			doer := newPeelReplayDoer(fake, budget)
			doer.emptyPage = func(index, page int) bool { return index == 2 && (page == 2 || page == 3) }
			deleted, highest := false, 0
			peelReplayYieldingWalk(t, peelReplayHandler(listing, pages, coalesce), listing.claim, doer, 1<<30, func(c githubTestsChunkCursor) bool {
				highest = max(highest, c.RunPages)
				if !deleted && listing.in(c) && peelReplaySecondRangeAt(c) {
					deleted = true
					return true
				}
				return false
			}, func() { resumeDeletionDelete(fake, 2580) })
			t.Logf("CELL anchor_progress/%s run_pages=%d budget=%d limit=%d replays=%d", deletionShiftFlag(coalesce), highest, budget, limit, doer.replays())
			if highest != budget+2 {
				t.Errorf("run_pages=%d, want %d: the grant of 2 empty pages and no grant at the same anchor", highest, budget+2)
			}
		})
	}
}

// F4: request cost. With no deletion, the walk with the replay charges, lists and reads exactly what the walk without it
// does, and makes one replay of each listing that stopped at its budget more (11 runs + 11 artifacts listing requests).
// With two deletions (one run newer than the cut, then 150 after the first grant, in a later invocation: two grants), the
// extra listing requests
// stay within 2 x replays x budget + X, replays within X + 1, and no survivor is left unread.
func TestGitHubTestsPeelReplayRequestCost(t *testing.T) {
	peelReplaySkipRace(t)
	listing := deletionShiftListings()[0]
	for _, coalesce := range []bool{false, true} {
		t.Run("no_deletion/"+deletionShiftFlag(coalesce), func(t *testing.T) {
			runs := peelReplayRuns(listing, 2500, 0, 0)
			measure := func(replay bool) *listingFake {
				handler := peelReplayHandler(listing, 11, coalesce)
				handler.noListingReplay = !replay
				fake := newListingFake(t, slices.Clone(runs))
				deletionShiftPlainWalk(t, handler, listing.claim, fake)
				return fake
			}
			on, off := measure(true), measure(false)
			t.Logf("CELL request_cost/no_deletion/%s requests on=%d off=%d list_pages on=%d off=%d", deletionShiftFlag(coalesce),
				on.requests, off.requests, on.listPages, off.listPages)
			if !mapsEqualInt(on.jobGets, off.jobGets) || !mapsEqualInt(on.artifactGets, off.artifactGets) {
				t.Errorf("the replay changed the runs read: jobs %d vs %d, artifacts %d vs %d", len(on.jobGets), len(off.jobGets), len(on.artifactGets), len(off.artifactGets))
			}
			if on.requests-off.requests != 22 || on.listPages-off.listPages != 22 {
				t.Errorf("requests on-off=%d list pages on-off=%d, want 22 (one replay of 11 pages per listing)", on.requests-off.requests, on.listPages-off.listPages)
			}
		})
		t.Run("two_grants/"+deletionShiftFlag(coalesce), func(t *testing.T) {
			const pages = 21
			budget, limit := peelReplayBudget(listing, pages)
			runs := peelReplayRuns(listing, 2600, 0, 0)
			measure := func(replay bool) (*listingFake, *peelReplayDoer, []int, int) {
				handler := peelReplayHandler(listing, pages, coalesce)
				handler.noListingReplay = !replay
				fake := newListingFake(t, slices.Clone(runs))
				doer := newPeelReplayDoer(fake, budget)
				var second []int
				for id := 2579; id > 2429; id-- {
					second = append(second, id)
				}
				deletions, highest := [][]int{{2580}, second}, 0
				peelReplayYieldingWalk(t, handler, listing.claim, doer, 1<<30, func(c githubTestsChunkCursor) bool {
					if !listing.in(c) {
						return false
					}
					highest = max(highest, c.RunPages)
					switch {
					case len(deletions) == 2 && peelReplaySecondRangeAt(c):
						return true
					case len(deletions) == 1 && c.RunPages > budget:
						return true
					}
					return false
				}, func() {
					if len(deletions) > 0 {
						resumeDeletionDelete(fake, deletions[0]...)
						deletions = deletions[1:]
					}
				})
				survivors := newListingFake(t, slices.Clone(fake.runs))
				deletionShiftPlainWalk(t, peelReplayHandler(listing, pages, coalesce), listing.claim, survivors)
				var unread []int
				for id := range survivors.jobGets {
					if fake.jobGets[id] == 0 {
						unread = append(unread, id)
					}
				}
				return fake, doer, unread, highest
			}
			on, doer, unread, highest := measure(true)
			off, _, mainUnread, _ := measure(false)
			extra := on.listPages - off.listPages
			t.Logf("CELL request_cost/two_grants/%s unread=%d main_unread=%d replays=%d counter=%d extra_list_pages=%d", deletionShiftFlag(coalesce),
				len(unread), len(mainUnread), doer.replays(), highest, extra)
			if len(mainUnread) == 0 || doer.replays() < 2 || highest <= budget+2 {
				t.Fatalf("premise: main leaves %d unread, %d replays, counter %d (want a gap and two grants)", len(mainUnread), doer.replays(), highest)
			}
			if len(unread) != 0 {
				t.Errorf("%d survivors unread, want 0", len(unread))
			}
			if doer.replays() > limit-budget+1 || extra > 2*doer.replays()*budget+(limit-budget) {
				t.Errorf("replays=%d extra listing pages=%d, want replays <= %d and extra <= 2 x replays x budget + X", doer.replays(), extra, limit-budget+1)
			}
		})
	}
}

func mapsEqualInt(a, b map[int]int) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

// F3: the page counter only rises, past the budget by a grant. A release without the replay resuming such a cursor (a
// rollback) stops the listing at once on its own budget stop: no error, no runs page charged, the observation recorded.
func TestGitHubTestsPeelReplayRollbackResumesACounterPastTheBudget(t *testing.T) {
	peelReplaySkipRace(t)
	listing := deletionShiftListings()[0]
	const pages = 11
	for _, coalesce := range []bool{false, true} {
		t.Run(deletionShiftFlag(coalesce), func(t *testing.T) {
			fake := newListingFake(t, peelReplayRuns(listing, 1500, 0, 0))
			client := githubTestsClient(t, fakehttp.Client(fake))
			handler := peelReplayHandler(listing, pages, coalesce)
			stop := errors.New("yield")
			resume, deleted := "", false
			var past githubTestsChunkCursor
			for pass := 1; past.RunPages <= pages; pass++ {
				if pass > 50 {
					t.Fatal("premise: the walk never passed its page budget")
				}
				err := handler.CollectChunks(context.Background(), listing.claim, providerfoundation.Credential{}, client,
					time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume, func(emission ChunkRouteEmission) error {
						if emission.Barrier || emission.Final {
							return nil
						}
						resume = emission.CursorAfter
						after, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
						if decodeErr != nil {
							t.Fatal(decodeErr)
						}
						if !deleted && listing.in(after) && peelReplaySecondRangeAt(after) {
							deleted = true
							return stop
						}
						if listing.in(after) && after.RunPages > pages {
							past = after
							return stop
						}
						return nil
					})
				if err != nil && !errors.Is(err, stop) {
					t.Fatal(err)
				}
				if deleted && pass == 1 {
					resumeDeletionDelete(fake, 1480)
				}
			}
			off := handler
			off.noListingReplay = true
			var final *CompleteRouteBatch
			charged := false
			err := off.CollectChunks(context.Background(), listing.claim, providerfoundation.Credential{}, client,
				time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume, func(emission ChunkRouteEmission) error {
					if emission.Final {
						batch := emission.Batch
						final = &batch
						return nil
					}
					if after, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter); decodeErr == nil &&
						after.Phase == githubTestsRunsPhase && after.RunPages > past.RunPages {
						charged = true
					}
					return nil
				})
			incomplete := []string(nil)
			if final != nil {
				incomplete = deletionShiftIncomplete(t, final.Result)
			}
			t.Logf("CELL rollback/%s run_pages=%d err=%v charged=%v incomplete=%v", deletionShiftFlag(coalesce), past.RunPages, err, charged, incomplete)
			if err != nil || final == nil || charged || !slices.Contains(incomplete, "run_inventory/page_budget_exhausted") {
				t.Errorf("rollback resume: err=%v final=%v charged=%v incomplete=%v, want main's budget stop", err, final != nil, charged, incomplete)
			}
		})
	}
}

// F5: the walk's own second. GitHub does not order runs inside a second, so it earns one page at most, inside a range,
// when the frontier walk listed more of that second's runs than up to the walk's anchor; never a computed distance.
func TestGitHubTestsPeelReplayGrantAtTheWalksSecond(t *testing.T) {
	second := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	frontier := githubTestsFrontier{seconds: map[int64]int{}, runs: map[string]githubTestsFrontierRun{}}
	add := func(at time.Time, ids ...string) {
		for _, id := range ids {
			frontier.runs[id] = githubTestsFrontierRun{second: at.Unix(), ordinal: frontier.seconds[at.Unix()]}
			frontier.seconds[at.Unix()]++
		}
	}
	add(second.Add(time.Second), "9")
	add(second, "5", "4", "3")
	inRange := func(anchor string) githubTestsChunkCursor {
		return githubTestsChunkCursor{RangeItems: 7, RangeOldest: second.Format(time.RFC3339), AnchorID: anchor}
	}
	for _, cell := range []struct {
		name          string
		cursor        githubTestsChunkCursor
		behind, pages int
	}{
		{"tie_behind_in_order", inRange("4"), 0, 1},
		{"tie_at_the_frontier", inRange("3"), 0, 0},
		{"tie_anchor_not_listed", inRange("77"), 0, 0},
		{"tie_at_a_range_start", githubTestsChunkCursor{RangeHi: second.Format(time.RFC3339), AnchorID: "4"}, 0, 0},
		{"walk_past_the_frontier", githubTestsChunkCursor{RangeItems: 7, RangeOldest: second.Add(-time.Hour).Format(time.RFC3339), AnchorID: "1"}, 0, 0},
		{"behind_older_seconds", githubTestsChunkCursor{RangeItems: 7, RangeOldest: second.Add(2 * time.Second).Format(time.RFC3339), AnchorID: "1"}, 4, 2},
	} {
		behind, pages := frontier.grant(cell.cursor)
		if behind != cell.behind || pages != cell.pages {
			t.Errorf("%s: behind=%d pages=%d, want %d and %d", cell.name, behind, pages, cell.behind, cell.pages)
		}
	}
}
