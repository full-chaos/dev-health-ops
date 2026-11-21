package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type replayGuardDoer func(*http.Request) (*http.Response, error)

func (doer replayGuardDoer) Do(request *http.Request) (*http.Response, error) { return doer(request) }

func replayGuardQuery(listing deletionShiftListing, handler GitHubTestsRouteHandler, hi string) url.Values {
	switch listing.name {
	case "deep_runs":
		return githubTestsLateRerunRunsQuery(listing.claim, handler.refreshLookback(), handler.lateRerunLookback(), hi)
	case "deep_reports":
		return githubTestsRelistQueryAt(listing.claim, handler.lateRerunLookback(), hi)
	case "artifacts":
		return githubTestsArtifactRunQueryAt(listing.claim, hi)
	}
	return githubTestsRunsQuery(listing.claim, handler.refreshLookback(), hi)
}

// replayGuardCheck is one listing replay Info line.
type replayGuardCheck struct {
	Msg, Listing, Status        string
	Behind, Granted, Pages, Cap int
	ReplayRequests              int `json:"replay_requests"`
	ReplayPages                 int `json:"replay_pages"`
}

func (check replayGuardCheck) String() string {
	return fmt.Sprintf("%d/%d:%s(%d,%d)", check.Pages, check.Cap, check.Status, check.Behind, check.Granted)
}

func replayGuardLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

func replayGuardChecks(logs *bytes.Buffer, listing string) []replayGuardCheck {
	var checks []replayGuardCheck
	for _, line := range strings.Split(logs.String(), "\n") {
		var check replayGuardCheck
		if json.Unmarshal([]byte(line), &check) == nil &&
			check.Msg == "provider inventory page budget stop checked by the listing replay" && check.Listing == listing {
			checks = append(checks, check)
		}
	}
	return checks
}

func replayGuardStatuses(checks []replayGuardCheck) []string {
	out := make([]string, 0, len(checks))
	for _, check := range checks {
		out = append(out, check.String())
	}
	return out
}

func replayGuardUnread(listing deletionShiftListing, fresh, resumed *listingFake) []int {
	var unread []int
	for id := range listing.reads(fresh) {
		if listing.reads(resumed)[id] == 0 {
			unread = append(unread, id)
		}
	}
	sort.Ints(unread)
	return unread
}

// replayGuardFresh is the survivors' uninterrupted walk without the replay.
func replayGuardFresh(t *testing.T, listing deletionShiftListing, handler GitHubTestsRouteHandler, runs []listingRun, wrap func(*listingFake) providerfoundation.HTTPDoer) *listingFake {
	t.Helper()
	fresh := newListingFake(t, slices.Clone(runs))
	plain := handler
	plain.noListingReplay = true
	deletionShiftPlainWalk(t, plain, listing.claim, wrap(fresh))
	return fresh
}

// A grant spent, then more runs the walk already listed are deleted (outside the active range): the next stop replays
// the listing again from its start, so its frontier is the survivors' one now, and the walk gets the pages to reach it
// (cap and progress first). 0 unread, no deep watermark over an unread survivor, at most X + 1 replays.
func TestGitHubTestsPeelReplayFreshReplayBeforeEachGrant(t *testing.T) {
	peelReplaySkipRace(t)
	for _, listing := range deletionShiftListings() {
		for _, pages := range []int{21, 28, 100} {
			if pages == 100 && listing.deep {
				continue
			}
			t.Run(fmt.Sprintf("%s/budget_%d", listing.name, pages), func(t *testing.T) {
				count := pages*100 + 500
				handler := peelReplayHandler(listing, pages, false)
				fake := newListingFake(t, peelReplayRuns(listing, count, 0, 0))
				base := deletionShiftDoer(t, fake, listing.reports)
				start := replayGuardQuery(listing, handler, "").Get("created")
				starts := 0
				doer := replayGuardDoer(func(request *http.Request) (*http.Response, error) {
					query := request.URL.Query()
					if request.URL.Path == "/repos/acme/api/actions/runs" && query.Get("created") == start && query.Get("page") == "" {
						starts++
					}
					return base.Do(request)
				})
				logs := replayGuardLog(t)
				deletedAgain := false
				final := peelReplayYieldingWalk(t, handler, listing.claim, doer, 1<<30, func(c githubTestsChunkCursor) bool {
					if !listing.in(c) {
						return false
					}
					if !deletedAgain && peelReplayCounter(listing, c) == pages+1 {
						deletedAgain = true
						var ids []int
						for id := count - 1; id >= count-224; id-- {
							ids = append(ids, id)
						}
						resumeDeletionDelete(fake, ids...)
					}
					return starts == 1 && peelReplaySecondRangeAt(c) && !fake.gone[count]
				}, func() { resumeDeletionDelete(fake, count) })
				checks := replayGuardChecks(logs, listing.name)
				fresh := replayGuardFresh(t, listing, handler, fake.runs, func(f *listingFake) providerfoundation.HTTPDoer {
					return deletionShiftDoer(t, f, listing.reports)
				})
				unread := replayGuardUnread(listing, fresh, fake)
				limit := pages + min((pages+9)/10, 10)
				t.Logf("CELL fresh_replay/%s/budget_%d starts=%d checks=%v unread=%d watermark=%v", listing.name, pages, starts,
					replayGuardStatuses(checks), len(unread), final.Watermark != nil)
				if !deletedAgain {
					t.Fatal("premise: the walk never spent a grant")
				}
				if len(unread) != 0 {
					t.Errorf("%d survivors unread (watermark advanced=%v), want 0", len(unread), final.Watermark != nil)
				}
				if starts-1 > limit-pages+1 {
					t.Errorf("replays=%d, want at most X + 1 = %d", starts-1, limit-pages+1)
				}
			})
		}
	}
}

// The walk's own created second is compared by run id, not by order: GitHub does not order a second's runs. A survivor of
// that second the replay listed and the walk did not earns pages (the tie order changed after a deletion; the anchor itself
// deleted; the anchor cleared by a peel at the stop); the same ids in another order with no deletion earn none.
func TestGitHubTestsPeelReplayBoundarySecondByRunID(t *testing.T) {
	peelReplaySkipRace(t)
	for _, listing := range deletionShiftListings() {
		for _, shape := range []string{"reordered", "peel_boundary", "anchor_deleted"} {
			t.Run(listing.name+"/"+shape, func(t *testing.T) {
				count, pages, from, to := 1500, 11, 1098, 1100
				if shape == "peel_boundary" {
					count, pages, from, to = 2500, 20, 1999, 2000
				}
				if shape == "anchor_deleted" {
					to = 1101
				}
				runs := peelReplayRuns(listing, count, from, to)
				handler := peelReplayHandler(listing, pages, false)
				fake := newListingFake(t, slices.Clone(runs))
				wrap := func(fake *listingFake) providerfoundation.HTTPDoer {
					base := deletionShiftDoer(t, fake, listing.reports)
					if shape != "reordered" {
						return base
					}
					shifted := replayGuardQuery(listing, handler, runs[count-1001].created.UTC().Format(time.RFC3339)).Get("created")
					return replayGuardDoer(func(request *http.Request) (*http.Response, error) {
						response, err := base.Do(request)
						if err != nil || request.URL.Path != "/repos/acme/api/actions/runs" ||
							request.URL.Query().Get("created") != shifted || request.URL.Query().Get("page") != "" {
							return response, err
						}
						return replayGuardSwapTail(t, response), nil
					})
				}
				logs := replayGuardLog(t)
				removed := false
				final := peelReplayYieldingWalk(t, handler, listing.claim, wrap(fake), 1<<30, func(c githubTestsChunkCursor) bool {
					if !listing.in(c) {
						return false
					}
					if peelReplayCounter(listing, c) == pages && c.Index == 0 && !removed {
						removed = true
						if shape == "anchor_deleted" {
							// The anchor goes while the walk is paused at its budget stop: the resume sees it.
							resumeDeletionDelete(fake, 402)
							return true
						}
					}
					return peelReplaySecondRangeAt(c) && !fake.gone[count]
				}, func() { resumeDeletionDelete(fake, count) })
				checks := replayGuardChecks(logs, listing.name)
				fresh := replayGuardFresh(t, listing, handler, fake.runs, wrap)
				unread := replayGuardUnread(listing, fresh, fake)
				t.Logf("CELL boundary_second/%s/%s checks=%v unread=%v watermark=%v", listing.name, shape, replayGuardStatuses(checks), unread, final.Watermark != nil)
				if !removed {
					t.Fatal("premise: the walk never reached its budget")
				}
				if len(unread) != 0 {
					t.Errorf("survivors %v unread, want none", unread)
				}
			})
		}
		t.Run(listing.name+"/reordered_no_deletion", func(t *testing.T) {
			const count, pages = 1500, 11
			runs := peelReplayRuns(listing, count, 1097, 1099)
			handler := peelReplayHandler(listing, pages, false)
			fake := newListingFake(t, slices.Clone(runs))
			base := deletionShiftDoer(t, fake, listing.reports)
			peeled := replayGuardQuery(listing, handler, runs[count-1000].created.UTC().Format(time.RFC3339)).Get("created")
			visits := 0
			doer := replayGuardDoer(func(request *http.Request) (*http.Response, error) {
				response, err := base.Do(request)
				if err != nil || request.URL.Path != "/repos/acme/api/actions/runs" ||
					request.URL.Query().Get("created") != peeled || request.URL.Query().Get("page") != "" {
					return response, err
				}
				visits++
				if visits != 2 {
					return response, nil
				}
				return replayGuardSwapTail(t, response), nil
			})
			logs := replayGuardLog(t)
			deletionShiftPlainWalk(t, handler, listing.claim, doer)
			checks := replayGuardChecks(logs, listing.name)
			t.Logf("CELL boundary_second/%s/reordered_no_deletion visits=%d checks=%v", listing.name, visits, replayGuardStatuses(checks))
			if visits < 2 || len(checks) == 0 {
				t.Fatalf("premise: the replay never re-listed the peeled page (visits %d, checks %d)", visits, len(checks))
			}
			for _, check := range checks {
				if check.Granted != 0 {
					t.Errorf("granted %d pages with no deletion (%s), want none", check.Granted, check)
				}
			}
		})
	}
}

// replayGuardSwapTail serves a full first page with the order of its last three runs (one created second) changed.
func replayGuardSwapTail(t *testing.T, response *http.Response) *http.Response {
	t.Helper()
	var envelope struct {
		TotalCount int               `json:"total_count"`
		Items      []json.RawMessage `json:"workflow_runs"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if len(envelope.Items) != 100 {
		t.Fatalf("premise: first page length=%d", len(envelope.Items))
	}
	items := envelope.Items
	items[97], items[98], items[99] = items[99], items[97], items[98]
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	response.Body = io.NopCloser(bytes.NewReader(encoded))
	response.ContentLength = int64(len(encoded))
	response.Header.Del("Content-Length")
	return response
}

type replayGuardCancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (body replayGuardCancelOnClose) Close() error {
	body.cancel()
	return body.ReadCloser.Close()
}

// A replay grants only when it finished: the rate floor is checked on every replay response and the context once more
// before the frontier is taken. A floor crossed on the 2nd or the last replay page, or a cancel after the last page's body,
// grants nothing, and the walk records the page-budget stop as the release before does. Under the floor the walk goes on
// to its final emission. Under a cancelled context the stop is recorded on the cursor; the walk then ends at its next
// request with context canceled (a runs listing: the artifacts listing's first request), as the release before's walk does
// after the same stop, or, when no request follows (a reports listing is the last listing), it emits its final batch with
// the stop recorded.
func TestGitHubTestsPeelReplayGrantsOnlyAfterAFinishedReplay(t *testing.T) {
	peelReplaySkipRace(t)
	for _, listing := range deletionShiftListings() {
		for _, pages := range []int{11, 20} {
			for _, condition := range []string{"rate_second_page", "rate_last_page", "context_last_page"} {
				t.Run(fmt.Sprintf("%s/budget_%d/%s", listing.name, pages, condition), func(t *testing.T) {
					count := pages*100 + 400
					handler := peelReplayHandler(listing, pages, false)
					runs := peelReplayRuns(listing, count, 0, 0)
					fake := newListingFake(t, slices.Clone(runs))
					base := deletionShiftDoer(t, fake, listing.reports)
					start := replayGuardQuery(listing, handler, "").Get("created")
					last := replayGuardQuery(listing, handler, fake.runs[count-1001].created.UTC().Format(time.RFC3339)).Get("created")
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					starts, replayPages, hit := 0, 0, false
					doer := replayGuardDoer(func(request *http.Request) (*http.Response, error) {
						response, err := base.Do(request)
						if err != nil || request.URL.Path != "/repos/acme/api/actions/runs" {
							return response, err
						}
						query := request.URL.Query()
						if query.Get("created") == start && query.Get("page") == "" {
							starts++
						}
						if starts != 2 || (query.Get("created") != start && query.Get("created") != last) {
							return response, nil
						}
						replayPages++
						lastPage := query.Get("created") == last && (pages == 11 && query.Get("page") == "" || pages == 20 && query.Get("page") == "10")
						if condition == "rate_second_page" && replayPages == 2 || condition == "rate_last_page" && lastPage {
							hit = true
							fake.remaining = 999
							response.Header.Set("X-RateLimit-Limit", strconv.Itoa(fake.limit))
							response.Header.Set("X-RateLimit-Remaining", strconv.Itoa(fake.remaining))
						}
						if condition == "context_last_page" && lastPage {
							hit = true
							response.Body = replayGuardCancelOnClose{response.Body, cancel}
						}
						return response, nil
					})
					logs := replayGuardLog(t)
					stop := errors.New("yield")
					client := githubTestsClient(t, doer)
					now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
					resume := ""
					err := handler.CollectChunks(ctx, listing.claim, providerfoundation.Credential{}, client, now, "", func(e ChunkRouteEmission) error {
						if e.Barrier {
							return nil
						}
						c, decodeErr := decodeGitHubTestsChunkCursor(e.CursorAfter)
						if decodeErr != nil {
							t.Fatal(decodeErr)
						}
						if listing.in(c) && peelReplaySecondRangeAt(c) {
							resume = e.CursorAfter
							return stop
						}
						return nil
					})
					if !errors.Is(err, stop) {
						t.Fatalf("premise: no pause: %v", err)
					}
					resumeDeletionDelete(fake, count)
					var final *CompleteRouteBatch
					charged := 0
					err = handler.CollectChunks(ctx, listing.claim, providerfoundation.Credential{}, client, now, resume, func(e ChunkRouteEmission) error {
						if e.Final {
							batch := e.Batch
							final = &batch
						}
						if c, decodeErr := decodeGitHubTestsChunkCursor(e.CursorAfter); decodeErr == nil && listing.in(c) {
							charged = max(charged, peelReplayCounter(listing, c))
						}
						return nil
					})
					checks := replayGuardChecks(logs, listing.name)
					t.Logf("CELL completion/%s/budget_%d/%s replay_pages=%d hit=%v checks=%v final=%v err=%v counter=%d", listing.name, pages, condition,
						replayPages, hit, replayGuardStatuses(checks), final != nil, err, charged)
					if !hit {
						t.Fatal("premise: the guard condition was never served")
					}
					for _, check := range checks {
						if check.Granted != 0 {
							t.Errorf("granted %d pages after an unfinished replay (%s)", check.Granted, check)
						}
					}
					if charged > pages {
						t.Errorf("page counter %d past the budget %d after an unfinished replay", charged, pages)
					}
					if condition == "context_last_page" && !listing.reports {
						if !errors.Is(err, context.Canceled) || final != nil {
							t.Errorf("cancelled walk: err=%v final=%v, want context canceled at the next request and no final emission", err, final != nil)
						}
						return
					}
					if err != nil || final == nil {
						t.Fatalf("floor walk: err=%v final=%v, want the recorded stop and a final emission", err, final != nil)
					}
					component := githubTestsRunInventoryComponent
					switch {
					case listing.name == "deep_runs":
						component = githubTestsLateRerunRunsComponent
					case listing.name == "deep_reports":
						component = githubTestsLateRerunReportsComponent
					case listing.reports:
						component = githubTestsArtifactInventoryComponent
					}
					if incomplete := deletionShiftIncomplete(t, final.Result); !slices.Contains(incomplete, component+"/"+githubTestsPageBudgetCause) {
						t.Errorf("incomplete=%v, want the %s page-budget stop recorded", incomplete, component)
					}
				})
			}
		}
	}
}

// replayGuardFinalCursor walks to the final emission (no yield) and returns its raw cursor and the replay Info lines.
func replayGuardFinalCursor(t *testing.T, listing deletionShiftListing, handler GitHubTestsRouteHandler, runs []listingRun, replay bool, perPage int) (string, githubTestsChunkCursor, []replayGuardCheck) {
	t.Helper()
	handler.noListingReplay = !replay
	fake := newListingFake(t, slices.Clone(runs))
	fake.perPage = perPage
	logs := replayGuardLog(t)
	var raw string
	err := handler.CollectChunks(context.Background(), listing.claim, providerfoundation.Credential{},
		githubTestsClient(t, deletionShiftDoer(t, fake, listing.reports)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(e ChunkRouteEmission) error {
			if e.Final {
				raw = e.CursorAfter
			}
			return nil
		})
	if err != nil || raw == "" {
		t.Fatalf("walk err=%v final=%v", err, raw != "")
	}
	cursor, err := decodeGitHubTestsChunkCursor(raw)
	if err != nil {
		t.Fatal(err)
	}
	var checks []replayGuardCheck
	for _, name := range []string{"runs", "deep_runs", "artifacts", "deep_reports"} {
		checks = append(checks, replayGuardChecks(logs, name)...)
	}
	return raw, cursor, checks
}

// The replay's requests and pages stay out of every counter the cursor serializes: at the decimal edges of requests and
// pages (a replay that would carry them from 9 to 10, 99 to 100, 999 to 1,000) the cursor of a walk whose replays grant
// nothing is byte for byte the cursor of the walk without the replay; with a grant, the cursor's requests are the walk's
// own requests (all requests less the replays' ones the Info lines count).
func TestGitHubTestsPeelReplayCursorHoldsNoReplayCount(t *testing.T) {
	peelReplaySkipRace(t)
	listing := deletionShiftListings()[0]
	for _, edge := range []int{10, 100, 1000} {
		t.Run(fmt.Sprintf("edge_%d", edge), func(t *testing.T) {
			pages := 1
			if edge == 1000 {
				pages = 11
			}
			handler := peelReplayHandler(listing, pages, false)
			since := listing.claim.SinceAt.UTC()
			const count = 2000
			// No run in the window (none read), so the requests grow with the page size: the listing pages and one artifacts
			// request per listed run.
			runs := makeListingRuns(1, count, since.Add(-time.Duration(count+10)*time.Second), time.Second)
			for i := range runs {
				runs[i].updated = runs[i].created
			}
			requestsAt := func(perPage int) int {
				_, off, _ := replayGuardFinalCursor(t, listing, handler, runs, false, perPage)
				return off.Requests
			}
			// The largest page size whose walk stays under the edge: the replays then would carry its counters across.
			lo, hi := 1, 100
			if requestsAt(lo) >= edge {
				t.Fatalf("premise: the smallest walk makes %d requests, not under %d", requestsAt(lo), edge)
			}
			for lo < hi {
				mid := (lo + hi + 1) / 2
				if requestsAt(mid) < edge {
					lo = mid
				} else {
					hi = mid - 1
				}
			}
			found := false
			for perPage := lo; perPage >= max(1, lo-5) && !found; perPage-- {
				rawOff, off, _ := replayGuardFinalCursor(t, listing, handler, runs, false, perPage)
				rawOn, on, checks := replayGuardFinalCursor(t, listing, handler, runs, true, perPage)
				replayed := 0
				for _, check := range checks {
					replayed += check.ReplayRequests
				}
				if off.Requests+max(replayed, on.Requests-off.Requests) < edge {
					continue
				}
				found = true
				t.Logf("CELL cursor_bytes/edge_%d per_page=%d bytes=%d->%d requests=%d->%d pages=%d->%d replay_requests=%d checks=%v", edge, perPage,
					len(rawOff), len(rawOn), off.Requests, on.Requests, off.Pages, on.Pages, replayed, replayGuardStatuses(checks))
				if rawOn != rawOff {
					t.Errorf("the replay changed the cursor: %d -> %d bytes, requests %d -> %d, pages %d -> %d", len(rawOff), len(rawOn),
						off.Requests, on.Requests, off.Pages, on.Pages)
				}
			}
			if !found {
				t.Fatalf("premise: no page size put the walk's requests within the replay's reach of %d", edge)
			}
		})
	}
	t.Run("with_a_grant", func(t *testing.T) {
		const pages = 11
		handler := peelReplayHandler(listing, pages, false)
		fake := newListingFake(t, peelReplayRuns(listing, 1500, 0, 0))
		logs := replayGuardLog(t)
		deleted := false
		final := peelReplayYieldingWalk(t, handler, listing.claim, fake, 1<<30, func(c githubTestsChunkCursor) bool {
			if !deleted && listing.in(c) && peelReplaySecondRangeAt(c) {
				deleted = true
				return true
			}
			return false
		}, func() { resumeDeletionDelete(fake, 1480) })
		var checks []replayGuardCheck
		for _, name := range []string{"runs", "artifacts"} {
			checks = append(checks, replayGuardChecks(logs, name)...)
		}
		replayed, granted := 0, 0
		for _, check := range checks {
			replayed += check.ReplayRequests
			granted += check.Granted
		}
		requests := final.Evidence.Requests
		t.Logf("CELL cursor_bytes/with_a_grant requests=%d fake_requests=%d replay_requests=%d granted=%d checks=%v", requests,
			fake.requests, replayed, granted, replayGuardStatuses(checks))
		if granted == 0 {
			t.Fatalf("premise: granted=%d", granted)
		}
		if replayed == 0 || requests != fake.requests-replayed {
			t.Errorf("cursor requests=%d, want the walk's own: all %d less the replays' %d", requests, fake.requests, replayed)
		}
	})
}

// Named residual: a run deleted inside the active range during one invocation (the anchor at the stop's page end, no
// resume) shifts the next page by its page number, as on the release before. The grant at the stop sees the walk's own
// second short by two ids and lists one page more, but the shifted page passes one survivor; the next stop cannot see it (its
// second is newer than the walk's). Pinned as is: without the replay 2 unread [400 401], with it 1 [401]; the runs and
// artifacts listings record the stop and hold the watermark (the next walk reads it), the deep listings record the stop
// and advance it.
func TestGitHubTestsPeelReplayResidualAnchorDeletedInsideOneInvocation(t *testing.T) {
	peelReplaySkipRace(t)
	for _, listing := range deletionShiftListings() {
		for _, replay := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/replay_%v", listing.name, replay), func(t *testing.T) {
				const count, pages = 1500, 11
				handler := peelReplayHandler(listing, pages, false)
				handler.noListingReplay = !replay
				runs := peelReplayRuns(listing, count, 1098, 1101)
				fake := newListingFake(t, slices.Clone(runs))
				removed := false
				final := peelReplayYieldingWalk(t, handler, listing.claim, deletionShiftDoer(t, fake, listing.reports), 1<<30, func(c githubTestsChunkCursor) bool {
					if !listing.in(c) {
						return false
					}
					if peelReplayCounter(listing, c) == pages && c.Index == 0 && !removed {
						resumeDeletionDelete(fake, 402)
						removed = true
					}
					return peelReplaySecondRangeAt(c) && !fake.gone[count]
				}, func() { resumeDeletionDelete(fake, count) })
				fresh := replayGuardFresh(t, listing, handler, fake.runs, func(f *listingFake) providerfoundation.HTTPDoer {
					return deletionShiftDoer(t, f, listing.reports)
				})
				unread := replayGuardUnread(listing, fresh, fake)
				incomplete := deletionShiftIncomplete(t, final.Result)
				t.Logf("CELL residual/%s/replay_%v unread=%v watermark=%v incomplete=%v", listing.name, replay, unread, final.Watermark != nil, incomplete)
				if !removed {
					t.Fatal("premise: the walk never reached its budget stop")
				}
				want := []int{400, 401}
				if replay {
					want = []int{401}
				}
				if !slices.Equal(unread, want) {
					t.Errorf("unread=%v, want %v", unread, want)
				}
				component := githubTestsRunInventoryComponent
				switch {
				case listing.name == "deep_runs":
					component = githubTestsLateRerunRunsComponent
				case listing.name == "deep_reports":
					component = githubTestsLateRerunReportsComponent
				case listing.reports:
					component = githubTestsArtifactInventoryComponent
				}
				if !slices.Contains(incomplete, component+"/"+githubTestsPageBudgetCause) || (final.Watermark != nil) != listing.deep {
					t.Errorf("incomplete=%v watermark=%v, want the %s stop recorded and the watermark advanced only in a deep listing", incomplete, final.Watermark != nil, component)
				}
			})
		}
	}
}
