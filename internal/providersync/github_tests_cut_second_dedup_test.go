package providersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// A peeled artifacts listing lists the runs of its cut second again (githubTestsPeel). A run read whole at that second
// is not read again by the next range; a run with any loss, a new attempt, or a run past the list's byte cap is.

var (
	cutDedupSince  = time.Date(2026, 7, 20, 6, 0, 0, 0, time.UTC)
	cutDedupBefore = time.Date(2026, 7, 20, 18, 0, 0, 0, time.UTC)
	cutDedupSecond = cutDedupSince.Add(30 * time.Minute)
	cutDedupIDs    = []int{5000, 5001, 5002, 5003, 5004}
)

// cutDedupDoer serves the listing fake, plus one report artifact for the runs in reports, hard failures of a run's
// artifact list, unavailable downloads, and a run_attempt of 2 for a run in the listings whose created range ends at hi.
type cutDedupDoer struct {
	t            *testing.T
	fake         *listingFake
	byID         map[int]listingRun
	archive      []byte
	reports      map[int]bool
	lists        map[int]int
	failList     map[int]int
	failDownload map[int]int
	attemptAt    map[int]string
}

func newCutDedupDoer(t *testing.T, runs []listingRun) *cutDedupDoer {
	byID := map[int]listingRun{}
	for _, run := range runs {
		byID[run.id] = run
	}
	return &cutDedupDoer{
		t: t, fake: newListingFake(t, runs), byID: byID,
		archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture, "lcov.info": githubTestsLCOVFixture}),
		reports: map[int]bool{}, lists: map[int]int{}, failList: map[int]int{}, failDownload: map[int]int{}, attemptAt: map[int]string{},
	}
}

func (doer *cutDedupDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	header := http.Header{"Content-Type": {"application/json"}}
	switch {
	case strings.HasPrefix(path, "/repos/acme/api/actions/runs/") && strings.HasSuffix(path, "/artifacts"):
		id, err := strconv.Atoi(strings.Split(strings.TrimPrefix(path, "/repos/acme/api/actions/runs/"), "/")[0])
		if err != nil {
			doer.t.Fatalf("unexpected request %s", request.URL.String())
		}
		doer.lists[id]++
		if doer.failList[id] > 0 {
			doer.failList[id]--
			return &http.Response{StatusCode: http.StatusUnprocessableEntity, Header: header, Body: io.NopCloser(strings.NewReader(`{"message":"injected"}`)), Request: request}, nil
		}
		if !doer.reports[id] {
			return githubTestsHTTPResponse(request, header, `{"artifacts":[]}`), nil
		}
		return githubTestsHTTPResponse(request, header, fmt.Sprintf(`{"artifacts":[{"id":%d,"name":"test-results-%d","expired":false}]}`, id, id)), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id := githubTestsArtifactIDFromPath(doer.t, path)
		if doer.failDownload[id] > 0 {
			doer.failDownload[id]--
			return &http.Response{StatusCode: http.StatusFound, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/zip"}}, Body: io.NopCloser(strings.NewReader(string(doer.archive))), Request: request}, nil
	case path == "/repos/acme/api/actions/runs" && len(doer.attemptAt) > 0:
		response, err := doer.fake.Do(request)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			doer.t.Fatal(readErr)
		}
		text := string(body)
		created := request.URL.Query().Get("created")
		for id, hi := range doer.attemptAt {
			if strings.HasSuffix(created, ".."+hi) {
				listed := doer.byID[id].json()
				text = strings.Replace(text, listed, strings.Replace(listed, `"run_attempt":1`, `"run_attempt":2`, 1), 1)
			}
		}
		response.Body = io.NopCloser(strings.NewReader(text))
		return response, nil
	}
	return doer.fake.Do(request)
}

func (doer *cutDedupDoer) reads(ids []int) []int {
	out := make([]int, 0, len(ids))
	for _, id := range ids {
		out = append(out, doer.lists[id])
	}
	return out
}

// cutDedupWalk is a walk to the final emission: every cursor it emitted, the rows per destination and run, and a digest
// of the rows in emission order.
type cutDedupWalk struct {
	final   CompleteRouteBatch
	cursors []string
	rows    map[string]map[string]int
	digest  string
	errors  int
}

// walkCutDedup drives CollectChunks to its final emission. yield ends an invocation after an emission (the next one
// resumes from its cursor); tolerate names an error after which the walk resumes from the last emitted cursor.
func walkCutDedup(
	t *testing.T, claim Claim, doer providerfoundation.HTTPDoer,
	yield func(before, after githubTestsChunkCursor) bool, tolerate func(error) bool,
) cutDedupWalk {
	t.Helper()
	client := githubTestsClient(t, doer)
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	walk := cutDedupWalk{rows: map[string]map[string]int{}}
	hash := sha256.New()
	stop := errors.New("yield")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	resume := ""
	for pass := 1; pass <= 1000; pass++ {
		last, finalSeen := resume, false
		err := GitHubTestsRouteHandler{}.CollectChunks(ctx, claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				before, beforeErr := decodeGitHubTestsChunkCursor(emission.CursorBefore)
				after, afterErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
				if beforeErr != nil || afterErr != nil {
					t.Fatalf("an emitted cursor does not decode: before=%v after=%v", beforeErr, afterErr)
				}
				walk.cursors = append(walk.cursors, emission.CursorBefore, emission.CursorAfter)
				last = emission.CursorAfter
				for _, effect := range emission.Batch.Effects {
					for _, raw := range effect.Rows {
						hash.Write([]byte(effect.Destination))
						hash.Write(raw)
						var row struct {
							RunID string `json:"run_id"`
						}
						_ = json.Unmarshal(raw, &row)
						if walk.rows[effect.Destination] == nil {
							walk.rows[effect.Destination] = map[string]int{}
						}
						walk.rows[effect.Destination][row.RunID]++
					}
				}
				if emission.Final {
					walk.final, finalSeen = emission.Batch, true
					return nil
				}
				if yield != nil && yield(before, after) {
					return stop
				}
				return nil
			})
		if finalSeen {
			if err != nil {
				t.Fatalf("final emission returned err=%v", err)
			}
			walk.digest = hex.EncodeToString(hash.Sum(nil))
			return walk
		}
		switch {
		case errors.Is(err, stop):
		case err != nil && tolerate != nil && tolerate(err):
			walk.errors++
		default:
			t.Fatalf("pass %d err=%v, want a yield", pass, err)
		}
		resume = last
	}
	t.Fatal("the walk never reached a final emission")
	return walk
}

// cutDedupRuns is the layout of the fcc repro: 998 runs newer than the cut, then five runs sharing the cut second. The
// first range ends after 5004 and 5003 (1,000 results); the next range lists all five again.
func cutDedupRuns(updated time.Time) []listingRun {
	runs := makeListingRuns(1, 998, cutDedupSince.Add(time.Hour), 30*time.Second)
	for _, id := range cutDedupIDs {
		runs = append(runs, listingRun{id: id, created: cutDedupSecond, updated: updated, status: "completed", conclusion: "success"})
	}
	return runs
}

func cutDedupClaim(backfill bool) Claim {
	claim := listingClaim(cutDedupSince, cutDedupBefore)
	if backfill {
		// The last chunk of its backfill (no next window): it reads every run updated after its own since.
		claim.Mode, claim.BackfillNext = "backfill", &BackfillNextWindow{}
	}
	return claim
}

var cutDedupLayouts = []struct {
	name     string
	updated  time.Time
	backfill bool
}{
	{"updated inside the window", cutDedupBefore.Add(-time.Hour), false},
	{"backfill, updated after the last before", cutDedupBefore.Add(time.Hour), true},
}

func TestGitHubTestsCutSecondRunsAreReadOnce(t *testing.T) {
	for _, layout := range cutDedupLayouts {
		doer := newCutDedupDoer(t, cutDedupRuns(layout.updated))
		walkCutDedup(t, cutDedupClaim(layout.backfill), doer, nil, nil)
		if got, want := doer.reads(cutDedupIDs), []int{1, 1, 1, 1, 1}; !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: artifact lists of %v = %v, want %v", layout.name, cutDedupIDs, got, want)
		}
	}
}

// The walk yields after the first cut-second run (mid-second), at the end of the first range, and after the first run
// of the next range: the list rides in the cursor across each resume.
func TestGitHubTestsCutSecondRunsAreReadOnceAcrossResumes(t *testing.T) {
	for _, layout := range cutDedupLayouts {
		doer := newCutDedupDoer(t, cutDedupRuns(layout.updated))
		yields := 0
		yield := func(before, after githubTestsChunkCursor) bool {
			if after.Phase != githubTestsArtifactsPhase && before.Phase != githubTestsArtifactsPhase {
				return false
			}
			hit := after.RangeHi == "" && after.RangeItems == 999 ||
				before.RangeHi == "" && after.RangeHi != "" ||
				after.RangeHi != "" && after.RangeItems == 1
			if hit {
				yields++
			}
			return hit
		}
		walkCutDedup(t, cutDedupClaim(layout.backfill), doer, yield, nil)
		if yields != 3 {
			t.Fatalf("%s: %d yields, want 3", layout.name, yields)
		}
		if got, want := doer.reads(cutDedupIDs), []int{1, 1, 1, 1, 1}; !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: artifact lists of %v = %v, want %v", layout.name, cutDedupIDs, got, want)
		}
	}
}

// Three ranges: three runs share each of two cut seconds. Every run is read exactly once and none is lost; a skipped run
// still counts toward its range's 1,000 results, so the second range still peels.
func TestGitHubTestsCutSecondRunsAreReadOnceOverTwoPeels(t *testing.T) {
	var runs []listingRun
	at, id := cutDedupSince.Add(10*time.Minute), 1
	add := func(count int, step time.Duration) {
		for index := 0; index < count; index++ {
			runs = append(runs, listingRun{id: id, created: at, updated: at.Add(time.Minute), status: "completed", conclusion: "success"})
			id++
			at = at.Add(step)
		}
	}
	add(50, 10*time.Second)
	add(2, 0)
	add(1, 10*time.Second) // cut B: three runs share one second
	add(994, 10*time.Second)
	add(2, 0)
	add(1, 10*time.Second) // cut A
	add(997, 10*time.Second)
	doer := newCutDedupDoer(t, runs)
	walkCutDedup(t, cutDedupClaim(false), doer, nil, nil)
	ranges := map[string]bool{}
	for _, created := range doer.fake.listRanges {
		ranges[created] = true
	}
	if len(ranges) != 6 {
		t.Fatalf("%d distinct created ranges listed, want 6 (3 per phase)", len(ranges))
	}
	for _, run := range runs {
		if got := doer.lists[run.id]; got != 1 {
			t.Fatalf("run %d artifact lists = %d, want 1", run.id, got)
		}
	}
}

// More cut-second runs than the list holds: the list overflows, nothing of that second is skipped, and the walk reads
// and writes exactly what main does (the 10 runs of the first range twice, the other 2 once).
func TestGitHubTestsCutSecondOverflowSkipsNothing(t *testing.T) {
	runs := makeListingRuns(1, 990, cutDedupSince.Add(time.Hour), 30*time.Second)
	var ids []int
	for index := 0; index < 12; index++ {
		id := 33301167231 + index
		ids = append(ids, id)
		runs = append(runs, listingRun{id: id, created: cutDedupSecond, updated: cutDedupBefore.Add(-time.Hour), status: "completed", conclusion: "success"})
	}
	doer := newCutDedupDoer(t, runs)
	for _, id := range ids {
		doer.reports[id] = true
	}
	walk := walkCutDedup(t, cutDedupClaim(false), doer, nil, nil)
	want := []int{1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}
	if got := doer.reads(ids); !reflect.DeepEqual(got, want) {
		t.Fatalf("artifact lists of %v = %v, want %v", ids, got, want)
	}
	for index, id := range ids {
		if got := walk.rows["test_suite_results"][strconv.Itoa(id)]; got != want[index] {
			t.Fatalf("run %d suite rows = %d, want %d (one per read)", id, got, want[index])
		}
	}
	t.Logf("CUT-OVERFLOW rows digest %s", walk.digest)
}

// A cut-second run whose report download failed is not read whole: it is read again by the next range.
func TestGitHubTestsCutSecondRunWithALossIsReadAgain(t *testing.T) {
	doer := newCutDedupDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour)))
	for _, id := range cutDedupIDs {
		doer.reports[id] = true
	}
	doer.failDownload[5004] = 1
	walkCutDedup(t, cutDedupClaim(false), doer, nil, nil)
	if got, want := doer.reads(cutDedupIDs), []int{1, 1, 1, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("artifact lists of %v = %v, want %v", cutDedupIDs, got, want)
	}
}

// A hard error on a cut-second run's artifact list ends the invocation before the run's emission; the resume reads it
// once, and the next range does not read it again. Its rows are written once.
func TestGitHubTestsCutSecondRunFailedReadIsReadOnResume(t *testing.T) {
	doer := newCutDedupDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour)))
	for _, id := range cutDedupIDs {
		doer.reports[id] = true
	}
	doer.failList[5004] = 1
	walk := walkCutDedup(t, cutDedupClaim(false), doer, nil, func(err error) bool {
		var providerErr *providerfoundation.ProviderError
		return errors.As(err, &providerErr) && providerErr.StatusCode == http.StatusUnprocessableEntity
	})
	if walk.errors != 1 {
		t.Fatalf("injected errors seen = %d, want 1", walk.errors)
	}
	if got, want := doer.reads(cutDedupIDs), []int{1, 1, 1, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("artifact lists of %v = %v, want %v (5004: the failed list and its resume)", cutDedupIDs, got, want)
	}
	for _, id := range cutDedupIDs {
		if got := walk.rows["test_suite_results"][strconv.Itoa(id)]; got != 1 {
			t.Fatalf("run %d suite rows = %d, want 1", id, got)
		}
	}
}

// A run listed again with a new attempt is a different key: it is read again.
func TestGitHubTestsCutSecondRunWithANewAttemptIsReadAgain(t *testing.T) {
	doer := newCutDedupDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour)))
	doer.attemptAt[5004] = cutDedupSecond.Format(time.RFC3339)
	walkCutDedup(t, cutDedupClaim(false), doer, nil, nil)
	if got, want := doer.reads(cutDedupIDs), []int{1, 1, 1, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("artifact lists of %v = %v, want %v", cutDedupIDs, got, want)
	}
}

// The unit counters count each run once: the peeled walk reports what the five report runs give alone (one range, no
// peel; the 998 other runs hold no report).
func TestGitHubTestsCutSecondCountersCountEachRunOnce(t *testing.T) {
	counters := func(runs []listingRun) map[string]any {
		doer := newCutDedupDoer(t, runs)
		for _, id := range cutDedupIDs {
			doer.reports[id] = true
		}
		walk := walkCutDedup(t, cutDedupClaim(false), doer, nil, nil)
		out := map[string]any{}
		for _, key := range []string{"test_suites_synced", "test_cases_synced", "coverage_snapshots_synced"} {
			out[key] = walk.final.Result[key]
		}
		return out
	}
	runs := cutDedupRuns(cutDedupBefore.Add(-time.Hour))
	peeled, single := counters(runs), counters(runs[len(runs)-len(cutDedupIDs):])
	if fmt.Sprint(single["test_suites_synced"]) == "0" || fmt.Sprint(single["coverage_snapshots_synced"]) == "0" {
		t.Fatalf("premise: the unpeeled walk wrote no report rows: %v", single)
	}
	if !reflect.DeepEqual(peeled, single) {
		t.Fatalf("peeled counters %v, want the one-range counters %v", peeled, single)
	}
}

// Every cursor form the code writes decodes: each cursor of the walks above, and the same cursor without cs/ci (the
// form every binary before the list wrote, in every phase), which decodes to an empty list. A walk resumed from such a
// cursor in the middle of the first range reads as main did and loses no run.
func TestGitHubTestsCutRunsEveryWrittenCursorFormDecodes(t *testing.T) {
	var cursors []string
	collect := func(doer *cutDedupDoer, tolerate func(error) bool) {
		cursors = append(cursors, walkCutDedup(t, cutDedupClaim(false), doer, nil, tolerate).cursors...)
	}
	plain := newCutDedupDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour)))
	collect(plain, nil)
	overflow := newCutDedupDoer(t, nil)
	for index := 0; index < 990; index++ {
		overflow.fake.runs = append(overflow.fake.runs, listingRun{id: 1 + index, created: cutDedupSince.Add(time.Hour + time.Duration(index)*30*time.Second), updated: cutDedupBefore.Add(-time.Hour), status: "completed", conclusion: "success"})
	}
	for index := 0; index < 12; index++ {
		overflow.fake.runs = append(overflow.fake.runs, listingRun{id: 33301167231 + index, created: cutDedupSecond, updated: cutDedupBefore.Add(-time.Hour), status: "completed", conclusion: "success"})
	}
	collect(overflow, nil)
	attempt := newCutDedupDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour)))
	attempt.attemptAt[5004] = cutDedupSecond.Format(time.RFC3339)
	collect(attempt, nil)
	cursors = append(cursors,
		`{"phase":"artifacts","next_url":"https://api.github.com/x?page=2","index":3,"pipelines":1,"jobs":0,"acceptance":0,"suites":0,"cases":0,"coverage":0,"requests":9,"pages":4,"run_pages":2,"artifact_pages":2,"repo":"acme/api","archives_seen":0,"archives_unreadable":0}`,
		`{"phase":"artifacts","next_url":"","index":0,"run_pages":1,"artifact_pages":1,"repo":"acme/api"}`,
		`{"phase":"runs","pipelines":0,"jobs":0,"acceptance":0,"suites":0,"cases":0,"coverage":0,"requests":1,"pages":0,"run_pages":0,"artifact_pages":0,"repo":"acme/api","archives_seen":0,"archives_unreadable":0}`,
		`{"phase":"open","pipelines":0,"jobs":0,"acceptance":0,"suites":0,"cases":0,"coverage":0,"requests":1,"pages":0,"run_pages":2,"artifact_pages":2,"repo":"acme/api","archives_seen":0,"archives_unreadable":0,"oc":100,"od":4}`,
		``,
	)
	forms, withList, overflowed := 0, 0, 0
	for _, raw := range cursors {
		cursor, err := decodeGitHubTestsChunkCursor(raw)
		if err != nil {
			t.Fatalf("a written cursor does not decode: %v: %s", err, raw)
		}
		if cursor.CutRuns != "" {
			withList++
		}
		if cursor.CutRuns == githubTestsCutRunsOverflow {
			overflowed++
		}
		var fields map[string]json.RawMessage
		if raw == "" || json.Unmarshal([]byte(raw), &fields) != nil {
			continue
		}
		delete(fields, "cs")
		delete(fields, "ci")
		stripped, _ := json.Marshal(fields)
		old, err := decodeGitHubTestsChunkCursor(string(stripped))
		if err != nil || old.CutSecond != 0 || old.CutRuns != "" {
			t.Fatalf("the pre-list form of a written cursor = %+v, %v; want an empty list: %s", old, err, stripped)
		}
		forms++
	}
	if withList == 0 || overflowed == 0 || forms < 2000 {
		t.Fatalf("premise: %d cursors carried a list, %d an overflowed one, %d forms checked", withList, overflowed, forms)
	}

	// Resume from the pre-list form of the cursor after the first cut-second run of the first range.
	doer := newCutDedupDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour)))
	stripNext := false
	yield := func(before, after githubTestsChunkCursor) bool {
		if after.Phase == githubTestsArtifactsPhase && after.RangeHi == "" && after.RangeItems == 999 {
			stripNext = true
			return true
		}
		return false
	}
	client := githubTestsClient(t, doer)
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	stop := errors.New("yield")
	resume, final := "", false
	for pass := 0; pass < 1000 && !final; pass++ {
		last := resume
		err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), cutDedupClaim(false), providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				last = emission.CursorAfter
				if emission.Final {
					final = true
					return nil
				}
				before, _ := decodeGitHubTestsChunkCursor(emission.CursorBefore)
				after, _ := decodeGitHubTestsChunkCursor(emission.CursorAfter)
				if !stripNext && yield(before, after) {
					return stop
				}
				return nil
			})
		if final {
			break
		}
		if !errors.Is(err, stop) {
			t.Fatalf("pass %d err=%v", pass, err)
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal([]byte(last), &fields) != nil {
			t.Fatal("cursor is not JSON")
		}
		if _, ok := fields["ci"]; !ok {
			t.Fatalf("premise: the yielded cursor carries no list: %s", last)
		}
		delete(fields, "cs")
		delete(fields, "ci")
		stripped, _ := json.Marshal(fields)
		resume = string(stripped)
	}
	if !final {
		t.Fatal("the resumed walk never reached a final emission")
	}
	if got, want := doer.reads(cutDedupIDs), []int{1, 1, 1, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after a pre-list resume: artifact lists of %v = %v, want %v (5004 read before the resume, so read again)", cutDedupIDs, got, want)
	}
	for id := 1; id <= 998; id++ {
		if doer.lists[id] != 1 {
			t.Fatalf("after a pre-list resume: run %d artifact lists = %d, want 1", id, doer.lists[id])
		}
	}
}

// A cut list this code cannot have written is a checkpoint conflict with a named reason, before any provider call. A
// list on a cursor outside the artifacts phase is cleared.
func TestGitHubTestsCutRunsMalformedCursorIsANamedConflict(t *testing.T) {
	base := `{"phase":"artifacts","next_url":"","index":0,"run_pages":1,"artifact_pages":1,"repo":"acme/api","archives_seen":0,"archives_unreadable":0,`
	for _, tail := range []string{
		`"cs":-1,"ci":"5"}`,
		`"cs":253402300800,"ci":"5"}`,
		`"cs":1784528400}`,
		`"ci":"5"}`,
		`"cs":1784528400,"ci":"x"}`,
		`"cs":1784528400,"ci":"5,,6"}`,
		`"cs":1784528400,"ci":"05"}`,
		`"cs":1784528400,"ci":"5.1"}`,
		`"cs":1784528400,"ci":"5.0"}`,
		`"cs":1784528400,"ci":"5,5"}`,
		`"cs":1784528400,"ci":"5,-"}`,
		`"cs":1784528400,"ci":"123456789012345678901"}`,
		`"cs":1784528400,"ci":"` + cutDedupDistinctIDs(9) + `,1"}`,
	} {
		_, err := decodeGitHubTestsChunkCursor(base + tail)
		var storeErr *ChunkStoreError
		if !errors.Is(err, ErrChunkCheckpointConflict) || errors.As(err, &storeErr) || !strings.Contains(fmt.Sprint(err), "("+githubTestsCutRunsConflict+")") {
			t.Fatalf("%s: err=%v, want the named checkpoint conflict", tail, err)
		}
	}
	doer := newCutDedupDoer(t, nil)
	client := githubTestsClient(t, doer)
	err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), cutDedupClaim(false), providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), base+`"cs":1784528400,"ci":"x"}`, func(ChunkRouteEmission) error { return nil })
	if !errors.Is(err, ErrChunkCheckpointConflict) || doer.fake.requests != 0 {
		t.Fatalf("CollectChunks err=%v after %d requests, want the conflict before any request", err, doer.fake.requests)
	}
	for _, raw := range []string{
		`{"phase":"artifacts","next_url":"","index":0,"repo":"acme/api","archives_seen":0,"archives_unreadable":0,"cs":1784528400,"ci":"5,6.2"}`,
		`{"phase":"artifacts","next_url":"","index":0,"repo":"acme/api","archives_seen":0,"archives_unreadable":0,"cs":1784528400,"ci":"-"}`,
		`{"phase":"artifacts","next_url":"","index":0,"repo":"acme/api","archives_seen":0,"archives_unreadable":0,"cs":253402300799,"ci":"` + cutDedupDistinctIDs(8) + `,12345"}`,
	} {
		if _, err := decodeGitHubTestsChunkCursor(raw); err != nil {
			t.Fatalf("%s: err=%v, want a valid list", raw, err)
		}
	}
	cleared, err := decodeGitHubTestsChunkCursor(`{"phase":"open","repo":"acme/api","archives_seen":0,"archives_unreadable":0,"cs":1784528400,"ci":"5"}`)
	if err != nil || cleared.CutSecond != 0 || cleared.CutRuns != "" {
		t.Fatalf("a list outside the artifacts phase = %+v, %v; want cleared", cleared, err)
	}
}

// The list helpers: an older second starts a new list, a newer one is ignored, a duplicate is kept once, the byte cap
// turns the list into the overflow mark, and an unkeyable run is never recorded or skipped.
func TestGitHubTestsCutRunsRecordAndSkip(t *testing.T) {
	cursor := githubTestsChunkCursor{}
	cursor = githubTestsRecordCutRun(cursor, 200, "7")
	cursor = githubTestsRecordCutRun(cursor, 300, "8")
	cursor = githubTestsRecordCutRun(cursor, 200, "9.2")
	cursor = githubTestsRecordCutRun(cursor, 200, "7")
	if cursor.CutSecond != 200 || cursor.CutRuns != "7,9.2" {
		t.Fatalf("list = %d %q, want 200 \"7,9.2\"", cursor.CutSecond, cursor.CutRuns)
	}
	if !githubTestsSkipCutRun(cursor, 200, "9.2") || githubTestsSkipCutRun(cursor, 200, "9") || githubTestsSkipCutRun(cursor, 201, "7") || githubTestsSkipCutRun(cursor, 200, "") {
		t.Fatal("skip must match the exact key at the exact second only")
	}
	cursor = githubTestsRecordCutRun(cursor, 100, "1")
	if cursor.CutSecond != 100 || cursor.CutRuns != "1" {
		t.Fatalf("an older second must start a new list: %d %q", cursor.CutSecond, cursor.CutRuns)
	}
	for index := 0; index < 8; index++ {
		cursor = githubTestsRecordCutRun(cursor, 100, strconv.Itoa(33301167231+index))
	}
	if cursor.CutRuns != githubTestsCutRunsOverflow || githubTestsSkipCutRun(cursor, 100, "1") {
		t.Fatalf("past %d bytes the list overflows and skips nothing: %q", githubTestsMaxCutRunsBytes, cursor.CutRuns)
	}
	cursor = githubTestsRecordCutRun(cursor, 100, "2")
	if cursor.CutRuns != githubTestsCutRunsOverflow {
		t.Fatalf("an overflowed second stays overflowed: %q", cursor.CutRuns)
	}
	if next := githubTestsRecordCutRun(cursor, 99, "3"); next.CutSecond != 99 || next.CutRuns != "3" {
		t.Fatalf("an older second clears the overflow: %d %q", next.CutSecond, next.CutRuns)
	}
	created := "2026-07-20T06:30:00Z"
	for _, tc := range []struct {
		id      string
		attempt any
		want    string
	}{
		{"5004", json.Number("1"), "5004"}, {"5004", json.Number("3"), "5004.3"}, {"5004", nil, ""}, {"5004", json.Number("0"), ""},
		{"5004", json.Number("1.5"), ""}, {"-5", json.Number("1"), ""}, {"1e3", json.Number("1"), ""}, {"123456789012345678901", json.Number("1"), ""},
	} {
		second, key := githubTestsCutRunKey(gitHubWorkflowRunPayload{ID: json.Number(tc.id), CreatedAt: &created, RunAttempt: tc.attempt})
		if key != tc.want || (key != "" && second != cutDedupSecond.Unix()) {
			t.Fatalf("key(%s, %v) = %d %q, want %q", tc.id, tc.attempt, second, key, tc.want)
		}
	}
	if _, key := githubTestsCutRunKey(gitHubWorkflowRunPayload{ID: json.Number("5"), RunAttempt: json.Number("1")}); key != "" {
		t.Fatal("a run with no created time must not be keyed")
	}
}

// cutDedupDistinctIDs is count distinct 10-digit ids, comma-joined (11 bytes each with its comma).
func cutDedupDistinctIDs(count int) string {
	ids := make([]string, 0, count)
	for index := 0; index < count; index++ {
		ids = append(ids, strconv.Itoa(1000000001+index))
	}
	return strings.Join(ids, ",")
}
