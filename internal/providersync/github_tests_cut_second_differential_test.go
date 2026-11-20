package providersync

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// A seeded differential of the cut-second skip against main: for each generated read sequence the suites the route
// writes must hold every suite main (which reads every listed occurrence) would write. The sequences mix complete, loss
// and no-verdict reads, duplicates, a shifted page, resumes, a re-anchor, list overflow, a new attempt in the revisit and
// reports that become listable between reads, over 3-4 runs that share the cut second of a peeled first range.
//
// One residual is excluded by construction, and counted: a report that becomes listable for an attempt only after that
// run's last read in the first range was complete (testops_run_report_completeness.go:37-39: an attempt's report set is
// fixed once the run is completed).

const cutDifferentialSeed = int64(20261120014251)

// The anchor is one filler run with one always-readable report, so a sequence whose every interest download fails does
// not trip the all-artifacts-unreadable gate (a unit-level refusal main makes too) before the comparison.
const (
	cutDiffAnchorRun      = 1
	cutDiffAnchorArtifact = int64(999)
)

var cutDiffAnchorZip struct {
	once    sync.Once
	archive []byte
}

func cutDiffAnchorArchive(t *testing.T) []byte {
	cutDiffAnchorZip.once.Do(func() {
		cutDiffAnchorZip.archive = githubTestsZip(t, map[string]string{"junit.xml": completenessJUnit("anchor-", 1)})
	})
	return cutDiffAnchorZip.archive
}

// cutDifferentialSequences is the sequence count: GITHUB_TESTS_CUT_DIFFERENTIAL_SEQUENCES when set (the recorded run uses
// 5,000), else 500 (50 under -race) so the package leg stays bounded.
func cutDifferentialSequences(t *testing.T) int {
	if raw := os.Getenv("GITHUB_TESTS_CUT_DIFFERENTIAL_SEQUENCES"); raw != "" {
		count, err := strconv.Atoi(raw)
		if err != nil || count < 50 || count%50 != 0 {
			t.Fatalf("GITHUB_TESTS_CUT_DIFFERENTIAL_SEQUENCES=%q, want a multiple of 50", raw)
		}
		return count
	}
	if cutDifferentialRace {
		return 50
	}
	return 500
}

type cutDiffArtifact struct {
	id      int64
	suite   string
	publish int // first artifacts-phase ordinal at which the artifact is listed
	attempt int // 2 = published by the new attempt, listed only in the revisit
	archive []byte
}

type cutDiffRun struct {
	id         int
	entries    int // listing entries at the cut second: 2 = a duplicate on the page
	newAttempt bool
	artifacts  []*cutDiffArtifact
}

type cutDiffEvent struct {
	at     int // fires after the artifacts-phase emission of this ordinal
	action string
}

type cutDiffSequence struct {
	seq, inRange1, fillers int
	noVerdict, fail        int // percent
	runs                   []*cutDiffRun
	events                 []cutDiffEvent
}

func (sq *cutDiffSequence) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "seq=%d fillers=%d in_range1=%d no_verdict=%d%% fail=%d%% events=%v runs=[", sq.seq, sq.fillers, sq.inRange1, sq.noVerdict, sq.fail, sq.events)
	for _, run := range sq.runs {
		fmt.Fprintf(&b, " {id=%d entries=%d new_attempt=%t artifacts=", run.id, run.entries, run.newAttempt)
		for _, artifact := range run.artifacts {
			fmt.Fprintf(&b, "%d@%d/a%d ", artifact.id, artifact.publish, artifact.attempt)
		}
		b.WriteString("}")
	}
	b.WriteString(" ]")
	return b.String()
}

func cutDiffHash(parts ...any) int {
	hash := fnv.New32a()
	fmt.Fprint(hash, parts...)
	return int(hash.Sum32() % 100)
}

func newCutDiffSequence(t *testing.T, seq int) *cutDiffSequence {
	rng := rand.New(rand.NewSource(cutDifferentialSeed + int64(seq)))
	sq := &cutDiffSequence{seq: seq, noVerdict: rng.Intn(25), fail: rng.Intn(30)}
	wide := rng.Intn(3) == 0
	count, entries := 3+rng.Intn(2), 0
	for index := 0; index < count; index++ {
		run := &cutDiffRun{id: 33301167231 + index, entries: 1}
		if wide {
			run.id = 9000000000000000000 + index
		}
		if rng.Intn(4) == 0 {
			run.entries = 2
		}
		entries += run.entries
		sq.runs = append(sq.runs, run)
	}
	sq.inRange1 = 1 + rng.Intn(entries)
	sq.fillers = githubTestsSearchResultCap - sq.inRange1
	region := func() int { return sq.fillers - 2 + rng.Intn(entries+sq.inRange1+4) }
	for runIndex, run := range sq.runs {
		for k := rng.Intn(3); k > 0; k-- {
			publish := 0
			if rng.Intn(2) == 0 {
				publish = region()
			}
			run.artifacts = append(run.artifacts, &cutDiffArtifact{publish: publish, attempt: 1})
		}
		if rng.Intn(5) == 0 {
			run.newAttempt = true
			run.artifacts = append(run.artifacts, &cutDiffArtifact{attempt: 2})
		}
		for k, artifact := range run.artifacts {
			artifact.id = int64(1000 + runIndex*10 + k)
			artifact.suite = fmt.Sprintf("r%d-a%d-suite", runIndex, k)
			artifact.archive = githubTestsZip(t, map[string]string{"junit.xml": completenessJUnit(fmt.Sprintf("r%d-a%d-", runIndex, k), 1)})
		}
	}
	for n := rng.Intn(3); n > 0; n-- {
		sq.events = append(sq.events, cutDiffEvent{at: region(), action: []string{"resume", "shift", "reanchor"}[rng.Intn(3)]})
	}
	return sq
}

// cutDiffOccurrence is one listed item of an interest run the artifacts phase processed: main reads every one.
type cutDiffOccurrence struct {
	run     int
	ordinal int
	revisit bool
}

type cutDiffDoer struct {
	t          *testing.T
	sq         *cutDiffSequence
	fake       *listingFake
	byID       map[int]*cutDiffRun
	artifacts  map[int64]*cutDiffArtifact
	cut        string
	ordinal    int
	revisit    bool
	lastListed []int
	calls      []cutDiffOccurrence
	shifted    int
}

func newCutDiffDoer(t *testing.T, sq *cutDiffSequence) *cutDiffDoer {
	doer := &cutDiffDoer{t: t, sq: sq, byID: map[int]*cutDiffRun{}, artifacts: map[int64]*cutDiffArtifact{}, cut: cutDedupSecond.Format(time.RFC3339)}
	runs := makeListingRuns(1, sq.fillers, cutDedupSince.Add(time.Hour), 20*time.Second)
	for _, run := range sq.runs {
		doer.byID[run.id] = run
		for _, artifact := range run.artifacts {
			doer.artifacts[artifact.id] = artifact
		}
		for entry := 0; entry < run.entries; entry++ {
			runs = append(runs, listingRun{id: run.id, created: cutDedupSecond, updated: cutDedupBefore.Add(-time.Hour), status: "completed", conclusion: "success"})
		}
	}
	doer.fake = newListingFake(t, runs)
	return doer
}

func (doer *cutDiffDoer) noVerdict(run, ordinal int) bool {
	return cutDiffHash(doer.sq.seq, "nv", run, ordinal) < doer.sq.noVerdict
}

func (doer *cutDiffDoer) fails(artifact int64, ordinal int) bool {
	return cutDiffHash(doer.sq.seq, "f", artifact, ordinal) < doer.sq.fail
}

// visible is the artifact set a list of run at ordinal answers (nil = an empty list).
func (doer *cutDiffDoer) visible(run *cutDiffRun, ordinal int, revisit bool) []*cutDiffArtifact {
	if doer.noVerdict(run.id, ordinal) {
		return nil
	}
	var out []*cutDiffArtifact
	for _, artifact := range run.artifacts {
		if artifact.attempt == 2 && !(revisit && run.newAttempt) || ordinal < artifact.publish {
			continue
		}
		out = append(out, artifact)
	}
	return out
}

func (doer *cutDiffDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	header := http.Header{"Content-Type": {"application/json"}}
	switch {
	case path == "/repos/acme/api/actions/runs":
		response, err := doer.fake.Do(request)
		if err != nil {
			return nil, err
		}
		body, _ := io.ReadAll(response.Body)
		text := string(body)
		doer.revisit = strings.HasSuffix(request.URL.Query().Get("created"), ".."+doer.cut)
		if doer.revisit {
			for _, run := range doer.sq.runs {
				if run.newAttempt {
					listed := listingRun{id: run.id, created: cutDedupSecond, updated: cutDedupBefore.Add(-time.Hour), status: "completed", conclusion: "success"}.json()
					text = strings.ReplaceAll(text, listed, strings.Replace(listed, `"run_attempt":1`, `"run_attempt":2`, 1))
				}
			}
		}
		var page struct {
			Runs []struct {
				ID json.Number `json:"id"`
			} `json:"workflow_runs"`
		}
		if json.Unmarshal([]byte(text), &page) != nil {
			doer.t.Fatalf("listing is not JSON")
		}
		doer.lastListed = doer.lastListed[:0]
		for _, run := range page.Runs {
			id, _ := strconv.Atoi(run.ID.String())
			doer.lastListed = append(doer.lastListed, id)
		}
		response.Body = io.NopCloser(strings.NewReader(text))
		return response, nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/runs/") && strings.HasSuffix(path, "/artifacts"):
		id, _ := strconv.Atoi(strings.Split(strings.TrimPrefix(path, "/repos/acme/api/actions/runs/"), "/")[0])
		run, ok := doer.byID[id]
		if id == cutDiffAnchorRun {
			return githubTestsHTTPResponse(request, header, fmt.Sprintf(`{"artifacts":[{"id":%d,"name":"anchor","expired":false}]}`, cutDiffAnchorArtifact)), nil
		}
		if !ok {
			return githubTestsHTTPResponse(request, header, `{"artifacts":[]}`), nil
		}
		doer.calls = append(doer.calls, cutDiffOccurrence{run: id, ordinal: doer.ordinal, revisit: doer.revisit})
		parts := []string{}
		for _, artifact := range doer.visible(run, doer.ordinal, doer.revisit) {
			parts = append(parts, fmt.Sprintf(`{"id":%d,"name":"rep-%d","expired":false}`, artifact.id, artifact.id))
		}
		return githubTestsHTTPResponse(request, header, `{"artifacts":[`+strings.Join(parts, ",")+`]}`), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id := int64(githubTestsArtifactIDFromPath(doer.t, path))
		if id == cutDiffAnchorArtifact {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/zip"}}, Body: io.NopCloser(strings.NewReader(string(cutDiffAnchorArchive(doer.t)))), Request: request}, nil
		}
		artifact := doer.artifacts[id]
		if artifact == nil {
			doer.t.Fatalf("download of unknown artifact %d", id)
		}
		if doer.fails(id, doer.ordinal) {
			if cutDiffHash(doer.sq.seq, "k", id, doer.ordinal)%2 == 0 {
				return &http.Response{StatusCode: http.StatusNotFound, Header: header, Body: io.NopCloser(strings.NewReader(`{"message":"Not Found"}`)), Request: request}, nil
			}
			return &http.Response{StatusCode: http.StatusFound, Header: http.Header{}, Body: http.NoBody, Request: request}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/zip"}}, Body: io.NopCloser(strings.NewReader(string(artifact.archive))), Request: request}, nil
	}
	return doer.fake.Do(request)
}

type cutDiffResult struct {
	miss                                                     string
	skipped, carried, overflowed, residual                   bool
	resumes, shifts, reanchors, occurrences, calls, required int
}

// runCutDiffSequence walks one sequence and compares the route's suites with main's.
func runCutDiffSequence(t *testing.T, sq *cutDiffSequence) cutDiffResult {
	doer := newCutDiffDoer(t, sq)
	var occurrences []cutDiffOccurrence
	result := cutDiffResult{}
	fired := map[int]bool{}
	reanchored := false
	yield := func(before, after githubTestsChunkCursor) bool {
		if before.Phase != githubTestsArtifactsPhase {
			return false
		}
		index := len(doer.lastListed) - 1
		if after.Phase == githubTestsArtifactsPhase && after.Index > 0 {
			index = after.Index - 1
		}
		if index < 0 || index >= len(doer.lastListed) {
			t.Fatalf("%s: emission at ordinal %d does not map to a listed item (index %d of %d)", sq, doer.ordinal, index, len(doer.lastListed))
		}
		if _, interest := doer.byID[doer.lastListed[index]]; interest {
			occurrences = append(occurrences, cutDiffOccurrence{run: doer.lastListed[index], ordinal: doer.ordinal, revisit: doer.revisit})
		}
		ordinal := doer.ordinal
		doer.ordinal++
		for eventIndex, event := range sq.events {
			if fired[eventIndex] || event.at != ordinal || after.Phase != githubTestsArtifactsPhase {
				continue
			}
			fired[eventIndex] = true
			switch {
			case event.action == "shift" && !doer.revisit:
				doer.shifted++
				doer.fake.runs = append(doer.fake.runs, listingRun{id: 800000 + doer.shifted, created: cutDedupBefore.Add(-time.Duration(doer.shifted) * time.Minute), updated: cutDedupBefore.Add(-time.Minute), status: "completed", conclusion: "success"})
				result.shifts++
			case event.action == "reanchor" && !doer.revisit && !reanchored && after.RangeItems > 900 && after.Index >= 50 && after.Index < 100:
				drop := 100 - after.Index
				kept := doer.fake.runs[:0]
				for _, run := range doer.fake.runs {
					if run.id > sq.fillers-drop && run.id <= sq.fillers {
						continue
					}
					kept = append(kept, run)
				}
				doer.fake.runs = kept
				reanchored = true
				result.reanchors++
			default:
				result.resumes++
			}
			return true
		}
		return false
	}
	walk := walkCutDedup(t, cutDedupClaim(false), doer, yield, nil)
	for _, raw := range walk.cursors {
		result.overflowed = result.overflowed || strings.Contains(raw, `"ak":"-"`)
		result.carried = result.carried || strings.Contains(raw, `"ck":"`)
	}
	seen := map[[2]int]bool{}
	for _, occurrence := range occurrences {
		seen[[2]int{occurrence.run, occurrence.ordinal}] = true
	}
	for _, call := range doer.calls {
		if !seen[[2]int{call.run, call.ordinal}] {
			t.Fatalf("%s: artifact list of run %d at ordinal %d is not a mapped occurrence", sq, call.run, call.ordinal)
		}
	}
	result.occurrences, result.calls = len(occurrences), len(doer.calls)
	result.skipped = len(doer.calls) < len(occurrences)
	for _, run := range sq.runs {
		lastRange1, lastComplete := -1, false
		required := map[string]bool{}
		for _, occurrence := range occurrences {
			if occurrence.run != run.id {
				continue
			}
			visible := doer.visible(run, occurrence.ordinal, occurrence.revisit)
			complete := len(visible) > 0
			for _, artifact := range visible {
				if doer.fails(artifact.id, occurrence.ordinal) {
					complete = false
					continue
				}
				required[artifact.suite] = true
			}
			if !occurrence.revisit {
				lastRange1, lastComplete = occurrence.ordinal, complete
			}
		}
		for _, artifact := range run.artifacts {
			if required[artifact.suite] && !run.newAttempt && artifact.attempt == 1 && lastComplete && artifact.publish > lastRange1 {
				delete(required, artifact.suite)
				result.residual = true
			}
		}
		result.required += len(required)
		got := walk.suites[strconv.Itoa(run.id)]
		var missing []string
		for suite := range required {
			if !got[suite] {
				missing = append(missing, suite)
			}
		}
		if len(missing) > 0 && result.miss == "" {
			sort.Strings(missing)
			result.miss = fmt.Sprintf("%s: run %d lost %v that main reads; occurrences %v; route lists %v", sq, run.id, missing, occurrences, doer.calls)
		}
	}
	return result
}

func TestGitHubTestsCutSecondDifferentialNeverLosesASuiteMainReads(t *testing.T) {
	sequences, groups := cutDifferentialSequences(t), 50
	var mu sync.Mutex
	firstMiss, firstSeq, misses := "", -1, 0
	var total cutDiffResult
	skipped, carried, overflowed, residual := 0, 0, 0, 0
	t.Run("sequences", func(t *testing.T) {
		per := sequences / groups
		for group := 0; group < groups; group++ {
			group := group
			t.Run(strconv.Itoa(group), func(t *testing.T) {
				t.Parallel()
				for seq := group * per; seq < (group+1)*per; seq++ {
					result := runCutDiffSequence(t, newCutDiffSequence(t, seq))
					mu.Lock()
					if result.miss != "" {
						misses++
						if firstSeq < 0 || seq < firstSeq {
							firstSeq, firstMiss = seq, result.miss
						}
					}
					if result.skipped {
						skipped++
					}
					if result.carried {
						carried++
					}
					if result.overflowed {
						overflowed++
					}
					if result.residual {
						residual++
					}
					total.resumes += result.resumes
					total.shifts += result.shifts
					total.reanchors += result.reanchors
					total.occurrences += result.occurrences
					total.calls += result.calls
					total.required += result.required
					mu.Unlock()
				}
			})
		}
	})
	t.Logf("CUT-DIFFERENTIAL seed=%d sequences=%d misses=%d skipped_seqs=%d carried_seqs=%d overflow_seqs=%d residual_seqs=%d resumes=%d shifts=%d reanchor_events=%d occurrences=%d route_lists=%d required_suites=%d",
		cutDifferentialSeed, sequences, misses, skipped, carried, overflowed, residual, total.resumes, total.shifts, total.reanchors, total.occurrences, total.calls, total.required)
	if misses > 0 {
		t.Fatalf("%d of %d sequences lost a suite main reads; first failing sequence: %s", misses, sequences, firstMiss)
	}
	// The short -race run checks for misses only; 500 or more sequences must also reach every shape.
	if sequences >= 500 && (skipped == 0 || carried == 0 || overflowed == 0 || total.reanchors == 0 || total.shifts == 0 || total.resumes == 0) {
		t.Fatalf("premise: the sequences must exercise skips, carried lists, overflow, re-anchors, shifts and resumes")
	}
}
