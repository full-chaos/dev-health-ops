package providersync

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// longLinkDoer pads the next-page URL of every runs listing to a fixed length, as a provider's long continuation URL does.
type longLinkDoer struct {
	*runsCutDoer
	urlBytes int
}

func (doer *longLinkDoer) Do(request *http.Request) (*http.Response, error) {
	response, err := doer.runsCutDoer.Do(request)
	if err != nil || request.URL.Path != "/repos/acme/api/actions/runs" {
		return response, err
	}
	link := response.Header.Get("Link")
	if link == "" {
		return response, nil
	}
	target := strings.TrimSuffix(strings.TrimPrefix(link, "<"), ">; rel=\"next\"")
	if len(target) < doer.urlBytes {
		target += "&pad=" + strings.Repeat("a", doer.urlBytes-len(target)-len("&pad="))
	}
	response.Header.Set("Link", "<"+target+">; rel=\"next\"")
	return response, nil
}

func runLongContinuation(t *testing.T, urlBytes int) (error, []githubTestsChunkCursor) {
	t.Helper()
	err, cursors, _, _ := runLongContinuationFrom(t, urlBytes, "", nil)
	return err, cursors
}

// runLongContinuationFrom is one invocation from resume (up to stopAfter emissions when stopAfter is set): the error, the
// emitted after cursors, the raw last after cursor, and the client's metrics.
func runLongContinuationFrom(t *testing.T, urlBytes int, resume string, stopAfter func(int) bool) (error, []githubTestsChunkCursor, string, *providerfoundation.Metrics) {
	t.Helper()
	doer := &longLinkDoer{runsCutDoer: newRunsCutDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour))), urlBytes: urlBytes}
	client := githubTestsClient(t, doer)
	client.Metrics = providerfoundation.NewMetrics()
	var cursors []githubTestsChunkCursor
	last := resume
	stop := errors.New("yield")
	err := GitHubTestsRouteHandler{}.CollectChunks(t.Context(), cutDedupClaim(false), providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume, func(emission ChunkRouteEmission) error {
			after, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
			if decodeErr != nil {
				t.Fatalf("emitted cursor does not decode: %v", decodeErr)
			}
			cursors = append(cursors, after)
			last = emission.CursorAfter
			if stopAfter != nil && !emission.Final && stopAfter(len(cursors)) {
				return stop
			}
			return nil
		})
	if errors.Is(err, stop) {
		err = nil
	}
	return err, cursors, last, client.Metrics
}

// longContinuationURLBytes puts the widest cursor of the walk within 20 bytes of 4096 without its cut lists. It was 3790
// until a cursor kept its resume anchor (CHAOS-14316: up to 15 more bytes in this walk).
const longContinuationURLBytes = 3775

// A provider continuation URL near 3.8 KB plus the cut lists must not fail the chunk with a checkpoint conflict. At
// longContinuationURLBytes the widest cursor of the whole walk is within 20 bytes of 4096 without its cut lists (main before
// the cut lists completes) and a few bytes over with them.
func TestGitHubTestsLongContinuationURLWithCutListsFinishesTheChunk(t *testing.T) {
	const urlBytes = longContinuationURLBytes
	err, cursors := runLongContinuation(t, urlBytes)
	if errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("continuation URL %d bytes: chunk failed with a checkpoint conflict after %d emissions", urlBytes, len(cursors))
	}
	if err != nil {
		t.Fatalf("continuation URL %d bytes: err=%v", urlBytes, err)
	}
	widest := 0
	for _, cursor := range cursors {
		cursor.ActiveCutKeys, cursor.CarriedCutKeys = "", ""
		raw, encodeErr := encodeGitHubTestsChunkCursor(cursor)
		if encodeErr != nil {
			t.Fatalf("a cursor without cut lists does not fit: %v", encodeErr)
		}
		widest = max(widest, len(raw))
	}
	if widest > maxChunkCursorBytes || widest < maxChunkCursorBytes-20 {
		t.Fatalf("widest cursor without cut lists is %d bytes, want %d-%d: the cell no longer sits on the budget edge", widest, maxChunkCursorBytes-20, maxChunkCursorBytes)
	}
}

func cutListDroppedMetric(t *testing.T, metrics *providerfoundation.Metrics, list string) string {
	t.Helper()
	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	prefix := `dev_health_github_tests_cursor_cut_list_dropped_total{list="` + list + `"} `
	for _, line := range strings.Split(output.String(), "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	return ""
}

// One degrade is one counter increment for the dropped list and one Warn line with the bytes before and after. The walk
// drops only the active list here (the carried list is empty until a peel).
func TestGitHubTestsCursorDegradeCountsAndLogs(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logging.NewJSON(&output, slog.LevelWarn))
	defer slog.SetDefault(previous)
	err, cursors, _, metrics := runLongContinuationFrom(t, longContinuationURLBytes, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = cursors
	degraded := strings.Count(output.String(), `"dropped":"active"`)
	if degraded == 0 {
		t.Fatal("no degrade was logged")
	}
	if got := cutListDroppedMetric(t, metrics, "active"); got != strconv.Itoa(degraded) {
		t.Fatalf("active counter = %q, want %d (one per Warn line)", got, degraded)
	}
	if got := cutListDroppedMetric(t, metrics, "carried"); got != "" {
		t.Fatalf("carried counter = %q, want none: no carried list was dropped", got)
	}
	line := output.String()
	for _, want := range []string{`"dropped":"active"`, `"bytes_before":`, `"bytes_after":`, `cut lists dropped`} {
		if !strings.Contains(line, want) {
			t.Fatalf("Warn output lacks %s:\n%s", want, line)
		}
	}
}

// A cursor that fits keeps both lists byte for byte and counts nothing.
func TestGitHubTestsCursorFitKeepsListsThatFit(t *testing.T) {
	cursor := githubTestsChunkCursor{Phase: githubTestsRunsPhase, Repo: "acme/api", RangeHi: "2026-07-20T23:59:59Z",
		RangeOldest: "2026-07-20T00:00:00Z", ActiveCutKeys: "12,13", CarriedCutKeys: "14"}
	fitted, dropped := githubTestsFitCursor(cursor)
	if len(dropped) != 0 || !reflect.DeepEqual(fitted, cursor) {
		t.Fatalf("a fitting cursor changed: dropped=%v fitted=%+v", dropped, fitted)
	}
}

// The order is active first, carried second, and the continuation URL is never touched.
func TestGitHubTestsCursorFitDropOrder(t *testing.T) {
	widest := githubTestsWidestCutKeys(t)
	base := githubTestsChunkCursor{Phase: githubTestsRunsPhase, Repo: "acme/api", RangeHi: "2026-07-20T23:59:59Z",
		RangeOldest: "2026-07-20T00:00:00Z", ActiveCutKeys: widest, CarriedCutKeys: widest}
	size := func(cursor githubTestsChunkCursor) int {
		raw, err := json.Marshal(cursor)
		if err != nil {
			t.Fatal(err)
		}
		return len(raw)
	}
	pad := func(cursor githubTestsChunkCursor, total int) githubTestsChunkCursor {
		cursor.NextURL = "https://api.github.com/repos/acme/api/actions/runs?" + strings.Repeat("a", 1)
		cursor.NextURL += strings.Repeat("a", total-size(cursor))
		return cursor
	}
	for _, tc := range []struct {
		name        string
		total       int
		wantDropped []string
		wantActive  string
		wantCarried string
	}{
		{"fits", maxChunkCursorBytes, nil, widest, widest},
		{"over by one: active only", maxChunkCursorBytes + 1, []string{"active"}, "", widest},
		{"over by more than the active list: both", maxChunkCursorBytes + 2*githubTestsMaxCutKeysBytes, []string{"active", "carried"}, "", ""},
	} {
		cursor := pad(base, tc.total)
		fitted, dropped := githubTestsFitCursor(cursor)
		if !reflect.DeepEqual(dropped, tc.wantDropped) || fitted.ActiveCutKeys != tc.wantActive || fitted.CarriedCutKeys != tc.wantCarried || fitted.NextURL != cursor.NextURL {
			t.Fatalf("%s: dropped=%v active=%q carried=%q urlKept=%t", tc.name, dropped, fitted.ActiveCutKeys, fitted.CarriedCutKeys, fitted.NextURL == cursor.NextURL)
		}
		raw, err := encodeGitHubTestsChunkCursor(fitted)
		if err != nil {
			t.Fatalf("%s: fitted cursor does not encode: %v", tc.name, err)
		}
		decoded, err := decodeGitHubTestsChunkCursor(raw)
		if err != nil || !githubTestsCutKeysValid(decoded) {
			t.Fatalf("%s: fitted cursor does not decode to a valid form: err=%v", tc.name, err)
		}
	}
}

// A continuation URL alone over the budget stays a hard checkpoint conflict, as on main; nothing is dropped to hide it.
func TestGitHubTestsCursorFitURLAloneOverBudgetStillFails(t *testing.T) {
	cursor := githubTestsChunkCursor{Phase: githubTestsRunsPhase, Repo: "acme/api", RangeHi: "2026-07-20T23:59:59Z",
		RangeOldest: "2026-07-20T00:00:00Z", ActiveCutKeys: "12", CarriedCutKeys: "14",
		NextURL: "https://api.github.com/x?" + strings.Repeat("a", maxChunkCursorBytes)}
	fitted, dropped := githubTestsFitCursor(cursor)
	if len(dropped) != 0 || !reflect.DeepEqual(fitted, cursor) {
		t.Fatalf("an unfittable cursor was changed: dropped=%v", dropped)
	}
	if _, err := encodeGitHubTestsChunkCursor(fitted); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("err=%v, want ErrChunkCheckpointConflict", err)
	}
	if err, _ := runLongContinuation(t, maxChunkCursorBytes+200); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("a %d byte continuation URL: err=%v, want ErrChunkCheckpointConflict", maxChunkCursorBytes+200, err)
	}
}

// Resumed from a stored degraded cursor, with a stop after every emission, the walk still reaches its final emission on the
// same pages: every stored cursor decodes, the continuation URL of each is the padded Link URL, and each run is written once.
func TestGitHubTestsCursorDegradeResumesOnTheSamePages(t *testing.T) {
	const urlBytes = longContinuationURLBytes
	reference, referenceCursors := runLongContinuation(t, urlBytes)
	if reference != nil {
		t.Fatal(reference)
	}
	resume, passes, emissions := "", 0, 0
	var finalCursor githubTestsChunkCursor
	for ; passes < 5000; passes++ {
		err, cursors, last, _ := runLongContinuationFrom(t, urlBytes, resume, func(int) bool { return true })
		if err != nil {
			t.Fatalf("pass %d: err=%v", passes, err)
		}
		emissions += len(cursors)
		for _, cursor := range cursors {
			if cursor.NextURL != "" && len(cursor.NextURL) != urlBytes && !strings.Contains(cursor.NextURL, "created=") {
				t.Fatalf("pass %d: stored continuation URL is not the provider's: %d bytes", passes, len(cursor.NextURL))
			}
		}
		if len(cursors) == 0 {
			break
		}
		resume = last
		finalCursor = cursors[len(cursors)-1]
		if finalCursor.Phase == "done" {
			break
		}
	}
	if finalCursor.Phase != "done" {
		t.Fatalf("the resumed walk did not finish: phase %q after %d passes", finalCursor.Phase, passes)
	}
	// A resume restarts from the degraded cursor, which lost its carried list: the runs of the cut second are read again
	// (the dedup that is lost), never fewer runs than the uninterrupted walk.
	want := referenceCursors[len(referenceCursors)-1].Pipelines
	if got := finalCursor.Pipelines; got < want || got > want+len(cutDedupIDs) {
		t.Fatalf("resumed walk wrote %d pipeline rows, uninterrupted walk %d (at most %d cut-second runs read again)", got, want, len(cutDedupIDs))
	}
}

type sampleReplayLink struct {
	*runsCutDoer
	kind       string
	extra      []byte
	firstLink  int
	secondLink int
}

func (d sampleReplayLink) Do(req *http.Request) (*http.Response, error) {
	const reportName = "test-results-5004"
	switch req.URL.Path {
	case "/repos/acme/api/actions/runs/5004/artifacts":
		otherID, otherName := 9000, "container-build-linux-arm64.spdx.json"
		switch d.kind {
		case "prefix":
			otherName = "go-test-timings-linux-amd64"
		case "superseded":
			otherID, otherName = 4000, reportName
		case "no_report_member":
			otherName = "build-debug-logs"
		}
		d.lists[5004]++
		return githubTestsHTTPResponse(req, http.Header{"Content-Type": {"application/json"}},
			fmt.Sprintf(`{"artifacts":[{"id":5004,"name":%q,"size_in_bytes":%d,"expired":false},{"id":%d,"name":%q,"size_in_bytes":%d,"expired":false}]}`, reportName, len(d.archive), otherID, otherName, len(d.extra))), nil
	case "/repos/acme/api/actions/artifacts/9000/zip", "/repos/acme/api/actions/artifacts/5004/zip":
		if d.kind == "no_report_member" {
			archive := d.extra
			if strings.Contains(req.URL.Path, "/5004/") {
				archive = d.archive
			}
			w := httptest.NewRecorder()
			http.ServeContent(w, req, "artifact.zip", time.Time{}, bytes.NewReader(archive))
			return w.Result(), nil
		}
	}
	resp, err := d.runsCutDoer.Do(req)
	if err != nil || req.URL.Path != "/repos/acme/api/actions/runs" {
		return resp, err
	}
	lo, hi, _ := strings.Cut(req.URL.Query().Get("created"), "..")
	if len(lo) != len(time.DateOnly) {
		return resp, nil
	}
	length := 0
	page := req.URL.Query().Get("page")
	if page == "9" && hi == cutDedupBefore.Format(time.RFC3339) {
		length = d.firstLink
	}
	if page == "" && hi == cutDedupSecond.Format(time.RFC3339) {
		length = d.secondLink
	}
	if length == 0 {
		return resp, nil
	}
	link := resp.Header.Get("Link")
	if link == "" {
		return resp, nil
	}
	target := strings.TrimSuffix(strings.TrimPrefix(link, "<"), ">; rel=\"next\"")
	target += "&pad=" + strings.Repeat("a", length-len(target)-len("&pad="))
	resp.Header.Set("Link", "<"+target+">; rel=\"next\"")
	return resp, nil
}

func TestGitHubTestsCursorDegradeReplaysEachExclusionSampleOnce(t *testing.T) {
	prior := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(prior)
	for _, kind := range []string{"suffix", "prefix", "superseded", "no_report_member"} {
		t.Run(kind, func(t *testing.T) {
			first, second := 0, 0
			for _, cell := range []string{"short", "first_only", "second_only", "both", "both_uninterrupted"} {
				t.Run(cell, func(t *testing.T) {
					run := cutDedupRuns(cutDedupBefore.Add(-time.Hour))
					run = append(run, makeListingRuns(7000, 105, cutDedupSecond.Add(-5*time.Minute), time.Second)...)
					doer := newRunsCutDoer(t, run)
					p := sampleReplayLink{runsCutDoer: doer, kind: kind, extra: githubTestsZip(t, map[string]string{"build.log": "build finished"})}
					if cell == "first_only" || cell == "both" || cell == "both_uninterrupted" {
						p.firstLink = first
					}
					if cell == "second_only" || cell == "both" || cell == "both_uninterrupted" {
						p.secondLink = second
					}
					h := GitHubTestsRouteHandler{}
					if kind == "no_report_member" {
						h.MaxArtifactsPerRun = 1
					}
					client := githubTestsClient(t, p)
					client.Metrics = providerfoundation.NewMetrics()
					stop := errors.New("yield")
					claim := cutDedupClaim(false)
					cursor := ""
					var last githubTestsChunkCursor
					yielded, final := false, false
					for {
						err := h.CollectChunks(t.Context(), claim, providerfoundation.Credential{}, client,
							time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), cursor, func(emission ChunkRouteEmission) error {
								after, err := decodeGitHubTestsChunkCursor(emission.CursorAfter)
								if err != nil {
									t.Fatal(err)
								}
								cursor, last = emission.CursorAfter, after
								if !yielded && after.Phase == githubTestsArtifactsPhase && after.RangeHi == "" && after.RangeItems == 999 {
									yielded = true
									if cell == "short" {
										if after.ActiveCutKeys == "" || len(after.ExcludedArtifactSample) != 1 {
											t.Fatalf("fixture did not yield at a completed run with one sample: incomplete=%v", after.Incomplete)
										}
										first = len(after.NextURL) + 4097 - len(cursor) - 5
									}
									if cell != "both_uninterrupted" {
										return stop
									}
								}
								final = emission.Final
								if after.Phase == githubTestsArtifactsPhase && after.RangeHi != "" && after.RangeItems == 100 {
									if cell == "short" {
										withoutCuts, _ := json.Marshal(githubTestsClearCutKeys(after))
										second = len(after.NextURL) + 4085 - len(withoutCuts) - 5
									}
								}
								return nil
							})
						if errors.Is(err, stop) {
							continue
						}
						if err != nil || !final {
							t.Fatalf("url1=%d url2=%d: err=%v final=%t, want the walk to finish like the uninterrupted one (conflict=%t)", p.firstLink, p.secondLink, err, final, errors.Is(err, ErrChunkCheckpointConflict))
						}
						if got := len(last.ExcludedArtifactSample); got != 1 {
							t.Fatalf("exclusion samples = %q, want exactly one (a re-read run adds none)", last.ExcludedArtifactSample)
						}
						break
					}
				})
			}
		})
	}
}

func TestGitHubTestsAddExcludedSampleIsBoundedAndIdempotent(t *testing.T) {
	var samples []string
	for index := 0; index < githubTestsMaxExcludedArtifactSampleRecords; index++ {
		added := githubTestsAddExcludedSample(samples, "sample-"+strconv.Itoa(index))
		if len(added) != index+1 {
			t.Fatalf("sample %d: %d kept, want %d", index, len(added), index+1)
		}
		if again := githubTestsAddExcludedSample(added, "sample-"+strconv.Itoa(index)); len(again) != index+1 {
			t.Fatalf("sample %d added twice: %q", index, again)
		}
		samples = added
	}
	if over := githubTestsAddExcludedSample(samples, "one-more"); len(over) != githubTestsMaxExcludedArtifactSampleRecords {
		t.Fatalf("bound broken: %d samples, want %d", len(over), githubTestsMaxExcludedArtifactSampleRecords)
	}
}
