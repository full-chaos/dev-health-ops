package providersync

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// A re-anchored or peeled walk reads a run's artifacts again. A skip whose (run, artifact, cause) marker is still in the
// sample is a replay and counts once; artifact_unavailable is counted per failed look.

type githubTestsReplayArtifact struct {
	id      int
	kind    string // healthy, archive, not_a_zip, empty_body, oversized, unavailable, expired
	archive []byte
}

type githubTestsReplayDedupDoer struct {
	t         *testing.T
	runs      map[int][]githubTestsReplayArtifact
	downloads map[int]int
}

func (doer *githubTestsReplayDedupDoer) artifact(id int) githubTestsReplayArtifact {
	for _, artifacts := range doer.runs {
		for _, artifact := range artifacts {
			if artifact.id == id {
				return artifact
			}
		}
	}
	doer.t.Fatalf("no artifact %d", id)
	return githubTestsReplayArtifact{}
}

func (doer *githubTestsReplayDedupDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, len(doer.runs))), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/runs/") && strings.HasSuffix(path, "/artifacts"):
		run, err := strconv.Atoi(strings.Split(strings.TrimPrefix(path, "/repos/acme/api/actions/runs/"), "/")[0])
		if err != nil {
			doer.t.Fatalf("unexpected request %s", request.URL.String())
		}
		return githubTestsHTTPResponse(request, header, githubTestsReplayArtifactsJSON(doer.runs[run])), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id := githubTestsArtifactIDFromPath(doer.t, path)
		doer.downloads[id]++
		return githubTestsReplayArtifactResponse(doer.t, request, doer.artifact(id)), nil
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	}
}

func githubTestsReplayArtifactsJSON(artifacts []githubTestsReplayArtifact) string {
	parts := make([]string, 0, len(artifacts))
	for _, artifact := range artifacts {
		parts = append(parts, fmt.Sprintf(`{"id":%d,"name":"test-results-%d","expired":%t}`, artifact.id, artifact.id, artifact.kind == "expired"))
	}
	return `{"artifacts":[` + strings.Join(parts, ",") + `]}`
}

func githubTestsReplayArtifactResponse(t *testing.T, request *http.Request, artifact githubTestsReplayArtifact) *http.Response {
	t.Helper()
	zipHeader := http.Header{"Content-Type": {"application/zip"}}
	var body io.Reader
	switch artifact.kind {
	case "unavailable":
		return &http.Response{StatusCode: http.StatusFound, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: request}
	case "oversized":
		body = &githubTestsRepeatingReader{remaining: githubTestsMaxDownloadSize + 16}
	case "empty_body":
		body = strings.NewReader("")
	case "not_a_zip":
		body = strings.NewReader(githubTestsBlobErrorDocument)
	case "archive":
		body = bytes.NewReader(artifact.archive)
	case "healthy":
		body = bytes.NewReader(githubTestsZip(t, map[string]string{"junit.xml": githubTestsNamedMultiSuiteJUnit("h"+strconv.Itoa(artifact.id)+"-", 1)}))
	default:
		t.Fatalf("artifact %d of kind %q must not be downloaded", artifact.id, artifact.kind)
	}
	return &http.Response{StatusCode: http.StatusOK, Header: zipHeader, Body: io.NopCloser(body), Request: request}
}

// githubTestsMemberSkipArchive is an archive that holds one healthy JUnit report and members skipped for each member cause.
func githubTestsMemberSkipArchive(t *testing.T, cause string, members int) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	add := func(name, body string) {
		w, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	add("junit.xml", silentSkipJUnit)
	for index := 1; index <= members; index++ {
		name := fmt.Sprintf("bad-%d.xml", index)
		switch cause {
		case githubTestsMalformedCause:
			add(name, fmt.Sprintf(`<testsuite name="m%d"><testcase classname="a" name="b">`, index))
		case githubTestsUnsafeDoctypeCause:
			add(name, fmt.Sprintf(`<?xml version="1.0"?><!DOCTYPE testsuite [ <!ELEMENT a ANY> ]><testsuite name="d%d"/>`, index))
		case githubTestsUnreadableCause:
			// A stored member whose CRC does not match its bytes: the member cannot be read.
			body := []byte(fmt.Sprintf(`<testsuite name="u%d"><testcase classname="a" name="b"/></testsuite>`, index))
			w, err := writer.CreateRaw(&zip.FileHeader{
				Name: name, Method: zip.Store, CRC32: 1,
				CompressedSize64: uint64(len(body)), UncompressedSize64: uint64(len(body)),
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write(body); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("no archive for %s", cause)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	archive := buffer.Bytes()
	rows, err := parseGitHubTestsArtifact(archive, "artifact-1", "c7198fbc-1945-3717-05d8-eb78866b4e79", "9001", "org-a", nil, nil, githubTestsDoctypeNow)
	if err != nil {
		t.Fatal(err)
	}
	incomplete, optional := rows.optionalIncomplete()
	want := GitHubTestsIncomplete{Component: githubTestsReportMemberComponent, Cause: cause, Count: members}
	if !optional || len(incomplete) != 1 || incomplete[0] != want || len(rows.Suites) == 0 {
		t.Fatalf("fixture for %s parses to incomplete=%+v suites=%d, want [%+v] beside a healthy report", cause, incomplete, len(rows.Suites), want)
	}
	return archive
}

func githubTestsArtifactSkipMetric(t *testing.T, metrics *providerfoundation.Metrics, reason string) int {
	t.Helper()
	var buffer bytes.Buffer
	if err := metrics.WritePrometheus(&buffer); err != nil {
		t.Fatal(err)
	}
	prefix := `dev_health_provider_artifact_skipped_total{provider="github",dataset="cicd",reason="` + reason + `"} `
	scanner := bufio.NewScanner(&buffer)
	for scanner.Scan() {
		if value, found := strings.CutPrefix(scanner.Text(), prefix); found {
			count, err := strconv.Atoi(value)
			if err != nil {
				t.Fatal(err)
			}
			return count
		}
	}
	return 0
}

type githubTestsReplayDedupWalk struct {
	checkpoint githubTestsChunkCursor
	final      githubTestsChunkCursor
	finalBatch CompleteRouteBatch
	firstRows  []githubTestsReplayPinVerdict
	replayRows []githubTestsReplayPinVerdict
	metrics    *providerfoundation.Metrics
}

// githubTestsReplayDedupRun walks until stopAt holds, then resumes from the start of the artifacts phase, as a re-anchor
// does, and walks to the end. Both walks share one client, so the metric holds both reads.
func githubTestsReplayDedupRun(t *testing.T, doer *githubTestsReplayDedupDoer, stopAt func(githubTestsChunkCursor) bool) githubTestsReplayDedupWalk {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(doer))
	client.Metrics = providerfoundation.NewMetrics()
	claim := nativeTestClaim("github", "cicd")
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	walk := githubTestsReplayDedupWalk{metrics: client.Metrics}
	stop := errors.New("stop at the checkpoint")
	err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, now, "",
		func(emission ChunkRouteEmission) error {
			walk.firstRows = append(walk.firstRows, githubTestsReplayVerdicts(t, emission.Batch)...)
			cursor, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
			if decodeErr == nil && stopAt(cursor) {
				walk.checkpoint = cursor
				return stop
			}
			return nil
		})
	if !errors.Is(err, stop) {
		t.Fatalf("err=%v, want the walk stopped at the checkpoint", err)
	}
	resume := walk.checkpoint
	resume.Phase, resume.Index, resume.NextURL, resume.Relist = "artifacts", 0, "", 0
	encoded, err := encodeGitHubTestsChunkCursor(resume)
	if err != nil {
		t.Fatal(err)
	}
	err = GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, now, encoded,
		func(emission ChunkRouteEmission) error {
			walk.replayRows = append(walk.replayRows, githubTestsReplayVerdicts(t, emission.Batch)...)
			if cursor, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter); decodeErr == nil {
				walk.final = cursor
			}
			if emission.Final {
				walk.finalBatch = emission.Batch
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	return walk
}

func githubTestsReplayVerdicts(t *testing.T, batch CompleteRouteBatch) []githubTestsReplayPinVerdict {
	t.Helper()
	var rows []githubTestsReplayPinVerdict
	for _, effect := range batch.Effects {
		if effect.Destination != testOpsRunReportCompletenessDestination {
			continue
		}
		for _, raw := range effect.Rows {
			var row githubTestsReplayPinVerdict
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func githubTestsMemberObservation(incomplete []GitHubTestsIncomplete, cause string) int {
	for _, observation := range incomplete {
		if observation.Component == githubTestsReportMemberComponent && observation.Cause == cause {
			return observation.Count
		}
	}
	return 0
}

func githubTestsMarkersOf(markers []GitHubTestsSkippedArtifact, cause string) int {
	count := 0
	for _, marker := range markers {
		if githubTestsSkippedArtifactCause(marker) == cause {
			count++
		}
	}
	return count
}

func githubTestsCauseRecorded(cause string) func(githubTestsChunkCursor) bool {
	return func(cursor githubTestsChunkCursor) bool { return cursor.SkippedArtifactCauseCount[cause] >= 1 }
}

// RC1: a whole-artifact skip of a deterministic cause counts once when a re-anchor reads the artifact again.
func TestGitHubTestsReanchorCountsWholeArtifactSkipOnce(t *testing.T) {
	for _, cell := range []struct{ name, kind, cause string }{
		{"artifact_oversized", "oversized", githubTestsArtifactOversizedCause},
		{"unreadable_archive/not_a_zip", "not_a_zip", githubTestsUnreadableArchiveCause},
		{"unreadable_archive/empty_body", "empty_body", githubTestsUnreadableArchiveCause},
	} {
		t.Run(cell.name, func(t *testing.T) {
			doer := &githubTestsReplayDedupDoer{t: t, downloads: map[int]int{}, runs: map[int][]githubTestsReplayArtifact{
				1: {{id: 1, kind: cell.kind}, {id: 2, kind: "healthy"}},
			}}
			walk := githubTestsReplayDedupRun(t, doer, githubTestsCauseRecorded(cell.cause))
			if doer.downloads[1] < 2 {
				t.Fatalf("downloads=%d: the artifact was not read again, so the cell proves nothing", doer.downloads[1])
			}
			count, causeCount := githubTestsMemberObservation(walk.final.Incomplete, cell.cause), walk.final.SkippedArtifactCauseCount[cell.cause]
			markers, metric := githubTestsMarkersOf(walk.final.SkippedArtifacts, cell.cause), githubTestsArtifactSkipMetric(t, walk.metrics, cell.cause)
			if count != 1 || causeCount != 1 || markers != 1 || metric != 1 {
				t.Fatalf("Count=%d cause_count=%d markers=%d metric=%d, want the one artifact counted once", count, causeCount, markers, metric)
			}
		})
	}
}

// artifact_unavailable can heal between looks, so each failed look counts (acp ruling): a replay that fails again counts again.
func TestGitHubTestsReanchorCountsUnavailableArtifactPerFailedLook(t *testing.T) {
	doer := &githubTestsReplayDedupDoer{t: t, downloads: map[int]int{}, runs: map[int][]githubTestsReplayArtifact{
		1: {{id: 1, kind: "unavailable"}, {id: 2, kind: "healthy"}},
	}}
	walk := githubTestsReplayDedupRun(t, doer, githubTestsCauseRecorded(githubTestsArtifactUnavailableCause))
	if doer.downloads[1] != 2 {
		t.Fatalf("downloads=%d, want 2 failed looks", doer.downloads[1])
	}
	cause := githubTestsArtifactUnavailableCause
	count, causeCount, metric := githubTestsMemberObservation(walk.final.Incomplete, cause), walk.final.SkippedArtifactCauseCount[cause], githubTestsArtifactSkipMetric(t, walk.metrics, cause)
	if count != 2 || causeCount != 2 || metric != 2 {
		t.Fatalf("Count=%d cause_count=%d metric=%d, want 2: each failed look counts", count, causeCount, metric)
	}
	if walk.finalBatch.Watermark == nil {
		t.Fatal("watermark withheld")
	}
}

// RC2: a member skip counts once when a re-anchor reads its artifact again.
func TestGitHubTestsReanchorCountsMemberSkipOnce(t *testing.T) {
	for _, cause := range []string{githubTestsMalformedCause, githubTestsUnreadableCause, githubTestsUnsafeDoctypeCause} {
		t.Run(cause, func(t *testing.T) {
			doer := &githubTestsReplayDedupDoer{t: t, downloads: map[int]int{}, runs: map[int][]githubTestsReplayArtifact{
				1: {{id: 1, kind: "archive", archive: githubTestsMemberSkipArchive(t, cause, 1)}},
			}}
			walk := githubTestsReplayDedupRun(t, doer, githubTestsCauseRecorded(cause))
			if doer.downloads[1] < 2 {
				t.Fatalf("downloads=%d: the artifact was not read again, so the cell proves nothing", doer.downloads[1])
			}
			count, causeCount := githubTestsMemberObservation(walk.final.Incomplete, cause), walk.final.SkippedArtifactCauseCount[cause]
			if count != 1 || causeCount != 1 {
				t.Fatalf("Count=%d cause_count=%d, want the one member counted once", count, causeCount)
			}
			if walk.finalBatch.Watermark == nil {
				t.Fatal("watermark withheld")
			}
		})
	}
}

// RC3: an artifact with five bad members keeps one marker, so the sample has room and a later run's expired artifact keeps
// its marker: a re-anchor then counts the expired artifact once.
func TestGitHubTestsPerMemberSkipsWriteOneMarker(t *testing.T) {
	members := githubTestsMaxSkippedArtifactRecords
	doer := &githubTestsReplayDedupDoer{t: t, downloads: map[int]int{}, runs: map[int][]githubTestsReplayArtifact{
		1: {{id: 1, kind: "archive", archive: githubTestsMemberSkipArchive(t, githubTestsMalformedCause, members)}},
		2: {{id: 2, kind: "expired"}},
	}}
	walk := githubTestsReplayDedupRun(t, doer, githubTestsCauseRecorded(githubTestsArtifactExpiredCause))
	if len(walk.checkpoint.SkippedArtifacts) == 0 || githubTestsSkippedArtifactCause(walk.checkpoint.SkippedArtifacts[0]) != githubTestsMalformedCause {
		t.Fatalf("checkpoint sample=%+v, want the malformed artifact read before the expired one", walk.checkpoint.SkippedArtifacts)
	}
	if doer.downloads[1] < 2 {
		t.Fatalf("downloads=%d: the artifact was not read again, so the cell proves nothing", doer.downloads[1])
	}
	if got := walk.checkpoint.SkippedArtifactCauseCount[githubTestsMalformedCause]; got != members {
		t.Errorf("first walk cause_count=%d, want %d: every member is counted", got, members)
	}
	if markers := githubTestsMarkersOf(walk.checkpoint.SkippedArtifacts, githubTestsMalformedCause); markers != 1 || walk.checkpoint.SkippedArtifactsOverflow != 0 {
		t.Errorf("first walk malformed markers=%d overflow=%d, want one marker per (run, artifact, cause) and room in the sample", markers, walk.checkpoint.SkippedArtifactsOverflow)
	}
	if count := githubTestsMemberObservation(walk.final.Incomplete, githubTestsArtifactExpiredCause); count != 1 {
		t.Errorf("expired Count=%d after the re-anchor, want 1: its marker must still be in the sample", count)
	}
	if count, causeCount := githubTestsMemberObservation(walk.final.Incomplete, githubTestsMalformedCause), walk.final.SkippedArtifactCauseCount[githubTestsMalformedCause]; count != members || causeCount != members {
		t.Errorf("malformed Count=%d cause_count=%d after the re-anchor, want %d", count, causeCount, members)
	}
	if walk.finalBatch.Watermark == nil {
		t.Error("watermark withheld")
	}
}

// githubTestsPeelReplayDoer serves the listing fake, plus one artifact for each run it names.
type githubTestsPeelReplayDoer struct {
	fake      *listingFake
	artifacts map[int]string // run id -> kind; the artifact id equals the run id
	downloads map[int]int
}

func (doer *githubTestsPeelReplayDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	if strings.HasPrefix(path, "/repos/acme/api/actions/runs/") && strings.HasSuffix(path, "/artifacts") {
		run, err := strconv.Atoi(strings.Split(strings.TrimPrefix(path, "/repos/acme/api/actions/runs/"), "/")[0])
		if kind, found := doer.artifacts[run]; err == nil && found {
			doer.fake.mu.Lock()
			doer.fake.artifactGets[run]++
			doer.fake.mu.Unlock()
			body := githubTestsReplayArtifactsJSON([]githubTestsReplayArtifact{{id: run, kind: kind}})
			return doer.fake.respond(request, http.Header{"Content-Type": {"application/json"}}, body, 200), nil
		}
	}
	if strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip") {
		id := githubTestsArtifactIDFromPath(doer.fake.t, path)
		doer.downloads[id]++
		return githubTestsReplayArtifactResponse(doer.fake.t, request, githubTestsReplayArtifact{id: id, kind: doer.artifacts[id]}), nil
	}
	return doer.fake.Do(request)
}

// RC4: runs that share the cut second of a peeled range are listed again by the next range; their skips count once.
func TestGitHubTestsPeelBoundaryReplayCountsOnce(t *testing.T) {
	runs := makeListingRuns(1, 998, listingSince.Add(time.Hour), 10*time.Second)
	artifacts := map[int]string{1: "healthy"}
	const boundary = 5
	for index := 0; index < boundary; index++ {
		id := 5000 + index
		runs = append(runs, listingRun{id: id, created: listingSince, updated: listingSince.Add(time.Minute), status: "completed", conclusion: "success"})
		artifacts[id] = "not_a_zip"
	}
	doer := &githubTestsPeelReplayDoer{fake: newListingFake(t, runs), artifacts: artifacts, downloads: map[int]int{}}
	client := githubTestsClient(t, fakehttp.Client(doer))
	client.Metrics = providerfoundation.NewMetrics()
	claim := nativeTestClaim("github", "cicd")
	since, before := listingSince, listingSince.Add(24*time.Hour)
	claim.SinceAt, claim.BeforeAt = &since, &before
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var final githubTestsChunkCursor
	var finalBatch CompleteRouteBatch
	resume := ""
	for pass := 1; ; pass++ {
		if pass > 1000 {
			t.Fatal("the walk never reached a final emission")
		}
		last, finalSeen := resume, false
		err := GitHubTestsRouteHandler{}.CollectChunks(ctx, claim, providerfoundation.Credential{}, client, now, resume,
			func(emission ChunkRouteEmission) error {
				last = emission.CursorAfter
				if emission.Final {
					finalBatch, finalSeen = emission.Batch, true
				}
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
		if finalSeen {
			cursor, decodeErr := decodeGitHubTestsChunkCursor(last)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			final = cursor
			break
		}
		resume = last
	}
	reread := 0
	for index := 0; index < boundary; index++ {
		if doer.downloads[5000+index] > 1 {
			reread++
		}
	}
	if reread == 0 {
		t.Fatalf("downloads=%v: no boundary run was read twice, so the cell proves nothing", doer.downloads)
	}
	cause := githubTestsUnreadableArchiveCause
	count, causeCount, metric := githubTestsMemberObservation(final.Incomplete, cause), final.SkippedArtifactCauseCount[cause], githubTestsArtifactSkipMetric(t, client.Metrics, cause)
	if count != boundary || causeCount != boundary || metric != boundary {
		t.Fatalf("Count=%d cause_count=%d metric=%d (%d boundary runs read twice), want %d: one per distinct artifact", count, causeCount, metric, reread, boundary)
	}
	if finalBatch.Watermark == nil {
		t.Fatal("watermark withheld")
	}
}

// RC5 (pin, green before the fix): when the replay of a run is not counted again, the run's verdict row still carries the loss
// and equals the first read's row.
func TestGitHubTestsReanchorDedupKeepsRunVerdictLoss(t *testing.T) {
	cells := []struct {
		name     string
		artifact func(t *testing.T) githubTestsReplayArtifact
		cause    string
	}{
		{"artifact_oversized", func(*testing.T) githubTestsReplayArtifact { return githubTestsReplayArtifact{id: 1, kind: "oversized"} }, githubTestsArtifactOversizedCause},
		{"unreadable_archive", func(*testing.T) githubTestsReplayArtifact { return githubTestsReplayArtifact{id: 1, kind: "not_a_zip"} }, githubTestsUnreadableArchiveCause},
		{"malformed", func(t *testing.T) githubTestsReplayArtifact {
			return githubTestsReplayArtifact{id: 1, kind: "archive", archive: githubTestsMemberSkipArchive(t, githubTestsMalformedCause, 1)}
		}, githubTestsMalformedCause},
		{"unsafe_doctype", func(t *testing.T) githubTestsReplayArtifact {
			return githubTestsReplayArtifact{id: 1, kind: "archive", archive: githubTestsMemberSkipArchive(t, githubTestsUnsafeDoctypeCause, 1)}
		}, githubTestsUnsafeDoctypeCause},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			doer := &githubTestsReplayDedupDoer{t: t, downloads: map[int]int{}, runs: map[int][]githubTestsReplayArtifact{
				1: {cell.artifact(t), {id: 2, kind: "healthy"}},
			}}
			walk := githubTestsReplayDedupRun(t, doer, githubTestsCauseRecorded(cell.cause))
			if len(walk.firstRows) == 0 || len(walk.replayRows) == 0 {
				t.Fatalf("verdict rows first=%d replay=%d, want the run's verdict in both walks", len(walk.firstRows), len(walk.replayRows))
			}
			first, replay := walk.firstRows[len(walk.firstRows)-1], walk.replayRows[len(walk.replayRows)-1]
			if first.Complete || !slices.Contains(first.Reasons, cell.cause) {
				t.Fatalf("first verdict=%+v, want partial with %s", first, cell.cause)
			}
			if !reflect.DeepEqual(replay, first) {
				t.Fatalf("replay verdict=%+v, want the first read's %+v", replay, first)
			}
		})
	}
}
