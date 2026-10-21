package providersync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// githubTestsSizedArtifact is one listed artifact: what the listing declares,
// what the final (post-redirect) response declares, and what its body holds.
type githubTestsSizedArtifact struct {
	listedSize    string // raw JSON for size_in_bytes; "" omits the field
	contentLength int64  // -1 = not declared
	body          []byte // nil = a synthetic body of bodyBytes bytes
	bodyBytes     int
	// redirectContentLength is the Content-Length of the API's 302 itself.
	redirectContentLength int64
}

type githubTestsSizedDoer struct {
	t         *testing.T
	artifacts map[int]githubTestsSizedArtifact
	archive   map[int]int // API /zip requests per artifact
	blob      map[int]int // blob GETs per artifact
	read      map[int]int // body bytes the route read per artifact
}

func newGitHubTestsSizedDoer(t *testing.T, artifacts map[int]githubTestsSizedArtifact) *githubTestsSizedDoer {
	return &githubTestsSizedDoer{t: t, artifacts: artifacts, archive: map[int]int{}, blob: map[int]int{}, read: map[int]int{}}
}

type githubTestsCountingBody struct {
	reader io.Reader
	add    func(int)
}

func (body githubTestsCountingBody) Read(p []byte) (int, error) {
	n, err := body.reader.Read(p)
	body.add(n)
	return n, err
}

func (doer *githubTestsSizedDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case request.URL.Host == "blob.example":
		id := githubTestsArtifactIDFromBlobPath(doer.t, path)
		doer.blob[id]++
		artifact := doer.artifacts[id]
		var source io.Reader = bytes.NewReader(artifact.body)
		if artifact.body == nil {
			source = &githubTestsRepeatingReader{remaining: artifact.bodyBytes}
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/zip"}},
			ContentLength: artifact.contentLength, Request: request,
			Body: io.NopCloser(githubTestsCountingBody{reader: source, add: func(n int) { doer.read[id] += n }}),
		}, nil
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, 1)), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		var list strings.Builder
		list.WriteString(`{"artifacts":[`)
		for id := 1; id <= len(doer.artifacts); id++ {
			if id > 1 {
				list.WriteByte(',')
			}
			list.WriteString(`{"id":` + strconv.Itoa(id) + `,"name":"test-results-` + strconv.Itoa(id) + `","expired":false`)
			if size := doer.artifacts[id].listedSize; size != "" {
				list.WriteString(`,"size_in_bytes":` + size)
			}
			list.WriteByte('}')
		}
		list.WriteString(`]}`)
		return githubTestsHTTPResponse(request, header, list.String()), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id := githubTestsArtifactIDFromPath(doer.t, path)
		doer.archive[id]++
		return &http.Response{
			StatusCode:    http.StatusFound,
			Header:        http.Header{"Location": {"https://blob.example/" + strconv.Itoa(id) + ".zip"}},
			ContentLength: doer.artifacts[id].redirectContentLength,
			Body:          io.NopCloser(strings.NewReader("")),
			Request:       request,
		}, nil
	}
	doer.t.Fatalf("unexpected request %s", request.URL.String())
	return nil, nil
}

func githubTestsHealthyArchive(t *testing.T) []byte {
	t.Helper()
	return githubTestsZip(t, map[string]string{"junit.xml": githubTestsMultiSuiteJUnit(1)})
}

func githubTestsSizedWalk(t *testing.T, doer *githubTestsSizedDoer, maxBytes int64) githubTestsWalk {
	t.Helper()
	return walkGitHubTestsChunks(t, GitHubTestsRouteHandler{MaxArtifactBytes: maxBytes},
		nativeTestClaim("github", "cicd"), githubTestsClient(t, fakehttp.Client(doer)), 8)
}

func githubTestsOversizedMarker(t *testing.T, walk githubTestsWalk, artifactID string) GitHubTestsSkippedArtifact {
	t.Helper()
	markers, _ := walk.final.Result["skipped_artifacts"].([]GitHubTestsSkippedArtifact)
	for _, marker := range markers {
		if marker.ArtifactID == artifactID {
			if marker.Cause != githubTestsArtifactOversizedCause {
				t.Fatalf("marker=%+v, want cause %s", marker, githubTestsArtifactOversizedCause)
			}
			return marker
		}
	}
	t.Fatalf("no oversized marker for artifact %s in %#v", artifactID, walk.final.Result["skipped_artifacts"])
	return GitHubTestsSkippedArtifact{}
}

const githubTestsSizeCap = 4096

// A listed size above the cap is the same artifact_oversized skip as today,
// reached without a download: no API request, no blob request, no byte read.
func TestGitHubTestsListedSizeAboveCapSkipsWithoutDownload(t *testing.T) {
	healthy := githubTestsHealthyArchive(t)
	doer := newGitHubTestsSizedDoer(t, map[int]githubTestsSizedArtifact{
		1: {listedSize: strconv.Itoa(githubTestsSizeCap + 500), contentLength: githubTestsSizeCap + 500, bodyBytes: githubTestsSizeCap + 500},
		2: {listedSize: strconv.Itoa(len(healthy)), contentLength: int64(len(healthy)), body: healthy},
	})
	walk := githubTestsSizedWalk(t, doer, githubTestsSizeCap)
	if doer.archive[1] != 0 || doer.blob[1] != 0 || doer.read[1] != 0 {
		t.Fatalf("oversized artifact: api=%d blob=%d bytes_read=%d, want 0/0/0", doer.archive[1], doer.blob[1], doer.read[1])
	}
	if doer.archive[2] != 1 || walk.cursor.Suites != 1 {
		t.Fatalf("healthy artifact: api=%d suites=%d, want 1/1", doer.archive[2], walk.cursor.Suites)
	}
	marker := githubTestsOversizedMarker(t, walk, "1")
	if marker.SizeBytes != githubTestsSizeCap+500 || marker.CapBytes != githubTestsSizeCap {
		t.Fatalf("marker size=%d cap=%d, want the declared %d and cap %d", marker.SizeBytes, marker.CapBytes, githubTestsSizeCap+500, githubTestsSizeCap)
	}
	if walk.cursor.ArchivesSeen == nil || *walk.cursor.ArchivesSeen != 1 {
		t.Fatalf("ArchivesSeen=%v, want 1 (oversized stays out of totality)", intPtrString(walk.cursor.ArchivesSeen))
	}
}

// A missing, zero, negative or non-integer listed size is unknown, never
// small: the artifact is downloaded, and the final response's Content-Length
// above the cap refuses it before any body byte is read.
func TestGitHubTestsUnknownListedSizeUsesTheFinalContentLength(t *testing.T) {
	for _, listed := range []string{"", "0", "-5", "null", `"abc"`, "1.5"} {
		t.Run("size_in_bytes="+listed, func(t *testing.T) {
			doer := newGitHubTestsSizedDoer(t, map[int]githubTestsSizedArtifact{
				1: {listedSize: listed, contentLength: githubTestsSizeCap + 500, bodyBytes: githubTestsSizeCap + 500},
			})
			walk := githubTestsSizedWalk(t, doer, githubTestsSizeCap)
			if doer.archive[1] != 1 || doer.blob[1] != 1 {
				t.Fatalf("api=%d blob=%d, want 1/1: an unknown size is downloaded", doer.archive[1], doer.blob[1])
			}
			if doer.read[1] != 0 {
				t.Fatalf("bytes_read=%d, want 0: Content-Length above the cap refuses before the read", doer.read[1])
			}
			marker := githubTestsOversizedMarker(t, walk, "1")
			if marker.SizeBytes != githubTestsSizeCap+500 {
				t.Fatalf("marker size=%d, want the declared Content-Length %d", marker.SizeBytes, githubTestsSizeCap+500)
			}
		})
	}
}

// Declared sizes that UNDER-report never widen the read: the body still stops
// at cap+1 and is the same oversized skip as today.
func TestGitHubTestsUnderReportedSizeStillStopsAtTheCap(t *testing.T) {
	for name, artifact := range map[string]githubTestsSizedArtifact{
		"listed and content-length small": {listedSize: "100", contentLength: 100, bodyBytes: githubTestsSizeCap + 500},
		"no content-length":               {listedSize: "100", contentLength: -1, bodyBytes: githubTestsSizeCap + 500},
	} {
		t.Run(name, func(t *testing.T) {
			doer := newGitHubTestsSizedDoer(t, map[int]githubTestsSizedArtifact{1: artifact})
			walk := githubTestsSizedWalk(t, doer, githubTestsSizeCap)
			if doer.read[1] > githubTestsSizeCap+1 {
				t.Fatalf("bytes_read=%d, want <= cap+1 (%d)", doer.read[1], githubTestsSizeCap+1)
			}
			marker := githubTestsOversizedMarker(t, walk, "1")
			if marker.SizeBytes != githubTestsSizeCap+1 {
				t.Fatalf("marker size=%d, want cap+1 (%d), the bound the read crossed", marker.SizeBytes, githubTestsSizeCap+1)
			}
		})
	}
}

// The cap boundary: a declared size equal to the cap is downloaded and parsed;
// one byte above is skipped without a download.
func TestGitHubTestsListedSizeCapBoundary(t *testing.T) {
	healthy := githubTestsHealthyArchive(t)
	capBytes := int64(len(healthy))
	for _, row := range []struct {
		name      string
		listed    int64
		download  int
		suites    int
		oversized bool
	}{
		{"cap-1", capBytes - 1, 1, 1, false},
		{"cap", capBytes, 1, 1, false},
		{"cap+1", capBytes + 1, 0, 0, true},
	} {
		t.Run(row.name, func(t *testing.T) {
			doer := newGitHubTestsSizedDoer(t, map[int]githubTestsSizedArtifact{
				1: {listedSize: strconv.FormatInt(row.listed, 10), contentLength: capBytes, body: healthy},
			})
			walk := githubTestsSizedWalk(t, doer, capBytes)
			if doer.archive[1] != row.download || walk.cursor.Suites != row.suites {
				t.Fatalf("api=%d suites=%d, want %d/%d", doer.archive[1], walk.cursor.Suites, row.download, row.suites)
			}
			if row.oversized {
				if marker := githubTestsOversizedMarker(t, walk, "1"); marker.SizeBytes != capBytes+1 {
					t.Fatalf("marker size=%d, want %d", marker.SizeBytes, capBytes+1)
				}
			}
		})
	}
}

// The size check reads the FINAL response: the API's redirect may carry its own
// Content-Length, which says nothing about the blob.
func TestGitHubTestsRedirectContentLengthIsNotTheArtifactSize(t *testing.T) {
	healthy := githubTestsHealthyArchive(t)
	doer := newGitHubTestsSizedDoer(t, map[int]githubTestsSizedArtifact{
		1: {contentLength: int64(len(healthy)), body: healthy, redirectContentLength: githubTestsSizeCap * 10},
	})
	walk := githubTestsSizedWalk(t, doer, githubTestsSizeCap)
	if doer.blob[1] != 1 || walk.cursor.Suites != 1 {
		t.Fatalf("blob=%d suites=%d, want 1/1: a large redirect Content-Length must not refuse the blob", doer.blob[1], walk.cursor.Suites)
	}
}

// The oracle Collect keeps its terminal disposition for an oversized artifact,
// and also reaches it without a download.
func TestGitHubTestsCollectListedSizeAboveCapFailsWithoutDownload(t *testing.T) {
	doer := newGitHubTestsSizedDoer(t, map[int]githubTestsSizedArtifact{
		1: {listedSize: strconv.Itoa(githubTestsSizeCap + 500), contentLength: githubTestsSizeCap + 500, bodyBytes: githubTestsSizeCap + 500},
	})
	_, err := (GitHubTestsRouteHandler{MaxArtifactBytes: githubTestsSizeCap}).Collect(
		context.Background(), nativeTestClaim("github", "cicd"), providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrGitHubTestsArtifactOversized) {
		t.Fatalf("err=%v, want ErrGitHubTestsArtifactOversized", err)
	}
	if doer.archive[1] != 0 || doer.read[1] != 0 {
		t.Fatalf("api=%d bytes_read=%d, want 0/0", doer.archive[1], doer.read[1])
	}
}

type gitLabSizedDoer struct {
	contentLength int64
	bodyBytes     int
	read          int
}

func (doer *gitLabSizedDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/zip"}},
		ContentLength: doer.contentLength, Request: request,
		Body: io.NopCloser(githubTestsCountingBody{reader: &githubTestsRepeatingReader{remaining: doer.bodyBytes}, add: func(n int) { doer.read += n }}),
	}, nil
}

func TestGitLabTestsArtifactContentLengthAboveCapReadsNothing(t *testing.T) {
	doer := &gitLabSizedDoer{contentLength: gitLabTestsMaxDownload + 500, bodyBytes: gitLabTestsMaxDownload + 500}
	_, err := downloadGitLabTestsArtifact(context.Background(), sizeDoerClient(t, "gitlab", doer), "/projects/1", "2")
	requireRouteFailure(t, err, gitlabJobArtifactsRoute, providerfoundation.ReasonBodyTooLarge)
	if !errors.Is(err, ErrGitLabTestsIncomplete) || doer.read != 0 {
		t.Fatalf("err=%v bytes_read=%d, want ErrGitLabTestsIncomplete and 0 bytes", err, doer.read)
	}
}

func TestGitLabTestsArtifactUnderReportedContentLengthStopsAtTheCap(t *testing.T) {
	doer := &gitLabSizedDoer{contentLength: 100, bodyBytes: gitLabTestsMaxDownload + 500}
	_, err := downloadGitLabTestsArtifact(context.Background(), sizeDoerClient(t, "gitlab", doer), "/projects/1", "2")
	requireRouteFailure(t, err, gitlabJobArtifactsRoute, providerfoundation.ReasonBodyTooLarge)
	if doer.read > gitLabTestsMaxDownload+1 {
		t.Fatalf("bytes_read=%d, want <= cap+1", doer.read)
	}
}

func sizeDoerClient(t *testing.T, provider string, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(provider, "https://api.example.test", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// githubTestsAllocated is the heap the call allocates (runtime TotalAlloc).
func githubTestsAllocated(call func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	call()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// githubTestsOverCapAllocationBound pins the fix by a number: refusing one
// artifact above the default 100 MiB cap allocates far below the cap. The
// old path allocated the capped read itself, cap+1 bytes and more.
const githubTestsOverCapAllocationBound = 1 << 20

func TestGitHubTestsOverCapArtifactAllocationIsFarBelowTheCap(t *testing.T) {
	overCap := githubTestsMaxDownloadSize + 500
	t.Run("declared size skip, whole unit", func(t *testing.T) {
		doer := newGitHubTestsSizedDoer(t, map[int]githubTestsSizedArtifact{
			1: {listedSize: strconv.Itoa(overCap), contentLength: int64(overCap), bodyBytes: overCap},
		})
		client := githubTestsClient(t, fakehttp.Client(doer))
		var walk githubTestsWalk
		allocated := githubTestsAllocated(func() {
			walk = walkGitHubTestsChunks(t, GitHubTestsRouteHandler{}, nativeTestClaim("github", "cicd"), client, 8)
		})
		githubTestsOversizedMarker(t, walk, "1")
		t.Logf("ALLOC bytes_read=%d allocated=%d bound=%d", doer.read[1], allocated, githubTestsOverCapAllocationBound)
		if doer.read[1] != 0 || allocated >= githubTestsOverCapAllocationBound {
			t.Fatalf("bytes_read=%d allocated=%d, want 0 and < %d", doer.read[1], allocated, githubTestsOverCapAllocationBound)
		}
	})
	t.Run("content-length refusal, one download", func(t *testing.T) {
		doer := newGitHubTestsSizedDoer(t, map[int]githubTestsSizedArtifact{
			1: {contentLength: int64(overCap), bodyBytes: overCap},
		})
		client := githubTestsClient(t, fakehttp.Client(doer))
		var err error
		allocated := githubTestsAllocated(func() {
			_, _, _, err = downloadGitHubTestsArtifact(context.Background(), client, "/repos/acme/api", "1", githubTestsMaxDownloadSize)
		})
		if !errors.Is(err, ErrGitHubTestsArtifactOversized) {
			t.Fatalf("err=%v, want ErrGitHubTestsArtifactOversized", err)
		}
		t.Logf("ALLOC bytes_read=%d allocated=%d bound=%d", doer.read[1], allocated, githubTestsOverCapAllocationBound)
		if doer.read[1] != 0 || allocated >= githubTestsOverCapAllocationBound {
			t.Fatalf("bytes_read=%d allocated=%d, want 0 and < %d", doer.read[1], allocated, githubTestsOverCapAllocationBound)
		}
	})
	t.Run("gitlab content-length refusal, one download", func(t *testing.T) {
		doer := &gitLabSizedDoer{contentLength: gitLabTestsMaxDownload + 500, bodyBytes: gitLabTestsMaxDownload + 500}
		client := sizeDoerClient(t, "gitlab", doer)
		var err error
		allocated := githubTestsAllocated(func() {
			_, err = downloadGitLabTestsArtifact(context.Background(), client, "/projects/1", "2")
		})
		if !errors.Is(err, ErrGitLabTestsIncomplete) {
			t.Fatalf("err=%v, want ErrGitLabTestsIncomplete", err)
		}
		t.Logf("ALLOC bytes_read=%d allocated=%d bound=%d", doer.read, allocated, githubTestsOverCapAllocationBound)
		if doer.read != 0 || allocated >= githubTestsOverCapAllocationBound {
			t.Fatalf("bytes_read=%d allocated=%d, want 0 and < %d", doer.read, allocated, githubTestsOverCapAllocationBound)
		}
	})
}
