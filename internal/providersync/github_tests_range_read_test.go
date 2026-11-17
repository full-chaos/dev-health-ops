package providersync

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// githubTestsRangeDoer serves one artifact zip with http.ServeContent, which
// answers Range requests natively. Like Azure Blob storage it ignores a suffix
// range (bytes=-N). The API either redirects to the blob or serves the zip
// itself; each redirect is a new pre-signed URL generation.
type githubTestsRangeDoer struct {
	archive        []byte
	direct         bool
	ignoreRange    bool
	noAcceptRanges bool
	noLength       bool
	goneAfter      int // blob answers 404 once this many range requests were served; 0 = never
	expireAfter    int // range requests after this many answer 403 on the first URL generation; 0 = never
	expireAlways   bool
	generation     int
	api, blob      int
	ranged         int
	ranges         []string
}

func (doer *githubTestsRangeDoer) Do(request *http.Request) (*http.Response, error) {
	switch request.URL.Host {
	case "api.github.com":
		doer.api++
		if !doer.direct {
			doer.generation++
			return &http.Response{
				StatusCode: http.StatusFound, Header: http.Header{"Location": {"https://blob.example/artifact.zip?sig=" + strconv.Itoa(doer.generation)}},
				Body: http.NoBody, Request: request,
			}, nil
		}
	case "blob.example":
		doer.blob++
	default:
		return &http.Response{StatusCode: http.StatusNotFound, Body: http.NoBody, Request: request}, nil
	}
	request = request.Clone(request.Context())
	if spec := request.Header.Get("Range"); spec != "" {
		doer.ranged++
		doer.ranges = append(doer.ranges, spec)
		if doer.goneAfter > 0 && doer.ranged > doer.goneAfter {
			return &http.Response{StatusCode: http.StatusNotFound, Body: http.NoBody, Request: request}, nil
		}
		if doer.expireAfter > 0 && doer.ranged > doer.expireAfter && (doer.expireAlways || request.URL.Query().Get("sig") == "1") {
			return &http.Response{StatusCode: http.StatusForbidden, Body: http.NoBody, Request: request}, nil
		}
		if doer.ignoreRange || strings.HasPrefix(spec, "bytes=-") {
			request.Header.Del("Range")
		}
	}
	recorder := httptest.NewRecorder()
	http.ServeContent(recorder, request, "artifact.zip", time.Time{}, bytes.NewReader(doer.archive))
	response := recorder.Result()
	response.Request = request
	if doer.noAcceptRanges {
		response.Header.Del("Accept-Ranges")
	}
	if doer.noLength && request.Header.Get("Range") == "" {
		response.Header.Del("Content-Length")
		response.ContentLength = -1
	}
	return response, nil
}

func (doer *githubTestsRangeDoer) requests() int { return doer.api + doer.blob }

type githubTestsRangeMember struct {
	name   string
	body   []byte
	store  bool
	rawLen uint64 // >0: a raw entry whose header declares this uncompressed size
}

func githubTestsRangeZip(t *testing.T, members ...githubTestsRangeMember) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for _, member := range members {
		if member.rawLen > 0 {
			entry, err := writer.CreateRaw(&zip.FileHeader{
				Name: member.name, Method: zip.Deflate, CRC32: crc32.ChecksumIEEE(member.body),
				CompressedSize64: uint64(len(member.body)), UncompressedSize64: member.rawLen,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := entry.Write(member.body); err != nil {
				t.Fatal(err)
			}
			continue
		}
		method := zip.Deflate
		if member.store {
			method = zip.Store
		}
		entry, err := writer.CreateHeader(&zip.FileHeader{Name: member.name, Method: method})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(member.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func githubTestsRandomBytes(seed uint64, n int) []byte {
	source := rand.New(rand.NewPCG(seed, seed))
	out := make([]byte, n)
	for index := range out {
		out[index] = byte(source.Uint32())
	}
	return out
}

// githubTestsPlaywrightLikeZip: a JUnit report first, then a big
// incompressible trace that puts the artifact over the cap.
func githubTestsPlaywrightLikeZip(t *testing.T, extra ...githubTestsRangeMember) []byte {
	t.Helper()
	members := []githubTestsRangeMember{
		{name: "results/junit.xml", body: []byte(githubTestsNamedMultiSuiteJUnit("pw-", 3))},
	}
	members = append(members, extra...)
	members = append(members,
		githubTestsRangeMember{name: "data/trace.zip", body: githubTestsRandomBytes(7, 9<<20), store: true},
		githubTestsRangeMember{name: "index.html", body: []byte("<html></html>")},
	)
	return githubTestsRangeZip(t, members...)
}

const githubTestsRangeCap = 1 << 20

func githubTestsRangeDownload(t *testing.T, doer *githubTestsRangeDoer, maxBytes int64) ([]byte, int, bool, error) {
	t.Helper()
	return downloadGitHubTestsArtifact(context.Background(), githubTestsClient(t, fakehttp.Client(doer)), "/repos/acme/api", "1", maxBytes)
}

func githubTestsParseRangeArchive(t *testing.T, archive []byte) githubTestsReportRows {
	t.Helper()
	rows, err := parseGitHubTestsArchive(archive, "1", "1", nil, "repo-1", "run-1", "org-1", nil, nil, time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("parse re-packed archive: %v", err)
	}
	return rows
}

func githubTestsWantOversized(t *testing.T, err error, size int64) {
	t.Helper()
	var oversized *githubTestsArtifactOversizedError
	if !errors.As(err, &oversized) {
		t.Fatalf("err=%v, want the artifact_oversized error", err)
	}
	if oversized.SizeBytes != size || oversized.CapBytes != githubTestsRangeCap {
		t.Fatalf("oversized size=%d cap=%d, want %d/%d", oversized.SizeBytes, oversized.CapBytes, size, githubTestsRangeCap)
	}
}

// (a)+(d) An over-cap artifact served with byte ranges yields its JUnit report:
// only the report member is fetched, and every request is counted.
func TestGitHubTestsOverCapArtifactRangeReadKeepsReports(t *testing.T) {
	for name, doer := range map[string]*githubTestsRangeDoer{
		"redirect":          {},
		"direct":            {direct: true},
		"no content-length": {noLength: true},
	} {
		t.Run(name, func(t *testing.T) {
			doer.archive = githubTestsPlaywrightLikeZip(t)
			archive, used, notFound, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
			if err != nil || notFound {
				t.Fatalf("err=%v notFound=%v, want the re-packed reports", err, notFound)
			}
			if len(archive) == 0 || len(archive) > 4096 {
				t.Fatalf("re-packed archive is %d bytes, want the report member only", len(archive))
			}
			rows := githubTestsParseRangeArchive(t, archive)
			if len(rows.Suites) != 3 || len(rows.Cases) != 3 || rows.ReportsParsed != 1 {
				t.Fatalf("suites=%d cases=%d reports=%d, want 3/3/1", len(rows.Suites), len(rows.Cases), rows.ReportsParsed)
			}
			if used != doer.requests() {
				t.Fatalf("used=%d, want every request sent (%d)", used, doer.requests())
			}
			if doer.ranged != 2 {
				t.Fatalf("range requests=%d (%v), want 2: the tail with the central directory and the block with the report", doer.ranged, doer.ranges)
			}
		})
	}
}

// (b) A server that ignores Range keeps today's artifact_oversized skip.
func TestGitHubTestsOverCapArtifactRangeIgnoredStaysOversized(t *testing.T) {
	doer := &githubTestsRangeDoer{ignoreRange: true}
	doer.archive = githubTestsPlaywrightLikeZip(t)
	archive, used, _, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
	githubTestsWantOversized(t, err, int64(len(doer.archive)))
	if archive != nil || used != doer.requests() || doer.ranged != 1 {
		t.Fatalf("archive=%d bytes used=%d requests=%d ranged=%d, want nil, all counted, one range probe", len(archive), used, doer.requests(), doer.ranged)
	}
}

// Without Accept-Ranges: bytes no range request is sent; today's skip.
func TestGitHubTestsOverCapArtifactWithoutAcceptRangesStaysOversized(t *testing.T) {
	doer := &githubTestsRangeDoer{noAcceptRanges: true}
	doer.archive = githubTestsPlaywrightLikeZip(t)
	_, used, _, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
	githubTestsWantOversized(t, err, int64(len(doer.archive)))
	if doer.ranged != 0 || used != 2 {
		t.Fatalf("ranged=%d used=%d, want 0/2", doer.ranged, used)
	}
}

// (c) Report members whose compressed bytes exceed the cap keep the skip.
func TestGitHubTestsOverCapArtifactReportsOverCapStayOversized(t *testing.T) {
	doer := &githubTestsRangeDoer{}
	doer.archive = githubTestsPlaywrightLikeZip(t,
		githubTestsRangeMember{name: "results/big-a.xml", body: githubTestsRandomBytes(11, githubTestsRangeCap/2+1), store: true},
		githubTestsRangeMember{name: "results/big-b.xml", body: githubTestsRandomBytes(12, githubTestsRangeCap/2+1), store: true},
	)
	archive, used, _, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
	githubTestsWantOversized(t, err, int64(len(doer.archive)))
	if archive != nil || used != doer.requests() {
		t.Fatalf("archive=%d bytes used=%d, want nil and %d", len(archive), used, doer.requests())
	}
}

// (e) A report member above the per-report bound is not fetched; the parser
// still records its archive_bounds skip from the header.
func TestGitHubTestsOverCapArtifactOverBoundReportIsNotFetched(t *testing.T) {
	doer := &githubTestsRangeDoer{}
	doer.archive = githubTestsPlaywrightLikeZip(t,
		githubTestsRangeMember{name: "results/huge.xml", body: githubTestsRandomBytes(13, 64<<10), rawLen: githubTestsMaxReportBytes + 1},
	)
	archive, _, _, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if len(archive) > 4096 {
		t.Fatalf("re-packed archive is %d bytes, want the over-bound member's data left out", len(archive))
	}
	rows := githubTestsParseRangeArchive(t, archive)
	if len(rows.Suites) != 3 || rows.Skipped != 1 {
		t.Fatalf("suites=%d skipped=%d, want 3/1", len(rows.Suites), rows.Skipped)
	}
	if len(rows.issues) != 1 || !rows.issues[0].blocking || rows.issues[0].evidence.Cause != "archive_bounds" {
		t.Fatalf("issues=%+v, want one blocking archive_bounds", rows.issues)
	}
}

// An artifact that disappears during the range read is notFound, as on the
// first response.
func TestGitHubTestsOverCapArtifactGoneDuringRangeRead(t *testing.T) {
	doer := &githubTestsRangeDoer{goneAfter: 1}
	doer.archive = githubTestsPlaywrightLikeZip(t)
	archive, used, notFound, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
	if err != nil || !notFound || archive != nil || used != doer.requests() {
		t.Fatalf("archive=%d err=%v notFound=%v used=%d, want nil/nil/true/%d", len(archive), err, notFound, used, doer.requests())
	}
}

// A pre-signed URL that expires mid-read (403) is re-resolved once through the
// API and the read resumes on the fresh URL.
func TestGitHubTestsOverCapArtifactExpiredURLIsRelocatedOnce(t *testing.T) {
	doer := &githubTestsRangeDoer{expireAfter: 1}
	doer.archive = githubTestsPlaywrightLikeZip(t)
	archive, used, notFound, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
	if err != nil || notFound {
		t.Fatalf("err=%v notFound=%v, want the re-packed reports", err, notFound)
	}
	if rows := githubTestsParseRangeArchive(t, archive); len(rows.Suites) != 3 {
		t.Fatalf("suites=%d, want 3", len(rows.Suites))
	}
	if doer.api != 2 || used != doer.requests() {
		t.Fatalf("api=%d used=%d requests=%d, want 2 API requests (one relocation) and every request counted", doer.api, used, doer.requests())
	}
}

// A second 403 after the one relocation keeps today's artifact_oversized skip.
func TestGitHubTestsOverCapArtifactExpiredTwiceStaysOversized(t *testing.T) {
	doer := &githubTestsRangeDoer{expireAfter: 1, expireAlways: true}
	doer.archive = githubTestsPlaywrightLikeZip(t)
	archive, used, _, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
	githubTestsWantOversized(t, err, int64(len(doer.archive)))
	if archive != nil || doer.api != 2 || used != doer.requests() {
		t.Fatalf("archive=%d api=%d used=%d requests=%d, want nil, one relocation, all counted", len(archive), doer.api, used, doer.requests())
	}
}

// (f) An artifact under the cap is read as before: one body, no range request.
func TestGitHubTestsUnderCapArtifactSendsNoRangeRequest(t *testing.T) {
	doer := &githubTestsRangeDoer{}
	doer.archive = githubTestsRangeZip(t, githubTestsRangeMember{name: "junit.xml", body: []byte(githubTestsMultiSuiteJUnit(2))})
	archive, used, _, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
	if err != nil || !bytes.Equal(archive, doer.archive) {
		t.Fatalf("err=%v identical=%v, want the served body unchanged", err, bytes.Equal(archive, doer.archive))
	}
	if doer.ranged != 0 || used != 2 {
		t.Fatalf("ranged=%d used=%d, want 0/2", doer.ranged, used)
	}
}

func TestParseGitHubTestsContentRange(t *testing.T) {
	for value, want := range map[string]bool{
		"bytes 0-9/10": true, "bytes 5-5/6": true,
		"bytes 0-10/10": false, "bytes */10": false, "bytes 0-9/*": false, "bytes 9-0/10": false, "0-9/10": false, "": false,
	} {
		if _, _, _, ok := parseGitHubTestsContentRange(value); ok != want {
			t.Errorf("%q: ok=%v, want %v", value, ok, want)
		}
	}
}
