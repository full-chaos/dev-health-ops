package providersync

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
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
	goneAfter      int // range requests after this many answer 404; 0 = never
	// range requests after expireAfter answer the blob store's
	// expired-signature 403 on the first URL generation (every generation with
	// expireAlways); forbidHeader replaces that answer with a plain 403
	// carrying these headers.
	expireAfter   int
	expireAlways  bool
	forbidHeader  http.Header
	refreshStatus int  // the second API request answers this status; 0 = redirect
	failFirst     bool // the first range request answers 503 once
	status416     int  // range requests after this many answer 416; 0 = never
	badEndFrom    int  // range requests from this one (1-based) carry a wrong Content-Range end; 0 = never
	sample        func()
	generation    int
	api, blob     int
	ranged        int
	ranges        []string
	refusedRead   int // body bytes read from range answers that are refused
}

const githubTestsExpiredSignatureBody = `<?xml version="1.0" encoding="utf-8"?><Error><Code>AuthenticationFailed</Code>` +
	`<Message>Server failed to authenticate the request.</Message><AuthenticationErrorDetail>Signature not valid in the specified time frame</AuthenticationErrorDetail></Error>`

func (doer *githubTestsRangeDoer) Do(request *http.Request) (*http.Response, error) {
	if doer.sample != nil {
		doer.sample()
	}
	switch request.URL.Host {
	case "api.github.com":
		doer.api++
		if doer.api == 2 && doer.refreshStatus != 0 {
			return &http.Response{StatusCode: doer.refreshStatus, Header: http.Header{}, Body: http.NoBody, Request: request}, nil
		}
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
	spec := request.Header.Get("Range")
	if spec != "" {
		doer.ranged++
		doer.ranges = append(doer.ranges, spec)
		switch {
		case doer.failFirst && doer.ranged == 1:
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{}, Body: http.NoBody, Request: request}, nil
		case doer.goneAfter > 0 && doer.ranged > doer.goneAfter:
			return &http.Response{StatusCode: http.StatusNotFound, Body: http.NoBody, Request: request}, nil
		case doer.expireAfter > 0 && doer.ranged > doer.expireAfter && (doer.expireAlways || request.URL.Query().Get("sig") == "1"):
			if doer.forbidHeader != nil {
				return &http.Response{StatusCode: http.StatusForbidden, Header: doer.forbidHeader.Clone(), Body: http.NoBody, Request: request}, nil
			}
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Header:     http.Header{"Content-Type": {"application/xml"}, "X-Ms-Error-Code": {"AuthenticationFailed"}},
				Body:       io.NopCloser(strings.NewReader(githubTestsExpiredSignatureBody)), Request: request,
			}, nil
		case doer.status416 > 0 && doer.ranged > doer.status416:
			return &http.Response{
				StatusCode: http.StatusRequestedRangeNotSatisfiable, Header: http.Header{"Content-Range": {"bytes */" + strconv.Itoa(len(doer.archive))}},
				Body: io.NopCloser(githubTestsCountingBody{reader: strings.NewReader("requested range not satisfiable"), add: func(n int) { doer.refusedRead += n }}), Request: request,
			}, nil
		}
		if doer.ignoreRange || strings.HasPrefix(spec, "bytes=-") {
			request.Header.Del("Range")
		}
	}
	var response *http.Response
	if request.Header.Get("Range") == "" {
		// The whole body streams from the fixture, so a heap sample does not count a copy of it.
		response = &http.Response{
			StatusCode: http.StatusOK, ContentLength: int64(len(doer.archive)), Request: request,
			Header: http.Header{"Accept-Ranges": {"bytes"}, "Content-Type": {"application/zip"}, "Content-Length": {strconv.Itoa(len(doer.archive))}},
			Body:   io.NopCloser(bytes.NewReader(doer.archive)),
		}
	} else {
		recorder := httptest.NewRecorder()
		http.ServeContent(recorder, request, "artifact.zip", time.Time{}, bytes.NewReader(doer.archive))
		response = recorder.Result()
		response.Request = request
	}
	if doer.noAcceptRanges {
		response.Header.Del("Accept-Ranges")
	}
	if doer.noLength && request.Header.Get("Range") == "" {
		response.Header.Del("Content-Length")
		response.ContentLength = -1
	}
	if doer.badEndFrom > 0 && doer.ranged >= doer.badEndFrom && spec != "" {
		start, end, total, _ := parseGitHubTestsContentRange(response.Header.Get("Content-Range"))
		response.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, total))
		response.Body = io.NopCloser(githubTestsCountingBody{reader: response.Body, add: func(n int) { doer.refusedRead += n }})
	}
	return response, nil
}

func (doer *githubTestsRangeDoer) requests() int { return doer.api + doer.blob }

type githubTestsRangeMember struct {
	name   string
	body   []byte
	store  bool
	rawLen uint64 // >0: a raw entry whose header declares this uncompressed size
	fill   func(io.Writer)
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
		if member.fill != nil {
			member.fill(entry)
		} else if _, err := entry.Write(member.body); err != nil {
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

// githubTestsRangeReason runs the range read alone on the blob URL and returns
// why it refused, or "".
func githubTestsRangeReason(t *testing.T, doer *githubTestsRangeDoer, maxBytes int64) string {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(doer))
	_, _, err := readGitHubTestsArtifactReports(context.Background(),
		githubTestsArtifactBlobFetch(client, "https://blob.example/artifact.zip?sig=0"), nil, int64(len(doer.archive)), maxBytes)
	var unusable *githubTestsRangeUnusableError
	if errors.As(err, &unusable) {
		return unusable.reason
	}
	if err != nil {
		t.Fatalf("err=%v, want a range refusal or none", err)
	}
	return ""
}

func githubTestsParseRangeArchive(t *testing.T, archive []byte) githubTestsReportRows {
	t.Helper()
	rows, err := parseGitHubTestsArchive(archive, "1", "1", nil, "repo-1", "run-1", "org-1", nil, nil, time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("parse archive: %v", err)
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

func githubTestsWantClass(t *testing.T, err error, class providerfoundation.ErrorClass) {
	t.Helper()
	var providerErr *providerfoundation.ProviderError
	if errors.Is(err, ErrGitHubTestsArtifactOversized) || !errors.As(err, &providerErr) || providerErr.Class != class {
		t.Fatalf("err=%v, want a %s provider error, not artifact_oversized", err, class)
	}
}

// An over-cap artifact served with byte ranges yields its JUnit report: only
// the end record, the directory and the report member are fetched, and every
// request is counted.
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
				t.Fatalf("re-packed archive is %d bytes, want the report member and two headers", len(archive))
			}
			githubTestsAssertRangeParity(t, doer.archive, archive)
			rows := githubTestsParseRangeArchive(t, archive)
			if len(rows.Suites) != 3 || len(rows.Cases) != 3 || rows.ReportsParsed != 1 {
				t.Fatalf("suites=%d cases=%d reports=%d, want 3/3/1", len(rows.Suites), len(rows.Cases), rows.ReportsParsed)
			}
			if used != doer.requests() {
				t.Fatalf("used=%d, want every request sent (%d)", used, doer.requests())
			}
			// The 1 KiB end-record window spans the last two 1 MiB blocks; the report is in the first.
			if doer.ranged != 3 {
				t.Fatalf("range requests=%d (%v), want 3", doer.ranged, doer.ranges)
			}
		})
	}
}

// A server that ignores Range keeps today's artifact_oversized skip.
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

// Report members the parser would read whose compressed bytes exceed the cap
// keep the skip, and nothing of the artifact is returned.
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
	if reason := githubTestsRangeReason(t, doer, githubTestsRangeCap); reason != "range_budget_exceeded" {
		t.Fatalf("reason=%q, want range_budget_exceeded", reason)
	}
}

// githubTestsAssertRangeParity: the parser gives the re-packed archive the
// same rows, counters, skip markers and blocking causes as the whole one.
func githubTestsAssertRangeParity(t *testing.T, full, packed []byte) {
	t.Helper()
	want, got := githubTestsParseRangeArchive(t, full), githubTestsParseRangeArchive(t, packed)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("range parse differs from the whole archive:\nwhole: suites=%d cases=%d reports=%d skipped=%d issues=%+v other=%d json=%d\nrange: suites=%d cases=%d reports=%d skipped=%d issues=%+v other=%d json=%d",
			len(want.Suites), len(want.Cases), want.ReportsParsed, want.Skipped, want.issues, want.NonReportOther, want.NonReportJSON,
			len(got.Suites), len(got.Cases), got.ReportsParsed, got.Skipped, got.issues, got.NonReportOther, got.NonReportJSON)
	}
	_, wantComplete := want.optionalIncomplete()
	if _, gotComplete := got.optionalIncomplete(); gotComplete != wantComplete {
		t.Fatalf("reports_complete whole=%v range=%v", wantComplete, gotComplete)
	}
}

func githubTestsRangeParityRead(t *testing.T, maxBytes int64, members ...githubTestsRangeMember) githubTestsReportRows {
	t.Helper()
	doer := &githubTestsRangeDoer{archive: githubTestsRangeZip(t, members...)}
	if int64(len(doer.archive)) <= maxBytes {
		t.Fatalf("fixture is %d bytes, want it over the %d cap", len(doer.archive), maxBytes)
	}
	archive, used, _, err := githubTestsRangeDownload(t, doer, maxBytes)
	if err != nil {
		t.Fatalf("err=%v, want the re-packed reports", err)
	}
	if used != doer.requests() {
		t.Fatalf("used=%d, want %d", used, doer.requests())
	}
	githubTestsAssertRangeParity(t, doer.archive, archive)
	return githubTestsParseRangeArchive(t, archive)
}

func githubTestsBlockingCause(rows githubTestsReportRows, cause string) bool {
	for _, issue := range rows.issues {
		if issue.blocking && issue.evidence.Cause == cause {
			return true
		}
	}
	return false
}

// The 201st report is past githubTestsMaxReportsPerRun: the parser records a
// blocking report_cap for it without reading it, so the range read neither
// fetches it nor charges it to the cap, whatever its size or compression.
func TestGitHubTestsOverCapArtifactRangeKeepsTheReportCap(t *testing.T) {
	members := []githubTestsRangeMember{}
	for index := range githubTestsMaxReportsPerRun {
		members = append(members, githubTestsRangeMember{name: fmt.Sprintf("results/r%03d.xml", index), body: []byte(githubTestsNamedMultiSuiteJUnit(fmt.Sprintf("r%03d-", index), 1))})
	}
	members = append(members,
		githubTestsRangeMember{name: "results/over-transfer-cap.xml", body: githubTestsRandomBytes(31, githubTestsRangeCap+(githubTestsRangeCap/2)), store: true},
		githubTestsRangeMember{name: "results/high-ratio.xml", body: make([]byte, 4<<20)},
		githubTestsRangeMember{name: "data/trace.zip", body: githubTestsRandomBytes(32, 9<<20), store: true},
	)
	rows := githubTestsRangeParityRead(t, githubTestsRangeCap, members...)
	if len(rows.Suites) != githubTestsMaxReportsPerRun || !githubTestsBlockingCause(rows, "report_cap") {
		t.Fatalf("suites=%d issues=%+v, want 200 and a blocking report_cap", len(rows.Suites), rows.issues)
	}
}

// A member above the compression ratio is skipped by the parser without a
// marker; the range read does not fetch it.
func TestGitHubTestsOverCapArtifactRangeKeepsTheRatioSkip(t *testing.T) {
	rows := githubTestsRangeParityRead(t, githubTestsRangeCap,
		githubTestsRangeMember{name: "results/junit.xml", body: []byte(githubTestsMultiSuiteJUnit(2))},
		githubTestsRangeMember{name: "results/zeros.xml", body: make([]byte, 4<<20)},
		githubTestsRangeMember{name: "data/trace.zip", body: githubTestsRandomBytes(33, 9<<20), store: true},
	)
	if len(rows.Suites) != 2 {
		t.Fatalf("suites=%d, want 2", len(rows.Suites))
	}
}

// A member above the per-report bound is not fetched; the parser still records
// its blocking archive_bounds from the header.
func TestGitHubTestsOverCapArtifactRangeKeepsTheReportBound(t *testing.T) {
	rows := githubTestsRangeParityRead(t, githubTestsRangeCap,
		githubTestsRangeMember{name: "results/junit.xml", body: []byte(githubTestsMultiSuiteJUnit(3))},
		githubTestsRangeMember{name: "results/huge.xml", body: githubTestsRandomBytes(13, 64<<10), rawLen: githubTestsMaxReportBytes + 1},
		githubTestsRangeMember{name: "data/trace.zip", body: githubTestsRandomBytes(34, 9<<20), store: true},
	)
	if len(rows.Suites) != 3 || !githubTestsBlockingCause(rows, "archive_bounds") {
		t.Fatalf("suites=%d issues=%+v, want 3 and a blocking archive_bounds", len(rows.Suites), rows.issues)
	}
}

// githubTestsLowEntropyFill writes n bytes that deflate at about 50:1, under
// githubTestsMaxCompressionRatio: one random byte, then 199 zeros.
func githubTestsLowEntropyFill(seed uint64, n int) func(io.Writer) {
	return func(writer io.Writer) {
		source := rand.New(rand.NewPCG(seed, seed))
		chunk := make([]byte, 1<<20)
		for written := 0; written < n; written += len(chunk) {
			clear(chunk)
			for index := 0; index < len(chunk); index += 200 {
				chunk[index] = byte(source.Uint32()) | 1
			}
			_, _ = writer.Write(chunk[:min(len(chunk), n-written)])
		}
	}
}

// The parser's cumulative expanded bound (githubTestsMaxArchiveBytes) is
// reached by the bodies it has read: the next report is a blocking
// archive_bounds without a read, and a later small one is still read.
func TestGitHubTestsOverCapArtifactRangeKeepsTheExpandedBound(t *testing.T) {
	if testing.Short() {
		t.Skip("expands about 200 MiB three times")
	}
	members := []githubTestsRangeMember{{name: "results/first.xml", body: []byte(githubTestsNamedMultiSuiteJUnit("first-", 1))}}
	for index := range 4 {
		members = append(members, githubTestsRangeMember{name: fmt.Sprintf("results/blob-%d.xml", index), fill: githubTestsLowEntropyFill(uint64(40+index), 49<<20)})
	}
	members = append(members,
		githubTestsRangeMember{name: "results/over-expanded.xml", body: githubTestsRandomBytes(45, 2<<20), rawLen: 61 << 20},
		githubTestsRangeMember{name: "results/last.xml", body: []byte(githubTestsNamedMultiSuiteJUnit("last-", 1))},
		githubTestsRangeMember{name: "data/trace.zip", body: githubTestsRandomBytes(46, 9<<20), store: true},
	)
	rows := githubTestsRangeParityRead(t, 8<<20, members...)
	if len(rows.Suites) != 2 || !githubTestsBlockingCause(rows, "archive_bounds") {
		t.Fatalf("suites=%d issues=%+v, want 2 and a blocking archive_bounds", len(rows.Suites), rows.issues)
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

// The blob store's expired-signature 403 (AuthenticationFailed) gets one
// fresh URL from the API and the read resumes.
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

// A second expired signature after the one relocation keeps the
// artifact_oversized skip.
func TestGitHubTestsOverCapArtifactExpiredTwiceStaysOversized(t *testing.T) {
	doer := &githubTestsRangeDoer{expireAfter: 1, expireAlways: true}
	doer.archive = githubTestsPlaywrightLikeZip(t)
	archive, used, _, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
	githubTestsWantOversized(t, err, int64(len(doer.archive)))
	if archive != nil || doer.api != 2 || used != doer.requests() {
		t.Fatalf("archive=%d api=%d used=%d requests=%d, want nil, one relocation, all counted", len(archive), doer.api, used, doer.requests())
	}
}

// A 403 that is not an expired signature keeps its classification, and no
// relocation is asked: a throttle stays retryable.
func TestGitHubTestsOverCapArtifactOtherForbiddenKeepsItsClass(t *testing.T) {
	for name, row := range map[string]struct {
		header http.Header
		class  providerfoundation.ErrorClass
	}{
		"throttled":  {http.Header{"Retry-After": {"30"}}, providerfoundation.ErrorRateLimited},
		"no code":    {http.Header{}, providerfoundation.ErrorAuthentication},
		"other code": {http.Header{"X-Ms-Error-Code": {"AuthorizationPermissionMismatch"}}, providerfoundation.ErrorAuthentication},
	} {
		t.Run(name, func(t *testing.T) {
			doer := &githubTestsRangeDoer{expireAfter: 1, forbidHeader: row.header}
			doer.archive = githubTestsPlaywrightLikeZip(t)
			_, used, _, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
			githubTestsWantClass(t, err, row.class)
			if doer.api != 1 || used != doer.requests() {
				t.Fatalf("api=%d used=%d requests=%d, want no relocation, all counted", doer.api, used, doer.requests())
			}
		})
	}
}

// A relocation that fails keeps the API's own classification; a vanished
// artifact is notFound.
func TestGitHubTestsOverCapArtifactRelocationFailureKeepsItsClass(t *testing.T) {
	doer := &githubTestsRangeDoer{expireAfter: 1, refreshStatus: http.StatusServiceUnavailable}
	doer.archive = githubTestsPlaywrightLikeZip(t)
	_, used, _, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
	githubTestsWantClass(t, err, providerfoundation.ErrorTransient)
	if used != doer.requests() {
		t.Fatalf("used=%d, want %d", used, doer.requests())
	}

	gone := &githubTestsRangeDoer{expireAfter: 1, refreshStatus: http.StatusNotFound}
	gone.archive = githubTestsPlaywrightLikeZip(t)
	archive, _, notFound, err := githubTestsRangeDownload(t, gone, githubTestsRangeCap)
	if err != nil || !notFound || archive != nil {
		t.Fatalf("err=%v notFound=%v archive=%d, want notFound", err, notFound, len(archive))
	}
}

// An unusable range answer is refused from its status and headers: a 416, or
// a Content-Range that is not the asked range, reads no body byte.
func TestGitHubTestsOverCapArtifactUnusableRangeReadsNoBody(t *testing.T) {
	for name, doer := range map[string]*githubTestsRangeDoer{
		"416":                   {status416: 1},
		"wrong end":             {badEndFrom: 2},
		"wrong end, size probe": {badEndFrom: 1, noLength: true},
	} {
		t.Run(name, func(t *testing.T) {
			doer.archive = githubTestsPlaywrightLikeZip(t)
			archive, used, _, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
			want := int64(len(doer.archive))
			if doer.noLength {
				want = githubTestsRangeCap + 1
			}
			githubTestsWantOversized(t, err, want)
			if archive != nil || doer.refusedRead != 0 || used != doer.requests() {
				t.Fatalf("archive=%d refused_body_bytes=%d used=%d requests=%d, want nil, 0, all counted", len(archive), doer.refusedRead, used, doer.requests())
			}
		})
	}
}

// used is every wire request, a retry included.
func TestGitHubTestsOverCapArtifactRangeCountsRetries(t *testing.T) {
	doer := &githubTestsRangeDoer{direct: true, failFirst: true}
	doer.archive = githubTestsPlaywrightLikeZip(t)
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(doer),
		func(request *http.Request) error { request.Header.Set("Authorization", "Bearer secret"); return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	archive, used, _, err := downloadGitHubTestsArtifact(context.Background(), client, "/repos/acme/api", "1", githubTestsRangeCap)
	if err != nil || len(archive) == 0 {
		t.Fatalf("err=%v archive=%d, want the re-packed reports", err, len(archive))
	}
	if used != doer.requests() || doer.ranged != 4 {
		t.Fatalf("used=%d requests=%d ranged=%d, want used = every request, the retried range included (4 range requests)", used, doer.requests(), doer.ranged)
	}
}

// The re-packed zip is bounded while it is written: headers count, and the
// write past the cap is refused.
func TestGitHubTestsOverCapArtifactPackedCapHoldsWhileWriting(t *testing.T) {
	members := []githubTestsRangeMember{{name: "results/junit.xml", body: []byte(githubTestsMultiSuiteJUnit(1))}}
	for index := range 1500 {
		members = append(members, githubTestsRangeMember{name: fmt.Sprintf("data/%04d-%s.bin", index, strings.Repeat("n", 590)), store: true})
	}
	doer := &githubTestsRangeDoer{archive: githubTestsRangeZip(t, members...)}
	if reason := githubTestsRangeReason(t, doer, githubTestsRangeCap); reason != "packed_over_cap" {
		t.Fatalf("reason=%q, want packed_over_cap", reason)
	}
	buffer := &githubTestsCappedBuffer{limit: 10}
	if _, err := buffer.Write(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := buffer.Write(make([]byte, 3)); err == nil || buffer.Len() != 8 {
		t.Fatalf("err=%v len=%d, want the write past the cap refused and nothing of it kept", err, buffer.Len())
	}
}

// githubTestsHeapGrowth returns the bytes the read allocated in all and the
// largest live-heap growth sampled at its requests. Allocation bounds both: a
// directory index built and dropped between two requests is still allocated.
func githubTestsHeapGrowth(t *testing.T, doer *githubTestsRangeDoer, read func()) (allocated, live uint64) {
	t.Helper()
	var stats runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&stats)
	base, total, peak := stats.HeapAlloc, stats.TotalAlloc, stats.HeapAlloc
	doer.sample = func() {
		runtime.GC()
		runtime.ReadMemStats(&stats)
		peak = max(peak, stats.HeapAlloc)
	}
	read()
	doer.sample = nil
	runtime.ReadMemStats(&stats)
	return stats.TotalAlloc - total, peak - base
}

const githubTestsRangeHeapBound = 6 << 20

func githubTestsManyEntriesZip(t *testing.T, entries int, name func(int) string) []byte {
	t.Helper()
	members := []githubTestsRangeMember{{name: "results/junit.xml", body: []byte(githubTestsMultiSuiteJUnit(1))}}
	for index := range entries {
		members = append(members, githubTestsRangeMember{name: name(index), store: true})
	}
	return githubTestsRangeZip(t, members...)
}

func githubTestsZipEnd(t *testing.T, archive []byte) int {
	t.Helper()
	at := bytes.LastIndex(archive, []byte{'P', 'K', 5, 6})
	if at < 0 || at != len(archive)-githubTestsZipEndLength {
		t.Fatalf("end record at %d of %d, want the last 22 bytes", at, len(archive))
	}
	return at
}

// A directory above the entry bound (96,000 entries, a zip64 end record) is
// refused before it is indexed, with a bounded heap.
func TestGitHubTestsOverCapArtifactHugeDirectoryIsRefused(t *testing.T) {
	doer := &githubTestsRangeDoer{archive: githubTestsManyEntriesZip(t, 96_000, func(index int) string { return fmt.Sprintf("f%06d", index) })}
	if !bytes.Contains(doer.archive[len(doer.archive)-200:], []byte{'P', 'K', 6, 7}) {
		t.Fatal("fixture has no zip64 end locator")
	}
	var err error
	allocated, live := githubTestsHeapGrowth(t, doer, func() { _, _, _, err = githubTestsRangeDownload(t, doer, githubTestsRangeCap) })
	githubTestsWantOversized(t, err, int64(len(doer.archive)))
	t.Logf("HEAP entries=96000 archive=%d allocated=%d live_growth=%d bound=%d", len(doer.archive), allocated, live, githubTestsRangeHeapBound)
	if allocated >= githubTestsRangeHeapBound || live >= githubTestsRangeHeapBound {
		t.Fatalf("allocated %d, live heap grew %d bytes, want both < %d", allocated, live, githubTestsRangeHeapBound)
	}
	if reason := githubTestsRangeReason(t, &githubTestsRangeDoer{archive: doer.archive}, githubTestsRangeCap); reason != "zip_directory_over_bound" {
		t.Fatalf("reason=%q, want zip_directory_over_bound", reason)
	}
}

// End records that lie: a small entry count with a large directory is refused
// by the size bound; a small stated size over a large directory does not
// meet its end and is refused, so the reader never indexes it.
func TestGitHubTestsOverCapArtifactLyingEndRecordIsRefused(t *testing.T) {
	for name, row := range map[string]struct {
		entries int
		patch   func([]byte, int)
		reason  string
	}{
		"small count, large directory": {30_000, func(archive []byte, end int) {
			binary.LittleEndian.PutUint16(archive[end+8:], 3)
			binary.LittleEndian.PutUint16(archive[end+10:], 3)
		}, "zip_directory_over_bound"},
		"small size, large directory": {30_000, func(archive []byte, end int) {
			binary.LittleEndian.PutUint16(archive[end+8:], 2)
			binary.LittleEndian.PutUint16(archive[end+10:], 2)
			binary.LittleEndian.PutUint32(archive[end+12:], 100)
		}, "zip_directory_unreadable"},
	} {
		t.Run(name, func(t *testing.T) {
			archive := githubTestsManyEntriesZip(t, row.entries, func(index int) string { return fmt.Sprintf("data/%06d-%s.bin", index, strings.Repeat("x", 40)) })
			row.patch(archive, githubTestsZipEnd(t, archive))
			doer := &githubTestsRangeDoer{archive: archive}
			var err error
			allocated, live := githubTestsHeapGrowth(t, doer, func() { _, _, _, err = githubTestsRangeDownload(t, doer, githubTestsRangeCap) })
			githubTestsWantOversized(t, err, int64(len(archive)))
			t.Logf("HEAP entries=%d allocated=%d live_growth=%d bound=%d", row.entries, allocated, live, githubTestsRangeHeapBound)
			if allocated >= githubTestsRangeHeapBound || live >= githubTestsRangeHeapBound {
				t.Fatalf("allocated %d, live heap grew %d bytes, want both < %d", allocated, live, githubTestsRangeHeapBound)
			}
			if reason := githubTestsRangeReason(t, &githubTestsRangeDoer{archive: archive}, githubTestsRangeCap); reason != row.reason {
				t.Fatalf("reason=%q, want %s", reason, row.reason)
			}
		})
	}
}

// githubTestsZip64EndRecord rewrites an archive's end record into the zip64
// form: a zip64 end record and locator, then an end record whose fields say
// "see zip64".
func githubTestsZip64EndRecord(t *testing.T, archive []byte) []byte {
	t.Helper()
	end := githubTestsZipEnd(t, archive)
	entries := uint64(binary.LittleEndian.Uint16(archive[end+10:]))
	size := uint64(binary.LittleEndian.Uint32(archive[end+12:]))
	offset := uint64(binary.LittleEndian.Uint32(archive[end+16:]))
	out := append([]byte(nil), archive[:end]...)
	record := make([]byte, 56)
	binary.LittleEndian.PutUint32(record[0:], 0x06064b50)
	binary.LittleEndian.PutUint64(record[4:], 44)
	binary.LittleEndian.PutUint16(record[12:], 45)
	binary.LittleEndian.PutUint16(record[14:], 45)
	binary.LittleEndian.PutUint64(record[24:], entries)
	binary.LittleEndian.PutUint64(record[32:], entries)
	binary.LittleEndian.PutUint64(record[40:], size)
	binary.LittleEndian.PutUint64(record[48:], offset)
	locator := make([]byte, 20)
	binary.LittleEndian.PutUint32(locator[0:], 0x07064b50)
	binary.LittleEndian.PutUint64(locator[8:], uint64(end))
	binary.LittleEndian.PutUint32(locator[16:], 1)
	tail := make([]byte, 22)
	binary.LittleEndian.PutUint32(tail[0:], 0x06054b50)
	binary.LittleEndian.PutUint16(tail[8:], 0xffff)
	binary.LittleEndian.PutUint16(tail[10:], 0xffff)
	binary.LittleEndian.PutUint32(tail[12:], 0xffffffff)
	binary.LittleEndian.PutUint32(tail[16:], 0xffffffff)
	out = append(append(append(out, record...), locator...), tail...)
	if reader, err := zip.NewReader(bytes.NewReader(out), int64(len(out))); err != nil || len(reader.File) != int(entries) {
		t.Fatalf("zip64 fixture does not open: %v", err)
	}
	return out
}

// A zip64 end record within the bounds is read and parsed.
func TestGitHubTestsOverCapArtifactZip64EndRecordIsRead(t *testing.T) {
	doer := &githubTestsRangeDoer{archive: githubTestsZip64EndRecord(t, githubTestsPlaywrightLikeZip(t))}
	archive, _, _, err := githubTestsRangeDownload(t, doer, githubTestsRangeCap)
	if err != nil {
		t.Fatalf("err=%v, want the re-packed reports", err)
	}
	githubTestsAssertRangeParity(t, doer.archive, archive)
	if rows := githubTestsParseRangeArchive(t, archive); len(rows.Suites) != 3 {
		t.Fatalf("suites=%d, want 3", len(rows.Suites))
	}
}

// An artifact under the cap is read as before: one body, no range request.
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
