package providersync

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	githubTestsRangeBlockBytes = 1 << 20
	githubTestsRangeBlocksKept = 4
	// The zip directory bounds hold far above a real web Playwright artifact
	// (138,800,172 bytes: 103 entries, a 10,899-byte central directory); a
	// directory above either is refused before it is indexed.
	githubTestsRangeMaxEntries        = 10_000
	githubTestsRangeMaxDirectoryBytes = 2 << 20
	githubTestsRangeMaxErrorBody      = 4 << 10
)

// githubTestsRangeUnusableError: the over-cap artifact cannot be read by
// range, so the caller keeps the artifact_oversized skip. reason names why in
// the log line; it is not a marker cause.
type githubTestsRangeUnusableError struct{ reason string }

func (e *githubTestsRangeUnusableError) Error() string {
	return "github tests artifact range read unusable: " + e.reason
}

func githubTestsRangeUnusable(reason string) error {
	return &githubTestsRangeUnusableError{reason: reason}
}

// errGitHubTestsRangeGone: the artifact answered 404/410 during the range
// read; the caller reports notFound, as for the first download response.
var errGitHubTestsRangeGone = errors.New("github tests artifact gone during range read")

// githubTestsArtifactFetch sends one GET for the artifact zip; a range rides
// on the context (providerfoundation.WithByteRange).
type githubTestsArtifactFetch func(context.Context) (*http.Response, error)

// githubTestsArtifactBlobFetch fetches the pre-signed blob URL the API
// redirected to, without the provider credential.
func githubTestsArtifactBlobFetch(client *providerfoundation.HTTPClient, location string) githubTestsArtifactFetch {
	return func(ctx context.Context) (*http.Response, error) {
		return client.DoUnauthenticatedRoute(ctx, http.MethodGet, githubArtifactZipRoute, location)
	}
}

// githubTestsArtifactRelocate asks the API again for the artifact. Its
// failures keep the class the client gave them.
func githubTestsArtifactRelocate(ctx context.Context, client *providerfoundation.HTTPClient, api githubTestsArtifactFetch) (githubTestsArtifactFetch, error) {
	response, err := api(ctx)
	if err != nil {
		if githubTestsGoneError(err) {
			return nil, errGitHubTestsRangeGone
		}
		return nil, err
	}
	response.Body.Close()
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		location := response.Header.Get("Location")
		if location == "" {
			return nil, providerfoundation.ResponseFailure(response, "redirect", fmt.Errorf(
				"%w: redirect status=%d carried no Location header",
				ErrGitHubTestsArtifactUnavailable, response.StatusCode,
			))
		}
		return githubTestsArtifactBlobFetch(client, location), nil
	}
	return api, nil
}

func githubTestsGoneError(err error) bool {
	var providerErr *providerfoundation.ProviderError
	return errors.As(err, &providerErr) && (providerErr.StatusCode == http.StatusNotFound || providerErr.StatusCode == http.StatusGone)
}

// githubTestsArtifactStatusFailure is the error for an artifact response with
// a status of 400 or more, other than 404/410.
func githubTestsArtifactStatusFailure(response *http.Response) error {
	message, readErr := providerfoundation.ReadResponseBodyTransport(response, 1<<20-1)
	if readErr != nil {
		return readErr
	}
	return githubTestsArtifactClassified(response, string(message))
}

func githubTestsArtifactClassified(response *http.Response, message string) error {
	// Classified like the GitLab artifact route: a rate limit of the
	// artifact store defers the unit with its dimension; every other
	// failure keeps the class ClassifyHTTPForRequest gives it.
	if classified := providerfoundation.ClassifyHTTPForRequest("github", responsePath(response), response.StatusCode, response.Header, message); classified != nil {
		classified.Route = githubArtifactZipRoute
		return classified
	}
	return &providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, StatusCode: response.StatusCode, Route: githubArtifactZipRoute}
}

// readGitHubTestsArtifactReports reads an over-cap artifact zip through HTTP
// range requests. It returns a new zip that parseGitHubTestsArchive gives the
// same rows, counters and skip markers for as the whole artifact: the first
// githubTestsMaxArchiveEntries members in order, with the data of each member
// the parser reads and only the header of every other one. size is the
// artifact size when known, else -1. refresh, when set, gets a fresh
// pre-signed URL once after an expired-signature 403.
func readGitHubTestsArtifactReports(
	ctx context.Context, fetch githubTestsArtifactFetch,
	refresh func(context.Context) (githubTestsArtifactFetch, error), size, maxBytes int64,
) (archive []byte, notFound bool, err error) {
	source := &githubTestsRangeReaderAt{ctx: ctx, fetch: fetch, refresh: refresh, size: size}
	defer func() {
		if errors.Is(err, errGitHubTestsRangeGone) {
			archive, notFound, err = nil, true, nil
		}
	}()
	members, err := source.members()
	if err != nil {
		return nil, false, err
	}
	packed := &githubTestsCappedBuffer{limit: maxBytes}
	writer := zip.NewWriter(packed)
	// processed and expanded follow parseGitHubTestsArchive's own counters, so
	// a member's data is fetched exactly when the parser would read it.
	processed := 0
	var expanded, fetched uint64
	for _, member := range members {
		lower := strings.ToLower(member.Name)
		read := githubTestsReportCandidateMember(member) &&
			processed < githubTestsMaxReportsPerRun &&
			member.UncompressedSize64 <= githubTestsMaxReportBytes &&
			expanded+member.UncompressedSize64 <= githubTestsMaxArchiveBytes &&
			member.UncompressedSize64 <= max(member.CompressedSize64, 1)*githubTestsMaxCompressionRatio
		if !read {
			header := member.FileHeader
			if _, err := writer.CreateRaw(&header); err != nil {
				return nil, false, packed.failure()
			}
			continue
		}
		fetched += member.CompressedSize64
		if fetched > uint64(maxBytes) {
			return nil, false, githubTestsRangeUnusable("range_budget_exceeded")
		}
		single, err := githubTestsSingleMember(member)
		if err != nil {
			if source.err != nil {
				return nil, false, source.err
			}
			return nil, false, githubTestsRangeUnusable("zip_unreadable")
		}
		if body, err := readZipReport(single); err == nil {
			expanded += uint64(len(body))
			if classifyGitHubTestReport(lower, body) != "" {
				processed++
			}
		}
		if err := writer.Copy(single); err != nil {
			return nil, false, packed.failure()
		}
	}
	if err := writer.Close(); err != nil {
		return nil, false, packed.failure()
	}
	return packed.Bytes(), false, nil
}

// members returns the members the parser examines: the first
// githubTestsMaxArchiveEntries of the directory.
func (r *githubTestsRangeReaderAt) members() ([]*zip.File, error) {
	members, err := r.directoryMembers()
	if len(members) > githubTestsMaxArchiveEntries {
		members = members[:githubTestsMaxArchiveEntries]
	}
	return members, err
}

// directoryMembers reads the zip directory by range (the end record, then
// the bounded central directory) and returns every member it lists.
func (r *githubTestsRangeReaderAt) directoryMembers() ([]*zip.File, error) {
	if err := r.probe(); err != nil {
		return nil, err
	}
	directory, err := r.directory()
	if err != nil {
		return nil, err
	}
	reader, zipErr := zip.NewReader(directory, directory.size())
	if r.err != nil {
		return nil, r.err
	}
	if zipErr != nil {
		return nil, githubTestsRangeUnusable("zip_unreadable")
	}
	return reader.File, nil
}

// githubTestsSingleMember copies one member's raw bytes into a zip of its
// own, so readZipReport reads it exactly as the parser will.
func githubTestsSingleMember(member *zip.File) (*zip.File, error) {
	var single bytes.Buffer
	writer := zip.NewWriter(&single)
	if err := writer.Copy(member); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	reader, err := zip.NewReader(bytes.NewReader(single.Bytes()), int64(single.Len()))
	if err != nil {
		return nil, err
	}
	if len(reader.File) != 1 {
		return nil, zip.ErrFormat
	}
	return reader.File[0], nil
}

// githubTestsCappedBuffer refuses a write that would take it past limit.
type githubTestsCappedBuffer struct {
	bytes.Buffer
	limit int64
	over  bool
}

func (b *githubTestsCappedBuffer) Write(p []byte) (int, error) {
	if int64(b.Len())+int64(len(p)) > b.limit {
		b.over = true
		return 0, githubTestsRangeUnusable("packed_over_cap")
	}
	return b.Buffer.Write(p)
}

func (b *githubTestsCappedBuffer) failure() error {
	if b.over {
		return githubTestsRangeUnusable("packed_over_cap")
	}
	return githubTestsRangeUnusable("zip_unreadable")
}

type githubTestsRangeBlock struct {
	offset int64
	data   []byte
}

// githubTestsRangeReaderAt is an io.ReaderAt over one artifact URL. It keeps
// the first error: zip may drop or wrap the error a ReadAt returns.
type githubTestsRangeReaderAt struct {
	ctx       context.Context
	fetch     githubTestsArtifactFetch
	refresh   func(context.Context) (githubTestsArtifactFetch, error)
	refreshed bool
	size      int64
	blocks    []githubTestsRangeBlock
	err       error
}

// probe learns an unknown size from the Content-Range of the first block.
// Every range is explicit: Azure Blob storage answers a suffix range
// (bytes=-N) with the whole blob.
func (r *githubTestsRangeReaderAt) probe() error {
	if r.size >= 0 {
		return nil
	}
	data, total, err := r.get(0, githubTestsRangeBlockBytes-1)
	if err != nil {
		return err
	}
	r.size = total
	r.blocks = append(r.blocks, githubTestsRangeBlock{offset: 0, data: data})
	return nil
}

func (r *githubTestsRangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if off < 0 {
		return 0, githubTestsRangeUnusable("zip_unreadable")
	}
	n := 0
	for n < len(p) {
		at := off + int64(n)
		if at >= r.size {
			return n, io.EOF
		}
		block, err := r.block(at)
		if err != nil {
			r.err = err
			return n, err
		}
		n += copy(p[n:], block.data[at-block.offset:])
	}
	return n, nil
}

func (r *githubTestsRangeReaderAt) block(at int64) (githubTestsRangeBlock, error) {
	for _, block := range r.blocks {
		if at >= block.offset && at < block.offset+int64(len(block.data)) {
			return block, nil
		}
	}
	start := at - at%githubTestsRangeBlockBytes
	data, _, err := r.get(start, start+githubTestsRangeBlockBytes-1)
	if err != nil {
		return githubTestsRangeBlock{}, err
	}
	if len(r.blocks) >= githubTestsRangeBlocksKept {
		r.blocks = append(r.blocks[:0], r.blocks[1:]...)
	}
	block := githubTestsRangeBlock{offset: start, data: data}
	r.blocks = append(r.blocks, block)
	return block, nil
}

// get sends one range request for bytes first..last and returns the bytes and
// the total size. The status, Content-Range and Content-Length are checked
// before any body byte is read.
func (r *githubTestsRangeReaderAt) get(first, last int64) ([]byte, int64, error) {
	if r.size >= 0 {
		last = min(last, r.size-1)
	}
	response, err := r.fetch(providerfoundation.WithByteRange(r.ctx, "bytes="+strconv.FormatInt(first, 10)+"-"+strconv.FormatInt(last, 10)))
	if err != nil {
		var providerErr *providerfoundation.ProviderError
		switch {
		case githubTestsGoneError(err):
			return nil, 0, errGitHubTestsRangeGone
		case errors.As(err, &providerErr) && providerErr.StatusCode == http.StatusRequestedRangeNotSatisfiable:
			return nil, 0, githubTestsRangeUnusable("range_unsatisfiable")
		}
		return nil, 0, err
	}
	defer response.Body.Close()
	switch {
	case response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusGone:
		return nil, 0, errGitHubTestsRangeGone
	case response.StatusCode == http.StatusRequestedRangeNotSatisfiable:
		return nil, 0, githubTestsRangeUnusable("range_unsatisfiable")
	case response.StatusCode == http.StatusForbidden:
		return r.forbidden(response, first, last)
	case response.StatusCode >= 400:
		return nil, 0, githubTestsArtifactStatusFailure(response)
	case response.StatusCode != http.StatusPartialContent:
		return nil, 0, githubTestsRangeUnusable("range_reply_unusable")
	}
	start, end, total, ok := parseGitHubTestsContentRange(response.Header.Get("Content-Range"))
	wantTotal := r.size
	if wantTotal < 0 {
		wantTotal = total
	}
	if !ok || total != wantTotal || start != first || end != min(last, total-1) ||
		(response.ContentLength >= 0 && response.ContentLength != end-start+1) {
		return nil, 0, githubTestsRangeUnusable("range_reply_unusable")
	}
	length := end - start + 1
	data, over, readErr := providerfoundation.ReadResponseBodyWithin(response, length, logging.TransportFailure)
	if readErr != nil {
		return nil, 0, providerfoundation.ResponseFailure(response, providerfoundation.ReasonBodyRead, fmt.Errorf(
			"%w: artifact range read failed: %w", ErrGitHubTestsIncomplete, errors.Unwrap(readErr),
		))
	}
	if over > 0 || int64(len(data)) != length {
		return nil, 0, githubTestsRangeUnusable("range_reply_unusable")
	}
	return data, total, nil
}

// forbidden handles a 403: an expired pre-signed URL gets one fresh URL from
// the API and the range is asked again; every other 403 keeps its class. The
// headers decide first, so a body that fails to read cannot hide a rate limit
// or an expiry they already show.
func (r *githubTestsRangeReaderAt) forbidden(response *http.Response, first, last int64) ([]byte, int64, error) {
	if classified := githubTestsArtifactClassified(response, ""); githubTestsRateLimited(classified) {
		return nil, 0, classified
	}
	if r.refresh != nil && strings.EqualFold(strings.TrimSpace(response.Header.Get("x-ms-error-code")), "AuthenticationFailed") {
		return r.relocate(first, last)
	}
	message, readErr := providerfoundation.ReadResponseBodyTransport(response, githubTestsRangeMaxErrorBody)
	if readErr != nil {
		return nil, 0, readErr
	}
	classified := githubTestsArtifactClassified(response, string(message))
	if r.refresh == nil || githubTestsRateLimited(classified) || !strings.Contains(string(message), "<Code>AuthenticationFailed</Code>") {
		return nil, 0, classified
	}
	return r.relocate(first, last)
}

func githubTestsRateLimited(err error) bool {
	var providerErr *providerfoundation.ProviderError
	return errors.As(err, &providerErr) && providerErr.Class == providerfoundation.ErrorRateLimited
}

func (r *githubTestsRangeReaderAt) relocate(first, last int64) ([]byte, int64, error) {
	if r.refreshed {
		return nil, 0, githubTestsRangeUnusable("signature_expired_twice")
	}
	r.refreshed = true
	fetch, err := r.refresh(r.ctx)
	if err != nil {
		return nil, 0, err
	}
	r.fetch = fetch
	return r.get(first, last)
}

// githubTestsZipDirectory is the view zip.NewReader indexes: the artifact up
// to the end of its central directory, then an end record written here.
// archive/zip indexes every directory header it can read, past the size the
// end record states, so this cut is what bounds the index.
type githubTestsZipDirectory struct {
	source *githubTestsRangeReaderAt
	cut    int64
	tail   []byte
}

func (d *githubTestsZipDirectory) size() int64 { return d.cut + int64(len(d.tail)) }

func (d *githubTestsZipDirectory) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, githubTestsRangeUnusable("zip_unreadable")
	}
	n := 0
	if off < d.cut {
		head := min(int64(len(p)), d.cut-off)
		read, err := d.source.ReadAt(p[:head], off)
		n = read
		if int64(read) < head {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return n, err
		}
	}
	at := off + int64(n) - d.cut
	if n < len(p) && at < int64(len(d.tail)) {
		n += copy(p[n:], d.tail[at:])
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

const (
	githubTestsZipEndSignature       = 0x06054b50
	githubTestsZip64LocatorSignature = 0x07064b50
	githubTestsZip64EndSignature     = 0x06064b50
	githubTestsZipEndLength          = 22
	githubTestsZip64LocatorLength    = 20
	githubTestsZip64EndLength        = 56
)

// directory reads the end of central directory record (and the zip64 one it
// points at), refuses a directory above the entry or byte bound before
// anything indexes it, and returns the bounded view zip.NewReader indexes.
func (r *githubTestsRangeReaderAt) directory() (*githubTestsZipDirectory, error) {
	end, endOffset, err := r.findEnd()
	if err != nil {
		return nil, err
	}
	entries := uint64(binary.LittleEndian.Uint16(end[10:12]))
	directorySize := uint64(binary.LittleEndian.Uint32(end[12:16]))
	directoryOffset := uint64(binary.LittleEndian.Uint32(end[16:20]))
	directoryEnd := endOffset
	if (entries == 0xffff || directorySize == 0xffffffff || directoryOffset == 0xffffffff) && endOffset >= githubTestsZip64LocatorLength {
		locator := make([]byte, githubTestsZip64LocatorLength)
		if _, err := r.ReadAt(locator, endOffset-githubTestsZip64LocatorLength); err != nil {
			return nil, r.readFailure(err)
		}
		if binary.LittleEndian.Uint32(locator) == githubTestsZip64LocatorSignature {
			recordOffset := binary.LittleEndian.Uint64(locator[8:16])
			if r.size < githubTestsZip64EndLength || recordOffset > uint64(r.size-githubTestsZip64EndLength) {
				return nil, githubTestsRangeUnusable("zip_directory_unreadable")
			}
			record := make([]byte, githubTestsZip64EndLength)
			if _, err := r.ReadAt(record, int64(recordOffset)); err != nil {
				return nil, r.readFailure(err)
			}
			if binary.LittleEndian.Uint32(record) != githubTestsZip64EndSignature {
				return nil, githubTestsRangeUnusable("zip_directory_unreadable")
			}
			entries = binary.LittleEndian.Uint64(record[32:40])
			directorySize = binary.LittleEndian.Uint64(record[40:48])
			directoryOffset = binary.LittleEndian.Uint64(record[48:56])
			directoryEnd = int64(recordOffset)
		}
	}
	if entries > githubTestsRangeMaxEntries || directorySize > githubTestsRangeMaxDirectoryBytes {
		return nil, githubTestsRangeUnusable("zip_directory_over_bound")
	}
	if directoryOffset > uint64(directoryEnd) || directoryOffset+directorySize != uint64(directoryEnd) {
		return nil, githubTestsRangeUnusable("zip_directory_unreadable")
	}
	tail := make([]byte, githubTestsZipEndLength)
	binary.LittleEndian.PutUint32(tail[0:4], githubTestsZipEndSignature)
	binary.LittleEndian.PutUint16(tail[8:10], uint16(entries))
	binary.LittleEndian.PutUint16(tail[10:12], uint16(entries))
	binary.LittleEndian.PutUint32(tail[12:16], uint32(directorySize))
	binary.LittleEndian.PutUint32(tail[16:20], uint32(directoryOffset))
	if directoryOffset >= 0xffffffff {
		record := make([]byte, githubTestsZip64EndLength+githubTestsZip64LocatorLength)
		binary.LittleEndian.PutUint32(record[0:4], githubTestsZip64EndSignature)
		binary.LittleEndian.PutUint64(record[4:12], githubTestsZip64EndLength-12)
		binary.LittleEndian.PutUint16(record[12:14], 45)
		binary.LittleEndian.PutUint16(record[14:16], 45)
		binary.LittleEndian.PutUint64(record[24:32], entries)
		binary.LittleEndian.PutUint64(record[32:40], entries)
		binary.LittleEndian.PutUint64(record[40:48], directorySize)
		binary.LittleEndian.PutUint64(record[48:56], directoryOffset)
		locator := record[githubTestsZip64EndLength:]
		binary.LittleEndian.PutUint32(locator[0:4], githubTestsZip64LocatorSignature)
		binary.LittleEndian.PutUint64(locator[8:16], uint64(directoryEnd))
		binary.LittleEndian.PutUint32(locator[16:20], 1)
		binary.LittleEndian.PutUint32(tail[16:20], 0xffffffff)
		tail = append(record, tail...)
	}
	return &githubTestsZipDirectory{source: r, cut: directoryEnd, tail: tail}, nil
}

// findEnd finds the end of central directory record as archive/zip does: the
// last signature in the last 1 KiB, else in the last 65 KiB, whose comment
// fits in the block.
func (r *githubTestsRangeReaderAt) findEnd() ([]byte, int64, error) {
	for _, length := range []int64{1024, 65 * 1024} {
		length = min(length, r.size)
		block := make([]byte, length)
		if _, err := r.ReadAt(block, r.size-length); err != nil && !errors.Is(err, io.EOF) {
			return nil, 0, r.readFailure(err)
		}
		for index := len(block) - githubTestsZipEndLength; index >= 0; index-- {
			if binary.LittleEndian.Uint32(block[index:]) != githubTestsZipEndSignature {
				continue
			}
			if comment := int(binary.LittleEndian.Uint16(block[index+20:])); index+githubTestsZipEndLength+comment <= len(block) {
				return block[index : index+githubTestsZipEndLength], r.size - length + int64(index), nil
			}
			break
		}
		if length == r.size {
			break
		}
	}
	return nil, 0, githubTestsRangeUnusable("zip_directory_unreadable")
}

func (r *githubTestsRangeReaderAt) readFailure(err error) error {
	if r.err != nil {
		return r.err
	}
	return githubTestsRangeUnusable("zip_directory_unreadable")
}

// parseGitHubTestsContentRange parses "bytes first-last/total".
func parseGitHubTestsContentRange(value string) (start, end, total int64, ok bool) {
	spec, found := strings.CutPrefix(strings.TrimSpace(value), "bytes ")
	if !found {
		return 0, 0, 0, false
	}
	span, size, found := strings.Cut(spec, "/")
	if !found {
		return 0, 0, 0, false
	}
	first, last, found := strings.Cut(span, "-")
	if !found {
		return 0, 0, 0, false
	}
	var err error
	if start, err = strconv.ParseInt(first, 10, 64); err != nil {
		return 0, 0, 0, false
	}
	if end, err = strconv.ParseInt(last, 10, 64); err != nil {
		return 0, 0, 0, false
	}
	if total, err = strconv.ParseInt(size, 10, 64); err != nil {
		return 0, 0, 0, false
	}
	return start, end, total, start >= 0 && end >= start && end < total
}
