package providersync

import (
	"archive/zip"
	"bytes"
	"context"
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
	githubTestsRangeBlockBytes = 4 << 20
	githubTestsRangeBlocksKept = 4
)

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

// githubTestsArtifactRelocate asks the API again for a fresh pre-signed URL.
func githubTestsArtifactRelocate(ctx context.Context, client *providerfoundation.HTTPClient, api githubTestsArtifactFetch) (githubTestsArtifactFetch, error) {
	response, err := api(ctx)
	if err != nil {
		var providerErr *providerfoundation.ProviderError
		if errors.As(err, &providerErr) && (providerErr.StatusCode == http.StatusNotFound || providerErr.StatusCode == http.StatusGone) {
			return nil, errGitHubTestsRangeGone
		}
		return nil, err
	}
	response.Body.Close()
	location := response.Header.Get("Location")
	if response.StatusCode < 300 || response.StatusCode >= 400 || location == "" {
		return nil, errGitHubTestsRangeUnusable
	}
	return githubTestsArtifactBlobFetch(client, location), nil
}

// errGitHubTestsRangeUnusable: the over-cap artifact cannot be read by range,
// so the caller keeps the artifact_oversized skip.
var errGitHubTestsRangeUnusable = errors.New("github tests artifact range read unusable")

// readGitHubTestsArtifactReports reads an over-cap artifact zip through HTTP
// range requests and returns a new zip holding only the members the report
// parser reads. size is the artifact size when known, else -1. refresh, when
// set, gets a fresh pre-signed URL once after a 403. requests counts every
// request sent.
func readGitHubTestsArtifactReports(
	ctx context.Context, fetch githubTestsArtifactFetch,
	refresh func(context.Context) (githubTestsArtifactFetch, error), size, maxBytes int64,
) (archive []byte, requests int, notFound bool, err error) {
	source := &githubTestsRangeReaderAt{ctx: ctx, fetch: fetch, refresh: refresh, size: size}
	defer func() {
		requests = source.requests
		if errors.Is(err, errGitHubTestsRangeGone) {
			archive, notFound, err = nil, true, nil
		}
	}()
	if err := source.probe(); err != nil {
		return nil, 0, false, err
	}
	reader, zipErr := zip.NewReader(source, source.size)
	if source.err != nil {
		return nil, 0, false, source.err
	}
	if zipErr != nil {
		return nil, 0, false, errGitHubTestsRangeUnusable
	}
	var packed bytes.Buffer
	writer := zip.NewWriter(&packed)
	members := reader.File
	if len(members) > githubTestsMaxArchiveEntries {
		members = members[:githubTestsMaxArchiveEntries]
	}
	var selected uint64
	for _, member := range members {
		lower := strings.ToLower(member.Name)
		if !isSafeGitHubTestsArchiveMemberName(member.Name) || member.FileInfo().IsDir() ||
			(!strings.HasSuffix(lower, ".xml") && !strings.HasSuffix(lower, ".info")) {
			continue
		}
		if member.UncompressedSize64 > githubTestsMaxReportBytes {
			// Header only: the parser records its archive_bounds skip from the header and never reads the data.
			header := member.FileHeader
			if _, err := writer.CreateRaw(&header); err != nil {
				return nil, 0, false, errGitHubTestsRangeUnusable
			}
			continue
		}
		compressed := max(member.CompressedSize64, 1)
		if member.UncompressedSize64 > compressed*githubTestsMaxCompressionRatio {
			continue
		}
		selected += member.CompressedSize64
		if selected > uint64(maxBytes) {
			return nil, 0, false, errGitHubTestsRangeUnusable
		}
		if err := writer.Copy(member); err != nil {
			if source.err != nil {
				return nil, 0, false, source.err
			}
			return nil, 0, false, errGitHubTestsRangeUnusable
		}
	}
	if err := writer.Close(); err != nil || int64(packed.Len()) > maxBytes {
		return nil, 0, false, errGitHubTestsRangeUnusable
	}
	return packed.Bytes(), 0, false, nil
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
	requests  int
	err       error
}

// probe learns an unknown size from the Content-Range of the first block.
// Every range is explicit: Azure Blob storage answers a suffix range
// (bytes=-N) with the whole blob.
func (r *githubTestsRangeReaderAt) probe() error {
	if r.size >= 0 {
		return nil
	}
	data, total, err := r.get(fmt.Sprintf("bytes=0-%d", githubTestsRangeBlockBytes-1), 0)
	if err != nil {
		return err
	}
	if int64(len(data)) != min(total, githubTestsRangeBlockBytes) {
		return errGitHubTestsRangeUnusable
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
		return 0, errGitHubTestsRangeUnusable
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
	end := min(start+githubTestsRangeBlockBytes, r.size) - 1
	data, total, err := r.get(fmt.Sprintf("bytes=%d-%d", start, end), start)
	if err != nil {
		return githubTestsRangeBlock{}, err
	}
	if total != r.size || int64(len(data)) != end-start+1 {
		return githubTestsRangeBlock{}, errGitHubTestsRangeUnusable
	}
	if len(r.blocks) >= githubTestsRangeBlocksKept {
		r.blocks = append(r.blocks[:0], r.blocks[1:]...)
	}
	block := githubTestsRangeBlock{offset: start, data: data}
	r.blocks = append(r.blocks, block)
	return block, nil
}

// get sends one range request; the answer must start at wantStart.
func (r *githubTestsRangeReaderAt) get(spec string, wantStart int64) ([]byte, int64, error) {
	response, err := r.fetch(providerfoundation.WithByteRange(r.ctx, spec))
	r.requests++
	if err != nil {
		var providerErr *providerfoundation.ProviderError
		if errors.As(err, &providerErr) && (providerErr.StatusCode == http.StatusNotFound || providerErr.StatusCode == http.StatusGone) {
			return nil, 0, errGitHubTestsRangeGone
		}
		return nil, 0, err
	}
	if response.StatusCode == http.StatusForbidden && r.refresh != nil {
		// A pre-signed URL lives about a minute: a 403 gets one fresh URL, a second 403 keeps the oversized skip.
		response.Body.Close()
		if r.refreshed {
			return nil, 0, errGitHubTestsRangeUnusable
		}
		r.refreshed = true
		fetch, err := r.refresh(r.ctx)
		r.requests++
		if err != nil {
			if errors.Is(err, errGitHubTestsRangeGone) {
				return nil, 0, err
			}
			return nil, 0, errGitHubTestsRangeUnusable
		}
		r.fetch = fetch
		return r.get(spec, wantStart)
	}
	defer response.Body.Close()
	switch {
	case response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusGone:
		return nil, 0, errGitHubTestsRangeGone
	case response.StatusCode >= 400:
		return nil, 0, githubTestsArtifactStatusFailure(response)
	case response.StatusCode != http.StatusPartialContent:
		return nil, 0, errGitHubTestsRangeUnusable
	}
	start, end, total, ok := parseGitHubTestsContentRange(response.Header.Get("Content-Range"))
	if !ok || start != wantStart || end-start+1 > githubTestsRangeBlockBytes {
		return nil, 0, errGitHubTestsRangeUnusable
	}
	length := end - start + 1
	data, over, readErr := providerfoundation.ReadResponseBodyWithin(response, length, logging.TransportFailure)
	if readErr != nil {
		return nil, 0, providerfoundation.ResponseFailure(response, providerfoundation.ReasonBodyRead, fmt.Errorf(
			"%w: artifact range read failed: %w", ErrGitHubTestsIncomplete, errors.Unwrap(readErr),
		))
	}
	if over > 0 || int64(len(data)) != length {
		return nil, 0, errGitHubTestsRangeUnusable
	}
	return data, total, nil
}

// githubTestsArtifactStatusFailure is the error for an artifact response with
// a status of 400 or more, other than 404/410.
func githubTestsArtifactStatusFailure(response *http.Response) error {
	message, readErr := providerfoundation.ReadResponseBodyTransport(response, 1<<20-1)
	if readErr != nil {
		return readErr
	}
	// Classified like the GitLab artifact route: a rate limit of the
	// artifact store defers the unit with its dimension; every other
	// failure keeps the class ClassifyHTTPForRequest gives it.
	if classified := providerfoundation.ClassifyHTTPForRequest("github", responsePath(response), response.StatusCode, response.Header, string(message)); classified != nil {
		classified.Route = githubArtifactZipRoute
		return classified
	}
	return &providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, StatusCode: response.StatusCode, Route: githubArtifactZipRoute}
}

// errGitHubTestsRangeGone: the artifact answered 404/410 during the range
// read; the caller reports notFound, as for the first download response.
var errGitHubTestsRangeGone = errors.New("github tests artifact gone during range read")

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
