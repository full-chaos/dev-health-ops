package providersync

import (
	"errors"
	"math"
	"strings"
	"testing"
)

// The resume key LastID ("li") costs at most 25 bytes of the 4 KiB cursor: the widest cursor the route can write (every
// counter at its type's maximum, the whole truncation vocabulary, LastID MaxInt64) with a 1024-byte repository path
// encodes in 1675 bytes. A repository path over 3445 bytes does not fit (ticket 14378 tracks that limit); a negative
// LastID is refused on decode.
func TestGitLabTestsChunkCursorWithLastIDFitsTheBudget(t *testing.T) {
	widest := gitLabTestsChunkCursor{
		Phase: "pipelines", Page: math.MaxInt, Index: math.MaxInt, Pipelines: math.MaxInt, Jobs: math.MaxInt,
		Acceptance: math.MaxInt, Suites: math.MaxInt, Cases: math.MaxInt, Coverage: math.MaxInt, Requests: math.MaxInt,
		Pages: math.MaxInt, PipelinePages: math.MaxInt, ReportPages: math.MaxInt, ProjectID: math.MaxInt64,
		Repo: strings.Repeat("a", 1024),
	}
	for component := range gitLabTestsTruncationComponents {
		widest.Truncated = append(widest.Truncated, component)
	}
	without, err := encodeGitLabTestsChunkCursor(widest)
	if err != nil {
		t.Fatal(err)
	}
	widest.LastID = math.MaxInt64
	with, err := encodeGitLabTestsChunkCursor(widest)
	if err != nil {
		t.Fatalf("the widest cursor with LastID does not encode: %v", err)
	}
	if cost := len(with) - len(without); cost > 25 || len(with) != 1675 {
		t.Fatalf("cursor %d bytes, LastID costs %d bytes; want 1675 and <= 25 (margin %d of %d)",
			len(with), cost, maxChunkCursorBytes-len(with), maxChunkCursorBytes)
	}
	decoded, err := decodeGitLabTestsChunkCursor(with)
	if err != nil || decoded.LastID != math.MaxInt64 {
		t.Fatalf("round trip LastID=%d err=%v", decoded.LastID, err)
	}
	widest.Repo = strings.Repeat("a", maxChunkCursorBytes-len(with)+1024)
	if _, err := encodeGitLabTestsChunkCursor(widest); err != nil {
		t.Fatalf("a %d-byte repository path should still fit: %v", len(widest.Repo), err)
	}
	widest.Repo += "a"
	if _, err := encodeGitLabTestsChunkCursor(widest); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("a %d-byte repository path encoded, want the cursor budget refusal", len(widest.Repo))
	}
	if _, err := decodeGitLabTestsChunkCursor(`{"phase":"pipelines","li":-1}`); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("a negative LastID decoded: %v", err)
	}
}
