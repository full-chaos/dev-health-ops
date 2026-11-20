package providersync

import (
	"errors"
	"math"
	"strings"
	"testing"
)

func gitLabWidestCursor(phase string, repoLen int) gitLabTestsChunkCursor {
	widest := gitLabTestsChunkCursor{
		Phase: phase, Page: math.MaxInt, Index: math.MaxInt, Pipelines: math.MaxInt, Jobs: math.MaxInt,
		Acceptance: math.MaxInt, Suites: math.MaxInt, Cases: math.MaxInt, Coverage: math.MaxInt, Requests: math.MaxInt,
		Pages: math.MaxInt, PipelinePages: math.MaxInt, ReportPages: math.MaxInt, ProjectID: math.MaxInt64,
		LastID: math.MaxInt64, Repo: strings.Repeat("a", repoLen),
	}
	for component := range gitLabTestsTruncationComponents {
		widest.Truncated = append(widest.Truncated, component)
	}
	return widest
}

// The widest cursor the route can write (every counter at its type's maximum, the whole truncation vocabulary, LastID and
// ProjectID MaxInt64) is 641 bytes without a repository path, whatever the path of the project: the LastID resume key
// ("li") costs at most 25 of them. A negative LastID is refused on decode.
func TestGitLabTestsChunkCursorWidestWithoutRepoIs641Bytes(t *testing.T) {
	widest := gitLabWidestCursor("pipelines", 0)
	with, err := encodeGitLabTestsChunkCursor(widest)
	if err != nil {
		t.Fatalf("the widest cursor does not encode: %v", err)
	}
	widest.LastID = 0
	without, err := encodeGitLabTestsChunkCursor(widest)
	if err != nil {
		t.Fatal(err)
	}
	if cost := len(with) - len(without); cost > 25 || len(with) != 641 {
		t.Fatalf("cursor %d bytes, LastID costs %d bytes; want 641 and <= 25", len(with), cost)
	}
	if len(with) > gitLabTestsCursorNonRepoMaxBytes {
		t.Fatalf("the widest cursor without Repo is %d bytes, gitLabTestsCursorNonRepoMaxBytes is %d", len(with), gitLabTestsCursorNonRepoMaxBytes)
	}
	decoded, err := decodeGitLabTestsChunkCursor(with)
	if err != nil || decoded.LastID != math.MaxInt64 {
		t.Fatalf("round trip LastID=%d err=%v", decoded.LastID, err)
	}
	if _, err := decodeGitLabTestsChunkCursor(`{"phase":"pipelines","li":-1}`); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("a negative LastID decoded: %v", err)
	}
}

// The repository path does not grow the cursor: for any path length the widest cursor of every phase encodes and keeps a
// margin of at least 300 bytes of the checkpoint budget. Pipelines and reports never persist the path; the done cursor
// keeps it up to gitLabTestsCursorRepoMaxBytes (a margin of 2938 bytes).
func TestGitLabTestsChunkCursorMarginDoesNotDependOnTheRepoPath(t *testing.T) {
	if want := maxChunkCursorBytes - gitLabTestsCursorNonRepoMaxBytes - gitLabTestsCursorRepoMaxBytes; want < 2880 {
		t.Fatalf("limit %d leaves a margin of %d", gitLabTestsCursorRepoMaxBytes, want)
	}
	for _, phase := range []string{"pipelines", "reports", "done"} {
		for _, repoLen := range []int{0, 1, gitLabTestsCursorRepoMaxBytes, gitLabTestsCursorRepoMaxBytes + 1, 3445, 3446, 4000, 100000} {
			raw, err := encodeGitLabTestsChunkCursor(gitLabWidestCursor(phase, repoLen))
			if err != nil {
				t.Fatalf("%s cursor with a %d-byte repository path does not encode: %v", phase, repoLen, err)
			}
			if margin := maxChunkCursorBytes - len(raw); margin < 2938 {
				t.Fatalf("%s cursor with a %d-byte repository path is %d bytes, margin %d, want >= 2938", phase, repoLen, len(raw), margin)
			}
		}
	}
}
