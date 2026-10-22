package remaining

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/complexity"
)

type fakeComplexitySource struct {
	gitRows       []complexityFileContent
	fetches       int
	maxFetchBytes int64
}

func (source *fakeComplexitySource) fileCounts(context.Context, uuid.UUID, string) (int, int, error) {
	return len(source.gitRows), len(source.gitRows), nil
}

func (source *fakeComplexitySource) listGitFiles(
	_ context.Context, _ uuid.UUID, _ string, limit *int,
) ([]complexityFileSize, error) {
	rows := append([]complexityFileContent(nil), source.gitRows...)
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Path < rows[j].Path })
	if limit != nil && len(rows) > *limit {
		rows = rows[:*limit]
	}
	sizes := make([]complexityFileSize, 0, len(rows))
	for _, row := range rows {
		sizes = append(sizes, complexityFileSize{Path: row.Path, Bytes: int64(len(row.Contents))})
	}
	return sizes, nil
}

func (source *fakeComplexitySource) fetchGitFiles(
	_ context.Context, _ uuid.UUID, _ string, paths []string,
) ([]complexityFileContent, error) {
	wanted := map[string]bool{}
	for _, path := range paths {
		wanted[path] = true
	}
	var out []complexityFileContent
	var bytes int64
	for _, row := range source.gitRows {
		if wanted[row.Path] {
			out = append(out, row)
			bytes += int64(len(row.Contents))
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	source.fetches++
	if bytes > source.maxFetchBytes {
		source.maxFetchBytes = bytes
	}
	return out, nil
}

func (source *fakeComplexitySource) missingPaths(context.Context, uuid.UUID, string, *int) ([]string, error) {
	return nil, nil
}

func (source *fakeComplexitySource) hasBlameText(context.Context, uuid.UUID, string) (bool, error) {
	return false, nil
}

func (source *fakeComplexitySource) listBlameFiles(
	context.Context, uuid.UUID, string, []string, *int,
) ([]complexityFileSize, error) {
	return nil, nil
}

func (source *fakeComplexitySource) fetchBlameFiles(
	context.Context, uuid.UUID, string, []string,
) ([]complexityFileContent, error) {
	return nil, nil
}

func fakePythonFile(path string, bytes int) complexityFileContent {
	body := "def f(x):\n    if x:\n        return 1\n    return 0\n"
	pad := bytes - len(body)
	if pad > 0 {
		body += "# " + strings.Repeat("x", pad-3) + "\n"
	}
	return complexityFileContent{Path: path, Contents: body}
}

func testScanConfig() complexity.ScanConfig {
	return complexity.ScanConfig{
		IncludeGlobs: []string{"*.py"},
		ExcludeGlobs: []string{"skip/*"},
		Thresholds:   complexity.DefaultThresholds(),
	}
}

func runAnalyze(t *testing.T, source *fakeComplexitySource, maxFiles *int, budget int64) []complexity.FileComplexity {
	t.Helper()
	results, _, err := analyzeRepoFiles(
		context.Background(), source, uuid.New(), "org", testScanConfig(), maxFiles, budget)
	if err != nil {
		t.Fatalf("analyzeRepoFiles: %v", err)
	}
	return results
}

func TestAnalyzeRepoFilesBoundsBytesHeldPerFetch(t *testing.T) {
	const fileBytes = 1 << 20
	source := &fakeComplexitySource{}
	for index := 0; index < 64; index++ {
		source.gitRows = append(source.gitRows, fakePythonFile(fmt.Sprintf("src/f%03d.py", index), fileBytes))
	}
	budget := int64(8 << 20)

	results := runAnalyze(t, source, nil, budget)

	if len(results) != 64 {
		t.Fatalf("results = %d, want 64", len(results))
	}
	if source.maxFetchBytes > budget {
		t.Fatalf("one fetch held %d bytes, budget %d (whole repo is %d)", source.maxFetchBytes, budget, 64*fileBytes)
	}
	if source.fetches < 8 {
		t.Fatalf("fetches = %d, want the 64 MiB repo split into >= 8 batches", source.fetches)
	}
}

func TestAnalyzeRepoFilesOversizedFileIsFetchedAlone(t *testing.T) {
	source := &fakeComplexitySource{gitRows: []complexityFileContent{
		fakePythonFile("a.py", 100), fakePythonFile("b.py", 4096), fakePythonFile("c.py", 100),
	}}
	results := runAnalyze(t, source, nil, 1024)
	if len(results) != 3 || source.fetches != 3 {
		t.Fatalf("results=%d fetches=%d, want 3 and 3", len(results), source.fetches)
	}
}

func TestAnalyzeRepoFilesMatchesOneShotResultsAndCap(t *testing.T) {
	source := &fakeComplexitySource{}
	for index := 0; index < 20; index++ {
		source.gitRows = append(source.gitRows, fakePythonFile(fmt.Sprintf("src/f%02d.py", index), 4000))
	}
	source.gitRows = append(source.gitRows,
		fakePythonFile("skip/x.py", 100), complexityFileContent{Path: "README.md", Contents: "text"},
		fakePythonFile("src/f05.py", 2000)) // duplicate path row, as ClickHouse can return

	for _, maxFiles := range []*int{nil, intPtr(7), intPtr(100)} {
		oneShot := runAnalyze(t, &fakeComplexitySource{gitRows: source.gitRows}, maxFiles, 1<<40)
		batched := runAnalyze(t, &fakeComplexitySource{gitRows: source.gitRows}, maxFiles, 9000)
		if fmt.Sprint(oneShot) != fmt.Sprint(batched) {
			t.Fatalf("maxFiles=%v: batched results differ from one-shot\none: %v\nbat: %v", maxFiles, oneShot, batched)
		}
	}
	if got := len(runAnalyze(t, &fakeComplexitySource{gitRows: source.gitRows}, intPtr(7), 9000)); got != 5 {
		// LIMIT 7 runs before the filter: README.md and skip/x.py sort first and are dropped.
		t.Fatalf("capped results = %d, want 5", got)
	}
}

func intPtr(value int) *int { return &value }
