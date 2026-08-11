package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitLabBlameDoer struct {
	t               *testing.T
	paths           *[]string
	fileCount       int
	failedPaths     map[string]bool
	rateLimitPaths  map[string]bool
	emptyBound      bool
	fullTreeOnFirst bool
	requests        *[]string
	blameLines      int
}

func (doer *gitLabBlameDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	if doer.requests != nil {
		*doer.requests = append(*doer.requests, request.URL.RequestURI())
	}
	if request.URL.Path == "/api/v4/projects/123" {
		return gitLabBlameResponse(request, http.StatusOK,
			`{"id":123,"name":"api","path_with_namespace":"acme/api","default_branch":"main"}`), nil
	}
	if request.URL.Path == "/api/v4/projects/123/repository/commits" {
		if doer.emptyBound {
			return gitLabBlameResponse(request, http.StatusOK, `[]`), nil
		}
		return gitLabBlameResponse(request, http.StatusOK, `[{"id":"tree-sha"}]`), nil
	}
	if request.URL.Path == "/api/v4/projects/123/repository/tree" {
		count := doer.fileCount
		if doer.fullTreeOnFirst && request.URL.Query().Get("page") == "1" {
			count = gitLabBlameDefaultPerPage
		}
		entries := make([]string, 0, count)
		for index := range count {
			entries = append(entries, fmt.Sprintf(`{"path":"src/file-%03d.go","type":"blob"}`, index))
		}
		return gitLabBlameResponse(request, http.StatusOK, `[`+strings.Join(entries, ",")+`]`), nil
	}
	prefix := "/api/v4/projects/123/repository/files/"
	if strings.HasPrefix(request.URL.Path, prefix) && strings.HasSuffix(request.URL.Path, "/blame") {
		path := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, prefix), "/blame")
		path, _ = urlPathUnescape(path)
		if doer.paths != nil {
			*doer.paths = append(*doer.paths, path)
		}
		if doer.rateLimitPaths[path] {
			response := gitLabBlameResponse(request, http.StatusTooManyRequests, `{"message":"rate limited"}`)
			response.Header.Set("Retry-After", "1")
			return response, nil
		}
		if doer.failedPaths[path] {
			return gitLabBlameResponse(request, http.StatusInternalServerError, `{"message":"unavailable"}`), nil
		}
		lines := `"package main","func main() {}"`
		if doer.blameLines == 1 {
			lines = `"package main"`
		}
		return gitLabBlameResponse(request, http.StatusOK,
			`[{"lines":[`+lines+`],"commit":{"id":"abc123","author_name":"Ada","author_email":"ada@example.com"}}]`), nil
	}
	doer.t.Fatalf("unexpected request %s", request.URL.String())
	return nil, nil
}

func urlPathUnescape(value string) (string, error) {
	return url.PathUnescape(value)
}

func gitLabBlameResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

type staticGitLabBlameCoverage struct {
	paths []string
	err   error
}

func (coverage staticGitLabBlameCoverage) BlamedPaths(context.Context, Claim, string) ([]string, error) {
	return coverage.paths, coverage.err
}

func TestGitLabBlameFoundationExpandsLiveRangesAndNormalizesDefaults(t *testing.T) {
	claim := nativeTestClaim("gitlab", "blame")
	client := gitLabRepositoryClient(t, &gitLabBlameDoer{t: t, fileCount: 1}, "https://gitlab.example")
	normalizedAt := time.Date(2026, 7, 23, 12, 30, 0, 987654321, time.UTC)
	batch, err := collectGitLabBlameFoundation(context.Background(), claim, client, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "git_blame" ||
		len(batch.Effects[0].Rows) != 2 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row gitBlameRow
	if err := json.Unmarshal(batch.Effects[0].Rows[1], &row); err != nil {
		t.Fatal(err)
	}
	if row.OrgID != claim.OrgID || row.Path != "src/file-000.go" || row.LineNo != 2 ||
		row.AuthorName == nil || *row.AuthorName != "Ada" || row.AuthorEmail == nil ||
		*row.AuthorEmail != "ada@example.com" || row.CommitHash == nil ||
		*row.CommitHash != "abc123" || row.Line == nil || *row.Line != "func main() {}" ||
		row.AuthorWhen != nil {
		t.Fatalf("row=%+v", row)
	}
	if !row.LastSynced.Equal(normalizedAt.Truncate(time.Millisecond)) {
		t.Fatalf("last_synced=%s", row.LastSynced)
	}
	if batch.Result["inventory_status"] != "complete" || batch.Evidence.Records != 2 ||
		batch.Evidence.Requests != 4 || batch.Evidence.Pages != 2 {
		t.Fatalf("result=%v evidence=%+v", batch.Result, batch.Evidence)
	}
}

func TestGitLabBlameRouteSelectsTenantCoverageAndAccountsPhysicalRequests(t *testing.T) {
	claim := nativeTestClaim("gitlab", "blame")
	var paths, requests []string
	client := gitLabRepositoryClient(t, &gitLabBlameDoer{
		t: t, fileCount: 4, paths: &paths, requests: &requests,
	}, "https://gitlab.example")
	batch, err := (GitLabBlameRouteHandler{
		Coverage: staticGitLabBlameCoverage{paths: []string{"src/file-000.go"}},
		MaxFiles: 2,
	}).Collect(context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(paths, []string{"src/file-001.go", "src/file-002.go"}) {
		t.Fatalf("blame paths=%v", paths)
	}
	if len(requests) != 5 || batch.Evidence.Requests != 5 || batch.Evidence.Pages != 2 ||
		batch.Evidence.Records != 4 || batch.Result["inventory_status"] != "partial" ||
		batch.Result["remaining_paths"] != 1 || batch.Watermark != nil {
		t.Fatalf("requests=%v result=%v evidence=%+v watermark=%v", requests, batch.Result, batch.Evidence, batch.Watermark)
	}
}

func TestGitLabBlameRouteContinuesOrdinaryFileFailuresWithoutWatermark(t *testing.T) {
	claim := nativeTestClaim("gitlab", "blame")
	var paths []string
	client := gitLabRepositoryClient(t, &gitLabBlameDoer{
		t: t, fileCount: 3, paths: &paths, failedPaths: map[string]bool{"src/file-000.go": true},
	}, "https://gitlab.example")
	batch, err := (GitLabBlameRouteHandler{Coverage: staticGitLabBlameCoverage{}}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(paths, []string{"src/file-000.go", "src/file-001.go", "src/file-002.go"}) ||
		batch.Result["retryable_path_failures"] != 1 || batch.Result["remaining_paths"] != 1 ||
		batch.Result["inventory_status"] != "partial" || batch.Watermark != nil ||
		len(batch.Effects[0].Rows) != 4 {
		t.Fatalf("paths=%v result=%v effects=%+v", paths, batch.Result, batch.Effects)
	}
}

func TestGitLabBlameRouteAbortsOnRateLimit(t *testing.T) {
	claim := nativeTestClaim("gitlab", "blame")
	client := gitLabRepositoryClient(t, &gitLabBlameDoer{
		t: t, fileCount: 1, rateLimitPaths: map[string]bool{"src/file-000.go": true},
	}, "https://gitlab.example")
	_, err := (GitLabBlameRouteHandler{Coverage: staticGitLabBlameCoverage{}}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("rate-limit error=%v", err)
	}
}

func TestGitLabBlameRouteNoCommitAtBoundDoesNotInventRows(t *testing.T) {
	claim := nativeTestClaim("gitlab", "blame")
	claim.BeforeAt = ptrTime(time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC))
	client := gitLabRepositoryClient(t, &gitLabBlameDoer{t: t, emptyBound: true}, "https://gitlab.example")
	batch, err := (GitLabBlameRouteHandler{Coverage: staticGitLabBlameCoverage{}}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Result["inventory_status"] != "no_commit_at_bound" || batch.Evidence.Pages != 1 ||
		batch.Evidence.Records != 0 || batch.Watermark == nil || len(batch.Effects[0].Rows) != 0 {
		t.Fatalf("result=%v evidence=%+v watermark=%v", batch.Result, batch.Evidence, batch.Watermark)
	}
}

func TestGitLabBlameRouteFailsClosedOnTreePaginationCap(t *testing.T) {
	claim := nativeTestClaim("gitlab", "blame")
	client := gitLabRepositoryClient(t, &gitLabBlameDoer{
		t: t, fullTreeOnFirst: true,
	}, "https://gitlab.example")
	_, err := (GitLabBlameRouteHandler{
		Coverage: staticGitLabBlameCoverage{}, MaxPages: 1,
	}).Collect(context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if !errors.Is(err, ErrGitLabBlameIncomplete) && !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("cap error=%v", err)
	}
}

func TestGitLabBlameRouteCoverageFailureMakesNoBlameRequest(t *testing.T) {
	claim := nativeTestClaim("gitlab", "blame")
	var paths []string
	client := gitLabRepositoryClient(t, &gitLabBlameDoer{t: t, fileCount: 1, paths: &paths}, "https://gitlab.example")
	batch, err := (GitLabBlameRouteHandler{
		Coverage: staticGitLabBlameCoverage{err: errors.New("readback unavailable")},
	}).Collect(context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if !errors.Is(err, ErrGitLabBlameCoverageUnavailable) || len(paths) != 0 ||
		len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("paths=%v err=%v batch=%+v", paths, err, batch)
	}
}

func TestSelectNextGitLabBlamePathsPreservesTreeOrderAndBound(t *testing.T) {
	selected, remaining, err := selectNextGitLabBlamePaths(
		[]string{"a.go", "b.go", "a.go", "c.go"}, []string{"b.go"}, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(selected, []string{"a.go", "a.go"}) || remaining != 1 {
		t.Fatalf("selected=%v remaining=%d", selected, remaining)
	}
}

func TestGitLabBlameRouteRejectsWrongDataset(t *testing.T) {
	claim := nativeTestClaim("gitlab", "files")
	client := gitLabRepositoryClient(t, &gitLabBlameDoer{t: t}, "https://gitlab.example")
	_, err := (GitLabBlameRouteHandler{Coverage: staticGitLabBlameCoverage{}}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong-dataset error=%v", err)
	}
}
