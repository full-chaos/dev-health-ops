package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitHubBlameDoer struct {
	t               *testing.T
	requests        *int
	blamePaths      *[]string
	fileCount       int
	graphQLErr      bool
	graphQLErrPaths map[string]bool
	emptyPaths      map[string]bool
	rateLimitPaths  map[string]bool
	emptyBound      bool
	oversized       bool
}

func (doer gitHubBlameDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	if doer.requests != nil {
		*doer.requests++
	}
	body := `{"full_name":"acme/api","default_branch":"main"}`
	statusCode := http.StatusOK
	header := http.Header{"Content-Type": []string{"application/json"}}
	switch request.URL.Path {
	case "/repos/acme/api":
	case "/repos/acme/api/commits":
		if doer.emptyBound {
			body = `[]`
		} else {
			body = `[{"sha":"tree-sha"}]`
		}
	case "/repos/acme/api/git/trees/tree-sha":
		entries := make([]string, 0, doer.fileCount)
		for index := range doer.fileCount {
			entries = append(entries, fmt.Sprintf(`{"path":"src/file-%03d.go","type":"blob","size":20}`, index))
		}
		body = `{"tree":[` + strings.Join(entries, ",") + `]}`
	case "/graphql":
		var requestBody struct {
			Variables map[string]string `json:"variables"`
		}
		if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
			doer.t.Fatal(err)
		}
		if requestBody.Variables["ref"] != "tree-sha" || requestBody.Variables["path"] == "" {
			doer.t.Fatalf("graphql variables=%v", requestBody.Variables)
		}
		if doer.blamePaths != nil {
			*doer.blamePaths = append(*doer.blamePaths, requestBody.Variables["path"])
		}
		path := requestBody.Variables["path"]
		if doer.rateLimitPaths[path] {
			statusCode = http.StatusTooManyRequests
			header.Set("Retry-After", "1")
			body = `{"message":"rate limited"}`
		} else if doer.graphQLErr || doer.graphQLErrPaths[path] {
			body = `{"errors":[{"message":"blame unavailable"}]}`
		} else if doer.emptyPaths[path] {
			body = `{"data":{"repository":{"object":{"blame":{"ranges":[]}}}}}`
		} else if doer.oversized {
			body = `{"data":{"padding":"` + strings.Repeat("x", nativeMaxObjectBytes) + `"}}`
		} else {
			body = `{"data":{"repository":{"object":{"blame":{"ranges":[{"startingLine":1,"endingLine":2,"commit":{"oid":"abc123","author":{"name":"Ada","email":"ada@example.com"}}}]}}}}}`
		}
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{
		StatusCode: statusCode,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

type staticGitHubBlameCoverage struct {
	state GitHubBlameProgressState
	err   error
}

func (coverage staticGitHubBlameCoverage) Progress(
	context.Context,
	Claim,
	string,
	string,
	string,
) (GitHubBlameProgressState, error) {
	return coverage.state, coverage.err
}

func TestGitHubBlameRouteFailsBeforeProviderWorkWithoutPersistedProgress(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	requests := 0
	client := gitHubRepositoryClient(t, gitHubBlameDoer{
		t: t, requests: &requests, fileCount: 1,
	}, "https://api.github.com")
	batch, err := (GitHubBlameRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrGitHubBlameProgressUnavailable) {
		t.Fatalf("progress error=%v, want ErrGitHubBlameProgressUnavailable", err)
	}
	if requests != 0 {
		t.Fatalf("provider requests=%d, want zero before persisted progress is available", requests)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("partial batch=%+v", batch)
	}
}

func TestGitHubBlameRouteSelectsTheNextPersistedCoverageBatch(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	normalizedAt := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)

	firstPaths := []string{}
	firstClient := gitHubRepositoryClient(t, gitHubBlameDoer{
		t: t, blamePaths: &firstPaths, fileCount: 7,
	}, "https://api.github.com")
	first, err := (GitHubBlameRouteHandler{
		Coverage: staticGitHubBlameCoverage{state: GitHubBlameProgressState{BlamedPaths: []string{"src/file-000.go"}}},
		MaxFiles: 3,
	}).Collect(context.Background(), claim, providerfoundation.Credential{}, firstClient, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := []string{"src/file-001.go", "src/file-002.go", "src/file-003.go"}
	if !slices.Equal(firstPaths, wantFirst) {
		t.Fatalf("first blame paths=%v want=%v", firstPaths, wantFirst)
	}
	if first.Result["inventory_status"] != "partial" ||
		first.Result["remaining_paths"] != 3 || first.Evidence.CapReached {
		t.Fatalf("first result=%v evidence=%+v", first.Result, first.Evidence)
	}

	secondPaths := []string{}
	secondClient := gitHubRepositoryClient(t, gitHubBlameDoer{
		t: t, blamePaths: &secondPaths, fileCount: 7,
	}, "https://api.github.com")
	second, err := (GitHubBlameRouteHandler{
		Coverage: staticGitHubBlameCoverage{state: GitHubBlameProgressState{BlamedPaths: append([]string{"src/file-000.go"}, wantFirst...)}},
		MaxFiles: 3,
	}).Collect(context.Background(), claim, providerfoundation.Credential{}, secondClient, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	wantSecond := []string{"src/file-004.go", "src/file-005.go", "src/file-006.go"}
	if !slices.Equal(secondPaths, wantSecond) {
		t.Fatalf("second blame paths=%v want=%v", secondPaths, wantSecond)
	}
	if second.Result["inventory_status"] != "complete" ||
		second.Result["remaining_paths"] != 0 || second.Evidence.CapReached {
		t.Fatalf("second result=%v evidence=%+v", second.Result, second.Evidence)
	}
}

func TestGitHubBlameRouteCoverageFailureHasNoBlameEffectsOrWatermark(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	blamePaths := []string{}
	client := gitHubRepositoryClient(t, gitHubBlameDoer{
		t: t, blamePaths: &blamePaths, fileCount: 2,
	}, "https://api.github.com")
	batch, err := (GitHubBlameRouteHandler{
		Coverage: staticGitHubBlameCoverage{err: errors.New("coverage unavailable")},
	}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrGitHubBlameProgressUnavailable) {
		t.Fatalf("coverage error=%v, want ErrGitHubBlameProgressUnavailable", err)
	}
	if len(blamePaths) != 0 || len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("blame paths=%v partial batch=%+v", blamePaths, batch)
	}
}

func TestGitHubBlameFoundationExpandsLiveBlameRangesIntoRows(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	client := gitHubRepositoryClient(t, gitHubBlameDoer{t: t, fileCount: 1}, "https://api.github.com")
	normalizedAt := time.Date(2026, 7, 23, 12, 30, 0, 987654321, time.UTC)
	batch, err := collectGitHubBlameFoundation(
		context.Background(), claim, client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 2 || batch.Effects[0].Destination != "github_blame_path_progress" ||
		len(batch.Effects[0].Rows) != 1 || batch.Effects[1].Destination != "git_blame" ||
		len(batch.Effects[1].Rows) != 2 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row gitBlameRow
	if err := json.Unmarshal(batch.Effects[1].Rows[1], &row); err != nil {
		t.Fatal(err)
	}
	if row.OrgID != claim.OrgID || row.Path != "src/file-000.go" || row.LineNo != 2 ||
		row.AuthorName == nil || *row.AuthorName != "Ada" ||
		row.AuthorEmail == nil || *row.AuthorEmail != "ada@example.com" ||
		row.CommitHash == nil || *row.CommitHash != "abc123" || row.Line != nil || row.AuthorWhen != nil {
		t.Fatalf("row=%+v", row)
	}
	if !row.LastSynced.Equal(normalizedAt.Truncate(time.Millisecond)) {
		t.Fatalf("last_synced=%s", row.LastSynced)
	}
	if batch.Result["inventory_status"] != "complete" || batch.Evidence.Records != 2 || batch.Evidence.CapReached {
		t.Fatalf("result=%v evidence=%+v", batch.Result, batch.Evidence)
	}
}

func TestGitHubBlameFoundationFailsClosedBeforeFetchingPartialInventory(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	client := gitHubRepositoryClient(t, gitHubBlameDoer{t: t, fileCount: gitHubBlameMaxFiles + 1}, "https://api.github.com")
	_, err := collectGitHubBlameFoundation(
		context.Background(), claim, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrGitHubBlameIncomplete) {
		t.Fatalf("cap error=%v, want ErrGitHubBlameIncomplete", err)
	}
}

func TestGitHubBlameRouteContinuesAfterPerFileGraphQLError(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	blamePaths := []string{}
	client := gitHubRepositoryClient(t, gitHubBlameDoer{
		t: t, fileCount: 3, blamePaths: &blamePaths,
		graphQLErrPaths: map[string]bool{"src/file-000.go": true},
	}, "https://api.github.com")
	batch, err := (GitHubBlameRouteHandler{
		Coverage: staticGitHubBlameCoverage{}, MaxFiles: 3,
	}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(blamePaths, []string{"src/file-000.go", "src/file-001.go", "src/file-002.go"}) {
		t.Fatalf("attempted paths=%v", blamePaths)
	}
	if batch.Result["retryable_path_failures"] != 1 || batch.Result["remaining_paths"] != 1 ||
		batch.Result["inventory_status"] != "partial" || len(batch.Effects[1].Rows) != 4 {
		t.Fatalf("result=%v effects=%+v", batch.Result, batch.Effects)
	}
	var failed gitHubBlamePathProgressRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &failed); err != nil {
		t.Fatal(err)
	}
	if failed.Path != "src/file-000.go" || failed.Outcome != gitHubBlameOutcomeRetryableError {
		t.Fatalf("failed progress=%+v", failed)
	}
}

func TestGitHubBlameRoutePersistsEmptyPathProgressWithoutBlameRow(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	client := gitHubRepositoryClient(t, gitHubBlameDoer{
		t: t, fileCount: 1, emptyPaths: map[string]bool{"src/file-000.go": true},
	}, "https://api.github.com")
	batch, err := (GitHubBlameRouteHandler{Coverage: staticGitHubBlameCoverage{}}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects[0].Rows) != 1 || len(batch.Effects[1].Rows) != 0 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var marker gitHubBlamePathProgressRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &marker); err != nil {
		t.Fatal(err)
	}
	if marker.Outcome != gitHubBlameOutcomeEmpty || marker.Path != "src/file-000.go" {
		t.Fatalf("marker=%+v", marker)
	}
}

func TestGitHubBlameRouteAbortsBatchOnRateLimit(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	client := gitHubRepositoryClient(t, gitHubBlameDoer{
		t: t, fileCount: 2, rateLimitPaths: map[string]bool{"src/file-000.go": true},
	}, "https://api.github.com")
	_, err := (GitHubBlameRouteHandler{Coverage: staticGitHubBlameCoverage{}}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("rate-limit error=%v", err)
	}
}

func TestGitHubBlameSelectionRotatesFailedPathsBehindNeverAttemptedPaths(t *testing.T) {
	paths, remaining, err := selectNextGitHubBlamePathsWithProgress(
		[]string{"bad.go", "fresh-a.go", "fresh-b.go"},
		GitHubBlameProgressState{FailedAttempts: map[string]uint64{"bad.go": 3}},
		2, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(paths, []string{"fresh-a.go", "fresh-b.go"}) || remaining != 1 {
		t.Fatalf("paths=%v remaining=%d", paths, remaining)
	}
}

func TestGitHubBlameProgressEffectCommitsBeforeBlame(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	progress, err := effectBatchFromValues(
		"github_blame_path_progress", EffectReadbackRequired,
		[]gitHubBlamePathProgressRow{newGitHubBlamePathProgressRow(
			claim, "c7198fbc-1945-3717-05d8-eb78866b4e79", "tree-sha", "a.go",
			gitHubBlameOutcomeEmpty, now,
		)},
	)
	if err != nil {
		t.Fatal(err)
	}
	blame, err := effectBatchFromValues("git_blame", EffectReadbackRequired, []gitBlameRow{})
	if err != nil {
		t.Fatal(err)
	}
	ledger := &memoryEffectLedger{}
	sink := &memoryEffectSink{}
	result, err := (EffectCommitter{
		Ledger: ledger, Sink: sink, Now: func() time.Time { return now },
	}).Commit(context.Background(), claim, []EffectBatch{blame, progress}, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Written != 2 || !slices.Equal(
		sink.destinations, []string{"github_blame_path_progress", "git_blame"},
	) {
		t.Fatalf("result=%+v destinations=%v", result, sink.destinations)
	}
}

func TestGitHubBlameFoundationRecordsOversizedGraphQLPayloadAsRetryablePathFailure(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	client := gitHubRepositoryClient(t, gitHubBlameDoer{t: t, fileCount: 1, oversized: true}, "https://api.github.com")
	batch, err := collectGitHubBlameFoundation(
		context.Background(), claim, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Result["retryable_path_failures"] != 1 || len(batch.Effects[0].Rows) != 1 ||
		len(batch.Effects[1].Rows) != 0 {
		t.Fatalf("result=%v effects=%+v", batch.Result, batch.Effects)
	}
}

func TestGitHubBlameFoundationDistinguishesNoCommitAtBound(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	client := gitHubRepositoryClient(t, gitHubBlameDoer{t: t, emptyBound: true}, "https://api.github.com")
	batch, err := collectGitHubBlameFoundation(
		context.Background(), claim, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Result["inventory_status"] != "no_commit_at_bound" || batch.Evidence.Pages != 0 || batch.Evidence.Records != 0 {
		t.Fatalf("result=%v evidence=%+v", batch.Result, batch.Evidence)
	}
}

func TestGitHubBlameFoundationReportsLegitimateEmptyTree(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	client := gitHubRepositoryClient(t, gitHubBlameDoer{t: t}, "https://api.github.com")
	batch, err := collectGitHubBlameFoundation(
		context.Background(), claim, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Result["inventory_status"] != "empty" || batch.Evidence.Pages != 1 || batch.Evidence.Records != 0 {
		t.Fatalf("result=%v evidence=%+v", batch.Result, batch.Evidence)
	}
}

func TestGitHubBlameRouteRejectsWrongDataset(t *testing.T) {
	claim := nativeTestClaim("github", "files")
	client := gitHubRepositoryClient(t, gitHubBlameDoer{t: t}, "https://api.github.com")
	_, err := (GitHubBlameRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong-dataset error=%v", err)
	}
}
