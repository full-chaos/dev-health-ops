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
	t          *testing.T
	requests   *int
	blamePaths *[]string
	fileCount  int
	graphQLErr bool
	emptyBound bool
	oversized  bool
}

func (doer gitHubBlameDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	if doer.requests != nil {
		*doer.requests++
	}
	body := `{"full_name":"acme/api","default_branch":"main"}`
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
		if doer.graphQLErr {
			body = `{"errors":[{"message":"blame unavailable"}]}`
		} else if doer.oversized {
			body = `{"data":{"padding":"` + strings.Repeat("x", nativeMaxObjectBytes) + `"}}`
		} else {
			body = `{"data":{"repository":{"object":{"blame":{"ranges":[{"startingLine":1,"endingLine":2,"commit":{"oid":"abc123","author":{"name":"Ada","email":"ada@example.com"}}}]}}}}}`
		}
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

type staticGitHubBlameCoverage struct {
	paths []string
	err   error
}

func (coverage staticGitHubBlameCoverage) BlamedPaths(
	context.Context,
	Claim,
	string,
) ([]string, error) {
	return append([]string(nil), coverage.paths...), coverage.err
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
		Coverage: staticGitHubBlameCoverage{paths: []string{"src/file-000.go"}},
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
		Coverage: staticGitHubBlameCoverage{paths: append([]string{"src/file-000.go"}, wantFirst...)},
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
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "git_blame" || len(batch.Effects[0].Rows) != 2 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row gitBlameRow
	if err := json.Unmarshal(batch.Effects[0].Rows[1], &row); err != nil {
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

func TestGitHubBlameFoundationKeepsGraphQLErrorsRetryable(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	client := gitHubRepositoryClient(t, gitHubBlameDoer{t: t, fileCount: 1, graphQLErr: true}, "https://api.github.com")
	_, err := collectGitHubBlameFoundation(
		context.Background(), claim, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrGitHubBlameTraversalFailed) {
		t.Fatalf("graphql error=%v, want retryable traversal failure", err)
	}
}

func TestGitHubBlameFoundationRejectsOversizedGraphQLPayload(t *testing.T) {
	claim := nativeTestClaim("github", "blame")
	client := gitHubRepositoryClient(t, gitHubBlameDoer{t: t, fileCount: 1, oversized: true}, "https://api.github.com")
	_, err := collectGitHubBlameFoundation(
		context.Background(), claim, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrGitHubBlameTraversalFailed) {
		t.Fatalf("oversized GraphQL error=%v, want retryable traversal failure", err)
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
