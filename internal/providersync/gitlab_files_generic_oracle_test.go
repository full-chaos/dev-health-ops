package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

var oracleGitLabFilesGoOnlyFields = map[string]string{
	"last_synced": "stamped from the Go complete-route handler after Python backfill_file_records",
	"org_id":      "carried from the Go claim to keep ClickHouse writes tenant-scoped",
}

type gitLabHTTPClassificationOracleRow struct {
	IsRateLimit bool `json:"is_rate_limit"`
}

func buildGitLabHTTPClassificationOracleRow(t *testing.T, input map[string]any) gitLabHTTPClassificationOracleRow {
	t.Helper()
	headers := http.Header{}
	for key, value := range input["headers"].(map[string]any) {
		headers.Set(key, value.(string))
	}
	classified := providerfoundation.ClassifyHTTP(
		"gitlab", int(input["status"].(int)), headers,
	)
	return gitLabHTTPClassificationOracleRow{
		IsRateLimit: classified != nil && classified.Class == providerfoundation.ErrorRateLimited,
	}
}

func TestGenericOracleMatchesLivePythonForGitLabFilesHTTPClassification(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/files/http-classification",
		[]oracleCase{
			{ID: "retry_after_qualified_403", Input: map[string]any{
				"status": 403, "headers": map[string]any{"Retry-After": "2"},
			}},
			{ID: "remaining_zero_qualified_403", Input: map[string]any{
				"status": 403, "headers": map[string]any{"RateLimit-Remaining": "0"},
			}},
			{ID: "plain_403_is_authentication", Input: map[string]any{
				"status": 403, "headers": map[string]any{},
			}},
			{ID: "remaining_nonzero_is_authentication", Input: map[string]any{
				"status": 403, "headers": map[string]any{"RateLimit-Remaining": "42"},
			}},
			{ID: "429_is_primary_rate_limit", Input: map[string]any{
				"status": 429, "headers": map[string]any{"Retry-After": "2"},
			}},
		},
		buildGitLabHTTPClassificationOracleRow,
		nil,
	)
}

func buildGitLabFileRowForOracle(t *testing.T, input map[string]any) gitFileRow {
	t.Helper()
	contents := make(map[string]string)
	if existing, ok := input["existing_contents"].(map[string]any); ok {
		for path, value := range existing {
			contents[path] = value.(string)
		}
	}
	for path, value := range input["contents_by_path"].(map[string]any) {
		contents[path] = value.(string)
	}
	return newGitLabFileRow(
		nativeTestClaim("gitlab", "files"),
		input["repo_id"].(string), input["path"].(string), contents,
		time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
}

func TestGenericOracleMatchesLivePythonForGitLabFilesWorkerRows(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/files/row",
		[]oracleCase{
			{ID: "scannable_content", Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "path": "src/main.go",
				"contents_by_path": map[string]any{"src/main.go": "package main\n"},
			}},
			{ID: "path_only", Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "path": "README.md",
				"contents_by_path": map[string]any{},
			}},
			{ID: "empty_text_is_distinct_from_missing", Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "path": "src/empty.go",
				"contents_by_path": map[string]any{"src/empty.go": ""},
			}},
			{ID: "paths_only_preserves_existing_content", Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "path": "src/main.go",
				"existing_contents": map[string]any{"src/main.go": "package existing\n"},
				"contents_by_path":  map[string]any{},
			}},
		},
		buildGitLabFileRowForOracle,
		oracleGitLabFilesGoOnlyFields,
	)
}

type gitLabFilesTraceRow struct {
	Path       string  `json:"path"`
	Executable bool    `json:"executable"`
	Contents   *string `json:"contents"`
}

type gitLabFilesTraversalTrace struct {
	ProducerRequests  []string                `json:"producer_requests"`
	UsageRequestCount int                     `json:"usage_request_count"`
	TreePaths         []string                `json:"tree_paths"`
	Rows              []gitLabFilesTraceRow   `json:"rows"`
	Incomplete        []GitLabFilesIncomplete `json:"incomplete"`
}

type gitLabFilesTraceDoer struct {
	t              *testing.T
	requests       []*http.Request
	treePages      [][]map[string]any
	nextPages      []string
	commitRows     []map[string]any
	sizes          map[string]int
	contents       map[string]string
	project        string
	contentFailure bool
}

func (doer *gitLabFilesTraceDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request)
	status := http.StatusOK
	headers := http.Header{"Content-Type": []string{"application/json"}}
	var body string
	switch {
	case request.URL.Path == "/api/v4/projects/123":
		body = doer.project
	case request.URL.Path == "/api/v4/projects/123/repository/commits":
		body = marshalJSON(doer.t, doer.commitRows)
	case request.URL.Path == "/api/v4/projects/123/repository/tree":
		page := queryInt(request, "page")
		if page < 1 || page > len(doer.treePages) {
			doer.t.Fatalf("unexpected tree page=%d", page)
		}
		body = marshalJSON(doer.t, doer.treePages[page-1])
		if page <= len(doer.nextPages) && doer.nextPages[page-1] != "" {
			headers.Set("X-Next-Page", doer.nextPages[page-1])
		}
	case request.URL.Path == "/api/graphql":
		var input struct {
			Query     string `json:"query"`
			Variables struct {
				Paths []string `json:"paths"`
			} `json:"variables"`
		}
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			t := doer.t
			t.Fatal(err)
		}
		if doer.contentFailure && strings.Contains(input.Query, "rawTextBlob") {
			status = http.StatusInternalServerError
			body = `{}`
			break
		}
		nodes := make([]map[string]any, 0, len(input.Variables.Paths))
		for _, filePath := range input.Variables.Paths {
			node := map[string]any{"path": filePath}
			if strings.Contains(input.Query, "rawSize") {
				node["rawSize"] = doer.sizes[filePath]
			} else if content, ok := doer.contents[filePath]; ok {
				node["rawTextBlob"] = content
			} else {
				node["rawTextBlob"] = nil
			}
			nodes = append(nodes, node)
		}
		body = marshalJSON(doer.t, map[string]any{
			"data": map[string]any{"project": map[string]any{
				"repository": map[string]any{"blobs": map[string]any{"nodes": nodes}},
			}},
		})
	default:
		doer.t.Fatalf("unexpected GitLab files trace request %s", request.URL)
	}
	return &http.Response{
		StatusCode: status, Header: headers,
		Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

func TestGenericOracleMatchesLivePythonForGitLabFilesTraversal(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/files/trace",
		[]oracleCase{{ID: "paged_tree_and_content", Input: map[string]any{
			"repo_id":           "c7198fbc-1945-3717-05d8-eb78866b4e79",
			"project_full_name": "Acme/API", "default_branch": "main",
			"tree_pages": []any{
				[]any{
					map[string]any{"path": "README.md", "type": "blob"},
					map[string]any{"path": "src/main.go", "type": "blob"},
				},
				[]any{
					map[string]any{"path": "tests/example.go", "type": "blob"},
					map[string]any{"path": "src/oversized.go", "type": "blob"},
				},
			},
			"tree_next_pages": []any{"2"},
			"sizes":           map[string]any{"src/main.go": 15, "src/oversized.go": 1000001},
			"contents":        map[string]any{"src/main.go": "package main\n"},
		}}},
		buildGitLabFilesTraversalTrace,
		nil,
	)
}

func TestGenericOracleMatchesLivePythonForGitLabFilesContentCap(t *testing.T) {
	const count = 2_001
	treePages := make([]any, 0, (count+99)/100)
	treeNextPages := make([]any, 0, (count+99)/100-1)
	sizes := make(map[string]any, count)
	contents := map[string]any{"src/file0000.go": "package main\n"}
	for index := 0; index < count; index++ {
		filePath := fmt.Sprintf("src/file%04d.go", index)
		pageIndex := index / 100
		if pageIndex == len(treePages) {
			treePages = append(treePages, []any{})
			if pageIndex > 0 {
				treeNextPages = append(treeNextPages, fmt.Sprintf("%d", pageIndex+1))
			}
		}
		page := treePages[pageIndex].([]any)
		page = append(page, map[string]any{"path": filePath, "type": "blob"})
		treePages[pageIndex] = page
		sizes[filePath] = 10
	}
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/files/trace",
		[]oracleCase{{ID: "content_cap", Input: map[string]any{
			"repo_id":           "c7198fbc-1945-3717-05d8-eb78866b4e79",
			"project_full_name": "Acme/API", "default_branch": "main",
			"tree_pages":      treePages,
			"tree_next_pages": treeNextPages,
			"sizes":           sizes,
			"contents":        contents,
		}}},
		buildGitLabFilesTraversalTrace,
		nil,
	)
}

func TestGenericOracleMatchesLivePythonForGitLabFilesContentFailure(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/files/trace",
		[]oracleCase{{ID: "ordinary_content_failure", Input: map[string]any{
			"repo_id":           "c7198fbc-1945-3717-05d8-eb78866b4e79",
			"project_full_name": "Acme/API", "default_branch": "main",
			"tree_pages": []any{[]any{
				map[string]any{"path": "README.md", "type": "blob"},
				map[string]any{"path": "src/main.go", "type": "blob"},
			}},
			"tree_next_pages": []any{},
			"sizes":           map[string]any{"src/main.go": 10},
			"contents":        map[string]any{"src/main.go": "package main\n"},
			"content_failure": true,
		}}},
		buildGitLabFilesTraversalTrace,
		nil,
	)
}

func buildGitLabFilesTraversalTrace(t *testing.T, input map[string]any) gitLabFilesTraversalTrace {
	t.Helper()
	treePages := make([][]map[string]any, 0)
	for _, rawPage := range input["tree_pages"].([]any) {
		page := make([]map[string]any, 0)
		for _, rawItem := range rawPage.([]any) {
			page = append(page, rawItem.(map[string]any))
		}
		treePages = append(treePages, page)
	}
	nextPages := make([]string, 0)
	for _, value := range input["tree_next_pages"].([]any) {
		nextPages = append(nextPages, value.(string))
	}
	sizes := make(map[string]int)
	for path, value := range input["sizes"].(map[string]any) {
		sizes[path] = int(value.(int))
	}
	contents := make(map[string]string)
	for path, value := range input["contents"].(map[string]any) {
		contents[path] = value.(string)
	}
	doer := &gitLabFilesTraceDoer{
		t: t, project: gitLabRepositoryFixture, treePages: treePages,
		nextPages: nextPages, sizes: sizes, contents: contents,
		contentFailure: input["content_failure"] == true,
	}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	normalizedAt := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	batch, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.test"), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) < 1 || batch.Evidence.Requests != len(doer.requests) {
		t.Fatalf("physical evidence=%+v requests=%d", batch.Evidence, len(doer.requests))
	}
	trace := gitLabFilesTraversalTrace{
		ProducerRequests:  make([]string, 0, len(doer.requests)-1),
		UsageRequestCount: batch.Evidence.Requests - 1,
		TreePaths:         make([]string, 0),
		Rows:              make([]gitLabFilesTraceRow, 0),
		Incomplete:        make([]GitLabFilesIncomplete, 0),
	}
	if raw, ok := batch.Result[gitLabFilesIncompleteResultKey]; ok {
		var encoded []byte
		encoded, err = json.Marshal(raw)
		if err != nil || json.Unmarshal(encoded, &trace.Incomplete) != nil {
			t.Fatalf("decode files incomplete evidence: %v", err)
		}
	}
	for _, request := range doer.requests[1:] {
		trace.ProducerRequests = append(trace.ProducerRequests, gitLabFilesTraceRequestURI(request))
	}
	for _, page := range treePages {
		for _, item := range page {
			if item["type"] == "blob" && item["path"] != nil {
				trace.TreePaths = append(trace.TreePaths, item["path"].(string))
			}
		}
	}
	for _, effect := range batch.Effects {
		if effect.Destination != "git_files" {
			continue
		}
		for _, raw := range effect.Rows {
			var row gitFileRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			trace.Rows = append(trace.Rows, gitLabFilesTraceRow{
				Path: row.Path, Executable: row.Executable, Contents: row.Contents,
			})
		}
	}
	sort.Slice(trace.Rows, func(left, right int) bool { return trace.Rows[left].Path < trace.Rows[right].Path })
	return trace
}

func gitLabFilesTraceRequestURI(request *http.Request) string {
	uri := request.URL.RequestURI()
	return strings.Replace(uri, "/api/v4/projects/123/", "/api/v4/projects/{project}/", 1)
}

func marshalJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func queryInt(request *http.Request, key string) int {
	value := request.URL.Query().Get(key)
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return 0
	}
	return parsed
}
