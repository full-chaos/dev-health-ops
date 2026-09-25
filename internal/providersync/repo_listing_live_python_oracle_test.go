package providersync

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

//go:embed testdata/repo_listing_oracle.py
var repoListingOracleProgram string

// listingScriptEntry is one canned provider answer, keyed by the request's canonical
// URI: escaped path, then the query sorted by key.
type listingScriptEntry struct {
	URI     string            `json:"uri"`
	Status  int               `json:"status,omitempty"`
	Link    string            `json:"link,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    any               `json:"body"`
}

type listingCase struct {
	Name     string               `json:"name"`
	Provider string               `json:"provider"`
	Listing  map[string]any       `json:"listing"`
	Script   []listingScriptEntry `json:"script"`
}

type listingOutcome struct {
	Results  [][3]any `json:"results"`
	Requests []string `json:"requests"`
	Error    *string  `json:"error"`
}

func listingCanonicalURI(request *http.Request) string {
	pairs := request.URL.Query()
	keys := make([]string, 0, len(pairs))
	for key := range pairs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var query []string
	for _, key := range keys {
		for _, value := range pairs[key] {
			query = append(query, key+"="+value)
		}
	}
	path := request.URL.EscapedPath()
	if len(query) == 0 {
		return path
	}
	return path + "?" + strings.Join(query, "&")
}

type listingScriptDoer struct {
	mu       sync.Mutex
	entries  map[string]listingScriptEntry
	requests []string
}

func (doer *listingScriptDoer) Do(request *http.Request) (*http.Response, error) {
	uri := listingCanonicalURI(request)
	doer.mu.Lock()
	doer.requests = append(doer.requests, uri)
	entry, ok := doer.entries[uri]
	doer.mu.Unlock()
	header := http.Header{"Content-Type": []string{"application/json"}}
	if !ok {
		return &http.Response{StatusCode: http.StatusNotFound, Header: header, Body: io.NopCloser(strings.NewReader(`{"message":"not scripted"}`)), Request: request}, nil
	}
	for key, value := range entry.Headers {
		header.Set(key, value)
	}
	if entry.Link != "" {
		header.Set("Link", entry.Link)
	}
	status := entry.Status
	if status == 0 {
		status = http.StatusOK
	}
	body, _ := json.Marshal(entry.Body)
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(bytes.NewReader(body)), Request: request}, nil
}

func goListing(kase listingCase) listingOutcome {
	doer := &listingScriptDoer{entries: map[string]listingScriptEntry{}}
	for _, entry := range kase.Script {
		doer.entries[entry.URI] = entry
	}
	base := "https://api.github.com"
	if kase.Provider == "gitlab" {
		base = "https://gitlab.com" // the production shape: providerfoundation.NewGitLabClient takes the instance host
	}
	client, err := providerfoundation.NewHTTPClient(kase.Provider, base, doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		panic(err)
	}
	str := func(key string) string { s, _ := kase.Listing[key].(string); return s }
	num := func(key string) *int {
		f, present := kase.Listing[key].(float64)
		if !present {
			return nil
		}
		n := int(f)
		return &n
	}
	var repos []ListedRepository
	if kase.Provider == "github" {
		repos, err = ListGitHubRepositories(context.Background(), client, GitHubListing{Org: str("org"), User: str("user"), Pattern: str("pattern"), MaxRepos: num("max")})
	} else {
		repos, err = ListGitLabProjects(context.Background(), client, GitLabListing{Group: str("group"), Pattern: str("pattern"), MaxProjects: num("max")})
	}
	outcome := listingOutcome{Results: [][3]any{}, Requests: doer.requests}
	if outcome.Requests == nil {
		outcome.Requests = []string{}
	}
	if err != nil {
		message := err.Error()
		outcome.Error = &message
		return outcome
	}
	for _, repo := range repos {
		outcome.Results = append(outcome.Results, [3]any{repo.Name, repo.FullName, repo.ProjectID})
	}
	return outcome
}

// TestRepoListingMatchesLivePython runs the REAL Python batch listing
// (_list_github_repositories_for_batch, GitLabCodeClient.list_projects behind
// _gitlab_effective_group) against a scripted provider and compares what it
// returns AND every request it made with the Go listing over the same script.
func TestRepoListingMatchesLivePython(t *testing.T) {
	requireLivePythonOracles(t)
	python := pythonExecutable(t)
	corpus := repoListingCorpus()
	input, err := json.Marshal(corpus)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "-c", repoListingOracleProgram)
	command.Stdin = bytes.NewReader(input)
	_, file, _, _ := runtime.Caller(0)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(filepath.Dir(file), "..", "..", "src"))
	output, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, stderr))
	}
	var want []listingOutcome
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode python answer: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d for %d cases", len(want), len(corpus))
	}
	mismatches, errored := 0, 0
	for index, kase := range corpus {
		got := goListing(kase)
		expected := want[index]
		if expected.Error != nil {
			errored++
		}
		if (got.Error != nil) != (expected.Error != nil) || !equalListingOutcome(got, expected) {
			mismatches++
			if mismatches <= 25 {
				g, _ := json.Marshal(got)
				w, _ := json.Marshal(expected)
				t.Errorf("case %d %s (%s %v):\n  go     %s\n  python %s", index, kase.Name, kase.Provider, kase.Listing, g, w)
			}
		}
	}
	t.Logf("%d listings compared (%d ended in a provider error), %d mismatches", len(corpus), errored, mismatches)
	if mismatches > 0 {
		t.Fatalf("%d of %d listings differ", mismatches, len(corpus))
	}
	proof := os.Getenv(livePythonOracleProofDir)
	if err := os.WriteFile(filepath.Join(proof, "repo-listing"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func equalListingOutcome(got, want listingOutcome) bool {
	if len(got.Requests) != len(want.Requests) || len(got.Results) != len(want.Results) {
		return false
	}
	for i := range got.Requests {
		if got.Requests[i] != want.Requests[i] {
			return false
		}
	}
	for i := range got.Results {
		for j := 0; j < 3; j++ {
			if fmt.Sprint(got.Results[i][j]) != fmt.Sprint(want.Results[i][j]) {
				return false
			}
		}
	}
	return true
}
