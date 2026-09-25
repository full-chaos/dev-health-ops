//go:build integration

package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

type inProcessDoer struct {
	mu    sync.Mutex
	body  string
	paths []string
	auth  []string
}

func (doer *inProcessDoer) Do(request *http.Request) (*http.Response, error) {
	doer.mu.Lock()
	doer.paths = append(doer.paths, request.URL.Path)
	doer.auth = append(doer.auth, request.Header.Get("Authorization")+request.Header.Get("PRIVATE-TOKEN"))
	doer.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(doer.body)),
		Request:    request,
	}, nil
}

func startInProcessClickHouse(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, reposDDL); err != nil {
		t.Fatal(err)
	}
	return conn
}

// TestRunInProcessWritesTheWorkersRowsWithoutAWorker runs github and gitlab
// repo-metadata through RunInProcess against a real ClickHouse: no Postgres,
// no sync_run, no unit row. The provider answers a fixture; the row that lands
// is the one the worker's own route writes, the credential reached the request,
// and running it again converges on one logical row.
func TestRunInProcessWritesTheWorkersRowsWithoutAWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn := startInProcessClickHouse(t, ctx)
	cases := []struct {
		provider, external, body, wantRepo, wantPath, wantCredential string
		credential                                                   map[string]string
	}{
		{"github", "Acme/API", gitHubRepositoryFixture, "Acme/API", "/repos/Acme/API",
			"token cli-github-token", map[string]string{"token": "cli-github-token"}},
		{"gitlab", "123", gitLabRepositoryFixture, "Acme/API", "/api/v4/projects/123",
			"cli-gitlab-token", map[string]string{"token": "cli-gitlab-token"}},
	}
	for _, tc := range cases {
		t.Run(tc.provider, func(t *testing.T) {
			doer := &inProcessDoer{body: tc.body}
			base := "https://api.github.com"
			if tc.provider == "gitlab" {
				base = "https://gitlab.example"
			}
			run := InProcessRun{
				OrgID: "org-inprocess", Provider: tc.provider, Dataset: "repo-metadata",
				SourceExternalID: tc.external, SourceName: tc.external,
				BeforeAt: time.Now().UTC(), Credential: tc.credential,
				Config: map[string]string{"base_url": base}, Conn: conn, Doer: doer,
			}
			result, err := RunInProcess(ctx, run)
			if err != nil {
				t.Fatalf("RunInProcess: %v", err)
			}
			if result.Effects.Written != 1 || result.Fetch.Records != 1 {
				t.Fatalf("result = %+v", result)
			}
			if len(doer.paths) != 1 || doer.paths[0] != tc.wantPath {
				t.Fatalf("provider requests = %v, want [%s]", doer.paths, tc.wantPath)
			}
			if doer.auth[0] != tc.wantCredential {
				t.Fatalf("the credential did not reach the request: %q", doer.auth[0])
			}
			if _, err := RunInProcess(ctx, run); err != nil {
				t.Fatalf("second run: %v", err)
			}
			var repo string
			var logical uint64
			if err := conn.QueryRow(ctx,
				`SELECT any(repo), count() FROM repos FINAL WHERE org_id = ? AND provider = ?`,
				"org-inprocess", tc.provider).Scan(&repo, &logical); err != nil {
				t.Fatal(err)
			}
			if repo != tc.wantRepo || logical != 1 {
				t.Fatalf("repos FINAL: repo=%q rows=%d, want %q and one logical row", repo, logical, tc.wantRepo)
			}
		})
	}
}
