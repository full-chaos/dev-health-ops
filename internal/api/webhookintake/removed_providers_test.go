package webhookintake

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestRoutesAreExactlyGitHubPagerDutyAndHealth(t *testing.T) {
	var got []string
	for _, route := range Routes(Deps{}) {
		got = append(got, route.Method+" "+route.Pattern)
	}
	sort.Strings(got)
	want := []string{
		"GET /api/v1/webhooks/health",
		"POST /api/v1/webhooks/github",
		"POST /api/v1/webhooks/pagerduty/{binding_id}",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("routes = %v; want %v", got, want)
	}
}

func TestProviderDoesNotNameRemovedRoutes(t *testing.T) {
	for _, path := range []string{"/api/v1/webhooks/gitlab", "/api/v1/webhooks/jira"} {
		if provider, ok := Provider(http.MethodPost, path); ok {
			t.Errorf("Provider(POST, %s) = %q; the route is gone", path, provider)
		}
	}
	for _, path := range []string{"/api/v1/webhooks/github", "/api/v1/webhooks/pagerduty/3f1e2b4c-0000-4000-8000-000000000000"} {
		if _, ok := Provider(http.MethodPost, path); !ok {
			t.Errorf("Provider(POST, %s) refused a kept route", path)
		}
	}
}

func TestHealthReportsOnlyKeptSecrets(t *testing.T) {
	recorder := httptest.NewRecorder()
	Deps{Secrets: Secrets{GitHub: "x"}}.handleHealth()(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/webhooks/health", nil))
	body := recorder.Body.String()
	for _, removed := range []string{`"gitlab"`, `"jira"`} {
		if strings.Contains(body, removed) {
			t.Errorf("health body %s names %s", body, removed)
		}
	}
	if !strings.Contains(body, `"github":true`) && !strings.Contains(body, `"github": true`) {
		t.Errorf("health body %s lacks github", body)
	}
}

// The two shared-secret env names must stay out of every non-test Go source file.
func TestNoSourceReadsTheRemovedSecretEnvNames(t *testing.T) {
	names := []string{"GITLAB_WEBHOOK" + "_TOKEN", "JIRA_WEBHOOK" + "_SECRET"}
	root := filepath.Join("..", "..", "..")
	scanned := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		scanned++
		for _, name := range names {
			if strings.Contains(string(data), name) {
				t.Errorf("%s names %s", path, name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 100 {
		t.Fatalf("scanned only %d Go files; the walk root is wrong", scanned)
	}
}
