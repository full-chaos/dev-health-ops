package providersync

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

// TestListInProcessUsesTheRunsCredentialAndBaseURL: the batch lists with the
// same token and base URL its per-repository runs use, GitHub and GitLab.
func TestListInProcessUsesTheRunsCredentialAndBaseURL(t *testing.T) {
	var seen []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.Path+"|"+r.Header.Get("Authorization")+"|"+r.Header.Get("PRIVATE-TOKEN"))
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v3/orgs/acme/repos":
			_, _ = w.Write([]byte(`[{"name":"api","full_name":"acme/api"},{"name":"other","full_name":"acme/other"}]`))
		case "/api/v4/groups/acme/projects":
			_, _ = w.Write([]byte(`[{"id":7,"name":"api","path_with_namespace":"acme/api"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	gh, err := ListInProcess(context.Background(), InProcessRun{
		Provider: "github", Credential: map[string]string{"token": "ghp-x"},
		Config: map[string]string{"base_url": server.URL + "/api/v3"}, Doer: server.Client(),
	}, InProcessListing{GitHub: &GitHubListing{Org: "acme", Pattern: "acme/api"}})
	if err != nil {
		t.Fatalf("github: %v", err)
	}
	if want := []ListedRepository{{Name: "api", FullName: "acme/api"}}; !reflect.DeepEqual(gh, want) {
		t.Fatalf("github = %+v, want %+v", gh, want)
	}

	gl, err := ListInProcess(context.Background(), InProcessRun{
		Provider: "gitlab", Credential: map[string]string{"token": "glpat-x"},
		Config: map[string]string{"base_url": server.URL + "/"}, Doer: server.Client(),
	}, InProcessListing{GitLab: &GitLabListing{Group: "acme"}})
	if err != nil {
		t.Fatalf("gitlab: %v", err)
	}
	if want := []ListedRepository{{Name: "api", FullName: "acme/api", ProjectID: 7}}; !reflect.DeepEqual(gl, want) {
		t.Fatalf("gitlab = %+v, want %+v", gl, want)
	}
	if want := []string{"/api/v3/orgs/acme/repos|token ghp-x|", "/api/v4/groups/acme/projects||glpat-x"}; !reflect.DeepEqual(seen, want) {
		t.Fatalf("requests = %q, want %q", seen, want)
	}
}

func TestListInProcessRefusesWhatItCannotList(t *testing.T) {
	if _, err := ListInProcess(context.Background(), InProcessRun{Provider: "github"}, InProcessListing{GitHub: &GitHubListing{}}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("no credential: %v", err)
	}
	run := InProcessRun{Provider: "github", Credential: map[string]string{"token": "t"}}
	if _, err := ListInProcess(context.Background(), run, InProcessListing{GitLab: &GitLabListing{}}); !errors.Is(err, ErrNotAGitFamilyRoute) {
		t.Fatalf("a gitlab listing for a github run: %v", err)
	}
	if _, err := ListInProcess(context.Background(), InProcessRun{Provider: "github", Credential: map[string]string{"bogus": "t"}}, InProcessListing{GitHub: &GitHubListing{}}); err == nil {
		t.Fatal("a credential of no known shape must be refused")
	}
}
