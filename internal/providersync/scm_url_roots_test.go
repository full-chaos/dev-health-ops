package providersync

import (
	"strings"
	"testing"
)

func TestParseGitLabReferenceNeedsNamespaceAndProject(t *testing.T) {
	t.Parallel()
	for _, full := range []string{"api!9", "!9", "/api!9", "api/!9"} {
		if _, err := parseGitLabReference(full, gitlabMergeRequestMarker); err == nil {
			t.Errorf("%q was accepted, want a path reject", full)
		}
	}
	if _, err := parseGitLabReference("acme/api!9", gitlabMergeRequestMarker); err != nil {
		t.Errorf("acme/api!9 rejected: %v", err)
	}
	if _, err := parseGitLabReference("api#3", gitlabIssueMarker); err == nil {
		t.Error("api#3 was accepted")
	}
}

func TestSeveralURLRootsPerHostBothResolve(t *testing.T) {
	t.Setenv("JIRA_TRUSTED_SCM_HOSTS", "git.example.com/gitlab,git.example.com/scm")
	t.Setenv("LINEAR_TRUSTED_SCM_HOSTS", "git.example.com/gitlab,git.example.com/scm")
	for name, c := range map[string]struct {
		url      string
		want     string
		rejected bool
	}{
		"first root":            {"https://git.example.com/gitlab/acme/api/-/merge_requests/9", "gitlab:acme/api!9", false},
		"second root":           {"https://git.example.com/scm/acme/api/-/merge_requests/9", "gitlab:acme/api!9", false},
		"under no root":         {"https://git.example.com/other/acme/api/-/merge_requests/9", "", true},
		"no root, pull request": {"https://git.example.com/acme/api/pull/9", "", true},
		"no root, not a change": {"https://git.example.com/acme/api/-/issues/9", "", false},
		"one segment project":   {"https://git.example.com/gitlab/api/-/merge_requests/9", "", true},
	} {
		jiraSource, jiraRejected := jiraDevStatusPullRequestSource(c.url)
		if jiraSource != c.want || jiraRejected != c.rejected {
			t.Errorf("jira %s: %q rejected=%v want %q %v", name, jiraSource, jiraRejected, c.want, c.rejected)
		}
		sourceType := "gitlab"
		if strings.Contains(c.url, "/pull/") {
			sourceType = "github"
		}
		linearSource, linearRejected := linearAttachmentSource(linearAttachmentPayload{SourceType: sourceType, URL: c.url})
		if linearSource != c.want || linearRejected != c.rejected {
			t.Errorf("linear %s: %q rejected=%v want %q %v", name, linearSource, linearRejected, c.want, c.rejected)
		}
	}
}

func TestNestedURLRootsTryTheLongestFirst(t *testing.T) {
	t.Setenv("JIRA_TRUSTED_SCM_HOSTS", "git.example.com/a,git.example.com/a/b")
	got, _ := jiraDevStatusPullRequestSource("https://git.example.com/a/b/acme/api/-/merge_requests/9")
	if got != "gitlab:acme/api!9" {
		t.Fatalf("longest root not taken: %q", got)
	}
	got, _ = jiraDevStatusPullRequestSource("https://git.example.com/a/acme/api/-/merge_requests/9")
	if got != "gitlab:acme/api!9" {
		t.Fatalf("shorter root not taken: %q", got)
	}
}

func TestBareHostEntryBesideRootsKeepsTheBareHostInEitherOrder(t *testing.T) {
	for _, setting := range []string{"git.example.com,git.example.com/gitlab", "git.example.com/gitlab,git.example.com"} {
		t.Setenv("JIRA_TRUSTED_SCM_HOSTS", setting)
		t.Setenv("LINEAR_TRUSTED_SCM_HOSTS", setting)
		for name, c := range map[string]struct{ url, want string }{
			"rooted": {"https://git.example.com/gitlab/acme/api/-/merge_requests/9", "gitlab:acme/api!9"},
			"bare":   {"https://git.example.com/acme/api/-/merge_requests/9", "gitlab:acme/api!9"},
		} {
			if got, rejected := jiraDevStatusPullRequestSource(c.url); got != c.want || rejected {
				t.Errorf("jira %q %s: %q rejected=%v", setting, name, got, rejected)
			}
			if got, rejected := linearAttachmentSource(linearAttachmentPayload{SourceType: "gitlab", URL: c.url}); got != c.want || rejected {
				t.Errorf("linear %q %s: %q rejected=%v", setting, name, got, rejected)
			}
		}
	}
}
