package providersync

import "testing"

func TestGitHubTypeBugLabelForms(t *testing.T) {
	for _, label := range []string{"type:bug", "type: bug", "Type:Bug", "bug"} {
		t.Run(label, func(t *testing.T) {
			if got := githubIssueType([]string{label}); got != "bug" {
				t.Errorf("githubIssueType(%q) = %q, want bug", label, got)
			}
			if _, got := issueStatusAndType("open", false, []string{label}); got != "bug" {
				t.Errorf("issueStatusAndType(%q) type = %q, want bug", label, got)
			}
		})
	}
}
