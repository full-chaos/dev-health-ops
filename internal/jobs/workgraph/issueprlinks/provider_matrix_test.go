package issueprlinks

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
)

// CHAOS-8526: every tracker (github, gitlab, jira, linear) x every SCM the platform syncs (github PRs, gitlab MRs) yields a
// native link through that tracker's own provider-attached raw kind. Each row is a donor dependency row; Derive never
// invents a link from an id shape alone.
func TestDeriveProviderMatrixTrackersByPullRequestSources(t *testing.T) {
	githubRepo := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	gitlabRepo := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	trackers := []struct {
		provider string
		target   string
		raw      string
	}{
		{"github", "gh:acme/api#5", "github_closing_reference"},
		{"gitlab", "gitlab:grp/proj#5", "gitlab_closing_reference"},
		{"jira", "jira:OPS-1", "jira_dev_status"},
		{"linear", "linear:CHAOS-1", "linear_attachment"},
	}
	sources := []struct {
		provider string
		id       string
	}{
		{"github", "ghpr:acme/api#12"},
		{"gitlab", "gitlab:grp/proj!12"},
	}
	inputs := Inputs{
		OrgID: testOrg,
		Repos: []RepoRow{
			{OrgID: testOrg, ID: githubRepo, Repo: "acme/api"},
			{OrgID: testOrg, ID: gitlabRepo, Repo: "grp/proj"},
		},
		PullRequests: []PullRequestRow{
			{OrgID: testOrg, RepoID: githubRepo, Number: 12},
			{OrgID: testOrg, RepoID: gitlabRepo, Number: 12},
		},
	}
	for _, tracker := range trackers {
		inputs.WorkItems = append(inputs.WorkItems, WorkItemRow{OrgID: testOrg, WorkItemID: tracker.target})
		for _, source := range sources {
			inputs.Dependencies = append(inputs.Dependencies, DependencyRow{
				OrgID: testOrg, SourceWorkItemID: source.id, TargetWorkItemID: tracker.target,
				RelationshipTypeRaw: tracker.raw, LastSynced: testSynced,
			})
		}
	}
	result := Derive(inputs)
	if !result.Balanced() || result.Written() != len(trackers)*len(sources) {
		t.Fatalf("wrote %d links, want %d (rejections %v)", result.Written(), len(trackers)*len(sources), result.Rejected)
	}
	got := map[string]Link{}
	for _, link := range result.Links {
		got[fmt.Sprintf("%s|%d|%s", link.WorkItemID, link.PRNumber, link.RepoID)] = link
	}
	for _, tracker := range trackers {
		for _, source := range sources {
			repo := githubRepo
			if source.provider == "gitlab" {
				repo = gitlabRepo
			}
			link, ok := got[fmt.Sprintf("%s|12|%s", tracker.target, repo)]
			if !ok {
				t.Errorf("no native link for %s issue %s from the %s source %s", tracker.provider, tracker.target, source.provider, source.id)
				continue
			}
			if link.Provenance != ProvenanceNative || link.Evidence != tracker.raw {
				t.Errorf("%s x %s: provenance %q evidence %q, want native / %q", tracker.provider, source.provider, link.Provenance, link.Evidence, tracker.raw)
			}
		}
	}
	for _, kind := range []string{"github_closing_reference", "gitlab_closing_reference", "jira_dev_status", "linear_attachment"} {
		if result.AdmittedByRawKind[kind] != len(sources) {
			t.Errorf("admitted[%s] = %d, want %d", kind, result.AdmittedByRawKind[kind], len(sources))
		}
	}
}

// gitlab_closing_reference admits only a well-formed GitLab ISSUE id (gitlab:<path>#<n>) as its target, the way
// github_closing_reference admits only gh:<repo>#<n>: a merge-request id, a zero number, an empty path, another
// provider's id space and an external-key prefix are all refused, and none of them produces a link.
func TestDeriveGitlabClosingReferenceAdmitsOnlyAGitlabIssueTarget(t *testing.T) {
	for name, target := range map[string]string{
		"merge request id":           "gitlab:grp/proj!5",
		"zero number":                "gitlab:grp/proj#0",
		"leading zero":               "gitlab:grp/proj#05",
		"signed number":              "gitlab:grp/proj#+5",
		"empty path":                 "gitlab:#5",
		"no separator":               "gitlab:grp/proj",
		"github id space":            "gh:grp/proj#5",
		"jira id space":              "jira:OPS-1",
		"external issue key prefix":  "extkey:OPS-1",
		"mr id inside an issue path": "gitlab:grp/proj#5!7",
	} {
		t.Run(name, func(t *testing.T) {
			inputs := baseInputs()
			inputs.Dependencies[0].SourceWorkItemID = "gitlab:" + testSlug + "!12"
			inputs.Dependencies[0].RelationshipTypeRaw = "gitlab_closing_reference"
			inputs.Dependencies[0].TargetWorkItemID = target
			inputs.WorkItems = []WorkItemRow{{OrgID: testOrg, WorkItemID: target}}
			assertSingleRejection(t, Derive(inputs), ReasonNotAdmissible)
		})
	}
	// The well-formed shape, with a path that itself contains "#", is admitted.
	inputs := baseInputs()
	target := "gitlab:grp/pro#j#5"
	inputs.Dependencies[0].SourceWorkItemID = "gitlab:" + testSlug + "!12"
	inputs.Dependencies[0].RelationshipTypeRaw = "gitlab_closing_reference"
	inputs.Dependencies[0].TargetWorkItemID = target
	inputs.WorkItems = []WorkItemRow{{OrgID: testOrg, WorkItemID: target}}
	if result := Derive(inputs); result.Written() != 1 || result.Links[0].Evidence != "gitlab_closing_reference" {
		t.Fatalf("well-formed gitlab issue target not admitted: %+v", result)
	}
}

// An external issue-key PREFIX (extkey:) or a text reference is not a provider-attached link: with no donor row of an
// admitted raw kind there is no link, however well the MR and the issue exist (AGENTS.md: a link needs an actual linked
// issue donor row).
func TestDeriveWithoutADonorRowWritesNoLink(t *testing.T) {
	for name, raw := range map[string]string{
		"external issue key":    "external_issue_key",
		"description reference": "description_reference",
		"blocks relation":       "blocks",
	} {
		t.Run(name, func(t *testing.T) {
			inputs := baseInputs()
			inputs.Dependencies[0].SourceWorkItemID = "gitlab:" + testSlug + "!12"
			inputs.Dependencies[0].RelationshipTypeRaw = raw
			inputs.Dependencies[0].TargetWorkItemID = "gitlab:" + testSlug + "#5"
			inputs.WorkItems = []WorkItemRow{{OrgID: testOrg, WorkItemID: "gitlab:" + testSlug + "#5"}}
			assertSingleRejection(t, Derive(inputs), ReasonNotAdmissible)
		})
	}
	inputs := baseInputs()
	inputs.Dependencies = nil
	if result := Derive(inputs); result.Written() != 0 || !result.Balanced() {
		t.Fatalf("no dependency rows must yield no links: %+v", result)
	}
}
