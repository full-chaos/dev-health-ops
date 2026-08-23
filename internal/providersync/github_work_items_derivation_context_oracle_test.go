package providersync

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

type githubWorkItemDerivationOracleInput struct {
	Subject      githubWorkItemDerivationSubject
	WorkItems    []githubWorkItemDerivationSubject
	Dependencies []githubWorkItemDependencyRow
	Facts        githubWorkItemDerivationFacts
}

// githubWorkItemDerivationOracleCandidates transposes the complete ordered
// candidate list into one column per persisted provenance field. The generic
// oracle requires a top-level object; column slices preserve candidate order
// while the Python pair's reflected TeamAttributionCandidate field set keeps
// this shape exhaustive when production adds or removes a field.
type githubWorkItemDerivationOracleCandidates struct {
	Source      []string    `json:"source"`
	TeamID      []*string   `json:"team_id"`
	TeamName    []*string   `json:"team_name"`
	Confidence  []string    `json:"confidence"`
	Evidence    []string    `json:"evidence"`
	IsPrimary   []int       `json:"is_primary"`
	Specificity []int       `json:"specificity"`
	Priority    []int       `json:"priority"`
	UpdatedAt   []time.Time `json:"updated_at"`
}

func TestGitHubWorkItemDerivationMatchesLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/derivation-context",
		githubWorkItemDerivationOracleCases(),
		buildGitHubWorkItemDerivationOracleCandidates,
		nil,
	)
}

func buildGitHubWorkItemDerivationOracleCandidates(
	t *testing.T,
	input map[string]any,
) githubWorkItemDerivationOracleCandidates {
	t.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var decoded githubWorkItemDerivationOracleInput
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	derived := newGitHubWorkItemDerivationContext(decoded.Facts)
	subjects := make(map[string]githubWorkItemDerivationSubject, len(decoded.WorkItems))
	for _, subject := range decoded.WorkItems {
		subjects[subject.WorkItemID] = subject
	}
	derived.linkedIssue, _, _ = derived.buildLinkedIssueIndex(
		"github", subjects, decoded.Dependencies, nil,
	)
	_, _, candidates := derived.resolve(decoded.Subject)
	result := githubWorkItemDerivationOracleCandidates{
		Source: make([]string, 0, len(candidates)), TeamID: make([]*string, 0, len(candidates)),
		TeamName: make([]*string, 0, len(candidates)), Confidence: make([]string, 0, len(candidates)),
		Evidence: make([]string, 0, len(candidates)), IsPrimary: make([]int, 0, len(candidates)),
		Specificity: make([]int, 0, len(candidates)), Priority: make([]int, 0, len(candidates)),
		UpdatedAt: make([]time.Time, 0, len(candidates)),
	}
	for _, candidate := range candidates {
		result.Source = append(result.Source, candidate.Source)
		result.TeamID = append(result.TeamID, candidate.TeamID)
		result.TeamName = append(result.TeamName, candidate.TeamName)
		result.Confidence = append(result.Confidence, candidate.Confidence)
		result.Evidence = append(result.Evidence, candidate.Evidence)
		result.IsPrimary = append(result.IsPrimary, candidate.IsPrimary)
		result.Specificity = append(result.Specificity, candidate.Specificity)
		result.Priority = append(result.Priority, candidate.Priority)
		result.UpdatedAt = append(result.UpdatedAt, candidate.UpdatedAt)
	}
	if len(result.Source) == 0 {
		t.Fatal("derivation produced no attribution candidates")
	}
	return result
}

func TestGitHubWorkItemDerivationOracleRetainsNonPrimaryCandidates(t *testing.T) {
	var input map[string]any
	for _, testCase := range githubWorkItemDerivationOracleCases() {
		if testCase.ID == "repo_ownership_retains_lower_provenance" {
			input = testCase.Input
			break
		}
	}
	if input == nil {
		t.Fatal("full-provenance oracle case is missing")
	}
	got := buildGitHubWorkItemDerivationOracleCandidates(t, input)
	wantSources := []string{"repo_ownership", "assignee_membership", "manual_fallback"}
	wantPrimary := []int{1, 0, 0}
	if !reflect.DeepEqual(got.Source, wantSources) || !reflect.DeepEqual(got.IsPrimary, wantPrimary) {
		t.Fatalf("oracle candidates sources=%v primary=%v", got.Source, got.IsPrimary)
	}
}

func githubWorkItemDerivationOracleCases() []oracleCase {
	const (
		orgID  = "org-acme"
		repoID = "c7198fbc-1945-3717-05d8-eb78866b4e79"
		now    = "2026-08-04T12:00:00Z"
	)
	githubSubject := func(id string) map[string]any {
		return map[string]any{
			"WorkItemID": id, "Provider": "github", "RepoID": repoID,
			"NativeTeamKey": nil, "ProjectID": "acme/api", "OrgID": orgID,
			"Assignees": []any{},
		}
	}
	donor := func(id, provider, projectID string) map[string]any {
		return map[string]any{
			"WorkItemID": id, "Provider": provider, "ProjectID": projectID,
			"OrgID": orgID, "Assignees": []any{},
		}
	}
	projectFact := func(provider, projectID, teamID, teamName string) map[string]any {
		return map[string]any{
			"Provider": provider, "ProjectID": projectID, "TeamID": teamID,
			"TeamName": teamName, "IsPrimary": 1, "Specificity": 80,
			"Priority": 0, "UpdatedAt": now,
		}
	}
	dependency := func(source, target string) map[string]any {
		return map[string]any{
			"source_work_item_id": source, "target_work_item_id": target,
			"relationship_type":              "external_issue_key",
			"relationship_type_raw":          "closes",
			"relationship_semantics_version": "canonical-blocks.v2",
			"last_synced":                    now, "org_id": orgID,
		}
	}

	repoSubject := githubSubject("gh:acme/api#1")
	linkedSubject := githubSubject("gh:acme/api#2")
	ambiguousSubject := githubSubject("gh:acme/api#3")
	unlinkedSubject := githubSubject("gh:acme/api#4")
	manualSubject := githubSubject("gh:acme/api#5")
	linear42 := donor("linear:CHAOS-42", "linear", "linear-project")
	linear43 := donor("linear:CHAOS-43", "linear", "linear-project")
	jira43 := donor("jira:CHAOS-43", "jira", "jira-project")
	linear44 := donor("linear:CHAOS-44", "linear", "linear-project")

	return []oracleCase{
		{ID: "github_native_team_none_uses_repo_ownership", Input: map[string]any{
			"Subject":   repoSubject,
			"WorkItems": []any{repoSubject},
			"Facts": map[string]any{"Repos": []any{map[string]any{
				"Provider": "github", "TeamID": "team-repo", "TeamName": "Repository Team",
				"RepoID": repoID, "RepoFullName": "acme/api", "IsPrimary": 1,
				"Specificity": 70, "Priority": 0, "UpdatedAt": now,
			}}},
		}},
		{ID: "actual_extkey_link_inherits_linear_donor", Input: map[string]any{
			"Subject":      linkedSubject,
			"WorkItems":    []any{linkedSubject, linear42},
			"Dependencies": []any{dependency("gh:acme/api#2", "extkey:CHAOS-42")},
			"Facts": map[string]any{"Projects": []any{
				projectFact("linear", "linear-project", "team-linear", "Linear Team"),
			}},
		}},
		{ID: "repo_ownership_retains_lower_provenance", Input: map[string]any{
			"Subject": map[string]any{
				"WorkItemID": "gh:acme/api#provenance", "Provider": "github", "RepoID": repoID,
				"ProjectID": "acme/api", "OrgID": orgID, "Assignees": []any{"dev@example.com"},
			},
			"WorkItems": []any{map[string]any{
				"WorkItemID": "gh:acme/api#provenance", "Provider": "github", "RepoID": repoID,
				"ProjectID": "acme/api", "OrgID": orgID, "Assignees": []any{"dev@example.com"},
			}},
			"Facts": map[string]any{
				"Repos": []any{map[string]any{
					"Provider": "github", "TeamID": "team-repo", "TeamName": "Repository Team",
					"RepoID": repoID, "RepoFullName": "acme/api", "IsPrimary": 1,
					"Specificity": 70, "Priority": 0, "UpdatedAt": now,
				}},
				"Members": []any{map[string]any{
					"Provider": "github", "TeamID": "team-member", "TeamName": "Member Team",
					"MemberID": "dev@example.com", "IsPrimary": 1,
					"Specificity": 50, "Priority": 0, "UpdatedAt": now,
				}},
				"ManualFallbacks": []any{map[string]any{
					"Provider": "github", "ScopeType": "repo", "ScopeID": repoID,
					"TeamID": "team-manual", "TeamName": "Manual Team", "Reason": "explicit",
					"Priority": 0,
				}},
			},
		}},
		{ID: "ambiguous_linear_jira_key_is_unassigned", Input: map[string]any{
			"Subject":      ambiguousSubject,
			"WorkItems":    []any{ambiguousSubject, linear43, jira43},
			"Dependencies": []any{dependency("gh:acme/api#3", "extkey:CHAOS-43")},
			"Facts": map[string]any{"Projects": []any{
				projectFact("linear", "linear-project", "team-linear", "Linear Team"),
				projectFact("jira", "jira-project", "team-jira", "Jira Team"),
			}},
		}},
		{ID: "key_shaped_donor_without_edge_is_unassigned", Input: map[string]any{
			"Subject":   unlinkedSubject,
			"WorkItems": []any{unlinkedSubject, linear44},
			"Facts": map[string]any{"Projects": []any{
				projectFact("linear", "linear-project", "team-linear", "Linear Team"),
			}},
		}},
		{ID: "padded_manual_repo_scope_preserves_zero_priority", Input: map[string]any{
			"Subject":   manualSubject,
			"WorkItems": []any{manualSubject},
			"Facts": map[string]any{"ManualFallbacks": []any{map[string]any{
				"Provider": "github", "ScopeType": "repo", "ScopeID": "  " + repoID + "  ",
				"TeamID": "team-manual", "TeamName": "Manual Team", "Reason": "explicit",
				"Priority": 0,
			}}},
		}},
	}
}
