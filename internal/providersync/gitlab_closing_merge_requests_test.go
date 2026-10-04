package providersync

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func closingMRs(t *testing.T, raw string) []gitlabClosingMergeRequestPayload {
	t.Helper()
	var payloads []gitlabClosingMergeRequestPayload
	if err := json.Unmarshal([]byte(raw), &payloads); err != nil {
		t.Fatal(err)
	}
	return payloads
}

// CHAOS-8526: GitLab's closed_by response becomes MR-source / issue-target rows of raw kind gitlab_closing_reference,
// the orientation every other PRIMARY producer uses (the PR/MR is the edge SOURCE).
func TestNormalizeGitLabClosingMergeRequests(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "work-items")
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	issue := "gitlab:acme/api#42"
	rows := normalizeGitLabClosingMergeRequests(claim, issue, "acme/api", closingMRs(t, `[
		{"iid":9,"references":{"full":"acme/api!9"}},
		{"iid":9,"references":{"full":"acme/api!9"}},
		{"iid":3,"references":{"full":"other/fork!3"}},
		{"iid":4,"references":{"full":""}},
		{"iid":5,"references":{"full":"!5"}},
		{"iid":0,"references":{"full":"acme/api!0"}}
	]`), at)
	got := make([][2]string, 0, len(rows))
	for _, row := range rows {
		got = append(got, [2]string{row.SourceWorkItemID, row.TargetWorkItemID})
		if row.RelationshipType != "relates_to" || row.RelationshipTypeRaw != "gitlab_closing_reference" ||
			row.RelationshipSemanticsVersion != "canonical-blocks.v2" || row.OrgID != claim.OrgID || !row.LastSynced.Equal(at) {
			t.Fatalf("row=%+v", row)
		}
	}
	want := [][2]string{
		{"gitlab:acme/api!9", issue}, {"gitlab:other/fork!3", issue}, {"gitlab:acme/api!4", issue}, {"gitlab:acme/api!5", issue},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows=%v want=%v (duplicates and iid 0 dropped; a cross-project MR keeps its own path; a missing path falls back to the issue's project)", got, want)
	}
}

// The real producer's rows (not hand-authored ones) go through the real Derive: the issue and the MR it closes end up as
// one native link in work_graph_issue_pr, and an MR in another project resolves to ITS repo.
func TestGitLabClosingReferenceRowsBecomeNativeLinks(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "work-items")
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	issue := "gitlab:acme/api#42"
	rows := normalizeGitLabClosingMergeRequests(claim, issue, "acme/api", closingMRs(t,
		`[{"iid":9,"references":{"full":"acme/api!9"}},{"iid":3,"references":{"full":"other/fork!3"}}]`), at)
	apiRepo, forkRepo := uuid.MustParse("44444444-4444-4444-8444-444444444444"), uuid.MustParse("55555555-5555-4555-8555-555555555555")
	inputs := issueprlinks.Inputs{
		OrgID:        claim.OrgID,
		Repos:        []issueprlinks.RepoRow{{OrgID: claim.OrgID, ID: apiRepo, Repo: "acme/api"}, {OrgID: claim.OrgID, ID: forkRepo, Repo: "other/fork"}},
		PullRequests: []issueprlinks.PullRequestRow{{OrgID: claim.OrgID, RepoID: apiRepo, Number: 9}, {OrgID: claim.OrgID, RepoID: forkRepo, Number: 3}},
		WorkItems:    []issueprlinks.WorkItemRow{{OrgID: claim.OrgID, WorkItemID: issue}},
	}
	for _, row := range rows {
		inputs.Dependencies = append(inputs.Dependencies, issueprlinks.DependencyRow{
			OrgID: row.OrgID, SourceWorkItemID: row.SourceWorkItemID, TargetWorkItemID: row.TargetWorkItemID,
			RelationshipTypeRaw: row.RelationshipTypeRaw, LastSynced: row.LastSynced,
		})
	}
	result := issueprlinks.Derive(inputs)
	if !result.Balanced() || result.Written() != 2 {
		t.Fatalf("wrote %d links (rejections %v), want 2", result.Written(), result.Rejected)
	}
	got := map[uint32]uuid.UUID{}
	for _, link := range result.Links {
		if link.WorkItemID != issue || link.Provenance != issueprlinks.ProvenanceNative || link.Evidence != "gitlab_closing_reference" {
			t.Fatalf("link=%+v", link)
		}
		got[link.PRNumber] = link.RepoID
	}
	if got[9] != apiRepo || got[3] != forkRepo {
		t.Fatalf("links resolved to repos %v, want MR 9 -> %s and MR 3 -> %s", got, apiRepo, forkRepo)
	}
}

// The closed_by response carries MRs of every state (opened, merged, closed without merging). The GitHub linker
// (extractGitHubClosingIssueReferences) emits a row for every closingIssuesReferences entry whatever the PR's state, and
// so does this one: provider-agnostic parity, CHAOS-8526. The state is the provider's own, not a filter of ours.
func TestNormalizeGitLabClosingMergeRequestsKeepsEveryMergeRequestState(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "work-items")
	rows := normalizeGitLabClosingMergeRequests(claim, "gitlab:acme/api#42", "acme/api", closingMRs(t, `[
		{"iid":1,"state":"opened","references":{"full":"acme/api!1"}},
		{"iid":2,"state":"merged","references":{"full":"acme/api!2"}},
		{"iid":3,"state":"closed","references":{"full":"acme/api!3"}}
	]`), time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC))
	if len(rows) != 3 {
		t.Fatalf("rows=%d want 3 (open, merged and closed-unmerged all link, as on GitHub)", len(rows))
	}
}

// The closed_by fetch of one issue failing is not swallowed and does not poison the batch: it is logged with the issue,
// counted, the other rows are kept, and the watermark is held so the next run asks again.
func TestGitLabWorkItemsRouteClosedByFailureIsCountedAndHoldsTheWatermark(t *testing.T) {
	for name, withClosedBy := range map[string]bool{"closed_by answers": true, "closed_by fails": false} {
		t.Run(name, func(t *testing.T) {
			classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
			if err != nil {
				t.Fatal(err)
			}
			deriver := GitLabWorkItemDeriver{Source: &githubMultiDayOracleSource{}, statusMapping: loadRealStatusMapping(t), investmentClassifier: classifier}
			responses := gitLabWorkItemResponses()
			root := "/api/v4/projects/123"
			responses[root+"/merge_requests?page=1"] = []string{
				`[{"iid":9,"title":"Ship the API","description":"","state":"opened","created_at":"2026-07-04T09:00:00Z","updated_at":"2026-07-04T10:00:00Z","labels":["priority::low"],"assignees":[],"author":{"username":"alice","bot":false},"source_branch":"feature/ship-api"}]`, `[]`,
			}
			if !withClosedBy {
				delete(responses, root+"/issues/42/closed_by?page=1")
			}
			doer := &gitLabWorkItemsDoer{responses: responses}
			claim := nativeTestClaim("gitlab", "work-items")
			claim.OrgID = "77777777-7777-4777-8777-777777777777"
			batch, err := (GitLabWorkItemsRouteHandler{
				StatusMapping: loadRealStatusMapping(t), Derived: deriver, PerPage: 2, MaxPages: 10, NestedMaxPages: 10,
			}).Collect(context.Background(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
				gitLabWorkItemsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatalf("a closed_by failure must not fail the batch: %v", err)
			}
			failed, synced := batch.Result["closing_reference_fetch_failed"], batch.Result["closing_reference_dependencies_synced"]
			_, incomplete := batch.Result["incomplete"]
			if withClosedBy {
				if failed != 0 || synced != 1 || incomplete || batch.Watermark == nil {
					t.Fatalf("answering closed_by: failed=%v synced=%v incomplete=%v watermark=%v", failed, synced, incomplete, batch.Watermark)
				}
				return
			}
			if failed != 1 || synced != 0 || !incomplete || batch.Watermark != nil {
				t.Fatalf("failing closed_by: failed=%v synced=%v incomplete=%v watermark=%v (counted, loud, watermark held)", failed, synced, incomplete, batch.Watermark)
			}
			if got := batch.Result["incomplete"].([]string); len(got) != 1 || got[0] != "gitlab:acme/api#42" {
				t.Fatalf("incomplete=%v want the issue ref", got)
			}
		})
	}
}
