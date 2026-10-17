package providersync

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func gitlabScopeOfIssue42(t *testing.T, reader GitLabRepoIdentityReader, nativeID, path string) (workItemID, scope string) {
	t.Helper()
	responses := map[string][]string{}
	for key, values := range gitLabWorkItemResponses() {
		key = strings.Replace(key, "/api/v4/projects/123", "/api/v4/projects/"+nativeID, 1)
		for index := range values {
			values[index] = strings.ReplaceAll(values[index], "acme/api", path)
		}
		responses[key] = values
	}
	responses["/api/v4/projects/"+nativeID] = []string{`{"id":` + nativeID + `,"path":"x","path_with_namespace":"` + path + `","name":"x"}`}
	claim := nativeTestClaim("gitlab", "work-items")
	claim.SourceExternalID, claim.SourceName = nativeID, path
	handler := GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 2, MaxPages: 10, NestedMaxPages: 10, RepoIdentity: reader}
	batch, err := handler.Collect(t.Context(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, fakehttp.Client(&gitLabWorkItemsDoer{responses: responses})), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, effect := range batch.Effects {
		if effect.Destination != "work_items" {
			continue
		}
		for _, raw := range effect.Rows {
			var row gitlabWorkItemRow
			if err := jsonUnmarshalEffect(raw, &row); err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(row.WorkItemID, "#42") {
				return row.WorkItemID, workitemmetrics.WorkScopeID(row.Provider, deref(row.ProjectKey), deref(row.ProjectID), "", "")
			}
		}
	}
	t.Fatal("issue #42 not written")
	return "", ""
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func TestGitLabRenameBetweenTwoRunsMetricScope(t *testing.T) {
	reader := &fakeGitLabRepoIdentityReader{}
	idBefore, scopeBefore := gitlabScopeOfIssue42(t, reader, "123", "acme/api")
	reader.rows = []GitLabStoredRepoIdentity{{ID: mustRepositoryIdentity(t, "acme/api"), Repo: "acme/api", IdentityPath: "acme/api", ProviderRepoID: "123"}}
	idAfter, scopeAfter := gitlabScopeOfIssue42(t, reader, "123", "platform/api-v2")
	t.Logf("before rename: work_item_id=%s work_scope_id=%s", idBefore, scopeBefore)
	t.Logf("after rename:  work_item_id=%s work_scope_id=%s", idAfter, scopeAfter)
	if idBefore != idAfter {
		t.Fatalf("work item id changed across the rename: %s -> %s", idBefore, idAfter)
	}
	if scopeBefore != scopeAfter {
		t.Errorf("SPLIT: one work item, two metric scopes: %q then %q", scopeBefore, scopeAfter)
	}
}

// Path reuse: project 456 takes acme/api after 123 left it. It gets its own
// series, not 123's.
func TestGitLabPathReuseGetsItsOwnMetricScope(t *testing.T) {
	reader := &fakeGitLabRepoIdentityReader{rows: []GitLabStoredRepoIdentity{
		{ID: mustRepositoryIdentity(t, "acme/api"), Repo: "platform/api-v2", IdentityPath: "acme/api", ProviderRepoID: "123"},
	}}
	_, oldScope := gitlabScopeOfIssue42(t, reader, "123", "platform/api-v2")
	newID, newScope := gitlabScopeOfIssue42(t, reader, "456", "acme/api")
	t.Logf("project 123: work_scope_id=%s; project 456: work_item_id=%s work_scope_id=%s", oldScope, newID, newScope)
	if oldScope != "acme/api" || newScope != "acme/api@456" {
		t.Fatalf("scopes = %q and %q, want acme/api and acme/api@456", oldScope, newScope)
	}
}
