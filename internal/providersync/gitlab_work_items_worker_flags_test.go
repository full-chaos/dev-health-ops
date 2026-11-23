package providersync

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type gitLabWorkerFlagCase struct {
	env  string
	read func(GitLabWorkItemsRouteHandler) *bool
}

var gitLabWorkerFlagCases = []gitLabWorkerFlagCase{
	{"GITLAB_INCLUDE_MRS", func(h GitLabWorkItemsRouteHandler) *bool { return h.IncludeMRs }},
	{"GITLAB_FETCH_NOTES", func(h GitLabWorkItemsRouteHandler) *bool { return h.FetchComments }},
	{"GITLAB_FETCH_LINKS", func(h GitLabWorkItemsRouteHandler) *bool { return h.FetchLinks }},
	{"GITLAB_FETCH_MILESTONES", func(h GitLabWorkItemsRouteHandler) *bool { return h.FetchMilestones }},
}

func selectedGitLabWorkItemsHandler(t *testing.T) GitLabWorkItemsRouteHandler {
	t.Helper()
	handler, ok := SelectWorkItemsRouteHandler("gitlab", "work-items", WorkItemsRouteDeps{StatusMapping: loadRealStatusMapping(t)})
	if !ok {
		t.Fatal("no gitlab work-items route")
	}
	return handler.(GitLabWorkItemsRouteHandler)
}

func TestGitLabWorkerRouteReadsOffFlagsFromTheEnvironment(t *testing.T) {
	for _, tc := range gitLabWorkerFlagCases {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv(tc.env, "false")
			got := tc.read(selectedGitLabWorkItemsHandler(t))
			if got == nil || *got {
				t.Fatalf("%s=false: handler field = %v, want false", tc.env, got)
			}
		})
	}
}

func TestGitLabWorkerRouteFlagsUnsetKeepTheDefault(t *testing.T) {
	for _, tc := range gitLabWorkerFlagCases {
		t.Setenv(tc.env, "")
		if err := os.Unsetenv(tc.env); err != nil {
			t.Fatal(err)
		}
	}
	handler := selectedGitLabWorkItemsHandler(t)
	for _, tc := range gitLabWorkerFlagCases {
		if got := tc.read(handler); got != nil {
			t.Fatalf("%s unset: handler field = %v, want nil (default on)", tc.env, *got)
		}
	}
}

func collectSelectedGitLabWorkItems(t *testing.T) map[string]any {
	t.Helper()
	selected := selectedGitLabWorkItemsHandler(t)
	selected.RepoIdentity = nil
	claim := nativeTestClaim("gitlab", "work-items")
	client := gitLabWorkItemsClient(t, fakehttp.Client(&gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}))
	batch, err := selected.Collect(
		context.Background(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID}, client,
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return batch.Result
}

func TestGitLabWorkerRouteOutputFollowsTheFlags(t *testing.T) {
	for _, tc := range gitLabWorkerFlagCases {
		t.Setenv(tc.env, "")
		if err := os.Unsetenv(tc.env); err != nil {
			t.Fatal(err)
		}
	}
	base := collectSelectedGitLabWorkItems(t)
	if base["closing_reference_dependencies_synced"] != 1 || base["dependencies_synced"] != 3 ||
		base["interactions_synced"] == 0 || base["sprints_synced"] != 1 {
		t.Fatalf("baseline result = %v", base)
	}
	t.Run("GITLAB_INCLUDE_MRS", func(t *testing.T) {
		t.Setenv("GITLAB_INCLUDE_MRS", "false")
		got := collectSelectedGitLabWorkItems(t)
		if got["work_items_synced"] != base["work_items_synced"].(int)-1 {
			t.Fatalf("work_items_synced = %v, want one fewer than %v", got["work_items_synced"], base["work_items_synced"])
		}
	})
	t.Run("GITLAB_FETCH_NOTES", func(t *testing.T) {
		t.Setenv("GITLAB_FETCH_NOTES", "false")
		if got := collectSelectedGitLabWorkItems(t); got["interactions_synced"] != 0 {
			t.Fatalf("interactions_synced = %v, want 0", got["interactions_synced"])
		}
	})
	t.Run("GITLAB_FETCH_MILESTONES", func(t *testing.T) {
		t.Setenv("GITLAB_FETCH_MILESTONES", "false")
		if got := collectSelectedGitLabWorkItems(t); got["sprints_synced"] != 0 {
			t.Fatalf("sprints_synced = %v, want 0", got["sprints_synced"])
		}
	})
	t.Run("GITLAB_FETCH_LINKS", func(t *testing.T) {
		t.Setenv("GITLAB_FETCH_LINKS", "false")
		got := collectSelectedGitLabWorkItems(t)
		if got["dependencies_synced"] != 1 || got["closing_reference_dependencies_synced"] != 1 {
			t.Fatalf("FETCH_LINKS=false: dependencies=%v closing=%v, want only the closing reference (1/1)",
				got["dependencies_synced"], got["closing_reference_dependencies_synced"])
		}
	})
}
