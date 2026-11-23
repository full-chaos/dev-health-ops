package providersync

import (
	"os"
	"testing"
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
