package providersync

import "testing"

func TestMapGitLabTestsPipelineStatusSkippedStoredRaw(t *testing.T) {
	cases := map[string]string{
		"skipped":   "skipped",
		"canceled":  "cancelled",
		"cancelled": "cancelled",
		"failed":    "failure",
		"success":   "success",
		"manual":    "queued",
		"running":   "running",
	}
	for in, want := range cases {
		if got := mapGitLabTestsPipelineStatus(in); got != want {
			t.Errorf("mapGitLabTestsPipelineStatus(%q) = %q, want %q", in, got, want)
		}
	}
	if got, want := mapGitLabTestsPipelineStatus("skipped"), mapGitHubTestsStatus("completed", "skipped"); got != want {
		t.Errorf("gitlab skipped %q != github skipped %q", got, want)
	}
}
