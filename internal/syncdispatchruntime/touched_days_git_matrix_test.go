package syncdispatchruntime

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// The fan-out records the days of the commits and pull requests of a run
// (CHAOS-9169) exactly when a successful unit of the run has the git or the prs
// target, for every provider of the matrix and every dataset it has: the answer
// is read from the dataset capability, never from a provider name. github and
// gitlab are the providers whose datasets write commits and pull requests; the
// others write none, so a jira, linear, launchdarkly or pagerduty run records
// no commit day.
func TestWritesGitRowsFollowsTheDatasetCapabilityForEveryProvider(t *testing.T) {
	writers := map[string]bool{}
	for _, provider := range providersync.MatrixProviders() {
		datasets := providersync.Capabilities(provider)
		if len(datasets) == 0 {
			t.Fatalf("provider %s has no dataset: the matrix would be empty", provider)
		}
		for _, capability := range datasets {
			targets := map[string]struct{}{}
			wantGit := false
			for _, target := range capability.LegacyTargets {
				targets[target] = struct{}{}
				if target == "git" || target == "prs" {
					wantGit = true
				}
			}
			if got := writesGitRows(targets); got != wantGit {
				t.Errorf("%s/%s: writesGitRows = %t, want %t (targets %v)", provider, capability.Dataset, got, wantGit, capability.LegacyTargets)
			}
			if wantGit {
				writers[provider] = true
			}
		}
	}
	for _, provider := range []string{"github", "gitlab"} {
		if !writers[provider] {
			t.Errorf("provider %s has no dataset with a git or prs target: the matrix does not reach it", provider)
		}
	}
	for _, provider := range []string{"jira", "linear", "launchdarkly", "pagerduty"} {
		if writers[provider] {
			t.Errorf("provider %s has a dataset with a git or prs target; its rows are not commits or pull requests", provider)
		}
	}
	// Several datasets: one unit with the target is enough.
	if !writesGitRows(map[string]struct{}{"work-items": {}, "prs": {}}) || writesGitRows(map[string]struct{}{"work-items": {}, "cicd": {}}) {
		t.Error("writesGitRows must be true for a mix that holds prs and false for one that holds neither git nor prs")
	}
}
