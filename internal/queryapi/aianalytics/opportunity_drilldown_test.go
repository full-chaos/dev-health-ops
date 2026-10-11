package aianalytics

import (
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-9225: an evidence ref is a pull request drilldown only when it is
// git_pull_requests:<repository uuid>:<number>. The toil rules' METRIC refs
// (git_pull_requests:migration_prs:<repository uuid>) have the same three-part
// shape and were served as a pull request named "PR <uuid>" with the root
// "migration_prs#<uuid>" (prod, rev 270). The root id of a real pull request is
// the id the edge tables store: "<repository uuid>:<number>", never "#".
func TestOpportunityDrilldownsAreOnlyPullRequestRefs(t *testing.T) {
	const repo = "33333333-3333-4333-8333-333333333333"
	for name, tc := range map[string]struct {
		refs []string
		want []model.AIWorkGraphDrilldownRef
	}{
		"a pull request ref": {
			refs: []string{"git_pull_requests:" + repo + ":7"},
			want: []model.AIWorkGraphDrilldownRef{{RootType: "pr", RootID: repo + ":7", Label: "PR 7"}},
		},
		"a metric ref is a key, never a drilldown": {
			refs: []string{"git_pull_requests:" + repo + ":7", "git_pull_requests:migration_prs:" + repo},
			want: []model.AIWorkGraphDrilldownRef{{RootType: "pr", RootID: repo + ":7", Label: "PR 7"}},
		},
		"the dependency metric ref": {
			refs: []string{"git_pull_requests:dependency_update_prs:" + repo},
			want: []model.AIWorkGraphDrilldownRef{},
		},
		"other tables are not pull requests": {
			refs: []string{"ai_impact_metrics_daily:test_gap_rate:" + repo},
			want: []model.AIWorkGraphDrilldownRef{},
		},
		"a number that is not digits": {
			refs: []string{"git_pull_requests:" + repo + ":7x", "git_pull_requests:" + repo + ":", "git_pull_requests:" + repo + ":-1"},
			want: []model.AIWorkGraphDrilldownRef{},
		},
		"a repository that is not a uuid": {
			refs: []string{"git_pull_requests:acme/api:7", "git_pull_requests::7"},
			want: []model.AIWorkGraphDrilldownRef{},
		},
		"too many parts": {
			refs: []string{"git_pull_requests:" + repo + ":7:8"},
			want: []model.AIWorkGraphDrilldownRef{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := opportunity{repoID: repo, refs: tc.refs}.project(nil, nil).WorkGraphDrilldowns
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("drilldowns = %+v, want %+v", got, tc.want)
			}
		})
	}
}
