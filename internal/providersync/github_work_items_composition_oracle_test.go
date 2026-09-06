package providersync

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// CHAOS-5321/CHAOS-3092 (R6): the multi-day differential pair for CHAOS-3494
// (#1541 review M8) -- githubDerivedMultiDayOracleCases,
// TestGitHubWorkItemTeamAttributionsMatchLivePythonProductionAcrossDays, and
// githubMultiDayOracleAttributions -- is DELETED. That pair proved Go
// bug-for-bug mirrored a Python write-amplification defect
// (compute_work_item_team_attributions called once per day inside job_work_
// items.py's day loop, rather than once per window); with that Python call
// site deleted (native Go executor + providersync ingest derivation own
// work_item_team_attributions now), there is no more defect to differentially
// prove Go mirrors -- its own oracle_pairs script asserted the defect's
// EXISTENCE in Python before comparing anything, which is now unrepresentable.
// Whether GitHubWorkItemDeriver's own multi-day re-derivation behavior is
// still worth pinning as a Go-only property (independent of Python) is a
// separate question, not resolved here -- CHAOS-3494's production-code
// comments (github_work_items_composition.go, github_work_item_derived_
// surfaces.go) are untouched.

// githubMultiDayOracleSource feeds loadGitHubWorkItemDerivationContext the same
// facts the Python pair reads, so neither side sees a fact set the other did
// not. Shared by other files' tests (jira/gitlab derived oracle tests, the
// engine-destinations oracle test) -- unrelated to the deletion above.
type githubMultiDayOracleSource struct {
	facts           teamattribution.GithubWorkItemDerivationFacts
	loads           int
	storedEdgeLoads int
}

func (source *githubMultiDayOracleSource) Load(
	context.Context, Claim, teamattribution.GithubWorkItemDerivationLoadRequest,
) (teamattribution.GithubWorkItemDerivationFacts, error) {
	source.loads++
	return source.facts, nil
}

// The oracle supplies every edge through the case input, so this source has no
// stored edges to add (CHAOS-3978). It counts the calls anyway, so callers
// that pin a once-per-window load can cover the stored-edge read too.
func (source *githubMultiDayOracleSource) LoadStoredInheritableEdges(
	context.Context, Claim, []string,
) ([]githubWorkItemDependencyRow, error) {
	source.storedEdgeLoads++
	return nil, nil
}
