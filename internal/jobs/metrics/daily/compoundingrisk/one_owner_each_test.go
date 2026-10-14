package compoundingrisk

import "github.com/full-chaos/dev-health-ops/internal/teamownership"

// oneOwnerEach is a repo-to-owners map in which every repo has one main owner
// holding it whole, the shape every repo had before several owners shared one.
func oneOwnerEach(repoToTeam map[string]string) map[string][]teamownership.OwnerShare {
	out := make(map[string][]teamownership.OwnerShare, len(repoToTeam))
	for repoID, teamID := range repoToTeam {
		out[repoID] = []teamownership.OwnerShare{{TeamID: teamID, Main: true, Share: 1}}
	}
	return out
}
