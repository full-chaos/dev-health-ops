package daily

import "github.com/full-chaos/dev-health-ops/internal/teamownership"

// oneOwnerEach is a repo-to-owners map in which every repo has one owner, the
// shape every repo had before a full ownership tie named several (D5990).
func oneOwnerEach(repoToTeam map[string]string) map[string][]string {
	out := make(map[string][]string, len(repoToTeam))
	for repoID, teamID := range repoToTeam {
		out[repoID] = []string{teamID}
	}
	return out
}

// soleOwnerShares is oneOwnerEach in the owner-shares shape: one main owner
// holding the repository whole.
func soleOwnerShares(repoToTeam map[string]string) map[string][]teamownership.OwnerShare {
	out := make(map[string][]teamownership.OwnerShare, len(repoToTeam))
	for repoID, teamID := range repoToTeam {
		out[repoID] = []teamownership.OwnerShare{{TeamID: teamID, Main: true, Share: 1}}
	}
	return out
}
