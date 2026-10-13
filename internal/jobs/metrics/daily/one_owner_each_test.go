package daily

// oneOwnerEach is a repo-to-owners map in which every repo has one owner, the
// shape every repo had before a full ownership tie named several (D5990).
func oneOwnerEach(repoToTeam map[string]string) map[string][]string {
	out := make(map[string][]string, len(repoToTeam))
	for repoID, teamID := range repoToTeam {
		out[repoID] = []string{teamID}
	}
	return out
}
