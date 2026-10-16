package teamattribution

import "strings"

// ProjectOwnershipJoinID is the work-item project_id a team_project_ownership
// row joins on. A GitLab ownership row is keyed by the project's catalog id
// ({org}:gitlab:{n}, which a path never looks like: a GitLab path holds no
// ':'), while a GitLab work item still names its project by the path, which
// the row carries as project_key. Every other row joins on its project_id.
func ProjectOwnershipJoinID(provider, projectID string, projectKey *string) string {
	if provider == "gitlab" && strings.Contains(projectID, ":gitlab:") && projectKey != nil {
		if key := strings.TrimSpace(*projectKey); key != "" {
			return key
		}
	}
	return projectID
}
