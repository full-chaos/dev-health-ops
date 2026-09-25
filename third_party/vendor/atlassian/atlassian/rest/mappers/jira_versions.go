package mappers

import (
	"atlassian/atlassian"
	"atlassian/atlassian/rest/gen"
)

func MapRESTVersion(projectKey string, v gen.Version) atlassian.JiraVersion {
	id := ""
	if v.ID != nil {
		id = *v.ID
	}
	name := ""
	if v.Name != nil {
		name = *v.Name
	}
	released := false
	if v.Released != nil {
		released = *v.Released
	}
	var releaseDate *string
	if v.ReleaseDate != nil {
		releaseDate = v.ReleaseDate
	}

	return atlassian.JiraVersion{
		ID:          id,
		Name:        name,
		ProjectKey:  projectKey,
		Released:    released,
		ReleaseDate: releaseDate,
	}
}
