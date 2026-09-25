package mappers

import (
	"errors"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/graph/gen"
)

func ProjectWithOpsgenieTeams(cloudID string, project gen.JiraProjectNode, teams []gen.OpsgenieTeamNode) (atlassian.CanonicalProjectWithOpsgenieTeams, error) {
	cloud := strings.TrimSpace(cloudID)
	if cloud == "" {
		return atlassian.CanonicalProjectWithOpsgenieTeams{}, errors.New("cloudID is required")
	}

	key := strings.TrimSpace(project.Key)
	if key == "" {
		return atlassian.CanonicalProjectWithOpsgenieTeams{}, errors.New("project.key is required")
	}
	name := strings.TrimSpace(project.Name)
	if name == "" {
		return atlassian.CanonicalProjectWithOpsgenieTeams{}, errors.New("project.name is required")
	}

	seen := map[string]struct{}{}
	outTeams := make([]atlassian.OpsgenieTeamRef, 0, len(teams))
	for _, t := range teams {
		id := strings.TrimSpace(t.ID)
		if id == "" {
			return atlassian.CanonicalProjectWithOpsgenieTeams{}, errors.New("opsgenie_team.id is required")
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}

		teamName := strings.TrimSpace(t.Name)
		if teamName == "" {
			return atlassian.CanonicalProjectWithOpsgenieTeams{}, errors.New("opsgenie_team.name is required")
		}
		outTeams = append(outTeams, atlassian.OpsgenieTeamRef{ID: id, Name: teamName})
	}

	return atlassian.CanonicalProjectWithOpsgenieTeams{
		Project: atlassian.JiraProject{
			CloudID: cloud,
			Key:     key,
			Name:    name,
		},
		OpsgenieTeams: outTeams,
	}, nil
}
