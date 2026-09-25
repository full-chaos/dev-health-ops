package mappers

import (
	"errors"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/rest/gen"
)

func normalizeProjectTypeKey(value string) string {
	clean := strings.TrimSpace(value)
	clean = strings.ReplaceAll(clean, "-", "_")
	clean = strings.ReplaceAll(clean, " ", "_")
	return strings.ToUpper(clean)
}

func JiraProjectFromREST(cloudID string, project map[string]any) (atlassian.JiraProject, error) {
	cloud := strings.TrimSpace(cloudID)
	if cloud == "" {
		return atlassian.JiraProject{}, errors.New("cloudID is required")
	}
	if project == nil {
		return atlassian.JiraProject{}, errors.New("project is required")
	}

	keyRaw, ok := project["key"]
	if !ok {
		return atlassian.JiraProject{}, errors.New("project.key is required")
	}
	key, ok := keyRaw.(string)
	if !ok || strings.TrimSpace(key) == "" {
		return atlassian.JiraProject{}, errors.New("project.key is required")
	}

	nameRaw, ok := project["name"]
	if !ok {
		return atlassian.JiraProject{}, errors.New("project.name is required")
	}
	name, ok := nameRaw.(string)
	if !ok || strings.TrimSpace(name) == "" {
		return atlassian.JiraProject{}, errors.New("project.name is required")
	}

	var projectType *string
	if raw, ok := project["projectTypeKey"]; ok {
		if s, ok := raw.(string); ok && strings.TrimSpace(s) != "" {
			normalized := normalizeProjectTypeKey(s)
			projectType = &normalized
		}
	}

	return atlassian.JiraProject{
		CloudID: cloud,
		Key:     strings.TrimSpace(key),
		Name:    strings.TrimSpace(name),
		Type:    projectType,
	}, nil
}

func JiraProjectFromRESTProject(cloudID string, project gen.Project) (atlassian.JiraProject, error) {
	cloud := strings.TrimSpace(cloudID)
	if cloud == "" {
		return atlassian.JiraProject{}, errors.New("cloudID is required")
	}

	key := strings.TrimSpace(project.Key)
	if key == "" {
		return atlassian.JiraProject{}, errors.New("project.key is required")
	}
	name := strings.TrimSpace(project.Name)
	if name == "" {
		return atlassian.JiraProject{}, errors.New("project.name is required")
	}

	var projectType *string
	if project.ProjectTypeKey != nil && strings.TrimSpace(*project.ProjectTypeKey) != "" {
		normalized := normalizeProjectTypeKey(*project.ProjectTypeKey)
		projectType = &normalized
	}

	return atlassian.JiraProject{
		CloudID: cloud,
		Key:     key,
		Name:    name,
		Type:    projectType,
	}, nil
}
