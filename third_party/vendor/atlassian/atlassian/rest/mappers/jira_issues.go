package mappers

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/rest/gen"
)

func requireNonEmptyString(value string, path string) (string, error) {
	clean := strings.TrimSpace(value)
	if clean == "" {
		return "", fmt.Errorf("%s is required", path)
	}
	return clean, nil
}

func requireStringField(obj map[string]any, key string, path string) (string, error) {
	raw, ok := obj[key]
	if !ok || raw == nil {
		return "", fmt.Errorf("%s.%s is required", path, key)
	}
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("%s.%s must be a string", path, key)
	}
	return requireNonEmptyString(s, path+"."+key)
}

func requireMapField(obj map[string]any, key string, path string) (map[string]any, error) {
	raw, ok := obj[key]
	if !ok || raw == nil {
		return nil, fmt.Errorf("%s.%s is required", path, key)
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s.%s must be an object", path, key)
	}
	return m, nil
}

func optionalStringField(obj map[string]any, key string) (*string, error) {
	raw, ok := obj[key]
	if !ok || raw == nil {
		return nil, nil
	}
	s, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("%s must be a string", key)
	}
	clean := strings.TrimSpace(s)
	if clean == "" {
		return nil, nil
	}
	return &clean, nil
}

func optionalUser(obj map[string]any, key string, path string) (*atlassian.JiraUser, error) {
	raw, ok := obj[key]
	if !ok || raw == nil {
		return nil, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s.%s must be an object", path, key)
	}
	accountID, err := requireStringField(m, "accountId", path+"."+key)
	if err != nil {
		return nil, err
	}
	displayName, err := requireStringField(m, "displayName", path+"."+key)
	if err != nil {
		return nil, err
	}
	email, err := optionalStringField(m, "emailAddress")
	if err != nil {
		return nil, fmt.Errorf("%s.%s.emailAddress: %w", path, key, err)
	}
	return &atlassian.JiraUser{
		AccountID:   accountID,
		DisplayName: displayName,
		Email:       email,
	}, nil
}

func parseStoryPoints(fields map[string]any, fieldName string) (*float64, error) {
	key := strings.TrimSpace(fieldName)
	if key == "" {
		return nil, nil
	}
	raw, ok := fields[key]
	if !ok || raw == nil {
		return nil, nil
	}
	if _, ok := raw.(bool); ok {
		return nil, fmt.Errorf("issue.fields.%s must be a number when present", key)
	}
	switch v := raw.(type) {
	case float64:
		return &v, nil
	case int:
		f := float64(v)
		return &f, nil
	case int64:
		f := float64(v)
		return &f, nil
	case string:
		clean := strings.TrimSpace(v)
		if clean == "" {
			return nil, nil
		}
		parsed, err := strconv.ParseFloat(clean, 64)
		if err != nil {
			return nil, fmt.Errorf("issue.fields.%s must be a number when present", key)
		}
		return &parsed, nil
	default:
		return nil, fmt.Errorf("issue.fields.%s must be a number when present", key)
	}
}

func coerceSprintID(value any, path string) (string, error) {
	if value == nil {
		return "", fmt.Errorf("%s is required", path)
	}
	switch v := value.(type) {
	case bool:
		return "", fmt.Errorf("%s must be a string or integer", path)
	case string:
		clean := strings.TrimSpace(v)
		if clean == "" {
			return "", fmt.Errorf("%s must be non-empty", path)
		}
		return clean, nil
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		if math.Trunc(v) != v {
			return "", fmt.Errorf("%s must be an integer", path)
		}
		return strconv.FormatInt(int64(v), 10), nil
	default:
		return "", fmt.Errorf("%s must be a string or integer", path)
	}
}

func parseSprintIDs(fields map[string]any, fieldName string) ([]string, error) {
	key := strings.TrimSpace(fieldName)
	if key == "" {
		return []string{}, nil
	}
	raw, ok := fields[key]
	if !ok || raw == nil {
		return []string{}, nil
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("issue.fields.%s must be a list when present", key)
	}
	out := make([]string, 0, len(arr))
	for idx, item := range arr {
		path := fmt.Sprintf("issue.fields.%s[%d]", key, idx)
		if obj, ok := item.(map[string]any); ok {
			idVal, exists := obj["id"]
			if !exists || idVal == nil {
				return nil, fmt.Errorf("%s.id is required", path)
			}
			coerced, err := coerceSprintID(idVal, path+".id")
			if err != nil {
				return nil, err
			}
			out = append(out, coerced)
			continue
		}
		coerced, err := coerceSprintID(item, path)
		if err != nil {
			return nil, err
		}
		out = append(out, coerced)
	}
	return out, nil
}

func JiraIssueFromREST(cloudID string, issue gen.IssueBean) (atlassian.JiraIssue, error) {
	return JiraIssueFromRESTWithFields(cloudID, issue, "", "")
}

func JiraIssueFromRESTWithFields(cloudID string, issue gen.IssueBean, storyPointsField string, sprintIDsField string) (atlassian.JiraIssue, error) {
	cloud := strings.TrimSpace(cloudID)
	if cloud == "" {
		return atlassian.JiraIssue{}, errors.New("cloudID is required")
	}
	if issue.Key == nil || strings.TrimSpace(*issue.Key) == "" {
		return atlassian.JiraIssue{}, errors.New("issue.key is required")
	}
	fields := issue.Fields
	if fields == nil {
		return atlassian.JiraIssue{}, errors.New("issue.fields is required")
	}

	issueKey := strings.TrimSpace(*issue.Key)

	projectObj, err := requireMapField(fields, "project", "issue.fields")
	if err != nil {
		return atlassian.JiraIssue{}, err
	}
	projectKey, err := requireStringField(projectObj, "key", "issue.fields.project")
	if err != nil {
		return atlassian.JiraIssue{}, err
	}

	issuetypeObj, err := requireMapField(fields, "issuetype", "issue.fields")
	if err != nil {
		return atlassian.JiraIssue{}, err
	}
	issueType, err := requireStringField(issuetypeObj, "name", "issue.fields.issuetype")
	if err != nil {
		return atlassian.JiraIssue{}, err
	}

	statusObj, err := requireMapField(fields, "status", "issue.fields")
	if err != nil {
		return atlassian.JiraIssue{}, err
	}
	status, err := requireStringField(statusObj, "name", "issue.fields.status")
	if err != nil {
		return atlassian.JiraIssue{}, err
	}

	createdAt, err := requireStringField(fields, "created", "issue.fields")
	if err != nil {
		return atlassian.JiraIssue{}, err
	}
	updatedAt, err := requireStringField(fields, "updated", "issue.fields")
	if err != nil {
		return atlassian.JiraIssue{}, err
	}

	var resolvedAt *string
	if raw, ok := fields["resolutiondate"]; ok && raw != nil {
		if s, ok := raw.(string); ok && strings.TrimSpace(s) != "" {
			clean := strings.TrimSpace(s)
			resolvedAt = &clean
		} else if !ok {
			return atlassian.JiraIssue{}, errors.New("issue.fields.resolutiondate must be a string when present")
		}
	}

	labels := []string{}
	if raw, ok := fields["labels"]; ok && raw != nil {
		arr, ok := raw.([]any)
		if !ok {
			return atlassian.JiraIssue{}, errors.New("issue.fields.labels must be a list when present")
		}
		for idx, item := range arr {
			s, ok := item.(string)
			if !ok || strings.TrimSpace(s) == "" {
				return atlassian.JiraIssue{}, fmt.Errorf("issue.fields.labels[%d] must be a non-empty string", idx)
			}
			labels = append(labels, strings.TrimSpace(s))
		}
	}

	components := []string{}
	if raw, ok := fields["components"]; ok && raw != nil {
		arr, ok := raw.([]any)
		if !ok {
			return atlassian.JiraIssue{}, errors.New("issue.fields.components must be a list when present")
		}
		for idx, item := range arr {
			obj, ok := item.(map[string]any)
			if !ok {
				return atlassian.JiraIssue{}, fmt.Errorf("issue.fields.components[%d] must be an object", idx)
			}
			name, err := requireStringField(obj, "name", fmt.Sprintf("issue.fields.components[%d]", idx))
			if err != nil {
				return atlassian.JiraIssue{}, err
			}
			components = append(components, name)
		}
	}

	assignee, err := optionalUser(fields, "assignee", "issue.fields")
	if err != nil {
		return atlassian.JiraIssue{}, err
	}
	reporter, err := optionalUser(fields, "reporter", "issue.fields")
	if err != nil {
		return atlassian.JiraIssue{}, err
	}

	storyPoints, err := parseStoryPoints(fields, storyPointsField)
	if err != nil {
		return atlassian.JiraIssue{}, err
	}
	sprintIDs, err := parseSprintIDs(fields, sprintIDsField)
	if err != nil {
		return atlassian.JiraIssue{}, err
	}

	return atlassian.JiraIssue{
		CloudID:     cloud,
		Key:         issueKey,
		ProjectKey:  projectKey,
		IssueType:   issueType,
		Status:      status,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		ResolvedAt:  resolvedAt,
		Assignee:    assignee,
		Reporter:    reporter,
		Labels:      labels,
		Components:  components,
		StoryPoints: storyPoints,
		SprintIDs:   sprintIDs,
	}, nil
}
