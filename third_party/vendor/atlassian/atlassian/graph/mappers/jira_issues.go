package mappers

import (
	"errors"
	"fmt"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/graph/gen"
)

func mapGraphUser(user *gen.JiraUser, path string) (*atlassian.JiraUser, error) {
	if user == nil {
		return nil, nil
	}
	accountID := strings.TrimSpace(user.AccountID)
	if accountID == "" {
		return nil, fmt.Errorf("%s.accountId is required", path)
	}
	displayName := strings.TrimSpace(user.Name)
	if displayName == "" {
		return nil, fmt.Errorf("%s.name is required", path)
	}
	return &atlassian.JiraUser{
		AccountID:   accountID,
		DisplayName: displayName,
	}, nil
}

func JiraIssueFromGraphQL(cloudID string, issue gen.JiraIssueNode) (atlassian.JiraIssue, error) {
	cloud := strings.TrimSpace(cloudID)
	if cloud == "" {
		return atlassian.JiraIssue{}, errors.New("cloudID is required")
	}
	issueKey := strings.TrimSpace(issue.Key)
	if issueKey == "" {
		return atlassian.JiraIssue{}, errors.New("issue.key is required")
	}
	projectKey := strings.TrimSpace(issue.ProjectField.Project.Key)
	if projectKey == "" {
		return atlassian.JiraIssue{}, errors.New("issue.projectField.project.key is required")
	}
	projectCloud := strings.TrimSpace(issue.ProjectField.Project.CloudID)
	if projectCloud == "" {
		return atlassian.JiraIssue{}, errors.New("issue.projectField.project.cloudId is required")
	}
	if projectCloud != cloud {
		return atlassian.JiraIssue{}, errors.New("issue.projectField.project.cloudId does not match cloudID")
	}
	issueType := strings.TrimSpace(issue.IssueType.Name)
	if issueType == "" {
		return atlassian.JiraIssue{}, errors.New("issue.issueType.name is required")
	}
	status := strings.TrimSpace(issue.Status.Name)
	if status == "" {
		return atlassian.JiraIssue{}, errors.New("issue.status.name is required")
	}
	if issue.CreatedField.DateTime == nil || strings.TrimSpace(*issue.CreatedField.DateTime) == "" {
		return atlassian.JiraIssue{}, errors.New("issue.createdField.dateTime is required")
	}
	if issue.UpdatedField.DateTime == nil || strings.TrimSpace(*issue.UpdatedField.DateTime) == "" {
		return atlassian.JiraIssue{}, errors.New("issue.updatedField.dateTime is required")
	}

	createdAt := strings.TrimSpace(*issue.CreatedField.DateTime)
	updatedAt := strings.TrimSpace(*issue.UpdatedField.DateTime)

	var resolvedAt *string
	if issue.ResolutionDateField != nil && issue.ResolutionDateField.DateTime != nil {
		if trimmed := strings.TrimSpace(*issue.ResolutionDateField.DateTime); trimmed != "" {
			resolvedAt = &trimmed
		}
	}

	var assignee *atlassian.JiraUser
	if issue.AssigneeField != nil {
		mapped, err := mapGraphUser(issue.AssigneeField.User, "issue.assigneeField.user")
		if err != nil {
			return atlassian.JiraIssue{}, err
		}
		assignee = mapped
	}
	reporter, err := mapGraphUser(issue.Reporter, "issue.reporter")
	if err != nil {
		return atlassian.JiraIssue{}, err
	}

	return atlassian.JiraIssue{
		CloudID:     projectCloud,
		Key:         issueKey,
		ProjectKey:  projectKey,
		IssueType:   issueType,
		Status:      status,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		ResolvedAt:  resolvedAt,
		Assignee:    assignee,
		Reporter:    reporter,
		Labels:      []string{},
		Components:  []string{},
		StoryPoints: nil,
		SprintIDs:   []string{},
	}, nil
}
