package mappers

import (
	"errors"
	"fmt"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/graph/gen"
)

func mapWorklogUser(user *gen.WorklogUser, path string) (*atlassian.JiraUser, error) {
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

func JiraWorklogFromGraphQL(issueKey string, worklog gen.JiraWorklogNode) (atlassian.JiraWorklog, error) {
	issue := strings.TrimSpace(issueKey)
	if issue == "" {
		return atlassian.JiraWorklog{}, errors.New("issueKey is required")
	}
	worklogID := strings.TrimSpace(worklog.WorklogID)
	if worklogID == "" {
		return atlassian.JiraWorklog{}, errors.New("worklog.worklogId is required")
	}
	if strings.TrimSpace(worklog.Created) == "" {
		return atlassian.JiraWorklog{}, errors.New("worklog.created is required")
	}
	if worklog.Updated == nil || strings.TrimSpace(*worklog.Updated) == "" {
		return atlassian.JiraWorklog{}, errors.New("worklog.updated is required")
	}
	if worklog.StartDate == nil || strings.TrimSpace(*worklog.StartDate) == "" {
		return atlassian.JiraWorklog{}, errors.New("worklog.startDate is required")
	}
	if worklog.TimeSpent.TimeInSeconds == nil || *worklog.TimeSpent.TimeInSeconds < 0 {
		return atlassian.JiraWorklog{}, errors.New("worklog.timeSpent.timeInSeconds is required and must be >= 0")
	}

	author, err := mapWorklogUser(worklog.Author, "worklog.author")
	if err != nil {
		return atlassian.JiraWorklog{}, err
	}

	started := strings.TrimSpace(*worklog.StartDate)
	updated := strings.TrimSpace(*worklog.Updated)
	created := strings.TrimSpace(worklog.Created)
	timeSpent := *worklog.TimeSpent.TimeInSeconds
	if timeSpent < 0 {
		return atlassian.JiraWorklog{}, fmt.Errorf("worklog.timeSpent.timeInSeconds must be >= 0")
	}

	return atlassian.JiraWorklog{
		IssueKey:         issue,
		WorklogID:        worklogID,
		Author:           author,
		StartedAt:        started,
		TimeSpentSeconds: timeSpent,
		CreatedAt:        created,
		UpdatedAt:        updated,
	}, nil
}
