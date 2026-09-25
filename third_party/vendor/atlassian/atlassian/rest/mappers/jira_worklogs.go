package mappers

import (
	"errors"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/rest/gen"
)

func JiraWorklogFromREST(issueKey string, worklog gen.Worklog) (atlassian.JiraWorklog, error) {
	issue := strings.TrimSpace(issueKey)
	if issue == "" {
		return atlassian.JiraWorklog{}, errors.New("issueKey is required")
	}
	if worklog.ID == nil || strings.TrimSpace(*worklog.ID) == "" {
		return atlassian.JiraWorklog{}, errors.New("worklog.id is required")
	}
	if worklog.Started == nil || strings.TrimSpace(*worklog.Started) == "" {
		return atlassian.JiraWorklog{}, errors.New("worklog.started is required")
	}
	if worklog.TimeSpentSeconds == nil || *worklog.TimeSpentSeconds < 0 {
		return atlassian.JiraWorklog{}, errors.New("worklog.timeSpentSeconds is required and must be >= 0")
	}
	if worklog.Created == nil || strings.TrimSpace(*worklog.Created) == "" {
		return atlassian.JiraWorklog{}, errors.New("worklog.created is required")
	}
	if worklog.Updated == nil || strings.TrimSpace(*worklog.Updated) == "" {
		return atlassian.JiraWorklog{}, errors.New("worklog.updated is required")
	}

	var author *atlassian.JiraUser
	if worklog.Author != nil {
		if worklog.Author.AccountID == nil || strings.TrimSpace(*worklog.Author.AccountID) == "" {
			return atlassian.JiraWorklog{}, errors.New("worklog.author.accountId is required")
		}
		if worklog.Author.DisplayName == nil || strings.TrimSpace(*worklog.Author.DisplayName) == "" {
			return atlassian.JiraWorklog{}, errors.New("worklog.author.displayName is required")
		}
		u := atlassian.JiraUser{
			AccountID:   strings.TrimSpace(*worklog.Author.AccountID),
			DisplayName: strings.TrimSpace(*worklog.Author.DisplayName),
		}
		if worklog.Author.EmailAddress != nil && strings.TrimSpace(*worklog.Author.EmailAddress) != "" {
			v := strings.TrimSpace(*worklog.Author.EmailAddress)
			u.Email = &v
		}
		author = &u
	}

	return atlassian.JiraWorklog{
		IssueKey:         issue,
		WorklogID:        strings.TrimSpace(*worklog.ID),
		Author:           author,
		StartedAt:        strings.TrimSpace(*worklog.Started),
		TimeSpentSeconds: *worklog.TimeSpentSeconds,
		CreatedAt:        strings.TrimSpace(*worklog.Created),
		UpdatedAt:        strings.TrimSpace(*worklog.Updated),
	}, nil
}
