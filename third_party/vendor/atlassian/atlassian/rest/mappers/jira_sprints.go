package mappers

import (
	"errors"
	"strconv"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/rest/gen"
)

// JiraSprintFromREST maps a Jira Agile API Sprint to a canonical JiraSprint.
func JiraSprintFromREST(sprint gen.Sprint) (atlassian.JiraSprint, error) {
	if sprint.ID == nil {
		return atlassian.JiraSprint{}, errors.New("sprint.id is required")
	}

	name := ""
	if sprint.Name != nil {
		name = strings.TrimSpace(*sprint.Name)
	}
	if name == "" {
		return atlassian.JiraSprint{}, errors.New("sprint.name is required")
	}

	state := ""
	if sprint.State != nil {
		state = strings.TrimSpace(*sprint.State)
	}
	if state == "" {
		return atlassian.JiraSprint{}, errors.New("sprint.state is required")
	}

	var startAt *string
	if sprint.StartDate != nil && strings.TrimSpace(*sprint.StartDate) != "" {
		s := strings.TrimSpace(*sprint.StartDate)
		startAt = &s
	}

	var endAt *string
	if sprint.EndDate != nil && strings.TrimSpace(*sprint.EndDate) != "" {
		e := strings.TrimSpace(*sprint.EndDate)
		endAt = &e
	}

	var completeAt *string
	if sprint.CompleteDate != nil && strings.TrimSpace(*sprint.CompleteDate) != "" {
		c := strings.TrimSpace(*sprint.CompleteDate)
		completeAt = &c
	}

	return atlassian.JiraSprint{
		ID:         strconv.Itoa(*sprint.ID),
		Name:       name,
		State:      state,
		StartAt:    startAt,
		EndAt:      endAt,
		CompleteAt: completeAt,
	}, nil
}
