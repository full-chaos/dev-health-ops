package mappers

import (
	"errors"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/graph/gen"
)

func JiraSprintFromGraphQL(sprint gen.JiraSprintNode) (atlassian.JiraSprint, error) {
	sprintID := strings.TrimSpace(sprint.SprintID)
	if sprintID == "" {
		return atlassian.JiraSprint{}, errors.New("sprint.sprintId is required")
	}
	if sprint.Name == nil || strings.TrimSpace(*sprint.Name) == "" {
		return atlassian.JiraSprint{}, errors.New("sprint.name is required")
	}
	if sprint.State == nil || strings.TrimSpace(*sprint.State) == "" {
		return atlassian.JiraSprint{}, errors.New("sprint.state is required")
	}

	startAt := ""
	if sprint.StartDate != nil {
		startAt = strings.TrimSpace(*sprint.StartDate)
	}
	endAt := ""
	if sprint.EndDate != nil {
		endAt = strings.TrimSpace(*sprint.EndDate)
	}
	completeAt := ""
	if sprint.CompletionDate != nil {
		completeAt = strings.TrimSpace(*sprint.CompletionDate)
	}

	var startAtPtr *string
	if startAt != "" {
		startAtPtr = &startAt
	}
	var endAtPtr *string
	if endAt != "" {
		endAtPtr = &endAt
	}
	var completeAtPtr *string
	if completeAt != "" {
		completeAtPtr = &completeAt
	}

	return atlassian.JiraSprint{
		ID:         sprintID,
		Name:       strings.TrimSpace(*sprint.Name),
		State:      strings.TrimSpace(*sprint.State),
		StartAt:    startAtPtr,
		EndAt:      endAtPtr,
		CompleteAt: completeAtPtr,
	}, nil
}
