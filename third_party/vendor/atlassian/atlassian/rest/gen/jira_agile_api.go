// Jira Agile REST API models for Sprint data.
// Note: The Jira Agile API is separate from the core Jira REST API.
// These models are manually maintained to match the Jira Software Cloud REST API.
package gen

import "encoding/json"

// Sprint represents a sprint from the Jira Agile REST API.
// Ref: GET /rest/agile/1.0/board/{boardId}/sprint
// Ref: GET /rest/agile/1.0/sprint/{sprintId}
type Sprint struct {
	ID            *int    `json:"id,omitempty"`
	Name          *string `json:"name,omitempty"`
	State         *string `json:"state,omitempty"`
	StartDate     *string `json:"startDate,omitempty"`
	EndDate       *string `json:"endDate,omitempty"`
	CompleteDate  *string `json:"completeDate,omitempty"`
	OriginBoardID *int    `json:"originBoardId,omitempty"`
	Goal          *string `json:"goal,omitempty"`
}

// SprintPage represents a paginated list of sprints from the Jira Agile REST API.
// Ref: GET /rest/agile/1.0/board/{boardId}/sprint
type SprintPage struct {
	StartAt    *int     `json:"startAt,omitempty"`
	MaxResults *int     `json:"maxResults,omitempty"`
	IsLast     *bool    `json:"isLast,omitempty"`
	Values     []Sprint `json:"values,omitempty"`
}

// Board represents a board from the Jira Agile REST API.
// Ref: GET /rest/agile/1.0/board
// Ref: GET /rest/agile/1.0/board/{boardId}
type Board struct {
	ID        *int    `json:"id,omitempty"`
	Name      *string `json:"name,omitempty"`
	BoardType *string `json:"type,omitempty"`
}

// BoardPage represents a paginated list of boards from the Jira Agile REST API.
// Ref: GET /rest/agile/1.0/board
type BoardPage struct {
	StartAt    *int    `json:"startAt,omitempty"`
	MaxResults *int    `json:"maxResults,omitempty"`
	IsLast     *bool   `json:"isLast,omitempty"`
	Values     []Board `json:"values,omitempty"`
}

// DecodeSprintPage decodes a map into a SprintPage struct.
func DecodeSprintPage(data map[string]any) (*SprintPage, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var out SprintPage
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DecodeBoardPage decodes a map into a BoardPage struct.
func DecodeBoardPage(data map[string]any) (*BoardPage, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var out BoardPage
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
