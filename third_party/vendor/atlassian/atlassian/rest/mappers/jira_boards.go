package mappers

import (
	"fmt"

	"atlassian/atlassian"
	"atlassian/atlassian/rest/gen"
)

func MapRESTBoard(b gen.Board) atlassian.JiraBoard {
	id := ""
	if b.ID != nil {
		id = fmt.Sprintf("%d", *b.ID)
	}
	name := ""
	if b.Name != nil {
		name = *b.Name
	}
	boardType := ""
	if b.BoardType != nil {
		boardType = *b.BoardType
	}

	return atlassian.JiraBoard{
		ID:   id,
		Name: name,
		Type: boardType,
	}
}
