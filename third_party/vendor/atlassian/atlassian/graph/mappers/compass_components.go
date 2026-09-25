package mappers

import (
	"errors"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/graph/gen"
)

// CompassRelationshipNode is a local type used by CompassRelationshipFromGraphQL.
// A dedicated relationship generator may produce a richer version in the gen package in the future.
type CompassRelationshipNode struct {
	ID        string
	Type      string
	StartNode *struct{ ID string }
	EndNode   *struct{ ID string }
}

func CompassComponentFromGraphQL(cloudID string, node *gen.CompassComponentNode) (atlassian.CompassComponent, error) {
	if node == nil {
		return atlassian.CompassComponent{}, errors.New("component is required")
	}
	component := node.Component
	if component == nil {
		return atlassian.CompassComponent{}, errors.New("component.component is required")
	}

	cloud := strings.TrimSpace(cloudID)
	if cloud == "" {
		return atlassian.CompassComponent{}, errors.New("cloudID is required")
	}

	id := strings.TrimSpace(component.ID)
	if id == "" {
		return atlassian.CompassComponent{}, errors.New("component.id is required")
	}
	name := strings.TrimSpace(component.Name)
	if name == "" {
		return atlassian.CompassComponent{}, errors.New("component.name is required")
	}
	componentType := strings.TrimSpace(component.TypeID)
	if componentType == "" {
		return atlassian.CompassComponent{}, errors.New("component.typeId is required")
	}

	var description *string
	if component.Description != nil {
		trimmed := strings.TrimSpace(*component.Description)
		if trimmed != "" {
			description = &trimmed
		}
	}

	var ownerTeamID *string
	var ownerTeamName *string
	if component.OwnerTeam != nil {
		teamID := strings.TrimSpace(component.OwnerTeam.ID)
		if teamID == "" {
			return atlassian.CompassComponent{}, errors.New("component.ownerTeam.id is required")
		}
		ownerTeamID = &teamID
		if component.OwnerTeam.DisplayName != nil {
			if trimmed := strings.TrimSpace(*component.OwnerTeam.DisplayName); trimmed != "" {
				ownerTeamName = &trimmed
			}
		}
	}

	return atlassian.CompassComponent{
		ID:            id,
		CloudID:       cloud,
		Name:          name,
		Type:          componentType,
		Description:   description,
		OwnerTeamID:   ownerTeamID,
		OwnerTeamName: ownerTeamName,
		Labels:        nil,
		CreatedAt:     nil,
		UpdatedAt:     nil,
	}, nil
}

func CompassRelationshipFromGraphQL(rel *CompassRelationshipNode) (atlassian.CompassRelationship, error) {
	if rel == nil {
		return atlassian.CompassRelationship{}, errors.New("relationship is required")
	}

	id := strings.TrimSpace(rel.ID)
	if id == "" {
		return atlassian.CompassRelationship{}, errors.New("relationship.id is required")
	}
	relationshipType := strings.TrimSpace(rel.Type)
	if relationshipType == "" {
		return atlassian.CompassRelationship{}, errors.New("relationship.type is required")
	}
	if rel.StartNode == nil {
		return atlassian.CompassRelationship{}, errors.New("relationship.startNode is required")
	}
	startID := strings.TrimSpace(rel.StartNode.ID)
	if startID == "" {
		return atlassian.CompassRelationship{}, errors.New("relationship.startNode.id is required")
	}
	if rel.EndNode == nil {
		return atlassian.CompassRelationship{}, errors.New("relationship.endNode is required")
	}
	endID := strings.TrimSpace(rel.EndNode.ID)
	if endID == "" {
		return atlassian.CompassRelationship{}, errors.New("relationship.endNode.id is required")
	}

	return atlassian.CompassRelationship{
		ID:               id,
		Type:             relationshipType,
		StartComponentID: startID,
		EndComponentID:   endID,
	}, nil
}

func CompassScorecardScoreFromGraphQL(componentID string, score *gen.CompassScorecardNode) (atlassian.CompassScorecardScore, error) {
	component := strings.TrimSpace(componentID)
	if component == "" {
		return atlassian.CompassScorecardScore{}, errors.New("componentID is required")
	}
	if score == nil {
		return atlassian.CompassScorecardScore{}, errors.New("score is required")
	}

	scorecardID := strings.TrimSpace(score.ScorecardID)
	if scorecardID == "" {
		return atlassian.CompassScorecardScore{}, errors.New("score.scorecardID is required")
	}

	var scorecardName *string
	if score.ScorecardName != nil {
		if trimmed := strings.TrimSpace(*score.ScorecardName); trimmed != "" {
			scorecardName = &trimmed
		}
	}

	scoreValue := score.Score

	var evaluatedAt *string
	if score.EvaluatedAt != nil {
		trimmed := strings.TrimSpace(*score.EvaluatedAt)
		if trimmed != "" {
			evaluatedAt = &trimmed
		}
	}

	return atlassian.CompassScorecardScore{
		ComponentID:   component,
		ScorecardID:   scorecardID,
		ScorecardName: scorecardName,
		Score:         scoreValue,
		MaxScore:      score.MaxScore,
		EvaluatedAt:   evaluatedAt,
	}, nil
}
