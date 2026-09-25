package mappers

import (
	"errors"
	"fmt"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/graph/gen"
)

func TeamFromGraphQL(team *gen.TeamNode) (atlassian.AtlassianTeam, error) {
	if team == nil {
		return atlassian.AtlassianTeam{}, errors.New("team is required")
	}

	teamID, err := requireNonEmpty(team.ID, "team.id")
	if err != nil {
		return atlassian.AtlassianTeam{}, err
	}
	displayName, err := requireNonEmpty(team.DisplayName, "team.displayName")
	if err != nil {
		return atlassian.AtlassianTeam{}, err
	}
	state, err := requireNonEmpty(team.State, "team.state")
	if err != nil {
		return atlassian.AtlassianTeam{}, err
	}

	return atlassian.AtlassianTeam{
		ID:          teamID,
		DisplayName: displayName,
		State:       state,
		Description: nil,
		AvatarURL:   optionalString(team.SmallAvatarImageURL),
		MemberCount: nil,
	}, nil
}

func TeamMemberFromGraphQL(teamID string, member *gen.TeamMemberNode) (atlassian.AtlassianTeamMember, error) {
	if member == nil {
		return atlassian.AtlassianTeamMember{}, errors.New("member is required")
	}

	canonicalTeamID, err := requireNonEmpty(teamID, "teamId")
	if err != nil {
		return atlassian.AtlassianTeamMember{}, err
	}
	accountID, err := requireNonEmpty(member.AccountID, "member.accountId")
	if err != nil {
		return atlassian.AtlassianTeamMember{}, err
	}

	return atlassian.AtlassianTeamMember{
		TeamID:      canonicalTeamID,
		AccountID:   accountID,
		DisplayName: optionalString(member.DisplayName),
		Role:        optionalString(member.Role),
	}, nil
}

func requireNonEmpty(value any, path string) (string, error) {
	switch v := value.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return "", fmt.Errorf("%s is required", path)
		}
		return trimmed, nil
	case *string:
		if v == nil {
			return "", fmt.Errorf("%s is required", path)
		}
		trimmed := strings.TrimSpace(*v)
		if trimmed == "" {
			return "", fmt.Errorf("%s is required", path)
		}
		return trimmed, nil
	default:
		return "", fmt.Errorf("%s is required", path)
	}
}

func optionalString(value any) *string {
	switch v := value.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return nil
		}
		return &trimmed
	case *string:
		if v == nil {
			return nil
		}
		trimmed := strings.TrimSpace(*v)
		if trimmed == "" {
			return nil
		}
		return &trimmed
	default:
		return nil
	}
}

func optionalInt(value any) *int {
	switch v := value.(type) {
	case int:
		return &v
	case *int:
		return v
	case int32:
		converted := int(v)
		return &converted
	case *int32:
		if v == nil {
			return nil
		}
		converted := int(*v)
		return &converted
	case int64:
		converted := int(v)
		return &converted
	case *int64:
		if v == nil {
			return nil
		}
		converted := int(*v)
		return &converted
	default:
		return nil
	}
}
