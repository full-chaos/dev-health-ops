package daily

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
)

// rosterTeam is a team with an explicit member roster, for the tests of the
// member resolver the Python reference falls back to. No production code
// builds one: the roster column teams.members is gone (CHAOS-9087) and no
// daily family resolves a team through a person.
type rosterTeam struct {
	ID      string
	Name    string
	Members []string
}

// memberResolver ports TeamResolver / _build_member_to_team
// (src/dev_health_ops/providers/teams.py:57,141).
type memberResolver struct {
	memberToTeam map[string][2]string
}

// newTestMemberResolver builds the membership resolver
// _build_member_to_team/load_team_resolver_from_store build: a normalized
// (lowercased, whitespace-collapsed) identity -> (team_id, team_name) map.
func newTestMemberResolver(teams []rosterTeam) numerical.MemberTeamResolver {
	memberToTeam := make(map[string][2]string)
	for _, team := range teams {
		teamID := strings.TrimSpace(team.ID)
		if teamID == "" {
			continue
		}
		teamName := strings.TrimSpace(team.Name)
		if teamName == "" {
			teamName = teamID
		}
		for _, member := range team.Members {
			key := normalizeKey(member)
			if key == "" {
				continue
			}
			memberToTeam[key] = [2]string{teamID, teamName}
		}
	}
	return &memberResolver{memberToTeam: memberToTeam}
}

func (resolver *memberResolver) ResolveMember(identity string) (string, string) {
	if resolver == nil || identity == "" {
		return "", ""
	}
	key := normalizeKey(identity)
	if key == "" {
		return "", ""
	}
	pair, ok := resolver.memberToTeam[key]
	if !ok {
		return "", ""
	}
	return pair[0], pair[1]
}
