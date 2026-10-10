package atlassianteams

import (
	"context"
	"errors"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// The per-kind provers of the three Atlassian Teams cursor kinds (CHAOS-9124).
//
// The gateway's lists are read by the provider's cursor, which states no
// contract for a list that changes while it is read, so the end of a walk is no
// proof that a fact is gone. An open fact the run does not hold is a CANDIDATE:
// the prover asks the gateway about it, inside the per-run budget
// (providersync.AbsenceLookups). A failed read, an answer outside the recorded
// ones and a spent budget close nothing.
//
// What the gateway answers (testdata/provider_answers/atlassian, recorded on
// 2026-10-10 through the product's own client):
//
//   - TeamById of an unknown team: HTTP 200, errors[0].extensions.classification
//     "NotFound" with statusCode 404, data.team.teamV2 null. This is the one
//     per-fact read that says a team is gone: it proves the team catalog fact,
//     and it proves every member and link of the team with it. A team that
//     exists with a state other than ACTIVE is an archived one: Collect reads no
//     member and no link for it and the catalog keeps its row inactive, so the
//     facts the run holds open for it are gone too.
//   - teamUsers and teamConnectedToContainer of an UNKNOWN team: HTTP 200 with
//     EMPTY edges, the same answer as a team with none. They cannot say a team
//     is gone, and teamworkGraph_userTeams of an unknown user is empty as well,
//     so it cannot tell "not a member" from "no such user" (the stored member id
//     is a lower-cased account id: a wrong id would read as an absence).
//
// A member or a link of a team that still exists has no per-fact read in the
// gateway. Its proof is a FRESH, COMPLETE re-read of that team's list (the
// provider's end signal reached, the read is made again after the walk, one per
// team and run): the fact is gone only when the team exists (TeamById answers)
// and the second read does not hold it. That is a second observation of the same
// list, not an independent per-fact answer: it catches a fact that the first
// walk lost to a moving cursor, not a list that is wrong in both reads. It is a
// named residual of the kinds.

// GatewayReads is the part of the gateway client the prover asks.
type GatewayReads interface {
	GetTeamByID(ctx context.Context, teamID string, siteID string) (*atlassian.AtlassianTeam, error)
	IterTeamUsers(ctx context.Context, teamID string, pageSize int) ([]atlassian.TeamworkUserRelation, error)
	IterTeamConnectedContainers(ctx context.Context, teamID string, pageSize int) ([]graph.TeamConnectedContainer, error)
}

// AbsenceProver gives the gateway's own answer for one fact: SnapshotAbsenceProven
// only when the answer says the fact is gone, SnapshotFactStillHeld when it says
// the fact is there, SnapshotAbsenceNotProven for every other answer.
type AbsenceProver interface {
	TeamAbsence(ctx context.Context, teamID string) providersync.SnapshotAbsence
	MembershipAbsence(ctx context.Context, teamID, memberID string) providersync.SnapshotAbsence
	LinkAbsence(ctx context.Context, teamID string, projectID providersync.ProjectID) providersync.SnapshotAbsence
}

// GatewayAbsence is the prover of one run over the gateway client. It keeps the
// per-team answers of the run (one TeamById and one re-read of each list per
// team), so a team with many candidates is asked once.
type GatewayAbsence struct {
	Reads  GatewayReads
	SiteID string

	teams      map[string]providersync.SnapshotAbsence
	users      map[string]map[string]bool
	containers map[string]map[string]bool
	failed     map[string]bool
}

const absencePageSize = defaultPage

// teamARI is the ARI of a stored team id ("jira:<uuid>").
func storedTeamARI(storedID string) (string, bool) {
	uuid, ok := strings.CutPrefix(strings.TrimSpace(storedID), "jira:")
	uuid = strings.TrimSpace(uuid)
	if !ok || uuid == "" {
		return "", false
	}
	return teamARIPrefix + uuid, true
}

// gatewayNotFound says the gateway answered, for the team, what it answered for a
// team that does not exist: an error whose first entry is classified NotFound
// with status 404 (the recorded TeamById answer).
func gatewayNotFound(err error) bool {
	var operation *atlassian.GraphQLOperationError
	if !errors.As(err, &operation) || len(operation.Errors) == 0 {
		return false
	}
	extensions := operation.Errors[0].Extensions
	classification, _ := extensions["classification"].(string)
	var status float64
	switch typed := extensions["statusCode"].(type) {
	case float64:
		status = typed
	case int:
		status = float64(typed)
	}
	return strings.TrimSpace(classification) == "NotFound" && status == 404
}

func (absence *GatewayAbsence) init() {
	if absence.teams == nil {
		absence.teams = map[string]providersync.SnapshotAbsence{}
		absence.users = map[string]map[string]bool{}
		absence.containers = map[string]map[string]bool{}
		absence.failed = map[string]bool{}
	}
}

// TeamAbsence asks TeamById for one stored team id: the team is gone only when
// the gateway answers what it answers for a team that does not exist.
func (absence *GatewayAbsence) TeamAbsence(ctx context.Context, teamID string) providersync.SnapshotAbsence {
	if absence == nil || absence.Reads == nil || strings.TrimSpace(absence.SiteID) == "" {
		return providersync.SnapshotAbsenceNotProven
	}
	absence.init()
	if answer, asked := absence.teams[teamID]; asked {
		return answer
	}
	ari, ok := storedTeamARI(teamID)
	if !ok {
		return providersync.SnapshotAbsenceNotProven
	}
	answer := providersync.SnapshotAbsenceNotProven
	team, err := absence.Reads.GetTeamByID(ctx, ari, absence.SiteID)
	switch {
	case err == nil && team != nil:
		// A team that exists and is not ACTIVE (archived) is, for the sync, a team with
		// no members and no project links (Collect reads none for it), and the catalog
		// keeps its row inactive: the fact the run holds open is gone. An empty state
		// is no answer.
		switch state := strings.TrimSpace(team.State); {
		case state == "":
		case strings.EqualFold(state, "ACTIVE"):
			answer = providersync.SnapshotFactStillHeld
		default:
			answer = providersync.SnapshotAbsenceProven
		}
	case err != nil && gatewayNotFound(err):
		answer = providersync.SnapshotAbsenceProven
	}
	absence.teams[teamID] = answer
	return answer
}

// MembershipAbsence: the team is gone, or the team exists and a fresh complete
// re-read of its members does not hold the member.
func (absence *GatewayAbsence) MembershipAbsence(ctx context.Context, teamID, memberID string) providersync.SnapshotAbsence {
	switch absence.TeamAbsence(ctx, teamID) {
	case providersync.SnapshotAbsenceProven:
		return providersync.SnapshotAbsenceProven
	case providersync.SnapshotFactStillHeld:
	default:
		return providersync.SnapshotAbsenceNotProven
	}
	members, ok := absence.teamMembers(ctx, teamID)
	if !ok {
		return providersync.SnapshotAbsenceNotProven
	}
	if members[memberID] {
		return providersync.SnapshotFactStillHeld
	}
	return providersync.SnapshotAbsenceProven
}

// LinkAbsence: the team is gone, or the team exists and a fresh complete re-read
// of its connected containers does not hold the project.
func (absence *GatewayAbsence) LinkAbsence(ctx context.Context, teamID string, projectID providersync.ProjectID) providersync.SnapshotAbsence {
	switch absence.TeamAbsence(ctx, teamID) {
	case providersync.SnapshotAbsenceProven:
		return providersync.SnapshotAbsenceProven
	case providersync.SnapshotFactStillHeld:
	default:
		return providersync.SnapshotAbsenceNotProven
	}
	projects, ok := absence.teamProjects(ctx, teamID)
	if !ok {
		return providersync.SnapshotAbsenceNotProven
	}
	if projects[projectID.String()] {
		return providersync.SnapshotFactStillHeld
	}
	return providersync.SnapshotAbsenceProven
}

func (absence *GatewayAbsence) queryContext(ctx context.Context) context.Context {
	return graph.WithQueryContext(ctx, siteQueryContext(absence.SiteID))
}

// teamMembers is the member ids of one team, read once per run, the way Collect
// reads them (TEAM_MEMBER relations, the member id of the account).
func (absence *GatewayAbsence) teamMembers(ctx context.Context, teamID string) (map[string]bool, bool) {
	key := "users:" + teamID
	if absence.failed[key] {
		return nil, false
	}
	if members, read := absence.users[teamID]; read {
		return members, true
	}
	ari, ok := storedTeamARI(teamID)
	if !ok {
		return nil, false
	}
	relations, err := absence.Reads.IterTeamUsers(absence.queryContext(ctx), ari, absencePageSize)
	if err != nil {
		absence.failed[key] = true
		return nil, false
	}
	members := map[string]bool{}
	for _, relation := range relations {
		if relation.RelationType != "TEAM_MEMBER" {
			continue
		}
		if account, ok := accountID(relation.SubjectUserID); ok {
			members[memberID(account)] = true
		}
	}
	absence.users[teamID] = members
	return members, true
}

// teamProjects is the Jira project ids connected to one team, read once per run.
func (absence *GatewayAbsence) teamProjects(ctx context.Context, teamID string) (map[string]bool, bool) {
	key := "containers:" + teamID
	if absence.failed[key] {
		return nil, false
	}
	if projects, read := absence.containers[teamID]; read {
		return projects, true
	}
	ari, ok := storedTeamARI(teamID)
	if !ok {
		return nil, false
	}
	containers, err := absence.Reads.IterTeamConnectedContainers(absence.queryContext(ctx), ari, absencePageSize)
	if err != nil {
		absence.failed[key] = true
		return nil, false
	}
	projects := map[string]bool{}
	for _, container := range containers {
		if id, ok := connectedProjectNativeID(container); ok {
			projects[id] = true
		}
	}
	absence.containers[teamID] = projects
	return projects, true
}

var _ AbsenceProver = (*GatewayAbsence)(nil)

// The real gateway client answers the reads of the prover: the collector of the
// worker takes the prover from the client it builds, so a client that stopped
// satisfying this interface would leave every candidate open without a failure.
var _ GatewayReads = (*graph.Client)(nil)
