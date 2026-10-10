package daily

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/icfinalize"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// ICFinalizeExecutor adapts icfinalize's run-scoped executor to this package's
// NativeFinalizeFamilyExecutor interface (CHAOS-4290).
//
// The adapter lives HERE rather than icfinalize taking daily.Run directly,
// matching repo_user_commit_native_executor.go's shape: the compute package
// stays free of this one, so the dependency points one way and the family can
// be tested without constructing a Run.
type ICFinalizeExecutor struct {
	inner *icfinalize.Executor
}

// NewICFinalizeExecutor builds the adapter. conn is the ClickHouse connection
// the family reads back from and writes through.
//
// The teams of a person come from the table team_memberships, through the
// read the work item attribution uses (teamattribution.LoadProviderMembers):
// one rule says who is a member of which team at a point in time, for the
// person's own rows and points here and for the work the person is assigned.
// The roster column teams.members is NOT read: it is a copy the team sync
// keeps beside the table, with no validity window, and it can be empty while
// the table holds the memberships.
//
// The mapper is set ONCE here, at construction, not per finalize call: the
// closure takes the organization and the time as parameters and does a fresh,
// independent ClickHouse read on every invocation, so concurrent
// ComputeFinalizeFamily calls for DIFFERENT organizations sharing this one
// Executor instance never mutate shared state.
func NewICFinalizeExecutor(conn driver.Conn) *ICFinalizeExecutor {
	inner := icfinalize.NewExecutor(conn)
	inner.SetTeamMapper(func(ctx context.Context, orgID string, asOf time.Time) (icfinalize.PersonTeams, error) {
		members, err := teamattribution.ClickHouseFactSource{Conn: conn}.LoadProviderMembers(ctx, orgID, asOf)
		if err != nil {
			return nil, fmt.Errorf("load the team memberships of the organization: %w", err)
		}
		return personTeamsOf(members), nil
	})
	// The stale-key rule (stale_team_keys.go): the family computes the
	// organization's day whole, so every stored point of the day that this
	// compute did not produce gets a row of zeros.
	inner.SetLandscapeSuperseder(func(
		ctx context.Context, orgID string, asOf, computedAt time.Time, written []icfinalize.LandscapeRecord,
	) (int, error) {
		repoID := icfinalize.LandscapeRowRepoID().String()
		produced := make([]staleKey, 0, len(written))
		for _, record := range written {
			produced = append(produced, staleKey{repoID, record.TeamID, record.MapName, record.IdentityID})
		}
		return supersedeStaleTeamKeys(ctx, conn, teamkeytables.ICLandscapeRolling30d, orgID, asOf, nil, produced, computedAt)
	})
	return &ICFinalizeExecutor{inner: inner}
}

// personTeamsOf indexes membership facts by person. A person is found by an
// identity facet of a membership row: the strings the team sync stores for
// "who this member can appear as" (the email, the provider-qualified name,
// the alias). A row with no facet is found by its member id and its email.
// The comparison is the one of the attribution
// (teamattribution.NormalizeDerivationIdentity).
//
// The teams of a person are every team with a membership, each one time, in
// the rank order of the attribution (teamattribution.RankDerivationCandidates:
// primary first, then the more specific, then the lower priority number, the
// newer row, the team id), so the FIRST team is a fixed choice and not the
// order of a query.
func personTeamsOf(members []teamattribution.GithubWorkItemDerivationMemberFact) icfinalize.PersonTeams {
	byPerson := map[string][]teamattribution.GithubWorkItemDerivationCandidate{}
	for _, member := range members {
		if strings.TrimSpace(member.TeamID) == "" {
			continue
		}
		candidate := teamattribution.GithubWorkItemDerivationCandidateFromFact(
			"team_membership", member.TeamID, member.TeamName, "",
			member.IsPrimary, member.Specificity, member.Priority, member.UpdatedAt,
		)
		names := member.IdentityFacets
		if len(names) == 0 {
			names = []string{member.MemberID, teamattribution.GithubWorkItemDerivationStringValue(member.RawEmail)}
		}
		seen := map[string]struct{}{}
		for _, name := range names {
			key := teamattribution.NormalizeDerivationIdentity(name)
			if key == "" {
				continue
			}
			if _, twice := seen[key]; twice {
				continue
			}
			seen[key] = struct{}{}
			byPerson[key] = append(byPerson[key], candidate)
		}
	}
	teamsByPerson := make(map[string][]string, len(byPerson))
	for key, candidates := range byPerson {
		var teams []string
		listed := map[string]struct{}{}
		for _, candidate := range teamattribution.RankDerivationCandidates(candidates) {
			teamID := teamattribution.GithubWorkItemDerivationStringValue(candidate.TeamID)
			if _, twice := listed[teamID]; twice || teamID == "" {
				continue
			}
			listed[teamID] = struct{}{}
			teams = append(teams, teamID)
		}
		teamsByPerson[key] = teams
	}
	return func(identity string) []string {
		return teamsByPerson[teamattribution.NormalizeDerivationIdentity(identity)]
	}
}

// ComputeFinalizeFamily implements NativeFinalizeFamilyExecutor.
//
// Run carries both the organization and the target day, and both are taken
// from it rather than from separate arguments -- computing one org's day
// against another's scope is the mistake this shape makes unrepresentable.
func (executor *ICFinalizeExecutor) ComputeFinalizeFamily(ctx context.Context, run Run) (int, error) {
	if executor == nil || executor.inner == nil {
		return 0, ErrUnavailable
	}
	return executor.inner.ComputeFinalizeFamily(ctx, icfinalize.RunScope{
		OrganizationID: run.OrganizationID,
		TargetDay:      run.TargetDay,
	})
}

// SetTeamMapper forwards the identity->team resolver.
func (executor *ICFinalizeExecutor) SetTeamMapper(mapper icfinalize.TeamMapper) {
	if executor == nil || executor.inner == nil {
		return
	}
	executor.inner.SetTeamMapper(mapper)
}

var _ NativeFinalizeFamilyExecutor = (*ICFinalizeExecutor)(nil)

// FamilyName re-exports the single source of truth for this family's
// families.json key, so registration sites do not restate the literal.
const ICFinalizeFamilyName = icfinalize.FamilyName
