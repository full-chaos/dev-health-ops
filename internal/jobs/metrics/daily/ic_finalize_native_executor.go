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
// The roster column teams.members is gone (CHAOS-9087): it was a copy the
// team sync kept beside the table, with no validity window.
//
// The mapper is set ONCE here, at construction, not per finalize call: the
// closure takes the organization and the time as parameters and does a fresh,
// independent ClickHouse read on every invocation, so concurrent
// ComputeFinalizeFamily calls for DIFFERENT organizations sharing this one
// Executor instance never mutate shared state.
func NewICFinalizeExecutor(conn driver.Conn) *ICFinalizeExecutor {
	inner := icfinalize.NewExecutor(conn)
	inner.SetTeamMapper(func(ctx context.Context, orgID string, asOf time.Time) (icfinalize.PersonTeams, icfinalize.TeamNames, error) {
		members, err := teamattribution.ClickHouseFactSource{Conn: conn}.LoadProviderMembers(ctx, orgID, asOf)
		if err != nil {
			return nil, nil, fmt.Errorf("load the team memberships of the organization: %w", err)
		}
		return personTeamsOf(members), teamNamesOf(members), nil
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
	// The stale-key rule of the synthesized keys of user_metrics_daily
	// (CHAOS-9135): a person with work-item metrics and no git row is written
	// under a repository only this family makes, so no repository partition can
	// retract that row; this compute holds every person of the day.
	inner.SetUserMetricsSuperseder(func(
		ctx context.Context, orgID string, day, computedAt time.Time, written []icfinalize.ICUserMetric,
	) (int, error) {
		produced := make([]staleKey, 0, len(written))
		for _, metric := range written {
			// The key the write used: a person with a stored row under the
			// synthesized repository is read back as a git record (its
			// RepoID is the synthesized one), so a flag alone would retract
			// the row this compute just wrote.
			repoID := metric.RepoID
			if metric.SynthesizedRepoID {
				repoID = icfinalize.SynthesizedRepoID(orgID, metric.IdentityID)
			}
			produced = append(produced, staleKey{repoID.String(), metric.IdentityID})
		}
		return supersedeSynthesizedUserKeys(ctx, conn, orgID, day, computedAt, produced)
	})
	return &ICFinalizeExecutor{inner: inner}
}

// SetDayLocker wires the lock that serializes the compute of one organization
// and day across workers (CHAOS-9945).
func (e *ICFinalizeExecutor) SetDayLocker(locker icfinalize.DayLocker) { e.inner.SetDayLocker(locker) }

// SetClock replaces the clock the family's computed_at is taken from.
func (e *ICFinalizeExecutor) SetClock(now func() time.Time) { e.inner.SetClock(now) }

// teamNamesOf gives a team's name from the membership facts the person's teams come
// from: the name of the team's newest fact that has one. One read gives a person's
// team id and its name (CHAOS-9134).
func teamNamesOf(members []teamattribution.GithubWorkItemDerivationMemberFact) icfinalize.TeamNames {
	names := map[string]string{}
	newest := map[string]time.Time{}
	for _, member := range members {
		teamID := strings.TrimSpace(member.TeamID)
		name := strings.TrimSpace(member.TeamName)
		if teamID == "" || name == "" {
			continue
		}
		if at, seen := newest[teamID]; seen && !member.UpdatedAt.After(at) {
			continue
		}
		names[teamID], newest[teamID] = name, member.UpdatedAt
	}
	return func(teamID string) string { return names[strings.TrimSpace(teamID)] }
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
