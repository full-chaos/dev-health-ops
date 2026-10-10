package providersync

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// A team that is no longer listed (CHAOS-9102).
//
// A catalog close used to read the open rows of the teams the run LISTED only,
// so the rows of a team that the provider stopped listing (a deleted GitHub
// team, a deleted GitLab subgroup, a Linear team that was removed) stayed open
// and kept owning repositories, projects and members. The catalog writers of
// GitHub, GitLab and Linear do not deactivate such a team, so no reader's
// inactive-team rule hides it either.
//
// A DROPPED team is a team that holds an open row of the close and that the
// run's team listing does not return. Its rows close under the same proof rule
// as every other close (decideOwnershipClose): the run is the only active
// integration of its provider, the team listing came in whole (its end proven,
// not empty), and the team is GONE:
//
//   - the listing was ONE response: nothing can move inside one response; or
//   - the provider's own lookup of that one team answers "not there" (a
//     listing read by page number loses a team that still exists when another
//     team is removed between two requests); or
//   - for a walk by the provider's cursor (Linear), the end of the walk, as the
//     kind's written statement (AbsenceWalkByCursor).
//
// A lookup that fails, is refused, is answered "still there" or does not fit
// the run's budget closes nothing, and the run says so with counts.

// TeamListingEvidence is what one run's listing of the teams of its scope
// proved. The zero value proves nothing.
type TeamListingEvidence struct {
	// Listed is every team id the listing returned, whatever else the run read
	// of the team: a team whose member read failed is still listed.
	Listed []string
	// ProvesEnd: the walk stopped on the provider's own end-of-list signal and
	// no bound stopped it first.
	ProvesEnd bool
	// Responses is the number of responses the listing took.
	Responses int
	// TeamIDPrefix limits the dropped teams to the team ids of the run's scope:
	// an id that does not start with it is no team of this listing.
	TeamIDPrefix string
	// Cursor: the listing is read by the provider's cursor, not by position;
	// the end of that walk is the proof of an absence (AbsenceWalkByCursor).
	Cursor bool
}

// TeamAbsenceProver gives the provider's own answer for ONE team: a direct
// read of the team. It answers SnapshotAbsenceProven only when the provider
// says, in its own words, that the team does not exist; SnapshotFactStillHeld
// when it says the team exists; and SnapshotAbsenceNotProven for every other
// answer (an error, a refusal, a rate limit, a body it cannot read).
type TeamAbsenceProver interface {
	TeamAbsence(ctx context.Context, teamID string) SnapshotAbsence
}

// TeamAbsenceLookupBudget is the budget of direct team answers of one run: the
// shared AbsenceLookupBudget.
const TeamAbsenceLookupBudget = AbsenceLookupBudget

// NewTeamAbsenceLookups makes the direct team answers of one run. Every close
// of the run (ownership and memberships) asks through the same object, so a
// team is asked once and the budget is one for the run. A nil prover answers
// nothing: every dropped team stays open.
func NewTeamAbsenceLookups(ctx context.Context, prover TeamAbsenceProver) *AbsenceLookups[TeamSnapshotRow] {
	if prover == nil {
		return NewAbsenceLookups[TeamSnapshotRow](ctx, TeamSnapshotKey, nil)
	}
	return NewAbsenceLookups(ctx, TeamSnapshotKey, func(ctx context.Context, row TeamSnapshotRow) SnapshotAbsence {
		return prover.TeamAbsence(ctx, row.TeamID)
	})
}

// The reasons of a dropped team that stays open. Each one is a DegradedLeg
// reason on the run's result.
const (
	TeamAbsenceNotProven  = "team_absence_not_proven"
	TeamAbsenceOverBudget = "team_absence_lookup_budget_ended"
	// TeamAbsenceStillThere: the provider's own answer says the team exists: the
	// listing lost it (the list changed while it was read).
	TeamAbsenceStillThere = "team_absent_from_listing_still_there"
	// OwnershipCloseSkippedTeamListingIncomplete: the team listing did not reach
	// its end, so no team of the run is known to be gone.
	OwnershipCloseSkippedTeamListingIncomplete = "team_listing_incomplete"

	teamAbsenceLeg = "team_absence"
)

// openTeamIDsNotListed reads the ids of the teams that hold an open row in
// table and that listed does not name. query must select ONE team id column
// and may end with a clause that takes `listed` as its LAST argument ("AND
// team_id NOT IN ?"); with no team listed the clause is left out. A failed read
// is an error, never "no team": the caller must not close, or skip, from a
// read that did not happen.
func openTeamIDsNotListed(ctx context.Context, conn driver.Conn, query, notIn string, listed []string, args ...any) ([]string, error) {
	if conn == nil {
		return nil, ErrInvalidConfiguration
	}
	if strings.TrimSpace(query) == "" || strings.TrimSpace(notIn) == "" {
		return nil, ErrInvalidConfiguration
	}
	statement := query
	if len(listed) > 0 {
		statement += " " + notIn
		args = append(args, listed)
	}
	result, err := conn.Query(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("providersync: read the teams with open rows that the listing does not return: %w", err)
	}
	defer result.Close()
	var teams []string
	for result.Next() {
		var teamID string
		if err := result.Scan(&teamID); err != nil {
			return nil, fmt.Errorf("providersync: read the teams with open rows that the listing does not return: %w", err)
		}
		teams = append(teams, teamID)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("providersync: read the teams with open rows that the listing does not return: %w", err)
	}
	return teams, nil
}

const openMembershipTeamIDsQuery = `SELECT DISTINCT team_id FROM team_memberships FINAL
WHERE org_id = ? AND provider = ? AND source = ? AND valid_to IS NULL AND startsWith(team_id, ?)`

// openMembershipTeamIDsNotListed is the dropped teams of a membership close:
// the teams of (provider, source) with an open membership that the run's team
// listing does not return. teamPrefix limits the read to the team ids of the
// run's scope ("" = every team of the provider).
func openMembershipTeamIDsNotListed(
	ctx context.Context, conn driver.Conn, orgID, provider, source, teamPrefix string, listed []string,
) ([]string, error) {
	return openTeamIDsNotListed(ctx, conn, openMembershipTeamIDsQuery, "AND team_id NOT IN ?", listed,
		orgID, provider, source, teamPrefix)
}

// teamGoneAbsence is the writer's proof for a member of a team that the run
// proved GONE: the team does not exist, so no person is a member of it, and the
// member needs no lookup of its own (which would spend the run's budget of
// member lookups one by one). A member of any other team is asked of the inner
// prover.
type teamGoneAbsence struct {
	gone  map[string]bool
	inner MembershipAbsenceProver
}

func (prover teamGoneAbsence) Absence(ctx context.Context, teamID, memberID string) MembershipAbsence {
	if prover.gone[teamID] {
		return AbsenceProven
	}
	if prover.inner == nil {
		return AbsenceNotAsked
	}
	return prover.inner.Absence(ctx, teamID, memberID)
}
