package providersync

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// CHAOS-9079: a member who is absent from the COMPLETE member read of a team is
// closed: the open membership is written again with valid_to = the run time,
// through the one shared snapshot rule (PlanSnapshot) and the team membership
// kinds (snapshot_kinds.go). A read that did not prove its end, a team whose
// read failed, and a scope another integration may share close nothing. A member
// who comes back is a new fact: the closed row is no longer open, so the fresh
// row keeps the valid_from its run gave it.

// openMembership is one open team_memberships row, every column, as the table
// stores it.
type openMembership struct {
	OrgID, Provider, TeamID, MemberID string
	RawProviderUserID, RawEmail       *string
	IdentityFacets                    []string
	Source                            string
	IsPrimary                         uint8
	Specificity                       uint16
	Priority                          int32
	ValidFrom, UpdatedAt              time.Time
}

const membershipOpenRowsFullQuery = `SELECT org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, updated_at
FROM team_memberships FINAL
WHERE org_id = ? AND provider = ? AND source = ? AND valid_to IS NULL`

func readOpenMembershipRows(ctx context.Context, conn driver.Conn, orgID, provider, source string) ([]openMembership, error) {
	result, err := conn.Query(ctx, membershipOpenRowsFullQuery, orgID, provider, source)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	var open []openMembership
	for result.Next() {
		var row openMembership
		if err := result.Scan(&row.OrgID, &row.Provider, &row.TeamID, &row.MemberID, &row.RawProviderUserID, &row.RawEmail,
			&row.IdentityFacets, &row.Source, &row.IsPrimary, &row.Specificity, &row.Priority, &row.ValidFrom, &row.UpdatedAt); err != nil {
			return nil, err
		}
		row.ValidFrom, row.UpdatedAt = row.ValidFrom.UTC(), row.UpdatedAt.UTC()
		open = append(open, row)
	}
	return open, result.Err()
}

// MembershipAbsence is the answer of a provider's DIRECT lookup for one member
// of one team. A list read says who the provider returned; only this says who is
// not a member: a list read by offset or page number can skip a member who did
// not leave when another member leaves between two page requests.
type MembershipAbsence int

const (
	// AbsenceUnproven: the lookup failed, was refused, was rate limited, timed
	// out, or its answer is not clear. The member stays open.
	AbsenceUnproven MembershipAbsence = iota
	// AbsenceStillMember: the provider says the person is a member of the team.
	AbsenceStillMember
	// AbsenceProven: the provider says, in so many words (a 404 of the direct
	// membership lookup), that the person is not a member of the team.
	AbsenceProven
	// AbsenceOverBudget: the run's lookup budget ended before this member.
	AbsenceOverBudget
	// AbsenceNotAsked: this prover has no answer for this member; the list rule
	// of the writer applies.
	AbsenceNotAsked
)

// MembershipAbsenceProver asks the provider about one member of one team.
type MembershipAbsenceProver interface {
	Absence(ctx context.Context, teamID, memberID string) MembershipAbsence
}

// MembershipLookupBudget bounds the lookups of one run: a run that finds more
// candidates than its budget proves the first ones and leaves the rest open.
type MembershipLookupBudget struct {
	Inner MembershipAbsenceProver
	Left  int
}

// Absence asks the inner prover while the budget lasts.
func (budget *MembershipLookupBudget) Absence(ctx context.Context, teamID, memberID string) MembershipAbsence {
	if budget == nil || budget.Inner == nil {
		return AbsenceUnproven
	}
	if budget.Left <= 0 {
		return AbsenceOverBudget
	}
	budget.Left--
	return budget.Inner.Absence(ctx, teamID, memberID)
}

// membershipLookupBudget is the number of direct lookups one catalog run may
// make: a departure is rare, and a run that finds more candidates than this
// leaves the rest open and says so.
const membershipLookupBudget = 100

// MembershipSnapshotWriter is one provider catalog's membership writer as the
// snapshot needs it: where its rows live, how to read a row, and how to make a
// row of its own type.
type MembershipSnapshotWriter[R any] struct {
	Provider, Source string
	TeamID           func(R) string
	MemberID         func(R) string
	ValidFrom        func(R) time.Time
	SetFrom          func(*R, time.Time)
	// Closed makes the row that closes an open membership: the open row written
	// again with valid_to = closedAt and updated_at = updatedAt.
	Closed func(open openMembership, closedAt, updatedAt time.Time) R
	// KeyedByEmail says that the member id of an open row was made from an
	// email, which is not stable. No provider stores its own stable user id in
	// raw_provider_user_id (it holds the first identity facet), so such a member
	// is never closed by the list rule: a changed or hidden email would read as
	// a departure.
	KeyedByEmail func(open openMembership) bool
}

// Skipped reasons of a candidate that is left open.
const (
	membershipSkipNewerRow       = "row_newer_than_read"
	membershipSkipNoStableUserID = "no_stable_user_id"
	membershipSkipStillMember    = "provider_says_member"
	membershipSkipLookupFailed   = "lookup_not_proven"
	membershipSkipOverBudget     = "lookup_budget_ended"
)

// MembershipSnapshotOutcome is what one membership write decided. The counts
// are of FACTS (a team and a member), not of rows.
type MembershipSnapshotOutcome struct {
	Plan SnapshotPlan
	// Closed: facts closed because the provider proved the person is not a
	// member (or, for a provider without a lookup, because the complete list
	// lacks them); every open row of the fact is closed.
	Closed int
	// DuplicatesRetired: later open rows of a fact the run holds again.
	DuplicatesRetired int
	// Skipped: facts left open, by reason.
	Skipped map[string]int
}

// membershipTeamLogToken names a team in a log line as a string: the id of a
// team is made from a provider slug or path, so the provider prefix is cut and
// "/" and "." become "_" to meet the shape logging.ProviderAssignedID lets
// through. A team that still does not pass is logged as "dropped".
func membershipTeamLogToken(teamID string) string {
	token := teamID
	if _, rest, ok := strings.Cut(teamID, ":"); ok {
		token = rest
	}
	token = strings.NewReplacer("/", "_", ".", "_").Replace(token)
	if id, dropped := logging.ProviderAssignedID(token); !dropped {
		return id
	}
	return "dropped"
}

type membershipTeamTally struct {
	closed, duplicates int
	skipped            map[string]int
}

// Snapshot returns the rows of one membership write: the rows to write on the
// first-seen valid_from of their fact, then the open memberships of this writer
// that the run closes, through the shared snapshot rule.
//
// observed is what the provider returned (it decides who is a CANDIDATE for a
// close); toWrite is the part of it the conflict guard keeps (it is what is
// written). A member the guard kept out is still observed, so it is never
// closed because of the guard. A failed read of the open rows is an error
// before any write.
//
// A candidate is a FACT (team and member), however many open rows it holds:
// it is decided once, asked about once, and a close closes every open row of
// it. It is closed only when: none of its rows is newer than the time the run
// read the provider; its member id is not made from an email (see KeyedByEmail)
// unless the provider says so itself; and, where the provider's list is paged
// by offset (prove is not nil), the provider's direct lookup answers "not a
// member". A lookup that fails or is over budget closes nothing, and the log
// says so with counts.
func (writer MembershipSnapshotWriter[R]) Snapshot(
	ctx context.Context, conn driver.Conn, orgID string, observed, toWrite []R, at time.Time,
	prove MembershipAbsenceProver, kinds ...KindSnapshot[MembershipSnapshotRow],
) ([]R, MembershipSnapshotOutcome, error) {
	if conn == nil || strings.TrimSpace(orgID) == "" || at.IsZero() {
		return nil, MembershipSnapshotOutcome{}, ErrInvalidConfiguration
	}
	open, err := readOpenMembershipRows(ctx, conn, orgID, writer.Provider, writer.Source)
	if err != nil {
		return nil, MembershipSnapshotOutcome{}, fmt.Errorf("providersync: read open memberships of %s/%s: %w", writer.Provider, writer.Source, err)
	}
	observedFacts := make([]MembershipSnapshotRow, len(observed))
	for index, row := range observed {
		observedFacts[index] = MembershipSnapshotRow{TeamID: writer.TeamID(row), MemberID: writer.MemberID(row), ValidFrom: writer.ValidFrom(row)}
	}
	stamp, retractions, plan := planMembershipSnapshot(open, observedFacts, at, kinds...)

	held := map[string]bool{}
	for _, fact := range observedFacts {
		held[MembershipSnapshotKey(fact)] = true
	}
	outcome := MembershipSnapshotOutcome{Plan: plan, Skipped: map[string]int{}}
	tally := map[string]*membershipTeamTally{}
	of := func(teamID string) *membershipTeamTally {
		if tally[teamID] == nil {
			tally[teamID] = &membershipTeamTally{skipped: map[string]int{}}
		}
		return tally[teamID]
	}
	type closing struct {
		open     openMembership
		closedAt time.Time
	}
	var closes []closing
	// One candidate per fact, in the order the rule retracted the first row of it.
	var order []string
	byFact := map[string][]membershipRetraction{}
	for _, retraction := range retractions {
		key := MembershipSnapshotKey(MembershipSnapshotRow{TeamID: retraction.open.TeamID, MemberID: retraction.open.MemberID})
		if held[key] {
			// A later open row of a fact the run holds again: not a departure.
			closes = append(closes, closing{retraction.open, retraction.closedAt})
			outcome.DuplicatesRetired++
			of(retraction.open.TeamID).duplicates++
			continue
		}
		if _, seen := byFact[key]; !seen {
			order = append(order, key)
		}
		byFact[key] = append(byFact[key], retraction)
	}
	for _, key := range order {
		rows := byFact[key]
		first := rows[0].open
		skip := func(reason string) {
			outcome.Skipped[reason]++
			of(first.TeamID).skipped[reason]++
		}
		newest := first.UpdatedAt
		for _, row := range rows {
			if row.open.UpdatedAt.After(newest) {
				newest = row.open.UpdatedAt
			}
		}
		if newest.After(at) {
			// A row of the fact was written by a run that read the provider AFTER
			// this one: this run's list is older than the row, so it cannot say
			// the member left.
			skip(membershipSkipNewerRow)
			continue
		}
		answer := AbsenceNotAsked
		if prove != nil {
			answer = prove.Absence(ctx, first.TeamID, first.MemberID)
		}
		switch answer {
		case AbsenceProven:
		case AbsenceStillMember:
			skip(membershipSkipStillMember)
			continue
		case AbsenceOverBudget:
			skip(membershipSkipOverBudget)
			continue
		case AbsenceNotAsked:
			// The writer's own list rule: the list is paged by a cursor, which a
			// departure between two requests cannot shift, unless the member id
			// is made from an email and no stable user id is stored.
			if writer.KeyedByEmail != nil && writer.KeyedByEmail(first) {
				skip(membershipSkipNoStableUserID)
				continue
			}
		default:
			skip(membershipSkipLookupFailed)
			continue
		}
		for _, row := range rows {
			closes = append(closes, closing{row.open, row.closedAt})
		}
		outcome.Closed++
		of(first.TeamID).closed++
	}

	teamIDs := make([]string, 0, len(tally))
	for teamID := range tally {
		teamIDs = append(teamIDs, teamID)
	}
	sort.Strings(teamIDs)
	for _, teamID := range teamIDs {
		counts := tally[teamID]
		if counts.closed+counts.duplicates > 0 {
			slog.Default().InfoContext(ctx, "team_membership_closed",
				"org_id", orgID, "provider", writer.Provider, "team", membershipTeamLogToken(teamID),
				"closed", counts.closed, "duplicates_retired", counts.duplicates)
		}
		if len(counts.skipped) > 0 {
			slog.Default().WarnContext(ctx, "team_membership_close_skipped",
				"org_id", orgID, "provider", writer.Provider, "team", membershipTeamLogToken(teamID),
				membershipSkipNewerRow, counts.skipped[membershipSkipNewerRow],
				membershipSkipNoStableUserID, counts.skipped[membershipSkipNoStableUserID],
				membershipSkipStillMember, counts.skipped[membershipSkipStillMember],
				membershipSkipLookupFailed, counts.skipped[membershipSkipLookupFailed],
				membershipSkipOverBudget, counts.skipped[membershipSkipOverBudget])
		}
	}

	rows := make([]R, 0, len(toWrite)+len(closes))
	for _, row := range toWrite {
		if from, ok := stamp[MembershipSnapshotKey(MembershipSnapshotRow{TeamID: writer.TeamID(row), MemberID: writer.MemberID(row)})]; ok {
			writer.SetFrom(&row, from)
		}
		rows = append(rows, row)
	}
	for _, closing := range closes {
		updatedAt := at
		if !updatedAt.After(closing.open.UpdatedAt) {
			updatedAt = closing.open.UpdatedAt.Add(time.Millisecond)
		}
		rows = append(rows, writer.Closed(closing.open, closing.closedAt, updatedAt))
	}
	return rows, outcome, nil
}

type membershipRetraction struct {
	open     openMembership
	closedAt time.Time
}

// planMembershipSnapshot is the pure half of Snapshot: the valid_from of each
// observed fact (by MembershipSnapshotKey) and the open rows the rule would
// close. observed is what the provider returned; open is what the table holds
// open for the writer. It decides nothing about WHY a row is absent: Snapshot
// refines the rule's retractions (candidates, renames, lookups).
func planMembershipSnapshot(
	open []openMembership, observed []MembershipSnapshotRow, at time.Time, kinds ...KindSnapshot[MembershipSnapshotRow],
) (map[string]time.Time, []membershipRetraction, SnapshotPlan) {
	openFacts := make([]MembershipSnapshotRow, len(open))
	for index, row := range open {
		openFacts[index] = MembershipSnapshotRow{TeamID: row.TeamID, MemberID: row.MemberID, ValidFrom: row.ValidFrom}
	}
	plan := PlanSnapshot(observed, openFacts, MembershipSnapshotKey,
		func(row MembershipSnapshotRow) time.Time { return row.ValidFrom }, at, kinds...)
	stamp := make(map[string]time.Time, len(observed))
	for index, fact := range observed {
		stamp[MembershipSnapshotKey(fact)] = plan.ValidFrom[index]
	}
	retractions := make([]membershipRetraction, 0, len(plan.Retract))
	for _, retraction := range plan.Retract {
		retractions = append(retractions, membershipRetraction{open: open[retraction.Open], closedAt: retraction.ClosedAt})
	}
	return stamp, retractions, plan
}

// membershipRowOf converts the open row to a provider's membership row type
// (the three catalogs' row types have the same fields).
type membershipColumns struct {
	OrgID             string
	Provider          string
	TeamID            string
	MemberID          string
	RawProviderUserID *string
	RawEmail          *string
	IdentityFacets    []string
	Source            string
	IsPrimary         uint8
	Specificity       uint16
	Priority          int32
	ValidFrom         time.Time
	ValidTo           *time.Time
	UpdatedAt         time.Time
}

func closedMembershipColumns(open openMembership, closedAt, updatedAt time.Time) membershipColumns {
	closed := closedAt.UTC()
	return membershipColumns{
		OrgID: open.OrgID, Provider: open.Provider, TeamID: open.TeamID, MemberID: open.MemberID,
		RawProviderUserID: open.RawProviderUserID, RawEmail: open.RawEmail, IdentityFacets: open.IdentityFacets,
		Source: open.Source, IsPrimary: open.IsPrimary, Specificity: open.Specificity, Priority: open.Priority,
		ValidFrom: open.ValidFrom, ValidTo: &closed, UpdatedAt: updatedAt.UTC(),
	}
}
