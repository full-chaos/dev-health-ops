package providersync

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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
}

// Snapshot returns the rows of one membership write: the rows to write on the
// first-seen valid_from of their fact, then the open memberships of this writer
// that the run no longer holds, closed, through the shared snapshot rule.
//
// observed is what the provider returned (it decides absence); toWrite is the
// part of it the conflict guard keeps (it is what is written). A member the
// guard kept out is still observed, so it is never closed because of the guard.
// A failed read of the open rows is an error before any write.
func (writer MembershipSnapshotWriter[R]) Snapshot(
	ctx context.Context, conn driver.Conn, orgID string, observed, toWrite []R, at time.Time,
	kinds ...KindSnapshot[MembershipSnapshotRow],
) ([]R, SnapshotPlan, error) {
	if conn == nil || strings.TrimSpace(orgID) == "" || at.IsZero() {
		return nil, SnapshotPlan{}, ErrInvalidConfiguration
	}
	open, err := readOpenMembershipRows(ctx, conn, orgID, writer.Provider, writer.Source)
	if err != nil {
		return nil, SnapshotPlan{}, fmt.Errorf("providersync: read open memberships of %s/%s: %w", writer.Provider, writer.Source, err)
	}
	facts := func(rows []R) []MembershipSnapshotRow {
		out := make([]MembershipSnapshotRow, len(rows))
		for index, row := range rows {
			out[index] = MembershipSnapshotRow{TeamID: writer.TeamID(row), MemberID: writer.MemberID(row), ValidFrom: writer.ValidFrom(row)}
		}
		return out
	}
	stamp, retractions, plan := planMembershipSnapshot(open, facts(observed), at, kinds...)
	rows := make([]R, 0, len(toWrite)+len(retractions))
	for _, row := range toWrite {
		if from, ok := stamp[MembershipSnapshotKey(MembershipSnapshotRow{TeamID: writer.TeamID(row), MemberID: writer.MemberID(row)})]; ok {
			writer.SetFrom(&row, from)
		}
		rows = append(rows, row)
	}
	for _, retraction := range retractions {
		updatedAt := at
		if !updatedAt.After(retraction.open.UpdatedAt) {
			updatedAt = retraction.open.UpdatedAt.Add(time.Millisecond)
		}
		rows = append(rows, writer.Closed(retraction.open, retraction.closedAt, updatedAt))
	}
	return rows, plan, nil
}

type membershipRetraction struct {
	open     openMembership
	closedAt time.Time
}

// planMembershipSnapshot is the pure half of Snapshot: the valid_from of each
// observed fact (by MembershipSnapshotKey) and the open rows to close. observed
// is what the provider returned; open is what the table holds open for the
// writer.
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
