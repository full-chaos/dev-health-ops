package providersync

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// team_memberships is ReplacingMergeTree(updated_at) ORDER BY (org_id,
// provider, team_id, member_id, source, valid_from). valid_from is a key
// column, so a membership written with the run time as its valid_from is a NEW
// key at every run: one more open row for the same fact (CHAOS-9007). The
// valid_from of a membership is the first time the fact was observed, and every
// writer of the four provider catalogs (Linear, GitHub, GitLab, Jira) takes it
// from the rows already open through this one function, which is the shared
// snapshot rule (PlanSnapshot) applied to memberships: a fresh row whose fact
// is already open takes the EARLIEST open valid_from. It adds no row, closes
// none, and changes nothing but valid_from.
//
// Closing a member who left the team is NOT done here (and is not done by
// these four writers at all); see CHAOS-9007.
//
// TestMembershipWritersReuseTheFirstSeenValidFrom keeps the set of writers
// that call it a named set.

// membershipOpenRowsQuery reads the open memberships of one writer. The read
// is bounded by (org_id, provider, source): provider is a prefix of the key,
// and the rows of one provider and source are the rows of one writer, so a run
// reads its own facts and no other writer's. FINAL collapses repeated versions
// of one key. The cost is the number of OPEN rows of the writer (about 1,500
// for the GitHub provider_access rows of one organization before this change,
// the number of facts after it).
const membershipOpenRowsQuery = `SELECT team_id, member_id, valid_from
FROM team_memberships FINAL
WHERE org_id = ? AND provider = ? AND source = ? AND valid_to IS NULL`

// MembershipFirstSeenAccessors reads and sets the four fields of a writer's
// membership row the rule needs.
type MembershipFirstSeenAccessors[R any] struct {
	Provider  func(R) string
	Source    func(R) string
	TeamID    func(R) string
	MemberID  func(R) string
	ValidFrom func(R) time.Time
	SetFrom   func(*R, time.Time)
}

// ReuseFirstSeenMembershipValidFrom returns rows with the valid_from of each
// membership whose fact (team, member) is already open under the same org,
// provider and source set to the earliest open valid_from of the fact. A new
// member keeps the valid_from the writer gave it. conn must not be nil: a
// writer that cannot read the open rows must not write.
func ReuseFirstSeenMembershipValidFrom[R any](
	ctx context.Context, conn driver.Conn, orgID string, rows []R, accessors MembershipFirstSeenAccessors[R],
) ([]R, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	if conn == nil || strings.TrimSpace(orgID) == "" {
		return nil, ErrInvalidConfiguration
	}
	type scope struct{ provider, source string }
	byScope := map[scope][]int{}
	for index, row := range rows {
		key := scope{accessors.Provider(row), accessors.Source(row)}
		byScope[key] = append(byScope[key], index)
	}
	out := append([]R(nil), rows...)
	for key, indexes := range byScope {
		open, err := readOpenMemberships(ctx, conn, orgID, key.provider, key.source)
		if err != nil {
			return nil, fmt.Errorf("providersync: read open memberships of %s/%s: %w", key.provider, key.source, err)
		}
		fresh := make([]MembershipSnapshotRow, len(indexes))
		for position, index := range indexes {
			fresh[position] = MembershipSnapshotRow{
				TeamID: accessors.TeamID(rows[index]), MemberID: accessors.MemberID(rows[index]),
				ValidFrom: accessors.ValidFrom(rows[index]),
			}
		}
		for position, from := range firstSeenMembershipValidFrom(fresh, open) {
			accessors.SetFrom(&out[indexes[position]], from)
		}
	}
	return out, nil
}

func readOpenMemberships(ctx context.Context, conn driver.Conn, orgID, provider, source string) ([]MembershipSnapshotRow, error) {
	result, err := conn.Query(ctx, membershipOpenRowsQuery, orgID, provider, source)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	var open []MembershipSnapshotRow
	for result.Next() {
		var row MembershipSnapshotRow
		if err := result.Scan(&row.TeamID, &row.MemberID, &row.ValidFrom); err != nil {
			return nil, err
		}
		row.ValidFrom = row.ValidFrom.UTC()
		open = append(open, row)
	}
	return open, result.Err()
}

// firstSeenMembershipValidFrom is the valid_from to write each fresh membership
// with. No kind is given to the rule, so it reuses the earliest open valid_from
// of a fact the run holds again and closes nothing.
func firstSeenMembershipValidFrom(fresh, open []MembershipSnapshotRow) []time.Time {
	if len(fresh) == 0 {
		return nil
	}
	return PlanSnapshot(fresh, open, MembershipSnapshotKey,
		func(row MembershipSnapshotRow) time.Time { return row.ValidFrom }, fresh[0].ValidFrom).ValidFrom
}
