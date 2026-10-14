//go:build integration

package providersync

import (
	"testing"
	"time"
)

// D5993: an empty interval (valid_to == valid_from) is a tombstone, never open
// and never counted. The manual-membership readers skip one even when it opens
// in the future, and still count a real open row.
func TestManualMembershipReadersSkipAFutureEmptyInterval(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const org, provider = "org-manual-empty", "linear"
	future := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Millisecond)
	insert := func(member string, validTo *time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, 'team-x', ?, [], 'manual', 1, 100, 10, ?, ?, ?)`,
			org, provider, member, future, validTo, future); err != nil {
			t.Fatal(err)
		}
	}
	insert("member-future-empty", &future)
	insert("member-open", nil)

	teams, err := resolveActiveManualMembershipTeams(ctx, conn, org, provider)
	if err != nil {
		t.Fatal(err)
	}
	if _, counted := teams["member-future-empty"]; counted {
		t.Errorf("resolveActiveManualMembershipTeams counted a future empty membership")
	}
	if _, counted := teams["member-open"]; !counted {
		t.Errorf("resolveActiveManualMembershipTeams dropped the open membership: %v", teams)
	}

	rows, err := fetchManualMembershipRows(ctx, conn, org, provider)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].MemberID != "member-open" {
		t.Errorf("fetchManualMembershipRows = %d rows, want only the open membership", len(rows))
	}
}
