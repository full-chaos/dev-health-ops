//go:build integration

package atlassianteams

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// CHAOS-9204 (Jira leg of the provider matrix): a fact (team and member) with an open row written AFTER this
// run read the provider is left open whole: neither a surplus open row nor a departure is closed.
func TestAMembershipRowNewerThanTheReadIsNeverClosed(t *testing.T) {
	conn := openClickHouse(t)
	const org = "org-1"
	team := "jira:aaaaaaaa-0000-4000-8000-000000000001"
	read := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	older, newer := read.Add(-2*time.Hour), read.Add(time.Hour)
	seed := func(member string, from, updated time.Time) {
		exec(t, conn, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, valid_from, valid_to, updated_at)
VALUES (?, 'jira', ?, ?, ?, NULL, [?], 'native', ?, NULL, ?)`, org, team, member, member, member, from, updated)
	}
	kind := func() providersync.KindSnapshot[providersync.MembershipSnapshotRow] {
		return providersync.AtlassianTeamMembershipKind().Snapshot(soleScope(), providersync.ProveSnapshot(
			providersync.SnapshotTerm{Holds: true, Reason: snapshotMemberReadsNotEnded}),
			providersync.AbsenceByWalk[providersync.MembershipSnapshotRow](providersync.AbsenceWalkByCursor))
	}
	fresh := func(members ...string) []MembershipRow {
		var out []MembershipRow
		for _, member := range members {
			out = append(out, MembershipRow{OrgID: org, Provider: Provider, TeamID: team, MemberID: member, RawProviderUserID: member, ValidFrom: read, UpdatedAt: read, Source: "native"})
		}
		return out
	}
	plan := func(members ...string) []openMembership {
		t.Helper()
		_, expired, _, err := planMemberships(context.Background(), conn, org, []string{team}, fresh(members...), read, kind())
		if err != nil {
			t.Fatal(err)
		}
		return expired
	}
	// Control: a departure of a member whose rows are all older than the read is closed (the test measures something).
	seed("jira:gone-old", older, older)
	if got := plan("jira:kept"); len(got) != 1 || got[0].memberID != "jira:gone-old" {
		t.Fatalf("control: expired = %v, want the one older departure", got)
	}
	// A departure with a newer row, and a surplus open row that is newer than the read, stay open.
	seed("jira:gone-new", older, newer)
	seed("jira:dup", older, older)
	seed("jira:dup", older.Add(time.Minute), newer)
	got := plan("jira:kept", "jira:dup")
	if len(got) != 1 || got[0].memberID != "jira:gone-old" {
		t.Fatalf("expired = %v, want only the older departure: a fact with a row newer than the read is never closed", got)
	}
}
