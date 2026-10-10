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

// CHAOS-9204 (lead scope, Jira leg of the matrix): a FAILED member read (HTTP 500, a GraphQL error, a page
// of another shape) closes no stored membership: the collection refuses the answer, Write never runs, and
// the control (the same collection answering normally with one member lost) closes the lost member. The
// truncated reads (no proven end) are in TestAMemberReadWithoutAProvenEndClosesNoMembership.
func TestAFailedAtlassianMemberReadClosesNoStoredMembership(t *testing.T) {
	failures := []struct {
		name    string
		respond func(request) (int, any)
	}{
		{"HTTP 500 on the member read", func(request) (int, any) { return 500, nil }},
		{"a GraphQL error on the member read", func(request) (int, any) {
			return 200, map[string]any{"errors": []any{map[string]any{"message": "internal"}}}
		}},
	}
	for _, c := range failures {
		t.Run(c.name, func(t *testing.T) {
			conn := openClickHouse(t)
			ctx := context.Background()
			first := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
			g := newGateway(t, standard)
			collect := func(now time.Time, respond func(request) (int, any)) (Rows, error) {
				g.respond = respond
				p := params(everything)
				p.Now = now
				return Collect(ctx, g.client(), p)
			}
			open := func() string {
				return lines(t, conn, `SELECT toString(count()) FROM team_memberships FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND source = 'native' AND valid_to IS NULL`)[0]
			}
			rows, err := collect(first, standard)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := writeForScopeTest(ctx, conn, "org-1", rows, integrationCensus{"integration-a": true}, "integration-a"); err != nil {
				t.Fatal(err)
			}
			before := open()
			if before != "3" {
				t.Fatalf("setup: open memberships = %s, want 3", before)
			}
			failing := func(req request) (int, any) {
				if req.Operation == "TeamworkGraphTeamUsers" && req.Variables["teamId"] == teamA {
					return c.respond(req)
				}
				return standard(req)
			}
			rows, err = collect(first.Add(time.Hour), failing)
			if err == nil {
				// A collection that did not refuse the answer must not have called the member read complete.
				if rows.MembershipsComplete {
					t.Fatalf("a failed member read left MembershipsComplete true")
				}
				if _, err := writeForScopeTest(ctx, conn, "org-1", rows, integrationCensus{"integration-a": true}, "integration-a"); err != nil {
					t.Fatal(err)
				}
			}
			if after := open(); after != before {
				t.Errorf("a failed member read changed the open memberships %s -> %s", before, after)
			}
		})
	}
}
