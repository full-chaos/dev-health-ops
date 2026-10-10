//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"
)

// The read of ReuseFirstSeenMembershipValidFrom is bounded by (org, provider,
// source) and by "open": every decoy below holds an EARLIER valid_from than the
// row of the fact, so a read that lets one of them in changes the stamp.
func TestReuseFirstSeenMembershipValidFromReadsOnlyTheOpenRowsOfItsOwnOrgProviderAndSource(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	seed := func(org, provider, source, team, member string, from time.Time, to *time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, source, valid_from, valid_to, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, org, provider, team, member, source, from, to, base.Add(24*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	const org, provider, source = "org-own", "github", "provider_access"
	own := base.Add(10 * 24 * time.Hour)
	seed(org, provider, source, "t", "m", own, nil)                              // the fact: its own open row
	seed(org, provider, source, "t", "m", own.Add(5*24*time.Hour), nil)          // a later open duplicate of the same fact
	seed("org-other", provider, source, "t", "m", base.Add(1*24*time.Hour), nil) // another org
	seed(org, "gitlab", source, "t", "m", base.Add(2*24*time.Hour), nil)         // another provider
	seed(org, provider, "native", "t", "m", base.Add(3*24*time.Hour), nil)       // another source
	closedAt := base.Add(4 * 24 * time.Hour)
	seed(org, provider, source, "t", "m", base.Add(24*time.Hour), &closedAt) // a closed row (valid_to set)

	rows := []firstSeenTestRow{{provider: provider, source: source, team: "t", member: "m", from: base.Add(30 * 24 * time.Hour)}}
	got, err := ReuseFirstSeenMembershipValidFrom(ctx, conn, org, rows, firstSeenTestAccessors)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got[0].from.Equal(own) {
		t.Errorf("the stamp is %v, want %v (the earliest OPEN row of this org, provider and source): "+
			"a decoy of another org, provider or source, or a closed row, was read", got, own)
	}
}

// A failed read is an error, never "no open rows": the closed connection of a
// cancelled context stands for a database that cannot answer.
func TestReuseFirstSeenMembershipValidFromReturnsTheErrorOfAFailedRead(t *testing.T) {
	_, conn := newWorkItemEffectsConn(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	rows := []firstSeenTestRow{{provider: "github", source: "provider_access", team: "t", member: "m", from: time.Now()}}
	if got, err := ReuseFirstSeenMembershipValidFrom(cancelled, conn, "org", rows, firstSeenTestAccessors); err == nil {
		t.Errorf("a read that failed answered %v and no error: the run would stamp every fact with the run time", got)
	}
}
