//go:build integration

package providersync

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
)

// CHAOS-9432 PR1: teams.provider_team_id, provider_name and gone_at on a real
// ClickHouse with the migrated schema. The catalog writer stores the
// provider's id and name; a later version that does not read them keeps the
// stored ones (the newest non-empty value); a rename (a new name under the same
// provider id) updates provider_name and keeps the id; gone_at is the newest
// version's, and a catalog write of a listed team clears it.
func TestTeamProviderIdentityIsStoredAndCarriedOnARealClickHouse(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	sink := GitHubTeamCatalogClickHouseEffects{Conn: conn}
	const orgID = "team-provider-identity-org"
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

	team, err := normalizeGitHubTeam(orgID, githubTeamPayload{ID: "4711", Slug: "platform", Name: "Platform"}, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if team.ProviderTeamID != "4711" || team.ProviderName != "Platform" {
		t.Fatalf("normalized provider identity = %q/%q, want 4711/Platform", team.ProviderTeamID, team.ProviderName)
	}
	read := func() string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx, `SELECT concat(provider_team_id, '|', provider_name, '|', ifNull(toString(gone_at), 'NULL')) FROM teams FINAL WHERE org_id = ? AND id = 'gh:platform'`, orgID).Scan(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if err := sink.WriteTeams(ctx, orgID, []githubTeamRow{team}); err != nil {
		t.Fatal(err)
	}
	if got := read(); got != "4711|Platform|NULL" {
		t.Fatalf("after the first write: %q", got)
	}

	// A version of the same team from a writer that did not read the provider id.
	later := team
	later.ProviderTeamID, later.ProviderName = "", ""
	later.UpdatedAt = now.Add(time.Hour)
	if err := sink.WriteTeams(ctx, orgID, []githubTeamRow{later}); err != nil {
		t.Fatal(err)
	}
	if got := read(); got != "4711|Platform|NULL" {
		t.Fatalf("a version without the provider identity reads %q, want it carried", got)
	}

	// A rename at the provider: the same id, a new name.
	renamed := team
	renamed.ProviderName = "Platform Engineering"
	renamed.UpdatedAt = now.Add(2 * time.Hour)
	if err := sink.WriteTeams(ctx, orgID, []githubTeamRow{renamed}); err != nil {
		t.Fatal(err)
	}
	if got := read(); got != "4711|Platform Engineering|NULL" {
		t.Fatalf("after a rename: %q", got)
	}

	// A later run proved the team gone (a version with gone_at, as the gone
	// writer of the follow-up change writes it): Carry returns it with the id.
	if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, org_id, provider, native_team_key, updated_at, provider_team_id, provider_name, gone_at)
		VALUES ('gh:platform', generateUUIDv4(), 'Platform', ?, 'github', 'platform', '2026-10-15 15:00:00.000000', '4711', 'Platform Engineering', '2026-10-15 15:00:00.000000')`, orgID); err != nil {
		t.Fatal(err)
	}
	stored, err := teamprovider.Carry(ctx, conn, orgID, []string{"gh:platform", "gh:unknown"})
	if err != nil {
		t.Fatal(err)
	}
	if got := stored["gh:platform"]; got.ProviderTeamID != "4711" || got.ProviderName != "Platform Engineering" || got.GoneAt == nil || !got.GoneAt.Equal(time.Date(2026, 10, 15, 15, 0, 0, 0, time.UTC)) {
		t.Fatalf("Carry = %+v, want id 4711, the newest name and the gone time", got)
	}
	if _, found := stored["gh:unknown"]; found {
		t.Fatal("a team with no stored row has no entry")
	}

	// A version that does not name the three columns (an older writer during a
	// roll) is the newest version: the newest non-empty id and name still read
	// through Carry; gone_at is that version's own (NULL).
	if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, org_id, provider, native_team_key, updated_at)
		VALUES ('gh:platform', generateUUIDv4(), 'Edited', ?, 'github', 'platform', '2026-10-15 16:00:00.000000')`, orgID); err != nil {
		t.Fatal(err)
	}
	stored, err = teamprovider.Carry(ctx, conn, orgID, []string{"gh:platform"})
	if err != nil {
		t.Fatal(err)
	}
	if got := stored["gh:platform"]; got.ProviderTeamID != "4711" || got.ProviderName != "Platform Engineering" || got.GoneAt != nil {
		t.Fatalf("after a version that omits the columns, Carry = %+v, want the id and name kept and gone_at NULL", got)
	}

	// The catalog lists the team again: gone_at stays cleared, the id stays.
	back := team
	back.UpdatedAt = now.Add(5 * time.Hour)
	if err := sink.WriteTeams(ctx, orgID, []githubTeamRow{back}); err != nil {
		t.Fatal(err)
	}
	if got := read(); !strings.HasPrefix(got, "4711|Platform|") || !strings.HasSuffix(got, "|NULL") {
		t.Fatalf("after the team is listed again: %q", got)
	}
}
