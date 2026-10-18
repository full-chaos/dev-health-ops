//go:build integration

package providersync

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
)

var interleaveTeamsInsert = regexp.MustCompile(`(?i)^\s*INSERT\s+INTO\s+teams\b`)

// teamsWriteInterleaver runs another writer's commit just before the wrapped
// writer's own INSERT INTO teams is sent: after every read the wrapped writer
// makes, before its insert. That is the window two writers of one team share
// when they run at the same time.
type teamsWriteInterleaver struct {
	driver.Conn
	other func()
	fired bool
}

func (c *teamsWriteInterleaver) interleave(query string) {
	if !c.fired && interleaveTeamsInsert.MatchString(query) {
		c.fired = true
		c.other()
	}
}

func (c *teamsWriteInterleaver) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.interleave(query)
	return c.Conn.PrepareBatch(ctx, query, opts...)
}

func (c *teamsWriteInterleaver) Exec(ctx context.Context, query string, args ...any) error {
	c.interleave(query)
	return c.Conn.Exec(ctx, query, args...)
}

// CHAOS-9999: a catalog write that commits the provider's id and name while
// another writer of the same team is between its reads and its insert is
// kept. The other writer here is the GitHub catalog writer itself on a run
// that did not read the provider id (it keeps the stored identity); its
// version is the newer one. The identity must survive that version, and must
// survive a merge of the versions (ReplacingMergeTree keeps one row per id).
func TestACatalogIdentityWrittenDuringAnotherTeamsWriteIsNotErased(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "team-provider-identity-interleave-org"
	start := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)

	seed, err := normalizeGitHubTeam(orgID, githubTeamPayload{Slug: "platform", Name: "Platform"}, nil, start)
	if err != nil {
		t.Fatal(err)
	}
	// A team stored before the provider identity was read: no id, no name.
	seed.ProviderTeamID, seed.ProviderName = "", ""
	if err := (GitHubTeamCatalogClickHouseEffects{Conn: conn}).WriteTeams(ctx, orgID, []githubTeamRow{seed}); err != nil {
		t.Fatal(err)
	}

	listed, err := normalizeGitHubTeam(orgID, githubTeamPayload{ID: "4711", Slug: "platform", Name: "Platform"}, nil, start.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	unread := seed
	unread.UpdatedAt = start.Add(2 * time.Hour)
	interleaved := &teamsWriteInterleaver{Conn: conn, other: func() {
		if err := (GitHubTeamCatalogClickHouseEffects{Conn: conn}).WriteTeams(ctx, orgID, []githubTeamRow{listed}); err != nil {
			t.Fatalf("the interleaved catalog write: %v", err)
		}
	}}
	if err := (GitHubTeamCatalogClickHouseEffects{Conn: interleaved}).WriteTeams(ctx, orgID, []githubTeamRow{unread}); err != nil {
		t.Fatal(err)
	}
	if !interleaved.fired {
		t.Fatal("the writer under test sent no INSERT INTO teams: the interleaving did not happen")
	}

	assertTeamProviderIdentity(ctx, t, conn, orgID, "gh:platform", "4711", "Platform", nil)
}

func assertTeamProviderIdentity(ctx context.Context, t *testing.T, conn driver.Conn, orgID, teamID, wantID, wantName string, wantGone *time.Time) {
	t.Helper()
	check := func(when string) {
		t.Helper()
		stored, err := teamprovider.Carry(ctx, conn, orgID, []string{teamID})
		if err != nil {
			t.Fatal(err)
		}
		got := stored[teamID]
		goneOK := (wantGone == nil && got.GoneAt == nil) || (wantGone != nil && got.GoneAt != nil && got.GoneAt.Equal(*wantGone))
		if got.ProviderTeamID != wantID || got.ProviderName != wantName || !goneOK {
			t.Errorf("%s: stored identity of %s = %q/%q gone %v, want %q/%q gone %v",
				when, teamID, got.ProviderTeamID, got.ProviderName, got.GoneAt, wantID, wantName, wantGone)
		}
		var final string
		if err := conn.QueryRow(ctx, `SELECT concat(provider_team_id, '|', provider_name) FROM teams FINAL WHERE org_id = ? AND id = ?`, orgID, teamID).Scan(&final); err != nil {
			t.Fatal(err)
		}
		if final != wantID+"|"+wantName {
			t.Errorf("%s: teams FINAL of %s reads %q, want %q", when, teamID, final, wantID+"|"+wantName)
		}
	}
	check("before a merge")
	if err := conn.Exec(ctx, `OPTIMIZE TABLE teams FINAL`); err != nil {
		t.Fatal(err)
	}
	check("after OPTIMIZE TABLE teams FINAL")
}
