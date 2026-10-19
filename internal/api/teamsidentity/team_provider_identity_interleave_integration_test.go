//go:build integration

package teamsidentity

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

var interleaveTeamsInsert = regexp.MustCompile(`(?i)^\s*INSERT\s+INTO\s+teams\b`)

// teamsWriteInterleaver runs another writer's commit just before the wrapped
// writer's own INSERT INTO teams is sent: after every read the wrapped writer
// makes, before its insert.
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

// teamVersionInsert is one version of a GitHub team as the catalog writer
// stores it, with the provider identity it read. updated_at is one second
// before the moment it commits: the catalog run took its time before the
// admin write did.
const teamVersionInsert = `INSERT INTO teams (id, team_uuid, name, org_id, provider, native_team_key, is_active, updated_at, created_at, provider_team_id, provider_name, gone_at)
	VALUES (?, '5a1e0000-0000-4000-8000-000000009999', 'Platform', ?, 'github', 'platform', 1, %s, '2026-10-01 00:00:00.000000', ?, ?, ?)`

func insertTeamVersion(ctx context.Context, t *testing.T, conn driver.Conn, updatedAt, orgID, teamID, providerTeamID, providerName string, goneAt *time.Time) {
	t.Helper()
	query := strings.Replace(teamVersionInsert, "%s", updatedAt, 1)
	if err := conn.Exec(ctx, query, teamID, orgID, providerTeamID, providerName, goneAt); err != nil {
		t.Fatalf("insert a version of %s: %v", teamID, err)
	}
}

func startMigratedTeamsStore(t *testing.T) (driver.Conn, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, ctx
}

// CHAOS-9999: an admin edit of a team does not know the provider identity
// and keeps the stored one. A catalog write (the provider's id and name) or a
// gone mark that commits while the admin write is between its reads and its
// insert is kept: the admin version is the newer one, and the identity must
// survive it and a merge of the versions.
func TestAnAdminEditDoesNotEraseAnIdentityWrittenDuringIt(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	const orgID = "team-provider-identity-admin-interleave-org"

	t.Run("the provider id and name", func(t *testing.T) {
		const teamID = "gh:platform"
		insertTeamVersion(ctx, t, conn, "now64(6) - INTERVAL 1 HOUR", orgID, teamID, "", "", nil)
		interleaved := &teamsWriteInterleaver{Conn: conn, other: func() {
			insertTeamVersion(ctx, t, conn, "now64(6) - INTERVAL 1 SECOND", orgID, teamID, "4711", "Platform", nil)
		}}
		if _, err := (Store{Conn: interleaved}).CreateOrUpdateTeam(ctx, orgID, TeamWrite{TeamID: teamID, Name: "Platform (edited)"}); err != nil {
			t.Fatal(err)
		}
		if !interleaved.fired {
			t.Fatal("the admin write sent no INSERT INTO teams: the interleaving did not happen")
		}
		assertStoredTeamIdentity(ctx, t, conn, orgID, teamID, "4711", "Platform", nil)
	})

	t.Run("gone_at", func(t *testing.T) {
		const teamID = "gh:infra"
		gone := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
		insertTeamVersion(ctx, t, conn, "now64(6) - INTERVAL 1 HOUR", orgID, teamID, "4712", "Infra", nil)
		interleaved := &teamsWriteInterleaver{Conn: conn, other: func() {
			insertTeamVersion(ctx, t, conn, "now64(6) - INTERVAL 1 SECOND", orgID, teamID, "4712", "Infra", &gone)
		}}
		if _, err := (Store{Conn: interleaved}).CreateOrUpdateTeam(ctx, orgID, TeamWrite{TeamID: teamID, Name: "Infra (edited)"}); err != nil {
			t.Fatal(err)
		}
		if !interleaved.fired {
			t.Fatal("the admin write sent no INSERT INTO teams: the interleaving did not happen")
		}
		assertStoredTeamIdentity(ctx, t, conn, orgID, teamID, "4712", "Infra", &gone)
	})
}

func assertStoredTeamIdentity(ctx context.Context, t *testing.T, conn driver.Conn, orgID, teamID, wantID, wantName string, wantGone *time.Time) {
	t.Helper()
	wantFinal := wantID + "|" + wantName + "|NULL"
	if wantGone != nil {
		wantFinal = wantID + "|" + wantName + "|" + wantGone.Format("2006-01-02 15:04:05.000000")
	}
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
		if err := conn.QueryRow(ctx, `SELECT concat(provider_team_id, '|', provider_name, '|', ifNull(toString(gone_at), 'NULL')) FROM teams FINAL WHERE org_id = ? AND id = ?`, orgID, teamID).Scan(&final); err != nil {
			t.Fatal(err)
		}
		if final != wantFinal {
			t.Errorf("%s: teams FINAL of %s reads %q, want %q", when, teamID, final, wantFinal)
		}
	}
	check("before a merge")
	if err := conn.Exec(ctx, `OPTIMIZE TABLE teams FINAL`); err != nil {
		t.Fatal(err)
	}
	check("after OPTIMIZE TABLE teams FINAL")
}
