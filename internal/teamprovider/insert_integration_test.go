//go:build integration

package teamprovider_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func migratedTeams(t *testing.T) (context.Context, driver.Conn) {
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
	return ctx, conn
}

// The types Insert sends are the migrated teams columns' own (a time column
// is sent as UTC text, the same DateTime64(6)), and every migrated column but
// the provider identity is one a writer can state.
func TestInsertColumnTypesAreTheMigratedTeamsColumns(t *testing.T) {
	ctx, conn := migratedTeams(t)
	rows, err := conn.Query(ctx, `SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'teams'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	sent := teamprovider.ColumnTypes()
	seen := 0
	for rows.Next() {
		var name, columnType string
		if err := rows.Scan(&name, &columnType); err != nil {
			t.Fatal(err)
		}
		if name == teamprovider.ColumnProviderTeamID || name == teamprovider.ColumnProviderName || name == teamprovider.ColumnGoneAt {
			continue
		}
		seen++
		if got := strings.ReplaceAll(sent[name], ", 'UTC'", ""); got != columnType {
			t.Errorf("teams.%s is %s, Insert sends %q", name, columnType, sent[name])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != len(sent) {
		t.Errorf("the migrated teams table has %d stated columns, Insert knows %d", seen, len(sent))
	}
}

// One statement writes the stated values exactly (microseconds, NULLs, a
// UUID, arrays) and takes the provider identity from the stored versions:
// kept, cleared by a listing writer, or stated by a move. The rows land in
// the order given: the last row of one id and one version wins.
func TestInsertWritesTheValuesAndCarriesTheIdentityOnARealClickHouse(t *testing.T) {
	ctx, conn := migratedTeams(t)
	const org = "teamprovider-insert-org"
	at := time.Date(2026, 10, 17, 9, 30, 15, 123456000, time.UTC)
	gone := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	columns := []string{"id", "team_uuid", "name", "description", "manual_members", "project_keys", "repo_patterns", "is_active", "updated_at", "org_id", "provider", "created_at", "deleted_at"}
	row := func(id, name string, updated time.Time) []any {
		return []any{id, uuid.NewSHA1(uuid.NameSpaceURL, []byte(id)), name, nil, []string{"m@example.com"}, []string{}, []string{"acme/*"}, uint8(1), updated, org, "github", updated, nil}
	}
	if err := teamprovider.Insert(ctx, conn, org, columns, []teamprovider.Write{
		{Values: row("gh:kept", "Kept", at), ProviderTeamID: "1", ProviderName: "Kept", Gone: teamprovider.GoneStated, GoneAt: &gone},
		{Values: row("gh:listed", "Listed", at), ProviderTeamID: "2", ProviderName: "Listed", Gone: teamprovider.GoneStated, GoneAt: &gone},
	}); err != nil {
		t.Fatal(err)
	}
	later := at.Add(time.Hour)
	described := "about"
	keptRow := row("gh:kept", "Kept edited", later)
	keptRow[3] = &described
	if err := teamprovider.Insert(ctx, conn, org, columns, []teamprovider.Write{
		{Values: keptRow, Gone: teamprovider.GoneKept},
		{Values: row("gh:listed", "first", later), Gone: teamprovider.GoneCleared},
		{Values: row("gh:listed", "second", later), ProviderName: "Listed again", Gone: teamprovider.GoneCleared},
		{Values: row("gh:new", "New", later), Gone: teamprovider.GoneKept},
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"gh:kept":   "Kept edited|about|m@example.com|acme/*|1|2026-10-17 10:30:15.123456|1|Kept|2026-10-02 00:00:00.000000",
		"gh:listed": "second|NULL|m@example.com|acme/*|1|2026-10-17 10:30:15.123456|2|Listed again|NULL",
		"gh:new":    "New|NULL|m@example.com|acme/*|1|2026-10-17 10:30:15.123456|||NULL",
	}
	for id, expected := range want {
		var got string
		if err := conn.QueryRow(ctx, `SELECT concat(name, '|', ifNull(description, 'NULL'), '|', arrayStringConcat(manual_members, ','), '|', arrayStringConcat(repo_patterns, ','), '|', toString(is_active), '|', toString(updated_at), '|', provider_team_id, '|', provider_name, '|', ifNull(toString(gone_at), 'NULL')) FROM teams FINAL WHERE org_id = ? AND id = ?`, org, id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != expected {
			t.Errorf("%s = %q, want %q", id, got, expected)
		}
	}
	var created string
	if err := conn.QueryRow(ctx, `SELECT toString(created_at) FROM teams FINAL WHERE org_id = ? AND id = 'gh:kept'`, org).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if created != "2026-10-17 10:30:15.123456" {
		t.Errorf("created_at = %s, want the stated time to the microsecond", created)
	}
}
