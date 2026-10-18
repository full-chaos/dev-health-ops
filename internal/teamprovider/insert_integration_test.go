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

// Restate writes a stored team again from its newest version, at that
// version: the members it gains are added once, it goes active, created_at is
// the oldest over every version and the identity is the newest non-empty one.
// A deleted team and an id with no row are not written, and the written count
// the server reports says so.
func TestRestateWritesTheStoredRowAgainOnARealClickHouse(t *testing.T) {
	ctx, conn := migratedTeams(t)
	const org = "teamprovider-restate-org"
	first := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	newest := first.Add(time.Hour)
	insert := `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key, created_at, deleted_at, provider_team_id, provider_name) ` +
		`VALUES (?, ?, ?, ?, ['ENG'], [], ?, ?, ?, ?, '', 'eng', ?, ?, ?, ?)`
	exec := func(args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, insert, args...); err != nil {
			t.Fatal(err)
		}
	}
	teamUUID := uuid.New()
	exec("custom:eng", teamUUID, "Eng", []string{"kept@example.com"}, uint8(0), first, first, org, first, nil, "id-1", "Eng")
	exec("custom:eng", teamUUID, "Eng renamed", []string{"kept@example.com"}, uint8(0), newest, newest, org, nil, nil, "", "")
	exec("custom:gone", uuid.New(), "Gone", []string{}, uint8(0), newest, newest, org, nil, newest, "", "")

	written, err := teamprovider.Restate(ctx, conn, org, []teamprovider.Restatement{
		{ID: "custom:eng", AddMembers: []string{"admin@example.com", "kept@example.com", "admin@example.com"}, Activate: true},
		{ID: "custom:gone", Activate: true},
		{ID: "custom:none", Activate: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if written != 1 {
		t.Errorf("written = %d, want 1 (the deleted team and the id with no row are not written)", written)
	}
	var got string
	if err := conn.QueryRow(ctx, `SELECT concat(name, '|', arrayStringConcat(manual_members, ','), '|', toString(is_active), '|', toString(updated_at), '|', toString(created_at), '|', provider_team_id, '|', provider_name, '|', arrayStringConcat(project_keys, ','), '|', toString(team_uuid)) FROM teams FINAL WHERE org_id = ? AND id = 'custom:eng'`, org).Scan(&got); err != nil {
		t.Fatal(err)
	}
	want := "Eng renamed|kept@example.com,admin@example.com|1|2026-09-01 01:00:00.000000|2026-09-01 00:00:00.000000|id-1|Eng|ENG|" + teamUUID.String()
	if got != want {
		t.Errorf("custom:eng = %q, want %q", got, want)
	}
	var gone, none uint64
	if err := conn.QueryRow(ctx, `SELECT countIf(id = 'custom:gone' AND deleted_at IS NOT NULL AND is_active = 0), countIf(id = 'custom:none') FROM teams FINAL WHERE org_id = ?`, org).Scan(&gone, &none); err != nil {
		t.Fatal(err)
	}
	if gone != 1 || none != 0 {
		t.Errorf("deleted team kept deleted = %d (want 1), rows of an id with no row = %d (want 0)", gone, none)
	}
}
