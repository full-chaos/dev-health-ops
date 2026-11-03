//go:build integration

package pgmigrate_test

import (
	"context"
	"log/slog"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

const plantedSetting = "dho.planted_walk_setting"

// plantedSettingReader is a revision planted above the baseline that records what it reads of plantedSetting; with
// fail it then fails, so its walk is rolled back.
func plantedSettingReader(t *testing.T, fail bool) {
	t.Helper()
	body := "-- Revision 0163 (down_revision 0162), planted test-only.\n" +
		"CREATE TABLE planted_setting_seen (value text);\n" +
		"INSERT INTO planted_setting_seen VALUES (current_setting('" + plantedSetting + "', true));\n"
	if fail {
		body += "SELECT 1/0;\n"
	}
	pgmigrate.UsePlantedChain(t, fstest.MapFS{
		"sql/0163_plant_setting.sql":      {Data: []byte(body)},
		"sql/down/0163_plant_setting.sql": {Data: []byte("DROP TABLE planted_setting_seen;\n")},
	})
}

func sessionValue(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	var value *string
	if err := conn.QueryRow(context.Background(), "SELECT current_setting($1, true)", plantedSetting).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value == nil {
		return ""
	}
	return *value
}

// The walk sets its settings for its own transaction only: every revision of the walk reads the walk's value -- over a
// session value and over a database default -- and after the walk, applied or rolled back, the session reads nothing
// the walk set.
func TestWalkSettingsAreSetForTheWalksTransactionOnly(t *testing.T) {
	ctx := context.Background()
	instance := sharedInstance(t)
	admin := connect(t, instance.URI)
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	settings := pgmigrate.WalkSettings{plantedSetting: "walk"}
	at := func(t *testing.T) (*pgx.Conn, string) {
		t.Helper()
		database := scratchDatabase(t, admin)
		conn := connect(t, databaseURI(t, instance.URI, database))
		if _, err := pgmigrate.Upgrade(ctx, conn, baseline, nil); err != nil {
			t.Fatal(err)
		}
		return conn, database
	}
	seen := func(t *testing.T, conn *pgx.Conn) string {
		t.Helper()
		var value string
		if err := conn.QueryRow(ctx, "SELECT value FROM planted_setting_seen").Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	t.Run("applied", func(t *testing.T) {
		plantedSettingReader(t, false)
		conn, _ := at(t)
		chain, err := pgmigrate.LoadChain()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pgmigrate.UpgradeLoggedWithSettings(ctx, conn, baseline, chain, slog.New(slog.DiscardHandler), settings); err != nil {
			t.Fatal(err)
		}
		if got := seen(t, conn); got != "walk" {
			t.Errorf("the revision read %q, want the walk's value", got)
		}
		if got := sessionValue(t, conn); got != "" {
			t.Errorf("after the walk the session reads %s = %q; the walk sets it for its transaction only", plantedSetting, got)
		}
	})
	t.Run("rolled back", func(t *testing.T) {
		plantedSettingReader(t, true)
		conn, _ := at(t)
		chain, err := pgmigrate.LoadChain()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pgmigrate.UpgradeLoggedWithSettings(ctx, conn, baseline, chain, slog.New(slog.DiscardHandler), settings); err == nil {
			t.Fatal("a failing revision applied")
		}
		if got := sessionValue(t, conn); got != "" {
			t.Errorf("after a rolled-back walk the session reads %s = %q", plantedSetting, got)
		}
	})
	t.Run("over a session value and a database default", func(t *testing.T) {
		plantedSettingReader(t, false)
		conn, database := at(t)
		if _, err := admin.Exec(ctx, "ALTER DATABASE "+database+" SET "+plantedSetting+" = 'database'"); err != nil {
			t.Fatal(err)
		}
		conn = connect(t, databaseURI(t, instance.URI, database))
		if _, err := conn.Exec(ctx, "SELECT set_config($1, 'session', false)", plantedSetting); err != nil {
			t.Fatal(err)
		}
		chain, err := pgmigrate.LoadChain()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pgmigrate.UpgradeLoggedWithSettings(ctx, conn, baseline, chain, slog.New(slog.DiscardHandler), settings); err != nil {
			t.Fatal(err)
		}
		if got := seen(t, conn); got != "walk" {
			t.Errorf("the revision read %q, want the walk's value over the session and database values", got)
		}
		if got := sessionValue(t, conn); got != "session" {
			t.Errorf("after the walk the session reads %q, want its own value back", got)
		}
	})
}
