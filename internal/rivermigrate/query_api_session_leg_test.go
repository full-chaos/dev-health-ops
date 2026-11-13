package rivermigrate

import (
	"reflect"
	"strconv"
	"testing"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

func TestQueryAPISessionReadFollowsTheSessionsFlagAndTheAuthSchema(t *testing.T) {
	t.Parallel()
	env := func(values map[string]string) func(string) (string, bool) {
		return func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	}
	if read, err := queryAPISessionReadFor(env(nil), false); read != nil || err != nil {
		t.Fatalf("flag off: %v %v", read, err)
	}
	want := postgresstore.QueryAPISessionReadFor("auth", false)
	read, err := queryAPISessionReadFor(env(nil), true)
	if err != nil || read == nil {
		t.Fatalf("flag on: %v %v", read, err)
	}
	wantGrant := riverstore.SessionReadGrant{Schema: want.Schema, Table: want.Table, Columns: want.Columns}
	for _, table := range want.Also {
		wantGrant.Also = append(wantGrant.Also, riverstore.SessionReadTable{Table: table.Table, Columns: table.Columns})
	}
	if !reflect.DeepEqual(*read, wantGrant) {
		t.Fatalf("read %+v, want the declared session read %+v", *read, want)
	}
	read, err = queryAPISessionReadFor(env(map[string]string{"AUTH_DATABASE_SCHEMA": "acp"}), true)
	if err != nil || read.Schema != "acp" {
		t.Fatalf("custom schema: %+v %v", read, err)
	}
	if _, err := queryAPISessionReadFor(env(map[string]string{"AUTH_DATABASE_SCHEMA": "a; drop"}), true); err == nil {
		t.Fatal("an unusable auth schema name must be refused")
	}
}

// The three lists are one: auth-migrate's reader grant
// (authschema.SessionReaderReads), query-api's manifest
// (postgres.QueryAPISessionReadFor) and this job's grant to the query-api role,
// in both states of the platform roles switch.
func TestQueryAPIAuthReadsEqualTheReaderColumnsInBothStates(t *testing.T) {
	t.Parallel()
	for _, owned := range []bool{false, true} {
		var reader [][2]any
		for _, grant := range authschema.SessionReaderReads(owned) {
			reader = append(reader, [2]any{grant.Table, grant.Columns})
		}
		var manifest [][2]any
		for _, table := range postgresstore.QueryAPISessionReadFor("auth", owned).Tables() {
			manifest = append(manifest, [2]any{table.Table, table.Columns})
		}
		read, err := queryAPISessionReadFor(func(key string) (string, bool) {
			if key == "AUTH_PLATFORM_ROLES_OWNED" {
				return strconv.FormatBool(owned), true
			}
			return "", false
		}, true)
		if err != nil || read == nil {
			t.Fatalf("owned %v: %v %v", owned, read, err)
		}
		var granted [][2]any
		for _, table := range read.Tables() {
			granted = append(granted, [2]any{table.Table, table.Columns})
		}
		if !reflect.DeepEqual(reader, manifest) || !reflect.DeepEqual(manifest, granted) {
			t.Errorf("owned %v: reader grant %v, query-api manifest %v, query-api grant %v", owned, reader, manifest, granted)
		}
		if want := 2 + map[bool]int{false: 0, true: 2}[owned]; len(granted) != want {
			t.Errorf("owned %v: %d auth tables, want %d", owned, len(granted), want)
		}
		// The read sessionrule.Revoked needs, written out: the row's and its web
		// session's revoke inputs, then the principal's password stamp.
		want := [][2]any{
			{"sessions", []string{"id", "principal_id", "revoked_at", "legacy_token_version", "created_at", "web_session_id"}},
			{"users", []string{"principal_id", "password_updated_at"}},
		}
		if !reflect.DeepEqual(granted[:min(2, len(granted))], want) {
			t.Errorf("owned %v: the query-api grant starts %v, want %v", owned, granted, want)
		}
	}
}

// The platform role read rides on the session read: owned with sessions off,
// or a malformed switch, is a configuration error.
func TestQueryAPIPlatformReadNeedsTheSessionReadAndAReadableSwitch(t *testing.T) {
	t.Parallel()
	env := func(value string) func(string) (string, bool) {
		return func(key string) (string, bool) { return value, key == "AUTH_PLATFORM_ROLES_OWNED" }
	}
	if _, err := queryAPISessionReadFor(env("true"), false); err == nil {
		t.Error("platform roles owned with the session read off must be refused")
	}
	if _, err := queryAPISessionReadFor(env("yes"), true); err == nil {
		t.Error("a malformed switch must be refused")
	}
}
