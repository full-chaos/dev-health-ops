package rivermigrate

import (
	"reflect"
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
	want := postgresstore.QueryAPISessionReadFor("auth")
	read, err := queryAPISessionReadFor(env(nil), true)
	if err != nil || read == nil {
		t.Fatalf("flag on: %v %v", read, err)
	}
	if !reflect.DeepEqual(*read, riverstore.SessionReadGrant{Schema: want.Schema, Table: want.Table, Columns: want.Columns}) {
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

func TestQueryAPISessionReadEqualsTheAuthSessionReaderColumns(t *testing.T) {
	t.Parallel()
	read := postgresstore.QueryAPISessionReadFor("auth")
	want := authschema.SessionReaderColumns()
	if read.Table != want.Table || !reflect.DeepEqual(read.Columns, want.Columns) {
		t.Fatalf("query-api session read %s%v differs from auth-migrate's %s%v", read.Table, read.Columns, want.Table, want.Columns)
	}
}
