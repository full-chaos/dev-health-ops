package pgmigrate_test

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseHandTablesReadsColumnsAndSkipsConstraintsCommentsAndTemp(t *testing.T) {
	src := "stmt := `\n" +
		"CREATE TABLE IF NOT EXISTS public.widgets (\n" +
		"  id uuid PRIMARY KEY, -- the key, with a comma, in a comment\n" +
		"  org_id text NOT NULL,\n" +
		"  \"Quoted\" text,\n" +
		"  amount numeric(10, 2) DEFAULT 0,\n" +
		"  UNIQUE (org_id, id),\n" +
		"  CONSTRAINT widgets_fk FOREIGN KEY (org_id) REFERENCES orgs(id),\n" +
		"  CHECK (amount >= 0)\n" +
		")`\n" +
		"other := \"CREATE TEMP TABLE scratch (a int)\"\n" +
		"escaped := \"CREATE TABLE gadgets (id int, -- note\\n name text)\"\n" +
		"alter := \"ALTER TABLE public.widgets ADD COLUMN IF NOT EXISTS extra text, ADD COLUMN more int\"\n"
	got, err := parseHandTables("x_test.go", src)
	if err != nil {
		t.Fatal(err)
	}
	want := []handTable{
		{File: "x_test.go", Schema: "public", Table: "widgets", Columns: []string{"amount", "extra", "id", "more", "org_id", "quoted"}},
		{File: "x_test.go", Schema: "public", Table: "gadgets", Columns: []string{"id", "name"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v\nwant %#v", got, want)
	}
}

func TestParseHandTablesJoinsConcatenatedStringLiterals(t *testing.T) {
	src := "x := \"CREATE TABLE orgs (id uuid, \" +\n\t\"is_active boolean NOT NULL)\"\n" +
		"y := `CREATE TABLE things (a int, ` +\n\t`b int)`\n"
	got, err := parseHandTables("z_test.go", src)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !reflect.DeepEqual(got[0].Columns, []string{"id", "is_active"}) || !reflect.DeepEqual(got[1].Columns, []string{"a", "b"}) || len(got[0].Unreadable)+len(got[1].Unreadable) != 0 {
		t.Fatalf("got %#v", got)
	}
}

func TestParseHandTablesKeepsAColumnItCannotReadInsteadOfDroppingIt(t *testing.T) {
	got, err := parseHandTables("y_test.go", "CREATE TABLE t (id int, %s text, ...)")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0].Columns, []string{"id"}) || !reflect.DeepEqual(got[0].Unreadable, []string{"%s text", "..."}) {
		t.Fatalf("got %#v", got)
	}
	if _, err := parseHandTables("y_test.go", "CREATE TABLE t (id int"); err == nil || !strings.Contains(err.Error(), "unbalanced") {
		t.Fatalf("want an unbalanced-parenthesis error, got %v", err)
	}
}

func TestClassifyHandTableKeepsShellsApartFromTheBacklog(t *testing.T) {
	venue := "internal/storage/postgres/runtime_authorization_integration_test.go"
	if _, ok := probeVenues[venue]; !ok {
		t.Fatalf("test premise: %s must be a probe venue", venue)
	}
	other := "internal/somewhere/else_integration_test.go"
	for _, c := range []struct {
		name     string
		file     string
		columns  []string
		invented []string
		want     string
	}{
		{"venue shell with a probe column", venue, []string{"id", "state"}, []string{"state"}, "PROBE"},
		{"venue shell that only omits columns", venue, []string{"id"}, nil, "PROBE"},
		{"venue shell at the size limit", venue, []string{"id", "a", "b"}, nil, "PROBE"},
		{"venue table past the shell size that invents", venue, []string{"id", "a", "b", "c"}, []string{"c"}, "INVENTED"},
		{"venue table past the shell size that omits", venue, []string{"id", "a", "b", "c"}, nil, "SUBSET"},
		{"same shell outside a venue that omits", other, []string{"id"}, nil, "SUBSET"},
		{"same shell outside a venue that invents", other, []string{"id", "state"}, []string{"state"}, "INVENTED"},
	} {
		if got := classifyHandTable(c.file, c.columns, c.invented); got != c.want {
			t.Errorf("%s: classifyHandTable = %q, want %q", c.name, got, c.want)
		}
	}
}
