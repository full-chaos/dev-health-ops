package venueoracle

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestDecodedJSONColumnsNamesEveryJSONType pins the guard TableRows runs
// on each result: every json and jsonb column (and their arrays) is named,
// and a column cast ::text (Postgres type text, OID 25) is not.
func TestDecodedJSONColumnsNamesEveryJSONType(t *testing.T) {
	fields := []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: 25},
		{Name: "changes", DataTypeOID: 114},
		{Name: "settings", DataTypeOID: 3802},
		{Name: "tags", DataTypeOID: 199},
		{Name: "scopes", DataTypeOID: 3807},
		{Name: "count", DataTypeOID: 20},
	}
	want := []string{"changes (json)", "settings (jsonb)", "tags (json[])", "scopes (jsonb[])"}
	if got := decodedJSONColumns(fields); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got := decodedJSONColumns([]pgconn.FieldDescription{{Name: "changes", DataTypeOID: 25}}); got != nil {
		t.Fatalf("a ::text column was named: %v", got)
	}
}

// TestCompareJSONSpacingGapFailsWhenItShould pins each failure the named
// spacing-gap check exists for, and its pass on the gap alone.
func TestCompareJSONSpacingGapFailsWhenItShould(t *testing.T) {
	python := []string{`{"a": 1, "b": "` + "\\u00e9" + `"}`, "<null>"}
	cases := map[string]struct {
		goTexts []string
		fails   bool
	}{
		"the spacing gap alone":         {[]string{`{"a":1,"b":"é"}`, "<null>"}, false},
		"indented, same values":         {[]string{"{\n  \"a\": 1,\n  \"b\": \"é\"\n}", "<null>"}, false},
		"gap closed":                    {[]string{python[0], "<null>"}, true},
		"a value differs":               {[]string{`{"a":2,"b":"é"}`, "<null>"}, true},
		"key order differs":             {[]string{`{"b":"é","a":1}`, "<null>"}, true},
		"null where Python has a value": {[]string{"<null>", "<null>"}, true},
		"row count differs":             {[]string{`{"a":1,"b":"é"}`}, true},
	}
	for name, test := range cases {
		probe := &recorder{}
		CompareJSONSpacingGap(probe, name, python, test.goTexts)
		if (len(probe.errors) > 0) != test.fails {
			t.Errorf("%s: errors %q, want failure=%v", name, probe.errors, test.fails)
		}
	}
	empty := &recorder{}
	CompareJSONSpacingGap(empty, "empty", nil, nil)
	if len(empty.errors) == 0 {
		t.Error("no rows compared must fail")
	}
}

type recorder struct{ errors []string }

func (r *recorder) Helper() {}

func (r *recorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}
