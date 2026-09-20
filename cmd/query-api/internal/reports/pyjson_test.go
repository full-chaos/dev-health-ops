package reports

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func sameJSONValue(t *testing.T, a, b string) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal([]byte(a), &x); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(b), &y); err != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

// Expected values come from running Python's json.loads followed by a compact
// json.dumps (ensure_ascii=False) on each stored document (CPython 3.14.7).
var jsonCells = []struct{ stored, want string }{
	{"{}", "{}"},
	{"[]", "[]"},
	{"{\"a\":1}", "{\"a\":1}"},
	{"{ \"a\" : 1 , \"b\" : [ 1 , 2 , { \"c\" : null } ] }", "{\"a\":1,\"b\":[1,2,{\"c\":null}]}"},
	{"{\"a\":1,\"b\":2,\"a\":3}", "{\"a\":3,\"b\":2}"},
	{"{\"b\":1,\"a\":2,\"b\":{\"x\":1,\"x\":2}}", "{\"b\":{\"x\":2},\"a\":2}"},
	{"[{\"k\":1,\"k\":2},{\"k\":3}]", "[{\"k\":2},{\"k\":3}]"},
	{"{\"s\":\"<a href=\\\"x\\\">&</a>\"}", "{\"s\":\"<a href=\\\"x\\\">&</a>\"}"},
	{"{\"u\":\"\\u00e9\\u4e2d\\ud83d\\ude00\"}", "{\"u\":\"\u00e9\u4e2d\U0001f600\"}"},
	{"{\"n\":[1,-2,3.5,0.25,10000000000000000000,true,false,null]}", "{\"n\":[1,-2,3.5,0.25,10000000000000000000,true,false,null]}"},
	{"null", "null"},
	{"\"text\"", "\"text\""},
	{"3", "3"},
	{"true", "true"},
	{"[[],{}]", "[[],{}]"},
	{"{\"\":1}", "{\"\":1}"},
	{"{\"a\":{\"b\":{\"c\":{\"d\":[]}}}}", "{\"a\":{\"b\":{\"c\":{\"d\":[]}}}}"},
	{"{\"k\":\"line\\nbreak\\ttab\\\\slash\"}", "{\"k\":\"line\\nbreak\\ttab\\\\slash\"}"},
}

func TestJSONValue_MatchesPythonCells(t *testing.T) {
	for _, c := range jsonCells {
		stored := c.stored
		got, err := jsonValue(&stored)
		if err != nil {
			t.Errorf("jsonValue(%s): %v", c.stored, err)
			continue
		}
		want := c.want
		if want == "null" {
			if !got.IsNull() {
				t.Errorf("jsonValue(%s) = %s, want null", c.stored, got)
			}
			continue
		}
		if strings.Contains(c.stored, `\u`) {
			// The expected text prints non-ASCII characters where the stored
			// text keeps its escapes; both decode to the same value.
			if !sameJSONValue(t, string(got), want) {
				t.Errorf("jsonValue(%s) = %s, want %s", c.stored, got, want)
			}
			continue
		}
		if string(got) != want {
			t.Errorf("jsonValue(%s) = %s, want %s", c.stored, got, want)
		}
	}
}

func TestJSONValue_DeepNestingMatchesPythonBoundary(t *testing.T) {
	for _, depth := range []int{901, 5000, 65199} {
		doc := strings.Repeat("[", depth) + strings.Repeat("]", depth)
		got, err := jsonValue(&doc)
		if err != nil || string(got) != doc {
			t.Errorf("depth %d: err=%v", depth, err)
		}
	}
}

func TestJSONValue_SQLNullAndInvalidText(t *testing.T) {
	got, err := jsonValue(nil)
	if err != nil || !got.IsNull() {
		t.Fatalf("SQL NULL = %s, %v; want null", got, err)
	}
	for _, bad := range []string{"", "{", `{"a":}`, `{"a":1} x`, `[1,]`, `"\x"`, `"\u12"`, `"\u12zz"`, "\"a\tb\"", `01`, `[1 2]`, `{"a" 1}`, `{1:2}`, `tru`, `-`, `1.`, `"open`, `{"a":1,}`, strings.Repeat("[", 65200) + strings.Repeat("]", 65200)} {
		text := bad
		if _, err := jsonValue(&text); err == nil {
			t.Errorf("jsonValue(%.40q) accepted invalid text", bad)
		}
	}
}

// Expected values come from running Python's json.loads and json.dumps
// (ensure_ascii=True): a lone surrogate survives, and object keys are
// compared by their decoded text.
func TestJSONValue_SurrogatesAndEscapedKeys(t *testing.T) {
	for _, c := range []struct{ stored, want string }{
		{`{"s":"\ud800"}`, `{"s":"\ud800"}`},
		{`{"k":"\ud83d"}`, `{"k":"\ud83d"}`},
		{`{"a":1,"\u0061":2}`, `{"a":2}`},
		{`{"\ud800":1,"\ud800":2}`, `{"\ud800":2}`},
		{`{"\ud800":1,"\ud801":2}`, `{"\ud800":1,"\ud801":2}`},
		{`{"\ud83d\ude00":1,"\ud83d\ude00":2}`, `{"\ud83d\ude00":2}`},
	} {
		stored := c.stored
		got, err := jsonValue(&stored)
		if err != nil || string(got) != c.want {
			t.Errorf("jsonValue(%s) = %s, %v; want %s", c.stored, got, err, c.want)
		}
	}
}
