package routing_test

import (
	"math"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/routing"
)

// The tools' output depends on how PyYAML's safe_load builds a value; each row is what Python prints for
// str(yaml.safe_load(text)["v"]) (python3 -c, PyYAML 6.0.1).
func TestValuesLoadTheWayPyYAMLSafeLoadBuildsThem(t *testing.T) {
	for text, want := range map[string]string{
		"v: true":                 "True",
		"v: yes":                  "True",
		"v: On":                   "True",
		"v: off":                  "False",
		"v: 'true'":               "true",
		"v: \"yes\"":              "yes",
		"v: !!str true":           "true",
		"v:":                      "None",
		"v: ~":                    "None",
		"v: 010":                  "8",
		"v: 0x1F":                 "31",
		"v: 1_000":                "1000",
		"v: 190:20:30":            "685230",
		"v: 1.0":                  "1.0",
		"v: 1e3":                  "1e3",
		"v: 1.5e+3":               "1500.0",
		"v: 100000000000000000.0": "1e+17",
		"v: 0.00001":              "1e-05",
		"v: .inf":                 "inf",
		"v: 2026-10-20":           "2026-10-20",
		"v: [a, 1, true]":         "['a', 1, True]",
		"v: {a: 1, b: null}":      "{'a': 1, 'b': None}",
		"v: |\n  yes\n":           "yes\n",
		"v: it's":                 "it's",
	} {
		doc, err := routing.LoadYAML([]byte(text), false)
		if err != nil {
			t.Errorf("%q: %v", text, err)
			continue
		}
		if got := routing.PyStr(routing.Lookup(doc, "v")); got != want {
			t.Errorf("%q: str() is %q, want %q", text, got, want)
		}
	}
	if got := routing.PyRepr("it's"); got != `"it's"` {
		t.Errorf("repr %s", got)
	}
	if got := routing.PyRepr(""); got != "''" {
		t.Errorf("repr %s", got)
	}
}

func TestMergeKeysAndDuplicateKeysLoadAsPyYAMLDoes(t *testing.T) {
	text := "base: &b {x: 1, y: 2}\nother: &o {y: 3, z: 4}\nv:\n  <<: [*b, *o]\n  z: 5\n  x: 6\n  x: 7\n"
	doc, err := routing.LoadYAML([]byte(text), false)
	if err != nil {
		t.Fatal(err)
	}
	if got := routing.PyStr(routing.Lookup(doc, "v")); got != "{'y': 2, 'z': 5, 'x': 7}" {
		t.Fatalf("merged %s", got)
	}
	if got := routing.PyStr(routing.MapKeys(doc)); got != "['base', 'other', 'v']" {
		t.Fatalf("keys %s", got)
	}
}

func TestLocalTagsAreRefusedBySafeLoadAndReadByTheComposeLoader(t *testing.T) {
	text := "services:\n  web:\n    build: !reset null\n    ports: !override\n      - \"1:2\"\n"
	if _, err := routing.LoadYAML([]byte(text), false); err == nil || !strings.Contains(err.Error(), "tag !reset (line 3) is refused") {
		t.Fatalf("safe load: %v", err)
	}
	doc, err := routing.LoadYAML([]byte(text), true)
	if err != nil {
		t.Fatal(err)
	}
	if got := routing.PyStr(routing.Lookup(doc, "services", "web")); got != "{'build': 'null', 'ports': ['1:2']}" {
		t.Fatalf("compose load %s", got)
	}
}

func TestDocumentCount(t *testing.T) {
	if doc, err := routing.LoadYAML([]byte(""), false); err != nil || doc != nil {
		t.Fatalf("empty: %v %v", doc, err)
	}
	if _, err := routing.LoadYAML([]byte("a: 1\n---\nb: 2\n"), false); err == nil {
		t.Fatal("two documents loaded as one")
	}
	if doc, err := routing.LoadYAML([]byte("v: .nan"), false); err != nil || !math.IsNaN(routing.Lookup(doc, "v").(float64)) {
		t.Fatalf("nan: %v %v", doc, err)
	}
}

func TestPyEscapeIsPythonsReEscape(t *testing.T) {
	// python3 -c 'import re; print(re.escape("a-b.c/d_e{f}[g]h+i*j?k|l^m$n&o~p#q r"))'
	if got := routing.PyEscape("a-b.c/d_e{f}[g]h+i*j?k|l^m$n&o~p#q r"); got != `a\-b\.c/d_e\{f\}\[g\]h\+i\*j\?k\|l\^m\$n\&o\~p\#q\ r` {
		t.Fatalf("re.escape %s", got)
	}
}

// A plain timestamp scalar loads as PyYAML's datetime.date / datetime.datetime; each row is str() and repr() as
// Python 3.12.3 + PyYAML 6.0.1 print them (python3 -c, yaml.safe_load(text)["v"]).
func TestPlainTimestampsLoadAsPyYAMLBuildsThem(t *testing.T) {
	for _, c := range []struct{ text, str, repr string }{
		{"v: 2002-12-14", "2002-12-14", "datetime.date(2002, 12, 14)"},
		{"v: 2002-1-1 1:02:03", "2002-01-01 01:02:03", "datetime.datetime(2002, 1, 1, 1, 2, 3)"},
		{"v: 2001-12-14t21:59:43.10-05:00", "2001-12-14 21:59:43.100000-05:00", "datetime.datetime(2001, 12, 14, 21, 59, 43, 100000, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400)))"},
		{"v: 2001-12-14 21:59:43.10 -5", "2001-12-14 21:59:43.100000-05:00", "datetime.datetime(2001, 12, 14, 21, 59, 43, 100000, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400)))"},
		{"v: 2001-12-15T02:59:43.1Z", "2001-12-15 02:59:43.100000+00:00", "datetime.datetime(2001, 12, 15, 2, 59, 43, 100000, tzinfo=datetime.timezone.utc)"},
		{"v: 2001-12-14 21:59:43.1234567", "2001-12-14 21:59:43.123456", "datetime.datetime(2001, 12, 14, 21, 59, 43, 123456)"},
		{"v: 2001-12-14 21:59:00", "2001-12-14 21:59:00", "datetime.datetime(2001, 12, 14, 21, 59)"},
		{"v: 2001-12-14 21:59:00+00:00", "2001-12-14 21:59:00+00:00", "datetime.datetime(2001, 12, 14, 21, 59, tzinfo=datetime.timezone.utc)"},
		{"v: 2001-12-14 21:59:43+05:30", "2001-12-14 21:59:43+05:30", "datetime.datetime(2001, 12, 14, 21, 59, 43, tzinfo=datetime.timezone(datetime.timedelta(seconds=19800)))"},
		{"v: '2002-12-14'", "2002-12-14", "'2002-12-14'"},
		{"v: 2002-12-14x", "2002-12-14x", "'2002-12-14x'"},
		{"v: [2002-12-14, a]", "[datetime.date(2002, 12, 14), 'a']", "[datetime.date(2002, 12, 14), 'a']"},
		{"v: {2002-12-14: a}", "{datetime.date(2002, 12, 14): 'a'}", "{datetime.date(2002, 12, 14): 'a'}"},
	} {
		doc, err := routing.LoadYAML([]byte(c.text), false)
		if err != nil {
			t.Errorf("%q: %v", c.text, err)
			continue
		}
		v := routing.Lookup(doc, "v")
		if got := routing.PyStr(v); got != c.str {
			t.Errorf("%q: str() is %q, want %q", c.text, got, c.str)
		}
		if got := routing.PyRepr(v); got != c.repr {
			t.Errorf("%q: repr() is %q, want %q", c.text, got, c.repr)
		}
	}
}
