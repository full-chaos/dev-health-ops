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
	if _, err := routing.LoadYAML([]byte(text), false); err == nil || !strings.Contains(err.Error(), "could not determine a constructor for the tag '!reset'") {
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
