package docstools_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/docstools"
)

// The yaml.safe_load emulation: each want is repr(yaml.safe_load(doc)) of PyYAML 6.0.3 on Python 3.14.7, recorded once.
func TestYAMLLoadMatchesPyYAMLSafeLoad(t *testing.T) {
	for _, c := range []struct{ doc, want string }{
		{"a: 1", "{'a': 1}"},
		{"- 1e16\n- 1.0e+16\n- 123456789012345678.0\n- 0.0001\n- 0.00001\n- 1.5e-7\n- -0.0\n- 12345678901234567890\n- 1_0.5_0",
			"['1e16', 1e+16, 1.2345678901234568e+17, 0.0001, 1e-05, 1.5e-07, -0.0, 12345678901234567890, 10.5]"},
		{"x: 2026-01-01 10:00:00 -05", "{'x': datetime.datetime(2026, 1, 1, 10, 0, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400)))}"},
		{"x: 2026-01-01T10:00:00.1234567+00:00", "{'x': datetime.datetime(2026, 1, 1, 10, 0, 0, 123456, tzinfo=datetime.timezone.utc)}"},
		{"x: 2026-01-01 10:00:01", "{'x': datetime.datetime(2026, 1, 1, 10, 0, 1)}"},
		{"- yes\n- No\n- on\n- OFF\n- y\n- n", "[True, False, True, False, 'y', 'n']"},
		{"- 0o17\n- 0x_1F\n- -0b11\n- +017\n- 190:20:30\n- -1:30.5", "['0o17', 31, -3, 15, 685230, -90.5]"},
		{"- \"\\u00e9\\t\\x01\\\"q'\"\n- 'it''s'", `['é\t\x01"q\'', "it's"]`},
		{"{1: a, 1.0: b, true: c, 2026-01-01: d, null: e, ~: f}", "{1: 'c', datetime.date(2026, 1, 1): 'd', None: 'f'}"},
		{"- ! 12\n- ! \"12\"\n- ! |\n  1.5\n", "[12, 12, 1.5]"},
		{"x: [a, {b: c}]", "{'x': ['a', {'b': 'c'}]}"},
		{"", "None"},
		{"# only a comment\n", "None"},
		{"---\n", "None"},
	} {
		got, err := docstools.YAMLReprForTest(c.doc)
		if err != nil || got != c.want {
			t.Errorf("%q: got %q, %v; PyYAML %q", c.doc, got, err, c.want)
		}
	}
}

// Stricter than yaml.safe_load, on purpose: PyYAML accepts each of these, the Go loader refuses it (RISK-NOTES).
func TestYAMLLoadRefusesAnchorsMergeKeysAndTags(t *testing.T) {
	for _, doc := range []string{"- !!str 12", "- &a 1\n- *a", "a: &x {b: 1}\nc:\n  <<: *x", "<<: {a: 1}", "x: !custom 1", "? !!str k\n: 1"} {
		if got, err := docstools.YAMLReprForTest(doc); err == nil || !strings.HasPrefix(err.Error(), "RefusedYAML: ") {
			t.Errorf("%q: got %q, %v; want a RefusedYAML error", doc, got, err)
		}
	}
}

func TestYAMLLoadRaisesWherePyYAMLRaises(t *testing.T) {
	for doc, kind := range map[string]string{
		"=":                      "yaml.constructor.ConstructorError: ",
		"x: 0x_":                 "ValueError: invalid literal for int() with base 16: ''",
		"x: ! \".inf\\n\"":       "ValueError: could not convert string to float: '.inf\\n'",
		"x: 2026-01-01 23:00:60": "ValueError: second must be in 0..59, not 60",
		"x: 2026-01-01 23:60:00": "ValueError: minute must be in 0..59, not 60",
		"x: 0000-01-01":          "ValueError: year must be in 1..9999, not 0",
		"a: 1\n---\nb: 2":        "yaml.composer.ComposerError: ",
		"x: \x7f":                "yaml.reader.ReaderError: ",
		"- [a]: 1":               "yaml.constructor.ConstructorError: while constructing a mapping",
	} {
		got, err := docstools.YAMLReprForTest(doc)
		if err == nil || !strings.HasPrefix(err.Error(), kind) {
			t.Errorf("%q: got %q, %v; want an error starting %q", doc, got, err, kind)
		}
	}
}
