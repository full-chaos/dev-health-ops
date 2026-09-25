package writeproof

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var epoch = time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)

func mustValue(t *testing.T, n *Normalizer, v any) any {
	t.Helper()
	out, err := n.Value(v)
	if err != nil {
		t.Fatalf("Value(%#v): %v", v, err)
	}
	return out
}

func canonical(t *testing.T, v any) string {
	t.Helper()
	encoded, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return strings.NewReplacer(`\u003c`, "<", `\u003e`, ">").Replace(string(encoded))
}

func TestGeneratedIdsAreMaskedAndNumberedByFirstAppearance(t *testing.T) {
	n := NewNormalizer(nil, "run-x", epoch)
	got := canonical(t, mustValue(t, n, []any{
		"a1111111-1111-4111-8111-111111111111",
		"b2222222-2222-4222-8222-222222222222",
		"A1111111-1111-4111-8111-111111111111", // same id, upper case
		"prefix a1111111-1111-4111-8111-111111111111 suffix",
	}))
	want := `["<uuid#1>","<uuid#2>","<uuid#1>","prefix <uuid#1> suffix"]`
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestANumberedIdDoesNotDependOnMapIterationOrder(t *testing.T) {
	// Twenty runs: Go randomizes map iteration, so a normalizer that numbered in
	// range order would produce more than one output.
	seen := map[string]bool{}
	for i := 0; i < 20; i++ {
		n := NewNormalizer(nil, "", epoch)
		seen[canonical(t, mustValue(t, n, map[string]any{
			"b": "22222222-2222-4222-8222-222222222222",
			"a": "11111111-1111-4111-8111-111111111111",
			"c": "33333333-3333-4333-8333-333333333333",
		}))] = true
	}
	if len(seen) != 1 {
		t.Fatalf("normalization is not deterministic: %d distinct outputs", len(seen))
	}
}

func TestKeptIdsAndTheRunTagAndTimes(t *testing.T) {
	fixed := "0000feed-0000-4000-8000-000000000001"
	n := NewNormalizer([]string{fixed}, "gwc-wp-abc", epoch)
	got := mustValue(t, n, map[string]any{
		"fixed":  strings.ToUpper(fixed),
		"name":   "report gwc-wp-abc one",
		"now":    epoch.Add(3 * time.Minute),
		"seeded": time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC),
	}).(map[string]any)
	if got["fixed"] != fixed {
		t.Errorf("a kept id was masked or altered: %v", got["fixed"])
	}
	if got["name"] != "report <run> one" {
		t.Errorf("the run tag survived: %v", got["name"])
	}
	if got["now"] != "<now>" {
		t.Errorf("a timestamp near now survived: %v", got["now"])
	}
	if got["seeded"] != "2026-01-02T03:04:05.000000006Z" {
		t.Errorf("a seeded instant far from now must stay exact, got %v", got["seeded"])
	}
	// The window is a boundary, not a suggestion.
	if v := mustValue(t, n, epoch.Add(nowWindow+time.Second)); v == "<now>" {
		t.Errorf("a time %s past the window was masked", time.Second)
	}
}

func TestJSONBytesAreComparedAsATreeAndOpaqueBytesByContent(t *testing.T) {
	a := mustValue(t, NewNormalizer(nil, "", epoch), []byte(`{"b": 1, "a": "11111111-1111-4111-8111-111111111111"}`))
	b := mustValue(t, NewNormalizer(nil, "", epoch), []byte(`{"a":"22222222-2222-4222-8222-222222222222","b":1}`))
	if canonical(t, a) != canonical(t, b) {
		t.Fatalf("key order or a differing generated id changed the normalized json: %s vs %s", canonical(t, a), canonical(t, b))
	}
	x := mustValue(t, NewNormalizer(nil, "", epoch), []byte{0xff, 0xfe})
	y := mustValue(t, NewNormalizer(nil, "", epoch), []byte{0xff, 0xfd})
	if canonical(t, x) == canonical(t, y) {
		t.Fatal("opaque bytes that differ compared equal")
	}
}

func TestBigIntegersSurviveNormalization(t *testing.T) {
	n := NewNormalizer(nil, "", epoch)
	got := canonical(t, mustValue(t, n, []byte(`{"n": 9007199254740993}`)))
	if !strings.Contains(got, "9007199254740993") {
		t.Fatalf("an integer beyond 2^53 was rounded: %s", got)
	}
}

func TestBinaryUUIDColumnIsMaskedLikeItsTextForm(t *testing.T) {
	id := [16]byte{0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x41, 0x11, 0x81, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11}
	n := NewNormalizer(nil, "", epoch)
	first := mustValue(t, n, id)
	second := mustValue(t, n, "11111111-1111-4111-8111-111111111111")
	if first != second || first != "<uuid#1>" {
		t.Fatalf("binary and text forms of one id must mask to the same token: %v %v", first, second)
	}
}

func TestTheDigestIsAFunctionOfTheEffectsAndTheCaseName(t *testing.T) {
	base := Effects{Case: "c", Response: "r", Tables: map[string][]any{"t": {"x"}}}
	digest := func(e Effects) string {
		d, err := e.Digest()
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	same := Effects{Case: "c", Response: "r", Tables: map[string][]any{"t": {"x"}}}
	if digest(base) != digest(same) || !strings.HasPrefix(digest(base), "sha256:") {
		t.Fatal("equal effects must give equal digests")
	}
	for name, changed := range map[string]Effects{
		"case":     {Case: "d", Response: "r", Tables: map[string][]any{"t": {"x"}}},
		"response": {Case: "c", Response: "s", Tables: map[string][]any{"t": {"x"}}},
		"row":      {Case: "c", Response: "r", Tables: map[string][]any{"t": {"y"}}},
		"label":    {Case: "c", Response: "r", Tables: map[string][]any{"u": {"x"}}},
		"extra":    {Case: "c", Response: "r", Tables: map[string][]any{"t": {"x", "x"}}},
	} {
		if digest(changed) == digest(base) {
			t.Errorf("changing the %s did not change the digest", name)
		}
	}
}

func TestACaseThatCouldProveNothingIsRefused(t *testing.T) {
	good := Case{Name: "n", Operation: "op", VariablesJSON: "{}", Seeder: noSeeder{}, Tables: []Table{{Label: "t", SQL: "select 1"}}, BaselineDigest: "sha256:abc"}
	if err := good.Validate(); err != nil {
		t.Fatalf("a well-formed case was refused: %v", err)
	}
	for name, mutate := range map[string]func(*Case){
		"no name":           func(c *Case) { c.Name = " " },
		"no operation":      func(c *Case) { c.Operation = "" },
		"no variables":      func(c *Case) { c.VariablesJSON = "" },
		"no seeder":         func(c *Case) { c.Seeder = nil },
		"no tables":         func(c *Case) { c.Tables = nil },
		"no baseline":       func(c *Case) { c.BaselineDigest = "" },
		"blank baseline":    func(c *Case) { c.BaselineDigest = "  \t" },
		"table with no sql": func(c *Case) { c.Tables = []Table{{Label: "t"}} },
		"duplicate label":   func(c *Case) { c.Tables = []Table{{Label: "t", SQL: "a"}, {Label: "t", SQL: "b"}} },
	} {
		c := good
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: a case that could not prove anything was accepted", name)
		}
	}
}
