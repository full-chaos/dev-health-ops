package pyargparse

import (
	"strings"
	"testing"
)

var rootSpecs = []Spec{
	{Strs: []string{"--org"}, Dest: "org", Kind: OptValue},
	{Strs: []string{"--llm-provider"}, Dest: "provider", Kind: OptValue},
	{Strs: []string{"--llm-key"}, Dest: "key", Kind: OptValue},
	{Strs: []string{"-h", "--help"}, Dest: "help", Kind: OptHelp},
}

func none(*Spec, string) *Error { return nil }

// TestParseRootStopsAtTheFirstPositional pins what argparse 3.14 does with a
// subcommand: everything from the first positional on belongs to it. The
// arguments up to it are classified (a value, an unknown option, an ambiguous
// abbreviation); the ones after it are not looked at, so what would be an
// ambiguous or unknown option at the root is the subcommand's to judge.
func TestParseRootStopsAtTheFirstPositional(t *testing.T) {
	p := New(rootSpecs)
	got, err := p.ParseRoot([]string{"--org", "R", "cmd", "--llm", "x", "--bogus", "--org", "L"}, none)
	if err != nil || got.Values["org"] != "R" || strings.Join(got.Rest, " ") != "cmd --llm x --bogus --org L" {
		t.Fatalf("got %+v, %v", got, err)
	}
	// Before the command they are errors.
	if _, err := p.ParseRoot([]string{"--llm", "x", "cmd"}, none); err == nil || !strings.Contains(err.Msg, "ambiguous option: --llm") {
		t.Errorf("an ambiguous abbreviation before the command: %v", err)
	}
	if _, err := p.ParseRoot([]string{"--bogus", "cmd"}, none); err == nil || !strings.Contains(err.Msg, "unrecognized arguments: --bogus") {
		t.Errorf("an unknown option before the command: %v", err)
	}
	// A value that looks like an option is not a value.
	if _, err := p.ParseRoot([]string{"--org", "--llm-key", "x", "cmd"}, none); err == nil || !strings.Contains(err.Msg, "expected one argument") {
		t.Errorf("an option-like value: %v", err)
	}
	// The value of an option is not the command: `--org cmd` takes cmd.
	got, err = p.ParseRoot([]string{"--org", "cmd", "next"}, none)
	if err != nil || got.Values["org"] != "cmd" || strings.Join(got.Rest, " ") != "next" {
		t.Errorf("--org cmd next: %+v, %v", got, err)
	}
	// Help stops the parse where it stands.
	got, err = p.ParseRoot([]string{"-h", "--bogus"}, none)
	if err != nil || !got.Help {
		t.Errorf("-h --bogus: %+v, %v", got, err)
	}
}

func TestParseClassifiesEveryArgumentUpFront(t *testing.T) {
	// Parse (a leaf parser with no positionals) sees an ambiguous abbreviation
	// anywhere, as argparse does.
	if _, err := New(rootSpecs).Parse([]string{"--org", "R", "--llm", "x"}, none); err == nil {
		t.Error("an ambiguous abbreviation after other options was not refused")
	}
}
