package pyargparse

import (
	"strings"
	"testing"
)

const canaryDSN = "postgres://canary_user:canary_pass@canary.invalid/x"

var canaryBytes = []string{"canary_user", "canary_pass", "canary.invalid", "postgres://", "canary_scope"}

func noCanary(t *testing.T, text string) {
	t.Helper()
	for _, c := range canaryBytes {
		if strings.Contains(text, c) {
			t.Errorf("error text holds typed bytes %q: %q", c, text)
		}
	}
}

// Every error that could hold typed bytes must name only the parser's own option strings.
func TestErrorsNeverHoldTypedBytes(t *testing.T) {
	specs := []Spec{
		{Strs: []string{"-h", "--help"}, Dest: "help", Kind: OptHelp},
		{Strs: []string{"--service"}, Dest: "service", Kind: OptValue},
		{Strs: []string{"--scope"}, Dest: "scope", Kind: OptValue},
		{Strs: []string{"-q", "--quiet"}, Dest: "quiet", Kind: OptFlag},
	}
	p := New(specs)
	root := func(args []string) (*Error, *Parsed) { r, e := p.ParseRoot(args, none); return e, r }
	plain := func(args []string) (*Error, *Parsed) { r, e := p.Parse(args, none); return e, r }
	withPos := func(args []string) (*Error, *Parsed) { r, _, e := p.ParseWithPositional(args, none); return e, r }

	cases := map[string]struct {
		run  func([]string) (*Error, *Parsed)
		args []string
	}{
		"root unknown dsn option":     {root, []string{"--" + canaryDSN, "cmd"}},
		"root unknown option value":   {root, []string{"--canary_pass=canary_scope", "cmd"}},
		"root ambiguous with value":   {root, []string{"--s=canary_pass", "cmd"}},
		"root ignored explicit":       {root, []string{"--help=canary_pass", "cmd"}},
		"root short ignored explicit": {root, []string{"-q=canary_pass", "cmd"}},
		"root short chain tail":       {root, []string{"-qcanary_pass-canary_user=x", "cmd"}},
		"parse ambiguous with value":  {plain, []string{"--s=canary_pass"}},
		"parse ignored explicit":      {plain, []string{"--quiet=canary_pass"}},
		"parse positional leftovers":  {plain, []string{canaryDSN}},
		"parse unknown option":        {plain, []string{"--canary_pass=canary_scope", canaryDSN}},
		"positional ambiguous":        {withPos, []string{"id", "--s=canary_pass"}},
		"positional ignored explicit": {withPos, []string{"id", "--help=canary_pass"}},
		"positional leftovers":        {withPos, []string{"id", canaryDSN, "--canary_pass=canary_scope"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err, parsed := tc.run(tc.args)
			if err != nil {
				noCanary(t, err.Msg)
				return
			}
			// A leftover is reported by the caller from Parsed.Unrecognized: the count is all it may print.
			if parsed == nil || len(parsed.Unrecognized) == 0 {
				t.Fatalf("expected an error or leftovers for %q", tc.args)
			}
			noCanary(t, UnrecognizedMessage(len(parsed.Unrecognized)))
		})
	}
}

func TestUnrecognizedStatesTheCountOnly(t *testing.T) {
	p := New([]Spec{{Strs: []string{"--org"}, Dest: "org", Kind: OptValue}})
	_, err := p.ParseRoot([]string{"--bogus_canary_pass", "cmd"}, none)
	if err == nil || err.Msg != "unrecognized arguments (1)" {
		t.Fatalf("got %v", err)
	}
	if got := UnrecognizedMessage(3); got != "unrecognized arguments (3)" {
		t.Fatalf("got %q", got)
	}
}
