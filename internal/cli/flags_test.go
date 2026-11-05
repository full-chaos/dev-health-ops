package cli

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"
)

const flagCanaryDSN = "postgres://canary_user:canary_pass@canary.invalid/x"

var flagCanaryBytes = []string{"canary_user", "canary_pass", "canary.invalid", "postgres://", "canary_scope", "99999999999999999999"}

type echoingValue struct{}

func (echoingValue) String() string { return "" }
func (echoingValue) Set(s string) error {
	return fmt.Errorf("rejected %q for flag -org: %s", s, s)
}

func newTestFlags() *flag.FlagSet {
	flags := flag.NewFlagSet("dho test verb", flag.ContinueOnError)
	flags.String("name", "", "a string flag")
	flags.Int("count", 1, "an int flag")
	flags.Bool("apply", false, "a bool flag")
	flags.Duration("wait", time.Second, "a duration flag")
	flags.Var(echoingValue{}, "org", "a custom value flag")
	return flags
}

func TestParseFlagsNeverEchoesTypedInput(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		want    string
		wantNot []string
	}{
		{"unknown flag", []string{"-" + flagCanaryDSN}, "unknown flag\n", nil},
		{"unknown flag with value", []string{"--canary_pass=canary_scope"}, "unknown flag\n", nil},
		{"bad int", []string{"-count=" + flagCanaryDSN}, "invalid value for flag -count\n", nil},
		{"int out of range", []string{"-count=99999999999999999999"}, "invalid value for flag -count\n", nil},
		{"bad bool", []string{"-apply=canary_scope"}, "invalid value for flag -apply\n", nil},
		{"bad duration", []string{"-wait=canary_scope"}, "invalid value for flag -wait\n", nil},
		{"double dash form", []string{"--count=" + flagCanaryDSN}, "invalid value for flag -count\n", nil},
		{"missing argument", []string{"-name"}, "invalid value for flag -name\n", nil},
		{"custom value error that echoes", []string{"-org=" + flagCanaryDSN}, "invalid value for flag -org\n", nil},
		{"value that spoofs a flag name", []string{`-count="x" for flag -name: ` + flagCanaryDSN}, "invalid value for flag -count\n", []string{"-name"}},
		{"bad syntax", []string{"---canary_scope"}, "invalid flag input\n", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var se bytes.Buffer
			code, ok := ParseFlags(newTestFlags(), &se, tc.args)
			if ok || code != ExitUsage {
				t.Fatalf("code %d ok %v, want %d false", code, ok, ExitUsage)
			}
			if !strings.HasPrefix(se.String(), tc.want) {
				t.Fatalf("stderr %q does not start with %q", se.String(), tc.want)
			}
			for _, c := range flagCanaryBytes {
				if strings.Contains(se.String(), c) {
					t.Fatalf("stderr holds %q: %q", c, se.String())
				}
			}
			for _, bad := range tc.wantNot {
				if strings.Contains(se.String(), bad) {
					t.Fatalf("stderr holds %q: %q", bad, se.String())
				}
			}
			if !strings.Contains(se.String(), "-count") || !strings.Contains(se.String(), "an int flag") {
				t.Fatalf("stderr has no usage from the definitions: %q", se.String())
			}
		})
	}
}

func TestParseFlagsSuccessKeepsOutputAndValues(t *testing.T) {
	flags := newTestFlags()
	var kept bytes.Buffer
	flags.SetOutput(&kept)
	var se bytes.Buffer
	code, ok := ParseFlags(flags, &se, []string{"-count=3", "-apply"})
	if !ok || code != ExitOK {
		t.Fatalf("code %d ok %v", code, ok)
	}
	if got := flags.Lookup("count").Value.String(); got != "3" {
		t.Fatalf("count = %s", got)
	}
	if flags.Output() != &kept {
		t.Fatal("the set's output was not restored")
	}
	if se.Len() != 0 {
		t.Fatalf("stderr %q on success", se.String())
	}
}

func TestParseFlagsHelpPrintsUsageOnceAndExitsZero(t *testing.T) {
	for _, arg := range []string{"-h", "-help", "--help"} {
		var se bytes.Buffer
		code, ok := ParseFlags(newTestFlags(), &se, []string{arg})
		if ok || code != ExitOK {
			t.Fatalf("%s: code %d ok %v", arg, code, ok)
		}
		if strings.Count(se.String(), "Usage of dho test verb") != 1 || !strings.Contains(se.String(), "an int flag") {
			t.Fatalf("%s: usage %q", arg, se.String())
		}
	}
}

func TestParseFlagsUsesTheSetsOwnUsageText(t *testing.T) {
	flags := newTestFlags()
	var custom bytes.Buffer
	flags.Usage = func() { fmt.Fprint(&custom, "custom static usage\n") }
	var se bytes.Buffer
	if code, ok := ParseFlags(flags, &se, []string{"-count=" + flagCanaryDSN}); ok || code != ExitUsage {
		t.Fatalf("code %d ok %v", code, ok)
	}
	if strings.Count(custom.String(), "custom static usage") != 1 {
		t.Fatalf("custom usage printed %q", custom.String())
	}
	if se.String() != "invalid value for flag -count\n" {
		t.Fatalf("stderr %q", se.String())
	}
}

func TestParseFlagsErrorNeverEchoesTypedInput(t *testing.T) {
	for _, args := range [][]string{{"-" + flagCanaryDSN}, {"-count=" + flagCanaryDSN}, {"-org=" + flagCanaryDSN}, {"-name"}} {
		var se bytes.Buffer
		err := ParseFlagsError(newTestFlags(), &se, args)
		var usage *FlagUsageError
		if !errors.As(err, &usage) || ExitForVerbError(err) != ExitUsage {
			t.Fatalf("%v: err %v", args, err)
		}
		for _, c := range flagCanaryBytes {
			if strings.Contains(err.Error(), c) || strings.Contains(se.String(), c) {
				t.Fatalf("%v: holds %q: err=%q stderr=%q", args, c, err.Error(), se.String())
			}
		}
	}
}

func TestParseFlagsErrorSuccessAndHelp(t *testing.T) {
	var se bytes.Buffer
	if err := ParseFlagsError(newTestFlags(), &se, []string{"-count=2"}); err != nil {
		t.Fatalf("err %v", err)
	}
	err := ParseFlagsError(newTestFlags(), &se, []string{"-h"})
	if !errors.Is(err, flag.ErrHelp) || ExitForVerbError(err) != ExitOK {
		t.Fatalf("err %v", err)
	}
	if !strings.Contains(se.String(), "an int flag") {
		t.Fatalf("usage %q", se.String())
	}
}
