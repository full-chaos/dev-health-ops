package cli

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// rootTree has a verb that lists every listable root flag, verbs that list none,
// a service that lists the log level, and a group.
func rootTree(calls *[]call, levels *[]bool) []Command {
	run := func(path string) func(context.Context, Env) int {
		return func(ctx context.Context, env Env) int {
			*calls = append(*calls, call{path: path, args: append([]string(nil), env.Args...)})
			if levels != nil {
				*levels = append(*levels, slog.Default().Enabled(ctx, slog.LevelDebug))
			}
			return ExitOK
		}
	}
	return []Command{
		{Name: "all", Summary: "lists every listable root flag", Kind: Verb, Run: run("all"),
			RootFlags: []RootFlag{RootLogLevel, RootLLMProvider, RootModel}},
		{Name: "org", Summary: "lists nothing", Kind: Verb, Run: run("org")},
		{Name: "none", Summary: "lists nothing", Kind: Verb, Run: run("none")},
		{Name: "svc", Summary: "service", Kind: Service, Run: run("svc"), RootFlags: []RootFlag{RootLogLevel},
			Children: []Command{{Name: "probe", Summary: "utility", Kind: Verb, Run: run("svc probe")}}},
		{Name: "grp", Summary: "group", Kind: Group, Children: []Command{
			{Name: "leaf", Summary: "leaf", Kind: Verb, Run: run("grp leaf")},
		}},
	}
}

func runRoot(t *testing.T, args ...string) (calls []call, levels []bool, code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	previous := slog.Default()
	defer slog.SetDefault(previous)
	slog.SetDefault(slog.New(slog.NewTextHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelInfo})))
	code = Execute(context.Background(), "dho", rootTree(&calls, &levels), Env{Args: args, Stdout: &out, Stderr: &errOut})
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		t.Fatalf("%q: the process logger was left at debug after Execute", args)
	}
	return calls, levels, code, out.String(), errOut.String()
}

func TestRootFlagsReachTheCommandsThatListThem(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want call
	}{
		{"every flag, in dev-hops's order", []string{"--model", "M", "-l", "L", "--org", "O", "--analytics-db", "A", "--db", "D", "--log-level", "WARNING", "all", "--extra", "x"},
			call{"all", []string{"--log-level=warn", "--db=D", "--analytics-db=A", "--org=O", "--llm-provider=L", "--model=M", "--extra", "x"}}},
		{"abbreviations and attached forms are canonical", []string{"--or=O", "--d", "D", "-lL", "-m=M", "all"},
			call{"all", []string{"--db=D", "--org=O", "--llm-provider=L", "--model=M"}}},
		{"the last typed value wins", []string{"--org", "R", "--org", "S", "org"}, call{"org", []string{"--org=S"}}},
		{"an empty value is a value", []string{"--org", "", "org"}, call{"org", []string{"--org="}}},
		{"where-it-acts flags reach every command, even one that lists nothing", []string{"--db", "D", "--org", "O", "org", "pos"}, call{"org", []string{"--db=D", "--org=O", "pos"}}},
		{"how-it-runs flags an unlisted command does not get", []string{"-l", "L", "-m", "M", "--log-level", "debug", "--org", "O", "none"}, call{"none", []string{"--org=O"}}},
		{"a flag typed after the command comes after the root one (and wins)", []string{"--org", "R", "org", "--org", "L"}, call{"org", []string{"--org=R", "--org", "L"}}},
		{"a service utility verb", []string{"--org", "O", "--log-level", "debug", "svc", "probe"}, call{"svc probe", []string{"--org=O"}}},
		{"a service that lists the level", []string{"--org", "O", "--log-level", "WARNING", "svc"}, call{"svc", []string{"--log-level=warn", "--org=O"}}},
		{"CRITICAL is handed over as itself (a service takes it above error)", []string{"--log-level", "CRITICAL", "svc"}, call{"svc", []string{"--log-level=critical"}}},
		{"an unknown level name is the default", []string{"--log-level", "nope", "svc"}, call{"svc", []string{"--log-level=info"}}},
		{"through a group", []string{"--org", "O", "--db", "D", "grp", "leaf", "x"}, call{"grp leaf", []string{"--db=D", "--org=O", "x"}}},
		{"after `--`", []string{"--org", "O", "--", "org"}, call{"org", []string{"--org=O"}}},
		{"llm-only options are accepted and dropped", []string{"--llm-api-key", "k", "--llm-base-url", "u", "--llm-concurrency", "3", "org"}, call{"org", []string{}}},
	}
	for _, c := range cases {
		calls, _, code, _, stderr := runRoot(t, c.args...)
		if code != ExitOK || len(calls) != 1 {
			t.Errorf("%s: %q: exit %d, %d calls, stderr %q", c.name, c.args, code, len(calls), stderr)
			continue
		}
		if calls[0].path != c.want.path || strings.Join(calls[0].args, "\x00") != strings.Join(c.want.args, "\x00") {
			t.Errorf("%s: %q:\n got  %s %q\n want %s %q", c.name, c.args, calls[0].path, calls[0].args, c.want.path, c.want.args)
		}
	}
}

func TestRootFlagsRefuseWhatArgparseRefuses(t *testing.T) {
	for _, args := range [][]string{
		{"--org"}, {"--org", "R"}, {"--org", "--db", "x", "org"}, {"--bogus", "org"}, {"--bogus", "x", "org"},
		{"--l", "x", "org"}, {"--llm-concurrency", "x", "org"}, {"--org", "-x", "org"}, {"--org", "R", "--", "nope"}, {"--org", "R", "nope"},
	} {
		calls, _, code, _, stderr := runRoot(t, args...)
		if code != ExitUsage || len(calls) != 0 || stderr == "" {
			t.Errorf("%q: exit %d, ran %v, stderr %q; want a usage error that ran nothing", args, code, calls, stderr)
		}
	}
}

func TestRootFlagsLeaveTheBuiltinsAlone(t *testing.T) {
	for _, args := range [][]string{{"--version"}, {"version"}, {"--help"}, {"-h"}, {"help"}} {
		if _, _, code, stdout, _ := runRoot(t, args...); code != ExitOK || stdout == "" {
			t.Errorf("%q: exit %d, stdout %q", args, code, stdout)
		}
	}
	// Help after root flags is the root help; it runs nothing.
	for _, args := range [][]string{{"--org", "R", "--help"}, {"--org", "R", "-h"}, {"--org", "R", "help"}} {
		calls, _, code, stdout, _ := runRoot(t, args...)
		if code != ExitOK || len(calls) != 0 || !strings.Contains(stdout, "commands:") {
			t.Errorf("%q: exit %d, ran %v, stdout %q", args, code, calls, stdout)
		}
	}
	// `--version` after a root flag is not a version request: dev-hops has no such option.
	if _, _, code, _, _ := runRoot(t, "--org", "R", "--version"); code != ExitUsage {
		t.Errorf("--org R --version: exit %d, want the usage error", code)
	}
}

func TestRootLogLevelIsTheProcessLevelForTheRun(t *testing.T) {
	for args, want := range map[string]bool{"--log-level DEBUG none": true, "--log-level debug none": true, "--log-level NOTSET none": true,
		"--log-level INFO none": false, "--log-level WARNING none": false, "--log-level nope none": false, "none": false} {
		_, levels, code, _, stderr := runRoot(t, strings.Fields(args)...)
		if code != ExitOK || len(levels) != 1 || levels[0] != want {
			t.Errorf("%q: exit %d, debug enabled %v, want %v (stderr %q)", args, code, levels, want, stderr)
		}
	}
}

func TestValidateRefusesBadRootFlagLists(t *testing.T) {
	verb := func(flags ...RootFlag) []Command {
		return []Command{{Name: "v", Summary: "s", Kind: Verb, Run: func(context.Context, Env) int { return 0 }, RootFlags: flags}}
	}
	if err := Validate(verb(RootLogLevel, RootModel)); err != nil {
		t.Fatalf("a valid list: %v", err)
	}
	if err := Validate(verb("nope")); err == nil {
		t.Error("an unknown root flag was accepted")
	}
	if err := Validate(verb(RootModel, RootModel)); err == nil {
		t.Error("a repeated root flag was accepted")
	}
	for _, flag := range []RootFlag{RootOrg, RootDB, RootAnalyticsDB} {
		if err := Validate(verb(flag)); err == nil {
			t.Errorf("%s (handed to every command) was accepted in a list", flag)
		}
	}
	group := []Command{{Name: "g", Summary: "s", Kind: Group, RootFlags: []RootFlag{RootLogLevel},
		Children: []Command{{Name: "v", Summary: "s", Kind: Verb, Run: func(context.Context, Env) int { return 0 }}}}}
	if err := Validate(group); err == nil {
		t.Error("a group listing root flags was accepted")
	}
}

func TestACommandThatIgnoresArgumentsRefusesAWhereItActsRootFlag(t *testing.T) {
	var calls []call
	tree := []Command{{Name: "q", Summary: "takes no arguments", Kind: Service, IgnoresArguments: true,
		Run: func(_ context.Context, env Env) int {
			calls = append(calls, call{path: "q", args: env.Args})
			return ExitOK
		}}}
	for _, flag := range []string{"--org=O", "--db=D", "--analytics-db=A"} {
		calls = nil
		var out, errOut bytes.Buffer
		code := Execute(context.Background(), "dho", tree, Env{Args: []string{flag, "q"}, Stdout: &out, Stderr: &errOut})
		if code != ExitUsage || len(calls) != 0 || !strings.Contains(errOut.String(), "takes no --") {
			t.Errorf("%s q: exit %d, ran %v, stderr %q; want a usage error that ran nothing", flag, code, calls, errOut.String())
		}
	}
	// How-it-runs flags and no flags at all run it, with nothing handed over.
	for _, args := range [][]string{{"q"}, {"--log-level", "debug", "-l", "L", "-m", "M", "q"}} {
		calls = nil
		if code := Execute(context.Background(), "dho", tree, Env{Args: args, Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}}); code != ExitOK || len(calls) != 1 || len(calls[0].args) != 0 {
			t.Errorf("%q: exit %d, calls %v; want it run with no arguments", args, code, calls)
		}
	}
}

func TestPythonReprIsPythonsStrRepr(t *testing.T) {
	for input, want := range map[string]string{
		"plain": "'plain'", "it's": `"it's"`, `say "hi"`: `'say "hi"'`, `both ' and "`: `'both \' and "'`, `back\slash`: `'back\\slash'`,
		"\x1b[2J": `'\x1b[2J'`, "a\tb\nc\rd": `'a\tb\nc\rd'`, "\x00\x7f": `'\x00\x7f'`, "é": "'é'", "\u00a0": `'\xa0'`,
		"\u200b": `'\u200b'`, "\U0001F600": "'\U0001F600'", "\U000e0001": `'\U000e0001'`, "": "''",
	} {
		if got := pythonRepr(input); got != want {
			t.Errorf("pythonRepr(%q) = %s, want %s", input, got, want)
		}
	}
}
