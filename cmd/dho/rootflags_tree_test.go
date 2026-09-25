package main

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// leaves lists the path of every command that runs (a verb or a service).
func leaves(level []cli.Command, prefix []string, out *[]leaf) {
	for _, command := range level {
		path := append(append([]string{}, prefix...), command.Name)
		if command.Kind != cli.Group {
			*out = append(*out, leaf{path: path, command: command})
		}
		leaves(command.Children, path, out)
	}
}

type leaf struct {
	path    []string
	command cli.Command
}

func (l leaf) name() string { return strings.Join(l.path, " ") }

func (l leaf) lists(flag cli.RootFlag) bool {
	for _, listed := range l.command.RootFlags {
		if listed == flag {
			return true
		}
	}
	return false
}

// help is what `dho <path> <args> --help` writes and the exit code.
func help(t *testing.T, tree []cli.Command, path []string, before ...string) (string, int) {
	return helpWith(t, tree, path, before, nil, true)
}

// helpWith is help with words typed after the command path; asking for --help
// or not.
func helpWith(t *testing.T, tree []cli.Command, path []string, before, after []string, wantHelp bool) (string, int) {
	t.Helper()
	var out, errOut bytes.Buffer
	args := append(append([]string{}, before...), path...)
	args = append(args, after...)
	if wantHelp {
		args = append(args, "--help")
	}
	// A command that swallows its arguments (and would then serve) is stopped
	// after a moment instead of hanging the test; it has then "accepted" them.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	code := cli.Execute(ctx, "dho", tree, cli.Env{Args: args, Stdout: &out, Stderr: &errOut,
		Lookup: func(string) (string, bool) { return "", false }})
	return out.String() + errOut.String(), code
}

func names(text string, flag string) bool {
	// The whole flag name: `--org` is not `--org-id`.
	return regexp.MustCompile(`(^|[\s\[,|])-{1,2}` + flag + `([\s=\],|]|$)`).MatchString(text)
}

// TestRootFlagsAreNeverDroppedSilentlyByAnyVertical walks the real tree. A
// where-it-acts root flag (--org, --db, --analytics-db) is handed to every
// command as if typed first among its own flags, so it is only as safe as the
// command's refusal of a flag it does not have: a command that swallowed it
// would run on the environment's organization or database while the operator
// typed another. For every command that runs:
//   - a flag no command has, typed after the command, must not succeed, unless
//     the command declares IgnoresArguments (and then the dispatcher itself must
//     refuse a where-it-acts root flag typed before it);
//   - where its help names --org, `--org=X <command> --help` must succeed (the
//     root flag reaches the command's own flag);
//   - a command whose help names --log-level must list RootLogLevel (so the root
//     value reaches it), and one that lists a root flag must name it in its help.
func TestRootFlagsAreNeverDroppedSilentlyByAnyVertical(t *testing.T) {
	tree := commands()
	var all []leaf
	leaves(tree, nil, &all)
	if len(all) < 40 {
		t.Fatalf("only %d commands found in the tree", len(all))
	}
	ignoring := 0
	for _, l := range all {
		text, _ := help(t, tree, l.path)
		// A flag no command has (the root parser would refuse it before the
		// command, so it is typed after): the command's own refusal.
		_, unknown := helpWith(t, tree, l.path, nil, []string{"--not-a-flag-of-any-command=X"}, false)
		switch {
		case l.command.IgnoresArguments:
			ignoring++
			for _, flag := range []string{"org", "db", "analytics-db"} {
				if _, code := helpWith(t, tree, l.path, []string{"--" + flag + "=X"}, nil, false); code != cli.ExitUsage {
					t.Errorf("%s takes no arguments: `--%s=X` before it gives exit %d, want the dispatcher's usage error", l.name(), flag, code)
				}
			}
		case unknown == cli.ExitOK:
			t.Errorf("%s: a flag no command has is accepted (exit 0): a root flag handed to it would be dropped silently; refuse unknown flags or declare IgnoresArguments", l.name())
		}
		if names(text, "org") {
			if _, code := help(t, tree, l.path, "--org=X"); code != cli.ExitOK {
				t.Errorf("%s: its help names --org, but `--org=X` before the command gives exit %d", l.name(), code)
			}
		}
		if l.lists(cli.RootLogLevel) {
			// Every level dev-hops's root parser takes, Python's CRITICAL and
			// FATAL included, must be one the command takes.
			for _, level := range []string{"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "FATAL"} {
				if out, code := help(t, tree, l.path, "--log-level", level); code != cli.ExitOK {
					t.Errorf("%s: `--log-level %s` before the command gives exit %d (%.120q)", l.name(), level, code, out)
				}
			}
		}
		if names(text, "log-?level") != l.lists(cli.RootLogLevel) {
			t.Errorf("%s: help names --log-level: %v, lists RootLogLevel: %v", l.name(), names(text, "log-?level"), l.lists(cli.RootLogLevel))
		}
		if l.lists(cli.RootLLMProvider) && !names(text, "llm-provider") {
			t.Errorf("%s: lists RootLLMProvider but its help does not name --llm-provider", l.name())
		}
		if l.lists(cli.RootModel) && !names(text, "model") {
			t.Errorf("%s: lists RootModel but its help does not name --model", l.name())
		}
	}
	if ignoring == 0 {
		t.Error("no command declares IgnoresArguments: the tree changed, so this test no longer covers the refusal it exists for")
	}
}

// TestEveryRootLogLevelIsAcceptedByTheServices links the two halves: the value
// the dispatcher hands a service for each level name dev-hops's root parser
// takes (Python's logging level names, any case) must load in the services'
// own config, so a --log-level dev-hops accepts never fails a dho service.
func TestEveryRootLogLevelIsAcceptedByTheServices(t *testing.T) {
	for _, level := range []string{"DEBUG", "debug", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL", "FATAL", "NOTSET", "nonsense", ""} {
		var handed []string
		tree := []cli.Command{{Name: "svc", Summary: "spy", Kind: cli.Service, RootFlags: []cli.RootFlag{cli.RootLogLevel},
			Run: func(_ context.Context, env cli.Env) int { handed = env.Args; return cli.ExitOK }}}
		code := cli.Execute(context.Background(), "dho", tree, cli.Env{Args: []string{"--log-level", level, "svc"}, Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
		if code != cli.ExitOK || len(handed) != 1 || !strings.HasPrefix(handed[0], "--log-level=") {
			t.Fatalf("%q: exit %d, handed %q", level, code, handed)
		}
		value := strings.TrimPrefix(handed[0], "--log-level=")
		_, err := platformconfig.Load(platformconfig.Spec{Service: "dev-health-worker", LookupEnv: func(key string) (string, bool) {
			if key == "DEV_HEALTH_LOG_LEVEL" {
				return value, true
			}
			return "", false
		}})
		if err != nil {
			t.Errorf("--log-level %q is handed to a service as %q, which its config refuses: %v", level, value, err)
		}
	}
}

// TestNamesMatchesTheWholeFlagName pins the help scan the walk relies on: `--org`
// is not `--org-id` (admin orgs delete has the second and no first).
func TestNamesMatchesTheWholeFlagName(t *testing.T) {
	for text, want := range map[string]bool{
		"  -org-id value\n": false, "  --org-id X": false, "  -org value\n": true, "Usage: dho x [--org <id>]": true,
		"--org=X": true, "  -org\n": true, "-organization": false, "a|--org|b": true,
	} {
		if got := names(text, "org"); got != want {
			t.Errorf("names(%q, org) = %v, want %v", text, got, want)
		}
	}
}
