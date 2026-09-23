// Package cli is the command tree of the dho operator binary.
//
// A vertical (a service such as the api, or a verb group such as the worker
// operator commands) is a package that returns one Command. cmd/dho lists
// every vertical in one slice and calls Main; there is no init-time
// self-registration and no runtime plugin loading, so the whole tree is
// visible in one file and a removed vertical is a one-line diff.
//
// This package owns dispatch, help, the version verb and the exit-code
// contract. It knows nothing about any vertical.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

// Exit codes. Every command in the tree returns one of these.
const (
	// ExitOK is success.
	ExitOK = 0
	// ExitFailure is a runtime failure: a dependency is down, the operation
	// failed, or the configuration is invalid.
	ExitFailure = 1
	// ExitUsage is a usage error: an unknown command, flag or positional
	// argument. Nothing ran.
	ExitUsage = 2
	// ExitRefused means a preflight said no and nothing was written.
	ExitRefused = 3
)

// FlagUsageError marks a (*flag.FlagSet).Parse failure that is not a help
// request -- an unknown flag or a bad value, exactly the surface ExitUsage
// documents ("nothing ran"). A verb wraps its own Parse error with
// WrapFlagParseError at the call site; Command().Run then maps it to
// ExitUsage instead of the verb's own generic failure code, keeping every
// leaf in the tree honoring the same exit-code contract as dispatch itself
// (an unknown top-level command already exits 2, see Execute below).
type FlagUsageError struct{ Err error }

func (e *FlagUsageError) Error() string { return e.Err.Error() }
func (e *FlagUsageError) Unwrap() error { return e.Err }

// WrapFlagParseError classifies a (*flag.FlagSet).Parse return value: nil
// stays nil, flag.ErrHelp stays flag.ErrHelp (a request, not a failure --
// the caller maps it to ExitOK and skips printing it as an error), anything
// else is wrapped as a *FlagUsageError.
func WrapFlagParseError(err error) error {
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return err
	}
	return &FlagUsageError{Err: err}
}

// ExitForVerbError maps a Verb's Run error to this binary's exit-code
// contract: ExitOK for a help request (nothing to report), ExitUsage for a
// *FlagUsageError, ExitFailure for everything else. A Verb that has its own
// finer classification (a refusal distinct from an internal failure, as
// internal/goapicli/routing has) is not obligated to use this helper.
func ExitForVerbError(err error) int {
	switch {
	case err == nil:
		return ExitOK
	case errors.Is(err, flag.ErrHelp):
		return ExitOK
	default:
		var usage *FlagUsageError
		if errors.As(err, &usage) {
			return ExitUsage
		}
		return ExitFailure
	}
}

// Kind says how a command runs.
type Kind int

const (
	// Group only holds children. Run on a group prints its help and exits
	// with ExitUsage.
	Group Kind = iota
	// Verb is a short-lived command: it does one thing and exits.
	Verb
	// Service is a long-running process. A service may also hold utility
	// child verbs (for example a healthcheck probe).
	Service
)

func (k Kind) String() string {
	switch k {
	case Group:
		return "group"
	case Verb:
		return "verb"
	case Service:
		return "service"
	default:
		return fmt.Sprintf("kind(%d)", int(k))
	}
}

// Env is what a command receives. Args are the arguments AFTER the command's
// own path, so a command never sees the words that selected it.
type Env struct {
	Args   []string
	Lookup secrets.LookupEnv
	Stdout io.Writer
	Stderr io.Writer
}

// Command is one node of the tree.
type Command struct {
	// Name is one path element. It must be non-empty, lower case, and must
	// not start with "-".
	Name string
	// Summary is one line for help.
	Summary string
	Kind    Kind
	// Run executes a Verb or a Service. It is nil for a Group.
	Run func(ctx context.Context, env Env) int
	// Children are required for a Group and optional for a Service. A Verb
	// has none.
	Children []Command
}

// Validate checks a tree before it runs. cmd/dho's test calls it on the real
// tree, so a malformed vertical fails the build's tests, not an operator.
func Validate(tree []Command) error {
	return validateLevel(tree, "")
}

func validateLevel(level []Command, parent string) error {
	seen := make(map[string]struct{}, len(level))
	for _, command := range level {
		path := strings.TrimSpace(parent + " " + command.Name)
		if err := validateName(command.Name); err != nil {
			return fmt.Errorf("command %q: %w", path, err)
		}
		if _, dup := seen[command.Name]; dup {
			return fmt.Errorf("command %q is registered twice", path)
		}
		seen[command.Name] = struct{}{}
		if parent == "" && isBuiltin(command.Name) {
			return fmt.Errorf("command %q collides with a built-in", path)
		}
		if strings.TrimSpace(command.Summary) == "" {
			return fmt.Errorf("command %q has no summary", path)
		}
		switch command.Kind {
		case Group:
			if command.Run != nil {
				return fmt.Errorf("group %q must not have Run", path)
			}
			if len(command.Children) == 0 {
				return fmt.Errorf("group %q has no children", path)
			}
		case Verb:
			if command.Run == nil {
				return fmt.Errorf("verb %q has no Run", path)
			}
			if len(command.Children) != 0 {
				return fmt.Errorf("verb %q must not have children", path)
			}
		case Service:
			if command.Run == nil {
				return fmt.Errorf("service %q has no Run", path)
			}
			for _, child := range command.Children {
				if child.Kind != Verb {
					return fmt.Errorf("service %q may only hold verbs, %q is a %s", path, child.Name, child.Kind)
				}
			}
		default:
			return fmt.Errorf("command %q has unknown kind %s", path, command.Kind)
		}
		if err := validateLevel(command.Children, path); err != nil {
			return err
		}
	}
	return nil
}

func validateName(name string) error {
	if name == "" {
		return errors.New("empty name")
	}
	if strings.HasPrefix(name, "-") {
		return errors.New("name must not start with -")
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
		default:
			return fmt.Errorf("name %q may only hold a-z, 0-9 and -", name)
		}
	}
	return nil
}

// builtins are the top-level words this package answers itself.
var builtins = []string{"help", "version"}

func isBuiltin(name string) bool {
	for _, builtin := range builtins {
		if name == builtin {
			return true
		}
	}
	return false
}

// Main runs the tree for the process and exits with its code.
//
// It installs the redacting JSON handler as the process default first, on
// stderr beside the command's own errors, so any vertical that logs through
// slog.Default before (or without) building its own logger still goes through
// the redactor. A service replaces it with the shell's logger for its run.
func Main(binary string, tree []Command) {
	logging.InstallDefault(logging.NewJSON(os.Stderr, slog.LevelInfo))
	os.Exit(Execute(context.Background(), binary, tree, Env{
		Args:   os.Args[1:],
		Lookup: os.LookupEnv,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}))
}

// Execute is the testable entry point. It never calls os.Exit.
func Execute(ctx context.Context, binary string, tree []Command, env Env) int {
	if env.Stdout == nil {
		env.Stdout = io.Discard
	}
	if env.Stderr == nil {
		env.Stderr = io.Discard
	}
	if env.Lookup == nil {
		env.Lookup = func(string) (string, bool) { return "", false }
	}
	if err := Validate(tree); err != nil {
		// A malformed tree is a programming error; say so and do not guess.
		fmt.Fprintf(env.Stderr, "%s: invalid command tree: %v\n", binary, err)
		return ExitFailure
	}
	args := env.Args
	if len(args) == 0 {
		writeHelp(env.Stderr, binary, nil, tree)
		return ExitUsage
	}
	switch args[0] {
	case "-h", "--help", "help":
		return help(env, binary, tree, args[1:])
	case "--version", "version":
		if len(args) != 1 {
			fmt.Fprintf(env.Stderr, "%s: version takes no arguments\n", binary)
			return ExitUsage
		}
		if err := version.Current(binary).WriteJSON(env.Stdout); err != nil {
			fmt.Fprintln(env.Stderr, "could not write version metadata")
			return ExitFailure
		}
		return ExitOK
	}

	level := tree
	var path []string
	for {
		command, found := find(level, args[0])
		if !found {
			fmt.Fprintf(env.Stderr, "%s: unknown command %q\n", binary, strings.Join(append(path, args[0]), " "))
			fmt.Fprintf(env.Stderr, "run %s help%s for the list\n", binary, prefixed(path))
			return ExitUsage
		}
		path = append(path, command.Name)
		args = args[1:]
		if len(args) > 0 && len(command.Children) > 0 {
			if _, isChild := find(command.Children, args[0]); isChild {
				level = command.Children
				continue
			}
		}
		if command.Kind == Group {
			if len(args) > 0 && (args[0] == "-h" || args[0] == "--help") {
				writeHelp(env.Stdout, binary, path, command.Children)
				return ExitOK
			}
			if len(args) > 0 {
				fmt.Fprintf(env.Stderr, "%s: unknown command %q\n", binary, strings.Join(append(path, args[0]), " "))
			}
			writeHelp(env.Stderr, binary, path, command.Children)
			return ExitUsage
		}
		return command.Run(ctx, Env{Args: args, Lookup: env.Lookup, Stdout: env.Stdout, Stderr: env.Stderr})
	}
}

func find(level []Command, name string) (Command, bool) {
	for _, command := range level {
		if command.Name == name {
			return command, true
		}
	}
	return Command{}, false
}

// help answers "dho help [path...]". It walks the tree like dispatch but
// never runs anything.
func help(env Env, binary string, tree []Command, path []string) int {
	level := tree
	var walked []string
	for _, word := range path {
		command, found := find(level, word)
		if !found {
			fmt.Fprintf(env.Stderr, "%s: unknown command %q\n", binary, strings.Join(append(walked, word), " "))
			return ExitUsage
		}
		walked = append(walked, command.Name)
		if len(command.Children) == 0 {
			fmt.Fprintf(env.Stdout, "%s%s: %s (%s)\n", binary, prefixed(walked), command.Summary, command.Kind)
			fmt.Fprintf(env.Stdout, "run %s%s --help for its flags\n", binary, prefixed(walked))
			return ExitOK
		}
		level = command.Children
	}
	writeHelp(env.Stdout, binary, walked, level)
	return ExitOK
}

func writeHelp(out io.Writer, binary string, path []string, level []Command) {
	fmt.Fprintf(out, "usage: %s%s <command> [flags]\n\ncommands:\n", binary, prefixed(path))
	names := make([]Command, len(level))
	copy(names, level)
	sort.SliceStable(names, func(i, j int) bool { return names[i].Name < names[j].Name })
	for _, command := range names {
		fmt.Fprintf(out, "  %-16s %s (%s)\n", command.Name, command.Summary, command.Kind)
	}
	if len(path) == 0 {
		fmt.Fprintf(out, "  %-16s %s\n", "help", "show help for a command")
		fmt.Fprintf(out, "  %-16s %s\n", "version", "print build metadata as JSON")
	}
}

func prefixed(path []string) string {
	if len(path) == 0 {
		return ""
	}
	return " " + strings.Join(path, " ")
}
