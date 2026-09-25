package cli

import (
	"fmt"
	"log/slog"
	"strings"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/pyargparse"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// RootFlag names a global flag of the operator command line: the options
// `dev-hops` takes before the subcommand and re-adds on every leaf
// (`dev-hops --org X fixtures generate` and `dev-hops fixtures generate --org X`
// mean the same). A value typed before the command is handed to the command
// as if typed first among its own flags, so a flag typed after it wins, as in
// dev-hops:
//
//   - the flags that say WHERE a command acts (--org, --db, --analytics-db) are
//     handed to every command. A command whose own flags lack one refuses it as
//     it would refuse it typed after the command; a root flag is never dropped
//     silently, because `dho --analytics-db X migrate clickhouse upgrade` that
//     ignored X would migrate the database the environment names;
//   - the rest (--log-level, --llm-provider, --model) configure how a command
//     runs and are handed only to a command that lists them in Command.RootFlags;
//     an unlisted command ignores them, as dev-hops's leaves ignore a global they
//     do not use. --log-level also sets the process logger for the run.
type RootFlag string

// The root flags a command can accept by the same name.
const (
	RootLogLevel    RootFlag = "log-level"
	RootDB          RootFlag = "db"
	RootAnalyticsDB RootFlag = "analytics-db"
	RootOrg         RootFlag = "org"
	RootLLMProvider RootFlag = "llm-provider"
	RootModel       RootFlag = "model"
)

// rootFlagOrder is the order dev-hops re-adds its globals on a leaf
// (_GLOBAL_FLAG_SPECS in cli.py), and so the order they are handed over.
var rootFlagOrder = []RootFlag{RootLogLevel, RootDB, RootAnalyticsDB, RootOrg, RootLLMProvider, RootModel}

// listable are the root flags a command lists in Command.RootFlags.
var listable = map[RootFlag]bool{RootLogLevel: true, RootLLMProvider: true, RootModel: true}

// handedToEvery are the root flags handed to every command.
var handedToEvery = map[RootFlag]bool{RootDB: true, RootAnalyticsDB: true, RootOrg: true}

func validateRootFlags(path string, command Command) error {
	if len(command.RootFlags) == 0 {
		return nil
	}
	if command.Kind == Group {
		return fmt.Errorf("group %q must not list root flags", path)
	}
	seen := map[RootFlag]bool{}
	for _, flag := range command.RootFlags {
		if handedToEvery[flag] {
			return fmt.Errorf("command %q lists root flag %q, which every command is handed", path, flag)
		}
		if !listable[flag] {
			return fmt.Errorf("command %q lists the unknown root flag %q", path, flag)
		}
		if seen[flag] {
			return fmt.Errorf("command %q lists root flag %q twice", path, flag)
		}
		seen[flag] = true
	}
	return nil
}

// rootSpecs are the options dev-hops's root parser takes (build_parser in
// cli.py: the four of its own plus add_llm_arguments), in that order. The last
// three exist only at the root (a leaf does not re-add them): dho takes them
// so a dev-hops command line is not refused, and does not use them.
var rootSpecs = []pyargparse.Spec{
	{Strs: []string{"-h", "--help"}, Dest: "help", Kind: pyargparse.OptHelp},
	{Strs: []string{"--log-level"}, Dest: "log-level", Kind: pyargparse.OptValue},
	{Strs: []string{"--db"}, Dest: "db", Kind: pyargparse.OptValue},
	{Strs: []string{"--analytics-db"}, Dest: "analytics-db", Kind: pyargparse.OptValue},
	{Strs: []string{"--org"}, Dest: "org", Kind: pyargparse.OptValue},
	{Strs: []string{"-l", "--llm-provider"}, Dest: "llm-provider", Kind: pyargparse.OptValue},
	{Strs: []string{"-m", "--model"}, Dest: "model", Kind: pyargparse.OptValue},
	{Strs: []string{"--llm-api-key"}, Dest: "llm-api-key", Kind: pyargparse.OptValue},
	{Strs: []string{"--llm-base-url"}, Dest: "llm-base-url", Kind: pyargparse.OptValue},
	{Strs: []string{"--llm-concurrency"}, Dest: "llm-concurrency", Kind: pyargparse.OptValue},
}

// rootParse is what the root parser made of the arguments before the command.
type rootParse struct {
	// values is the last value typed for each root flag (by dest).
	values map[string]string
	help   bool
	// rest is the command and everything after it.
	rest []string
}

// parseRootFlags is the root parser of dev-hops over args, which start with an
// option. It stops at the first positional (the command).
func parseRootFlags(args []string) (rootParse, *pyargparse.Error) {
	parsed, err := pyargparse.New(rootSpecs).ParseRoot(args, func(spec *pyargparse.Spec, value string) *pyargparse.Error {
		if spec.Dest == "llm-concurrency" {
			if _, convErr := pythonparity.ParseInt(value); convErr != nil {
				return &pyargparse.Error{Msg: fmt.Sprintf("argument --llm-concurrency: invalid int value: %s", pythonRepr(value))}
			}
		}
		return nil
	})
	if err != nil {
		return rootParse{}, err
	}
	rest := parsed.Rest
	if len(rest) > 0 && rest[0] == "--" {
		// argparse drops the first `--`: the command follows it.
		rest = rest[1:]
	}
	return rootParse{values: parsed.Values, help: parsed.Help, rest: rest}, nil
}

// refusedRootFlag is the first where-it-acts root flag typed before the command,
// in dev-hops's order.
func refusedRootFlag(root rootParse) (RootFlag, bool) {
	for _, flag := range rootFlagOrder {
		if _, typed := root.values[string(flag)]; typed && handedToEvery[flag] {
			return flag, true
		}
	}
	return "", false
}

// pythonRepr is repr(str): the quote Python picks, backslash, \t \n \r and
// the other control or unprintable code points as \xNN, \uNNNN or \UNNNNNNNN,
// printable text kept, so an argument error reads as argparse's does and never
// carries a raw control sequence to the operator's terminal.
func pythonRepr(text string) string {
	quote := '\''
	if strings.ContainsRune(text, '\'') && !strings.ContainsRune(text, '"') {
		quote = '"'
	}
	var out strings.Builder
	out.WriteRune(quote)
	for _, r := range text {
		switch {
		case r == quote || r == '\\':
			out.WriteByte('\\')
			out.WriteRune(r)
		case r == '\t':
			out.WriteString(`\t`)
		case r == '\n':
			out.WriteString(`\n`)
		case r == '\r':
			out.WriteString(`\r`)
		case r < ' ' || r == 0x7f:
			fmt.Fprintf(&out, `\x%02x`, r)
		case r < 0x7f || unicode.IsPrint(r):
			out.WriteRune(r)
		case r <= 0xff:
			fmt.Fprintf(&out, `\x%02x`, r)
		case r <= 0xffff:
			fmt.Fprintf(&out, `\u%04x`, r)
		default:
			fmt.Fprintf(&out, `\U%08x`, r)
		}
	}
	out.WriteRune(quote)
	return out.String()
}

// handOver returns args with the root flags the command lists in front, each as
// one `--name=value` argument so an empty or option-like value stays a value.
func handOver(command Command, root rootParse, args []string) []string {
	if len(root.values) == 0 {
		return args
	}
	listed := map[RootFlag]bool{}
	for _, flag := range command.RootFlags {
		listed[flag] = true
	}
	var front []string
	for _, flag := range rootFlagOrder {
		value, typed := root.values[string(flag)]
		if !typed || !(handedToEvery[flag] || listed[flag]) {
			continue
		}
		if flag == RootLogLevel {
			value = levelName(pythonLevel(value))
		}
		front = append(front, "--"+string(flag)+"="+value)
	}
	if len(front) == 0 {
		return args
	}
	return append(front, args...)
}

// pythonLevel is `getattr(logging, name.upper(), logging.INFO)` for the level
// names of the logging module (the process default for anything else).
func pythonLevel(name string) slog.Level {
	switch strings.ToUpper(name) {
	case "DEBUG", "NOTSET":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	case "CRITICAL", "FATAL":
		return slog.LevelError + 4
	default:
		return slog.LevelInfo
	}
}

// levelName is the spelling the services' --log-level takes for a level (they
// take Python's CRITICAL as "critical", above error, so ERROR records are
// silenced there as in dev-hops; nothing is lowered).
func levelName(level slog.Level) string {
	switch {
	case level <= slog.LevelDebug:
		return "debug"
	case level <= slog.LevelInfo:
		return "info"
	case level <= slog.LevelWarn:
		return "warn"
	case level <= slog.LevelError:
		return "error"
	default:
		return "critical"
	}
}
