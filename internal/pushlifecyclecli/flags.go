package pushlifecyclecli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// parseFlags parses args without ever echoing typed text. The flag package puts the rejected value or
// flag name into its errors, and operator input can hold a DSN; so output is discarded and only names
// from the FlagSet's own definitions are printed. ok is false when the caller must return code.
func parseFlags(flags *flag.FlagSet, stderr io.Writer, args []string) (code int, ok bool) {
	flags.SetOutput(io.Discard)
	err := flags.Parse(args)
	if err == nil {
		return cli.ExitOK, true
	}
	if errors.Is(err, flag.ErrHelp) {
		printUsage(flags, stderr)
		return cli.ExitOK, false
	}
	msg := err.Error()
	switch known := knownFlagIn(flags, msg); {
	case strings.HasPrefix(msg, "flag provided but not defined"):
		fmt.Fprintln(stderr, "unknown flag")
	case known != "":
		fmt.Fprintf(stderr, "invalid value for flag -%s\n", known)
	default:
		fmt.Fprintln(stderr, "invalid flag input")
	}
	printUsage(flags, stderr)
	return cli.ExitUsage, false
}

func knownFlagIn(flags *flag.FlagSet, msg string) string {
	found := ""
	flags.VisitAll(func(f *flag.Flag) {
		for _, tail := range []string{" for flag -" + f.Name + ": parse error", " for flag -" + f.Name + ": value out of range",
			" for -" + f.Name + ": parse error", " for -" + f.Name + ": value out of range", "flag needs an argument: -" + f.Name} {
			if strings.HasSuffix(msg, tail) {
				found = f.Name
			}
		}
	})
	return found
}

func printUsage(flags *flag.FlagSet, w io.Writer) {
	fmt.Fprintf(w, "usage: %s [flags]\n", flags.Name())
	flags.VisitAll(func(f *flag.Flag) {
		fmt.Fprintf(w, "  -%s (default %q)\n      %s\n", f.Name, f.DefValue, f.Usage)
	})
}
