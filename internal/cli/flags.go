package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ParseFlags parses args without ever echoing typed text. The flag package puts the rejected value or
// flag name into its errors, and operator input can hold a DSN; so output is discarded and only names
// from the FlagSet's own definitions are printed. ok is false when the caller must return code. The
// set's own Usage func (static text) prints after the fixed error line; -h prints it and exits 0.
func ParseFlags(flags *flag.FlagSet, stderr io.Writer, args []string) (code int, ok bool) {
	msg, err := parseRedacted(flags, stderr, args)
	switch {
	case err == nil:
		return ExitOK, true
	case errors.Is(err, flag.ErrHelp):
		return ExitOK, false
	}
	fmt.Fprintln(stderr, msg)
	printFlagUsage(flags, stderr)
	return ExitUsage, false
}

// ParseFlagsError is ParseFlags for a verb that returns an error: nil on success, flag.ErrHelp after
// the usage printed, else a *FlagUsageError whose text is the fixed line (the caller prints it).
func ParseFlagsError(flags *flag.FlagSet, stderr io.Writer, args []string) error {
	msg, err := parseRedacted(flags, stderr, args)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, flag.ErrHelp):
		return flag.ErrHelp
	}
	printFlagUsage(flags, stderr)
	return &FlagUsageError{Err: errors.New(msg)}
}

func parseRedacted(flags *flag.FlagSet, stderr io.Writer, args []string) (string, error) {
	prevOut, prevUsage := flags.Output(), flags.Usage
	flags.SetOutput(io.Discard)
	flags.Usage = func() {}
	err := flags.Parse(args)
	flags.Usage = prevUsage
	flags.SetOutput(prevOut)
	if err == nil || errors.Is(err, flag.ErrHelp) {
		if err != nil {
			printFlagUsage(flags, stderr)
		}
		return "", err
	}
	return redactedFlagMessage(flags, err.Error()), err
}

func printFlagUsage(flags *flag.FlagSet, stderr io.Writer) {
	prev := flags.Output()
	flags.SetOutput(stderr)
	defer flags.SetOutput(prev)
	if flags.Usage != nil {
		flags.Usage()
		return
	}
	flags.PrintDefaults()
}

func redactedFlagMessage(flags *flag.FlagSet, msg string) string {
	if strings.HasPrefix(msg, "flag provided but not defined") {
		return "unknown flag"
	}
	if name := definedFlagIn(flags, msg); name != "" {
		return "invalid value for flag -" + name
	}
	return "invalid flag input"
}

// definedFlagIn is the name of the defined flag a bad-value or missing-argument error is about, or "". The
// name comes from the FlagSet's definitions, never from the message; the message is only read past its
// quoted value, so typed text cannot choose it.
func definedFlagIn(flags *flag.FlagSet, msg string) string {
	var name string
	switch {
	case strings.HasPrefix(msg, "flag needs an argument: -"):
		name = strings.TrimPrefix(msg, "flag needs an argument: -")
		name = strings.TrimPrefix(name, "-")
	case strings.HasPrefix(msg, "invalid value "), strings.HasPrefix(msg, "invalid boolean value "):
		rest := msg[strings.Index(msg, "value ")+len("value "):]
		quoted, err := strconv.QuotedPrefix(rest)
		if err != nil {
			return ""
		}
		rest = rest[len(quoted):]
		switch {
		case strings.HasPrefix(rest, " for flag -"):
			rest = strings.TrimPrefix(rest, " for flag -")
		case strings.HasPrefix(rest, " for -"):
			rest = strings.TrimPrefix(rest, " for -")
		default:
			return ""
		}
		end := strings.Index(rest, ": ")
		if end < 0 {
			return ""
		}
		name = rest[:end]
	default:
		return ""
	}
	if f := flags.Lookup(name); f != nil {
		return f.Name
	}
	return ""
}
