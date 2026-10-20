package routing

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

// errHelp is -h/--help: the usage goes to stdout and the exit is 0, as argparse does.
var errHelp = errors.New("help")

// parseArgs reads `--name value`, `--name=value` and positionals the way the scripts' argparse parsers did for the
// shapes they accepted. Every option takes one value; an unknown option or a missing value is an error (exit 2).
func parseArgs(args, options []string) (map[string]string, []string, error) {
	known := map[string]bool{}
	for _, o := range options {
		known[o] = true
	}
	opts := map[string]string{}
	var pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			pos = append(pos, args[i+1:]...)
			break
		}
		if a == "-h" || a == "--help" {
			return nil, nil, errHelp
		}
		if !strings.HasPrefix(a, "-") || a == "-" {
			pos = append(pos, a)
			continue
		}
		name, value, hasValue := strings.Cut(a, "=")
		if !known[name] {
			return nil, nil, fmt.Errorf("unrecognized arguments: %s", a)
		}
		if !hasValue {
			if i+1 >= len(args) || (strings.HasPrefix(args[i+1], "-") && args[i+1] != "-") {
				return nil, nil, fmt.Errorf("argument %s: expected one argument", name)
			}
			i++
			value = args[i]
		}
		opts[name] = value
	}
	return opts, pos, nil
}

// usageError prints an argparse-style error and returns its exit code.
func usageError(stdout, stderr io.Writer, usage, prog string, err error) int {
	if errors.Is(err, errHelp) {
		fmt.Fprint(stdout, usage)
		return 0
	}
	fmt.Fprint(stderr, usage)
	fmt.Fprintf(stderr, "%s: error: %v\n", prog, err)
	return 2
}
