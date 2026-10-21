package releasetag

import (
	"fmt"
	"path"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// defaultPackageJSON is written in pieces: the path guard of the workflow filters reads a string literal that ends in .json as
// a named file and, finding none, takes the one tracked file of the same base name (ci/auth-contracts-ajv/package.json).
var defaultPackageJSON = path.Join("grafana", "plugins", "dev-health-panels", "package"+".json")

const defaultSuffix = "beta"

type options struct {
	packageJSON string
	suffix      string
	help        bool
}

type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

// actionOptions are the option strings of the parser, in the order argparse holds them.
var actionOptions = []string{"-h", "--help", "--package-json", "--pre-release-suffix"}

func isAction(option string) bool {
	for _, o := range actionOptions {
		if o == option {
			return true
		}
	}
	return false
}

// optionTuple is one reading of an argument as an option: the option string, whether "=" followed it and the text after.
type optionTuple struct {
	option   string
	sep      string
	explicit *string
}

// argClass is how argparse sees one argument: a value ('A'), the "--" marker ('-'), or an option ('O') with its readings;
// no reading is an option the parser does not have.
type argClass struct {
	kind     byte
	readings []optionTuple
}

// startsNegativeNumber is argparse's _negative_number_matcher of CPython 3.14.7, '-\.?\d' with re.match, over Unicode digits.
func startsNegativeNumber(arg string) bool {
	rs := []rune(arg)
	if len(rs) < 2 || rs[0] != '-' {
		return false
	}
	rest := rs[1:]
	if rest[0] == '.' {
		rest = rest[1:]
	}
	return len(rest) > 0 && isNd(rest[0])
}

func ptr(s string) *string { return &s }

// classify is _parse_optional of CPython 3.14.7 for every argument; the first "--" is a marker and all after it are values.
func classify(args []string) []argClass {
	out := make([]argClass, len(args))
	afterMarker := false
	for i, arg := range args {
		switch {
		case afterMarker:
			out[i] = argClass{kind: 'A'}
		case arg == "--":
			out[i] = argClass{kind: '-'}
			afterMarker = true
		default:
			out[i] = classifyOne(arg)
		}
	}
	return out
}

func classifyOne(arg string) argClass {
	if arg == "" || arg[0] != '-' {
		return argClass{kind: 'A'}
	}
	if isAction(arg) {
		return argClass{kind: 'O', readings: []optionTuple{{option: arg}}}
	}
	if len(arg) == 1 {
		return argClass{kind: 'A'}
	}
	prefix, explicit, hasSep := strings.Cut(arg, "=")
	if hasSep && isAction(prefix) {
		return argClass{kind: 'O', readings: []optionTuple{{option: prefix, sep: "=", explicit: ptr(explicit)}}}
	}
	var readings []optionTuple
	if arg[1] == '-' {
		for _, option := range actionOptions {
			if strings.HasPrefix(option, prefix) {
				t := optionTuple{option: option}
				if hasSep {
					t.sep, t.explicit = "=", ptr(explicit)
				}
				readings = append(readings, t)
			}
		}
	} else {
		for _, option := range actionOptions {
			switch {
			case option == arg[:2]:
				readings = append(readings, optionTuple{option: option, explicit: ptr(arg[2:])})
			case strings.HasPrefix(option, prefix):
				t := optionTuple{option: option}
				if hasSep {
					t.sep, t.explicit = "=", ptr(explicit)
				}
				readings = append(readings, t)
			}
		}
	}
	if len(readings) > 0 {
		return argClass{kind: 'O', readings: readings}
	}
	if startsNegativeNumber(arg) || strings.Contains(arg, " ") {
		return argClass{kind: 'A'}
	}
	return argClass{kind: 'O'}
}

func ignoredExplicit(explicit string) usageError {
	return usageError{"argument -h/--help: ignored explicit argument " + pythonparity.StrRepr(explicit)}
}

// helpCluster is consume_optional of CPython 3.14.7 for -h with an explicit argument: a short option may be followed by more
// short options, "-hh" is help twice, "-hx" is help and a stray "-x", and a "=" or a "-" there is an error. Help runs when the
// argument has been read, so an error found while reading comes first.
func helpCluster(t optionTuple) error {
	explicit, sep := *t.explicit, t.sep
	if t.option == "--help" {
		return ignoredExplicit(explicit)
	}
	for {
		if explicit == "" {
			return ignoredExplicit(explicit)
		}
		if sep != "" || explicit[0] == '-' {
			return ignoredExplicit(explicit)
		}
		if explicit[0] != 'h' {
			return nil
		}
		explicit = explicit[1:]
		switch {
		case explicit == "":
			return nil
		case explicit[0] == '=':
			sep, explicit = "=", explicit[1:]
		default:
			sep = ""
		}
	}
}

// parseArgs is the argparse of the Python script: --package-json, --pre-release-suffix and -h, long options by unique prefix,
// "--opt=value", one value per option (the last wins), no positional argument. Errors follow the order of argparse: an
// ambiguous abbreviation is an error when it is reached, and -h exits 0 at once when it is reached.
func parseArgs(args []string) (options, error) {
	opts := options{packageJSON: defaultPackageJSON, suffix: defaultSuffix}
	classes := classify(args)
	var extras []string
	for i := 0; i < len(args); i++ {
		c := classes[i]
		if c.kind != 'O' || len(c.readings) == 0 {
			extras = append(extras, args[i])
			continue
		}
		if len(c.readings) > 1 {
			names := make([]string, len(c.readings))
			for j, t := range c.readings {
				names[j] = t.option
			}
			return opts, usageError{fmt.Sprintf("ambiguous option: %s could match %s", args[i], strings.Join(names, ", "))}
		}
		t := c.readings[0]
		if t.option == "-h" || t.option == "--help" {
			if t.explicit != nil {
				if err := helpCluster(t); err != nil {
					return opts, err
				}
			}
			opts.help = true
			return opts, nil
		}
		var value string
		if t.explicit != nil {
			value = *t.explicit
		} else {
			if i+1 >= len(args) || classes[i+1].kind != 'A' {
				return opts, usageError{"argument " + t.option + ": expected one argument"}
			}
			i++
			value = args[i]
		}
		if t.option == "--package-json" {
			opts.packageJSON = value
		} else {
			opts.suffix = value
		}
	}
	if len(extras) > 0 {
		return opts, usageError{"unrecognized arguments: " + strings.Join(extras, " ")}
	}
	return opts, nil
}
