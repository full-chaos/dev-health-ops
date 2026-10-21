package docstools

import (
	"fmt"
	"io"
	"regexp"
	"strings"
)

// apParser is argparse.ArgumentParser of Python 3.14 for scripts whose options all take one value (plus -h/--help),
// prefix ap: abbreviations (--inv), --opt=value, -h combined with nothing, "--", values that look like negative
// numbers (Unicode digits), and its errors (usage on stderr, "prog: error: ...", exit 2), in the order argparse finds
// them: an ambiguous or valueless option where it stands, then missing required options, then unrecognized arguments.
type apParser struct {
	prog, description string
	opts              []apOpt
}

type apOpt struct {
	flag, metavar string
	required      bool
}

type apTuple struct {
	opt           string // "" for an option-like argument no option matches
	sep, explicit *string
}

var apNegative = regexp.MustCompile(`^-\.?\p{Nd}`)

func (p apParser) optionStrings() []string {
	out := []string{"-h", "--help"}
	for _, o := range p.opts {
		out = append(out, o.flag)
	}
	return out
}

func (p apParser) has(s string) bool {
	for _, o := range p.optionStrings() {
		if o == s {
			return true
		}
	}
	return false
}

func apStr(s string) *string { return &s }

// parseOptional is ArgumentParser._parse_optional: nil for a positional argument.
func (p apParser) parseOptional(arg string) []apTuple {
	rs := []rune(arg)
	if len(rs) == 0 || rs[0] != '-' {
		return nil
	}
	if p.has(arg) {
		return []apTuple{{opt: arg}}
	}
	if len(rs) == 1 {
		return nil
	}
	if opt, explicit, found := strings.Cut(arg, "="); found && p.has(opt) {
		return []apTuple{{opt: opt, sep: apStr("="), explicit: apStr(explicit)}}
	}
	var out []apTuple
	prefix, explicit, found := strings.Cut(arg, "=")
	var sep, exp *string
	if found {
		sep, exp = apStr("="), apStr(explicit)
	}
	if rs[1] == '-' {
		for _, o := range p.optionStrings() {
			if strings.HasPrefix(o, prefix) {
				out = append(out, apTuple{opt: o, sep: sep, explicit: exp})
			}
		}
	} else {
		short, shortExplicit := string(rs[:2]), string(rs[2:])
		for _, o := range p.optionStrings() {
			if o == short {
				out = append(out, apTuple{opt: o, sep: apStr(""), explicit: apStr(shortExplicit)})
			} else if strings.HasPrefix(o, prefix) {
				out = append(out, apTuple{opt: o, sep: sep, explicit: exp})
			}
		}
	}
	if len(out) > 0 {
		return out
	}
	if apNegative.MatchString(arg) || strings.Contains(arg, " ") {
		return nil
	}
	return []apTuple{{}}
}

func (p apParser) usage() string {
	parts := []string{"usage: " + p.prog, "[-h]"}
	for _, o := range p.opts {
		s := o.flag + " " + o.metavar
		if !o.required {
			s = "[" + s + "]"
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, " ") + "\n"
}

func (p apParser) help() string {
	var b strings.Builder
	b.WriteString(p.usage())
	b.WriteString("\n" + p.description + "\n\noptions:\n  -h, --help            show this help message and exit\n")
	for _, o := range p.opts {
		b.WriteString("  " + o.flag + " " + o.metavar + "\n")
	}
	return b.String()
}

// parse returns the value of each option given (by flag). done is true when the process stops with rc: help printed
// (0) or a usage error (2).
func (p apParser) parse(args []string, stdout, stderr io.Writer) (vals map[string]string, rc int, done bool) {
	fail := func(msg string) (map[string]string, int, bool) {
		fmt.Fprint(stderr, p.usage())
		fmt.Fprintf(stderr, "%s: error: %s\n", p.prog, msg)
		return nil, 2, true
	}
	patterns := make([]byte, len(args))
	tuples := map[int][]apTuple{}
	maxOpt := -1
	for i, dd := 0, false; i < len(args); i++ {
		switch {
		case dd:
			patterns[i] = 'A'
		case args[i] == "--":
			patterns[i], dd = '-', true
		default:
			if t := p.parseOptional(args[i]); t != nil {
				patterns[i], tuples[i], maxOpt = 'O', t, i
			} else {
				patterns[i] = 'A'
			}
		}
	}
	vals = map[string]string{}
	var extras []string
	start := 0
	for start <= maxOpt {
		next := start
		for next <= maxOpt && tuples[next] == nil {
			next++
		}
		if tuples[start] == nil {
			extras = append(extras, args[start:next]...)
			start = next
		}
		ts := tuples[start]
		if len(ts) > 1 {
			names := make([]string, len(ts))
			for i, t := range ts {
				names[i] = t.opt
			}
			return fail(fmt.Sprintf("ambiguous option: %s could match %s", args[start], strings.Join(names, ", ")))
		}
		t := ts[0]
		stop := start + 1
		help := false
		for {
			if t.opt == "" {
				extras = append(extras, args[start])
				break
			}
			isHelp := t.opt == "-h" || t.opt == "--help"
			if t.explicit != nil {
				if isHelp && t.opt[1] != '-' && *t.explicit != "" {
					if (t.sep != nil && *t.sep != "") || (*t.explicit)[0] == '-' {
						return fail("argument -h/--help: ignored explicit argument " + a1Repr(*t.explicit))
					}
					help = true
					rest := []rune(*t.explicit)
					next := "-" + string(rest[0])
					if p.has(next) {
						t.opt = next
						after := string(rest[1:])
						switch {
						case after == "":
							t.sep, t.explicit = nil, nil
						case after[0] == '=':
							t.sep, t.explicit = apStr("="), apStr(after[1:])
						default:
							t.sep, t.explicit = apStr(""), apStr(after)
						}
						continue
					}
					extras = append(extras, next+string(rest[1:]))
					break
				}
				if isHelp {
					return fail("argument -h/--help: ignored explicit argument " + a1Repr(*t.explicit))
				}
				vals[t.opt] = *t.explicit
				break
			}
			if isHelp {
				help = true
				break
			}
			if start+1 >= len(args) || patterns[start+1] != 'A' {
				return fail("argument " + t.opt + ": expected one argument")
			}
			vals[t.opt] = args[start+1]
			stop = start + 2
			break
		}
		if help {
			fmt.Fprint(stdout, p.help())
			return nil, 0, true
		}
		start = stop
	}
	extras = append(extras, args[start:]...)
	var missing []string
	for _, o := range p.opts {
		if _, ok := vals[o.flag]; o.required && !ok {
			missing = append(missing, o.flag)
		}
	}
	if len(missing) > 0 {
		return fail("the following arguments are required: " + strings.Join(missing, ", "))
	}
	if len(extras) > 0 {
		return fail("unrecognized arguments: " + strings.Join(extras, " "))
	}
	return vals, 0, false
}

// apPathStr is str(Path(s)): "" and "." are ".", repeated and trailing slashes go, "." parts go, exactly two leading
// slashes stay.
func apPathStr(s string) string {
	lead := ""
	switch {
	case strings.HasPrefix(s, "//") && !strings.HasPrefix(s, "///"):
		lead = "//"
	case strings.HasPrefix(s, "/"):
		lead = "/"
	}
	var parts []string
	for _, c := range strings.Split(s, "/") {
		if c != "" && c != "." {
			parts = append(parts, c)
		}
	}
	if len(parts) == 0 {
		if lead != "" {
			return lead
		}
		return "."
	}
	return lead + strings.Join(parts, "/")
}
